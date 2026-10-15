# -*- coding: utf-8 -*-
"""
Local Load Test — локальный бенчмарк нагрузки.

Приложение поднимает тестовый HTTP-сервер на 127.0.0.1:8000 и позволяет
прогнать по нему контролируемую нагрузку, отслеживая метрики системы.

Нагрузочная часть — open-loop (constant rate): запросы создаются строго по
расписанию с заданной скоростью RPS и не ждут ответа сервера. Если сервер
не успевает, растёт число «висящих» запросов (in-flight), а не падает темп
отправки. Это отличается от closed-loop, где корутины ждут ответа и тормозят
друг друга.

ВНИМАНИЕ: инструмент предназначен ТОЛЬКО для локального сервера
127.0.0.1:8000. Не использовать против других хостов.

Зависимости: pip install aiohttp requests psutil
Запуск:      python main.py
"""

import asyncio
import json
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import tkinter as tk
from tkinter import ttk

import aiohttp
import psutil
import requests

# ---------------------------------------------------------------------------
# Жёстко зашитые константы (не изменяются из интерфейса)
# ---------------------------------------------------------------------------
TARGET = "http://127.0.0.1:8000"   # единственная допустимая цель
SERVER_HOST = "127.0.0.1"          # хост тестового сервера
SERVER_PORT = 8000                 # порт тестового сервера
MAX_RPS = 5000                     # верхняя граница задаваемой скорости
MAX_IN_FLIGHT = 10000              # предел одновременных «висящих» запросов
PING_TIMEOUT = 2.0                 # таймаут запроса мониторинга, сек
REQUEST_TIMEOUT = 5.0              # таймаут запроса нагрузки, сек


# ===========================================================================
# Блок 1. Тестовый сервер
# ===========================================================================
class ServerState:
    """Потокобезопасное состояние тестового сервера."""

    def __init__(self):
        self.lock = threading.Lock()
        self.request_count = 0        # суммарное число запросов
        self.start_time = None        # время запуска сервера
        self.timestamps = deque(maxlen=10000)  # метки времени для расчёта RPS

    def reset(self):
        """Сбрасывает счётчики и фиксирует время старта."""
        with self.lock:
            self.request_count = 0
            self.start_time = time.time()
            self.timestamps.clear()

    def count_request(self):
        """Регистрирует запрос и возвращает его порядковый номер."""
        with self.lock:
            self.request_count += 1
            self.timestamps.append(time.time())
            return self.request_count

    def uptime(self):
        """Время работы сервера в секундах."""
        with self.lock:
            if self.start_time is None:
                return 0.0
            return time.time() - self.start_time

    def rps(self):
        """Число запросов за последнюю секунду."""
        with self.lock:
            now = time.time()
            # считаем только метки за последнюю секунду
            return sum(1 for t in self.timestamps if now - t <= 1.0)


class TestHandler(BaseHTTPRequestHandler):
    """Обработчик GET-запросов тестового сервера."""

    state = None  # ссылка на ServerState, задаётся при запуске сервера

    def do_GET(self):
        # фиксируем запрос и формируем JSON-ответ
        number = self.state.count_request()
        payload = {
            "status": "ok",
            "request_number": number,
            "uptime_seconds": round(self.state.uptime(), 2),
            "requests_per_second": self.state.rps(),
        }
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        # отключаем стандартный вывод логов сервера в консоль
        pass


class ServerManager:
    """Управляет жизненным циклом тестового HTTP-сервера."""

    def __init__(self):
        self.state = ServerState()
        self.httpd = None
        self.thread = None
        self.running = False

    def start(self):
        """Запускает сервер в отдельном потоке."""
        if self.running:
            return
        TestHandler.state = self.state
        self.state.reset()
        # ThreadingHTTPServer обрабатывает каждый запрос в своём потоке
        self.httpd = ThreadingHTTPServer((SERVER_HOST, SERVER_PORT), TestHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.running = True

    def stop(self):
        """Останавливает сервер и освобождает порт."""
        if not self.running:
            return
        # shutdown() обязан вызываться из другого потока, а не из serve_forever
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2.0)
        self.httpd = None
        self.thread = None
        self.running = False


# ===========================================================================
# Блок 2. Тест нагрузки — open-loop (constant rate)
# ===========================================================================
class LoadTestManager:
    """
    Асинхронный нагрузчик с открытым контуром (open-loop).

    Работает в отдельном потоке со своим event loop. Запросы создаются
    строго по расписанию: раз в (1 / RPS) секунды запускается новая задача
    через asyncio.create_task и сразу «забывается» — генератор не ждёт её
    ответа. Поэтому темп отправки не зависит от того, как быстро отвечает
    сервер.

    Если сервер не справляется, ответы приходят медленнее, чем уходят
    запросы, и значение in-flight (sent - completed) растёт — это и есть
    индикатор перегрузки сервера.
    """

    def __init__(self):
        # счётчики пишет только поток event loop, поэтому lock не нужен
        self.sent = 0                 # сколько запросов отправлено
        self.completed = 0            # сколько ответов получено (успех + ошибка)
        self.errors = 0               # из них с ошибкой / не-200
        self.timestamps = deque(maxlen=500000)  # метки завершений для RPS_real

        self._tasks = set()           # «висящие» задачи-запросы
        self.stop_event = threading.Event()  # взводится из GUI-потока
        self.thread = None            # поток, в котором крутится event loop
        self.running = False
        self.start_time = None
        self.target_rps = 0           # заданный RPS (для отображения)

    def reset(self):
        """Сбрасывает счётчики теста (вызывается до старта потока)."""
        self.sent = 0
        self.completed = 0
        self.errors = 0
        self.timestamps.clear()
        self._tasks.clear()

    def start(self, rps, duration):
        """Запускает нагрузку.

        rps      — задаваемая скорость (запросов в секунду),
        duration — длительность прогона в секундах.
        """
        if self.running:
            return
        self.reset()
        self.stop_event.clear()
        self.start_time = time.time()
        self.target_rps = rps
        self.running = True

        # нагрузчик живёт в отдельном потоке, чтобы не блокировать GUI
        self.thread = threading.Thread(
            target=self._run_loop, args=(rps, duration), daemon=True,
        )
        self.thread.start()

    def _run_loop(self, rps, duration):
        """Создаёт собственный event loop и прогоняет нагрузку до конца."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main(rps, duration))
        finally:
            # корректно гасим асинхронные генераторы и закрываем loop
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    async def _main(self, rps, duration):
        """Генератор нагрузки: создаёт задачи по расписанию, не ожидая ответов."""
        # соединений разрешаем столько же, сколько максимум висящих запросов
        connector = aiohttp.TCPConnector(limit=MAX_IN_FLIGHT)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        interval = 1.0 / float(rps)
        deadline = self.start_time + duration

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            t0 = time.monotonic()
            next_time = t0  # момент, когда нужно отправить следующий запрос

            while not self.stop_event.is_set():
                now = time.monotonic()
                if now >= deadline:
                    break  # время теста вышло

                # защита от перегрузки собственной машины: слишком много висящих
                if len(self._tasks) >= MAX_IN_FLIGHT:
                    await asyncio.sleep(0.01)
                    continue

                # если во время паузы график «убежал» — сбрасываем долг,
                # чтобы после разгона не было залпового выброса
                if next_time < now - 1.0:
                    next_time = now

                # отправляем все запросы, время которых уже наступило
                while next_time <= now:
                    if len(self._tasks) >= MAX_IN_FLIGHT:
                        break
                    self.sent += 1
                    task = asyncio.create_task(self._one_request(session))
                    self._tasks.add(task)
                    # по завершении задача сама убирается из набора
                    task.add_done_callback(self._tasks.discard)
                    next_time += interval

                # пауза до следующего запланированного момента
                now = time.monotonic()
                wait = next_time - now
                if wait > 0.001:
                    # на Windows точность таймера ~15 мс, поэтому не спим дольше 10 мс
                    await asyncio.sleep(min(wait, 0.01))
                else:
                    # почти «сейчас» — просто уступаем управление другим задачам
                    await asyncio.sleep(0)

            # генерация остановлена — разбираемся с уже отправленными запросами
            await self._finish_in_flight()

    async def _one_request(self, session):
        """Один запрос: отправляет GET, учитывает результат и «самоуничтожается»."""
        ok = False
        try:
            async with session.get(TARGET) as resp:
                await resp.read()  # дочитываем тело, чтобы освободить сокет
                ok = resp.status == 200
        except asyncio.CancelledError:
            # отмену при остановке пробрасываем выше, не считая ответом
            raise
        except Exception:
            # таймаут (ClientTimeout) и прочие сетевые ошибки — это ошибка
            ok = False

        # это поток event loop — обычные инкременты, lock не нужен
        self.completed += 1
        if not ok:
            self.errors += 1
        self.timestamps.append(time.monotonic())

    async def _finish_in_flight(self):
        """Завершает незакрытые запросы после остановки генерации."""
        if not self._tasks:
            return
        tasks = list(self._tasks)
        if self.stop_event.is_set():
            # ручная остановка — отменяем всё, что ещё висит
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # естественное завершение по времени — даём досчитать
            done, pending = await asyncio.wait(tasks, timeout=REQUEST_TIMEOUT + 1.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    def stop(self):
        """Перестаёт создавать запросы и дожидается завершения потока."""
        if not self.running:
            return
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=REQUEST_TIMEOUT + 3.0)
        self.thread = None
        self.running = False

    def check_finished(self):
        """Если поток нагрузки завершился сам (по времени) — сбрасывает флаг."""
        if self.running and self.thread is not None and not self.thread.is_alive():
            self.thread = None
            self.running = False
            return True
        return False

    def snapshot(self):
        """Возвращает (sent, completed, in_flight, errors, rps_real).

        Читается из GUI-потока, поэтому копию меток берём защищённо:
        deque нельзя итерировать параллельно с append.
        """
        now = time.monotonic()
        try:
            times = list(self.timestamps)
        except RuntimeError:
            # редкий случай мутации во время итерации — пропускаем такт
            times = []
        rps_real = sum(1 for t in times if now - t <= 1.0)
        in_flight = self.sent - self.completed
        return self.sent, self.completed, in_flight, self.errors, rps_real


# ===========================================================================
# Блок 3. Мониторинг
# ===========================================================================
class Monitor:
    """Собирает метрики системы и измеряет время ответа сервера."""

    def __init__(self):
        # первый вызов cpu_percent инициализирует счётчик (иначе вернёт 0.0)
        psutil.cpu_percent(interval=None)

    def cpu_percent(self):
        return psutil.cpu_percent(interval=None)

    def ram_percent(self):
        return psutil.virtual_memory().percent

    def ping(self):
        """Возвращает (время ответа в мс или None, статус)."""
        t0 = time.time()
        try:
            resp = requests.get(TARGET, timeout=PING_TIMEOUT)
            dt_ms = (time.time() - t0) * 1000.0
            return dt_ms, "OK" if resp.status_code == 200 else "ERROR"
        except requests.exceptions.Timeout:
            return None, "TIMEOUT"
        except requests.exceptions.ConnectionError:
            return None, "DOWN"
        except Exception:
            return None, "ERROR"


# ===========================================================================
# GUI
# ===========================================================================
class App(tk.Tk):
    """Главное окно приложения."""

    def __init__(self):
        super().__init__()
        self.title("Local Load Test")
        self.geometry("580x640")
        self.minsize(540, 600)

        self.server = ServerManager()
        self.load = LoadTestManager()
        self.monitor = Monitor()

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        # запускаем периодическое обновление интерфейса
        self.after(500, self._tick)

    # ---------- построение интерфейса ----------
    def _build_ui(self):
        # предупреждение вверху окна
        warn = tk.Label(
            self,
            text="⚠ Только для локального сервера 127.0.0.1:8000. "
                 "Не использовать против других хостов.",
            fg="white", bg="#b00020", wraplength=540, justify="left", padx=8, pady=6,
        )
        warn.pack(fill="x", padx=8, pady=(8, 4))

        self._build_server_block()
        self._build_test_block()
        self._build_monitor_block()

    def _build_server_block(self):
        frame = ttk.LabelFrame(self, text="Сервер")
        frame.pack(fill="x", padx=8, pady=6)

        row = ttk.Frame(frame)
        row.pack(fill="x", padx=8, pady=6)

        self.btn_server = ttk.Button(row, text="Запустить сервер", command=self.toggle_server)
        self.btn_server.pack(side="left")

        self.lbl_server = tk.Label(row, text="Сервер: остановлен", fg="red")
        self.lbl_server.pack(side="left", padx=12)

        self.lbl_server_info = ttk.Label(frame, text="запросов: 0    RPS: 0")
        self.lbl_server_info.pack(anchor="w", padx=8, pady=(0, 6))

    def _build_test_block(self):
        frame = ttk.LabelFrame(self, text="Тест нагрузки")
        frame.pack(fill="x", padx=8, pady=6)

        # слайдер задаёт скорость в запросах в секунду (open-loop)
        scale_row = ttk.Frame(frame)
        scale_row.pack(fill="x", padx=8, pady=(6, 0))
        ttk.Label(scale_row, text="RPS (запросов/сек):").pack(side="left")
        self.scale_var = tk.IntVar(value=10)
        self.scale = ttk.Scale(
            scale_row, from_=1, to=MAX_RPS, orient="horizontal",
            command=self._on_scale, length=260,
        )
        self.scale.set(10)
        self.scale.pack(side="left", padx=8)
        self.lbl_scale = ttk.Label(scale_row, text="10")
        self.lbl_scale.pack(side="left")

        # длительность
        dur_row = ttk.Frame(frame)
        dur_row.pack(fill="x", padx=8, pady=6)
        ttk.Label(dur_row, text="Длительность (сек):").pack(side="left")
        self.dur_var = tk.StringVar(value="30")
        ttk.Entry(dur_row, textvariable=self.dur_var, width=8).pack(side="left", padx=8)

        # кнопка запуска/остановки
        ctrl_row = ttk.Frame(frame)
        ctrl_row.pack(fill="x", padx=8, pady=(0, 6))
        self.btn_test = ttk.Button(ctrl_row, text="Начать тест", command=self.toggle_test)
        self.btn_test.pack(side="left")
        self.lbl_test = tk.Label(ctrl_row, text="Тест: остановлен", fg="red")
        self.lbl_test.pack(side="left", padx=12)

        # счётчики: заданный/реальный RPS
        counters1 = ttk.Frame(frame)
        counters1.pack(fill="x", padx=8)
        self.lbl_target_rps = ttk.Label(counters1, text="Заданный RPS: 0")
        self.lbl_target_rps.pack(side="left")
        self.lbl_rps_real = ttk.Label(counters1, text="Реальный RPS: 0")
        self.lbl_rps_real.pack(side="left", padx=16)

        # счётчики: отправлено/завершено/в полёте/ошибки
        counters2 = ttk.Frame(frame)
        counters2.pack(fill="x", padx=8, pady=(2, 8))
        self.lbl_sent = ttk.Label(counters2, text="Отправлено: 0")
        self.lbl_sent.pack(side="left")
        self.lbl_completed = ttk.Label(counters2, text="Завершено: 0")
        self.lbl_completed.pack(side="left", padx=16)
        self.lbl_inflight = ttk.Label(counters2, text="В полёте: 0")
        self.lbl_inflight.pack(side="left", padx=16)
        self.lbl_errors = ttk.Label(counters2, text="Ошибок: 0")
        self.lbl_errors.pack(side="left", padx=16)

    def _build_monitor_block(self):
        frame = ttk.LabelFrame(self, text="Мониторинг")
        frame.pack(fill="both", expand=True, padx=8, pady=(6, 8))

        self.tree = ttk.Treeview(
            frame, columns=("metric", "value"), show="headings", height=5,
        )
        self.tree.heading("metric", text="Метрика")
        self.tree.heading("value", text="Значение")
        self.tree.column("metric", width=220, anchor="w")
        self.tree.column("value", width=220, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)

        # строки таблицы
        self.mon_rows = {}
        for key, title in (
            ("cpu", "CPU (%)"),
            ("ram", "RAM (%)"),
            ("resp", "Время ответа (мс)"),
            ("status", "Статус"),
        ):
            self.mon_rows[key] = self.tree.insert("", "end", values=(title, "—"))

    # ---------- обработчики ----------
    def _on_scale(self, value):
        # Scale отдаёт float — округляем до целого
        self.scale_var.set(int(float(value)))
        self.lbl_scale.config(text=str(int(float(value))))

    def toggle_server(self):
        """Включает/выключает тестовый сервер."""
        if self.server.running:
            self.server.stop()
        else:
            try:
                self.server.start()
            except OSError as exc:
                # чаще всего — порт уже занят
                self.lbl_server_info.config(text=f"Не удалось запустить: {exc}")
                return
        self._refresh_server_ui()

    def toggle_test(self):
        """Включает/выключает прогон нагрузки."""
        if self.load.running:
            self.load.stop()
        else:
            # проверяем корректность длительности
            try:
                duration = int(self.dur_var.get())
                if duration <= 0:
                    raise ValueError
            except ValueError:
                self.lbl_test.config(text="Некорректная длительность", fg="red")
                return
            rps = self.scale_var.get()
            self.load.start(rps, duration)
        self._refresh_test_ui()

    # ---------- обновление интерфейса ----------
    def _refresh_server_ui(self):
        if self.server.running:
            self.btn_server.config(text="Остановить сервер")
            self.lbl_server.config(text="Сервер: работает", fg="green")
        else:
            self.btn_server.config(text="Запустить сервер")
            self.lbl_server.config(text="Сервер: остановлен", fg="red")

    def _refresh_test_ui(self):
        if self.load.running:
            self.btn_test.config(text="Остановить тест")
            self.lbl_test.config(text="Тест: идёт", fg="green")
        else:
            self.btn_test.config(text="Начать тест")
            self.lbl_test.config(text="Тест: остановлен", fg="red")

    def _update_monitor(self):
        """Обновляет таблицу метрик, пингует сервер."""
        cpu = self.monitor.cpu_percent()
        ram = self.monitor.ram_percent()
        resp_ms, status = self.monitor.ping()

        self.tree.item(self.mon_rows["cpu"], values=("CPU (%)", f"{cpu:.1f}"))
        self.tree.item(self.mon_rows["ram"], values=("RAM (%)", f"{ram:.1f}"))
        self.tree.item(
            self.mon_rows["resp"],
            values=("Время ответа (мс)", f"{resp_ms:.1f}" if resp_ms is not None else "—"),
        )
        self.tree.item(self.mon_rows["status"], values=("Статус", status))

    def _tick(self):
        """Периодический вызов: обновляет счётчики и метрики."""
        # если тест завершился сам по времени — обновляем кнопку
        if self.load.check_finished():
            self._refresh_test_ui()

        sent, completed, in_flight, errors, rps_real = self.load.snapshot()
        self.lbl_target_rps.config(text=f"Заданный RPS: {self.load.target_rps}")
        self.lbl_rps_real.config(text=f"Реальный RPS: {rps_real}")
        self.lbl_sent.config(text=f"Отправлено: {sent}")
        self.lbl_completed.config(text=f"Завершено: {completed}")
        self.lbl_inflight.config(text=f"В полёте: {in_flight}")
        self.lbl_errors.config(text=f"Ошибок: {errors}")

        if self.server.running:
            self.lbl_server_info.config(
                text=f"запросов: {self.server.state.request_count}    "
                     f"RPS: {self.server.state.rps()}"
            )
        else:
            self.lbl_server_info.config(text="запросов: 0    RPS: 0")

        self._update_monitor()
        self.after(1000, self._tick)

    # ---------- завершение ----------
    def on_close(self):
        """Корректно останавливает нагрузчик и сервер перед закрытием окна."""
        self.load.stop()
        self.server.stop()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
