# -*- coding: utf-8 -*-
"""
Local Hammer — агрессивный локальный тест нагрузки.

Инструмент непрерывно «долбит» сервер 127.0.0.1:8000 множеством корутин,
которые отправляют запросы без ожидания ответа. Предназначен ТОЛЬКО для
локального сервера на своей машине.

ВНИМАНИЕ: только для локального сервера 127.0.0.1:8000. Не использовать
против других хостов.

ПРЕДУПРЕЖДЕНИЕ О НАГРУЗКЕ НА СИСТЕМУ:
  * инструмент может загрузить CPU и память на 100% и подвесить систему;
  * если система зависнет и окно не отвечает — завершите процесс
    local_hammer.py через «Диспетчер задач» (Ctrl+Shift+Esc → снять задачу).

Зависимости: aiohttp (pip install aiohttp).
Запуск:      python local_hammer.py
"""

import asyncio
import threading
import time
from collections import deque

import tkinter as tk

import aiohttp

# ---------------------------------------------------------------------------
# Жёстко зашитые константы (из интерфейса не меняются)
# ---------------------------------------------------------------------------
TARGET = "http://127.0.0.1:8000"   # единственная допустимая цель
WORKERS = 1000                     # число одновременных корутин (500-1000)
REQUEST_TIMEOUT = 1.0              # таймаут запроса, сек
UPDATE_MS = 250                    # период обновления интерфейса, мс

# Тёмная тема
BG = "#0f0f12"
PANEL = "#17171c"
TEXT = "#e8e8ee"
SUB = "#9a9aa4"
ACCENT = "#ff8c00"
GREEN = "#2ecc71"
RED = "#ff4136"


class Hammer:
    """Ядро нагрузчика: пул корутин и счётчики (один поток — свой event loop)."""

    def __init__(self):
        # счётчики пишет только поток event loop, поэтому lock не нужен
        self.sent = 0            # сколько запросов отправлено
        self.completed = 0       # сколько завершилось (успех + ошибка)
        self.errors = 0          # из них с ошибкой
        self.timestamps = deque(maxlen=500000)  # метки завершений для RPS

        self._tasks = set()      # «живые» задачи-запросы
        self.stop_event = threading.Event()  # взводится из GUI-потока
        self.thread = None       # поток с event loop
        self.running = False

    def reset(self):
        """Сбрасывает счётчики перед стартом."""
        self.sent = 0
        self.completed = 0
        self.errors = 0
        self.timestamps.clear()
        self._tasks.clear()

    def start(self):
        """Запускает нагрузчик в отдельном потоке."""
        if self.running:
            return
        self.reset()
        self.stop_event.clear()
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _run_loop(self):
        """Создаёт отдельный event loop и крутит нагрузку до остановки."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    async def _main(self):
        """Поднимает сессию и пул бесконечных воркеров."""
        # limit=0 — без ограничения одновременных соединений (no in_flight limit)
        connector = aiohttp.TCPConnector(limit=0)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            workers = [
                asyncio.create_task(self._worker(session))
                for _ in range(WORKERS)
            ]
            # ждём сигнала остановки из GUI
            while not self.stop_event.is_set():
                await asyncio.sleep(0.1)
            # останавливаем воркеров
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            # и отменяем все ещё висящие запросы
            pending = list(self._tasks)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _worker(self, session):
        """Бесконечный цикл: создать запрос-задачу и сразу идти дальше."""
        while True:
            self.sent += 1
            # задача «в фоне» — её результат обрабатывается асинхронно
            task = asyncio.create_task(self._send(session))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            # уступаем управление event loop, НЕ ожидая ответа сервера
            await asyncio.sleep(0)

    async def _send(self, session):
        """Один GET-запрос. Ошибка/таймаут не останавливают отправку."""
        ok = False
        try:
            async with session.get(TARGET) as resp:
                await resp.read()  # дочитываем тело, освобождаем сокет
                ok = resp.status == 200
        except asyncio.CancelledError:
            raise  # отмену при остановке пробрасываем выше
        except Exception:
            # таймаут (1 с), обрыв соединения и прочее — это ошибка
            ok = False

        # это поток event loop — обычные инкременты, lock не нужен
        self.completed += 1
        if not ok:
            self.errors += 1
        self.timestamps.append(time.monotonic())

    def stop(self):
        """Останавливает нагрузку и дожидается завершения потока."""
        if not self.running:
            return
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5.0)
        self.thread = None
        self.running = False

    def snapshot(self):
        """Возвращает (sent, completed, errors, rps за последнюю секунду)."""
        now = time.monotonic()
        try:
            times = list(self.timestamps)
        except RuntimeError:
            times = []
        rps = sum(1 for t in times if now - t <= 1.0)
        return self.sent, self.completed, self.errors, rps


class App(tk.Tk):
    """Главное окно Local Hammer."""

    def __init__(self):
        super().__init__()
        self.title("Local Hammer")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.geometry("560x520")

        self.hammer = Hammer()
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.after(UPDATE_MS, self._tick)

    # ---------- интерфейс ----------
    def _build_ui(self):
        # предупреждение сверху
        tk.Label(
            self,
            text="⚠ Только для локального сервера 127.0.0.1:8000. "
                 "Не использовать против других хостов.",
            fg="white", bg="#b00020", wraplength=540, justify="left",
            font=("Segoe UI", 10), padx=8, pady=6,
        ).pack(fill="x")

        tk.Label(
            self, text="Local Hammer — локальный тест нагрузки",
            fg=TEXT, bg=BG, font=("Segoe UI", 14, "bold"),
        ).pack(pady=(12, 2))

        tk.Label(
            self, text=f"Цель: {TARGET}",
            fg=SUB, bg=BG, font=("Segoe UI", 10),
        ).pack()

        # сетка крупных счётчиков
        grid = tk.Frame(self, bg=BG)
        grid.pack(pady=16)

        self.lbl_sent, row1 = self._stat(grid, "Отправлено", "0", 0, 0)
        self.lbl_completed, row2 = self._stat(grid, "Завершено", "0", 0, 1)
        self.lbl_errors, row3 = self._stat(grid, "Ошибок", "0", 1, 0)
        self.lbl_rps, row4 = self._stat(grid, "RPS", "0", 1, 1)

        # статус
        self.lbl_status = tk.Label(
            self, text="Статус: остановлен", fg=RED, bg=BG,
            font=("Segoe UI", 16, "bold"),
        )
        self.lbl_status.pack(pady=(4, 12))

        # кнопка Старт/Стоп
        self.btn = tk.Button(
            self, text="Старт", command=self.toggle,
            bg=ACCENT, fg="black", activebackground="#ffa733",
            activeforeground="black", relief="flat",
            font=("Segoe UI", 14, "bold"), width=16, height=1,
        )
        self.btn.pack()

        # напоминание про диспетчер задач
        tk.Label(
            self,
            text="Внимание: возможна нагрузка на CPU и память до 100%.\n"
                 "Если система зависнет — снимите процесс local_hammer.py "
                 "в «Диспетчере задач».",
            fg=SUB, bg=BG, font=("Segoe UI", 9), justify="center",
        ).pack(side="bottom", pady=14)

    def _stat(self, parent, title, value, r, c):
        """Создаёт блок со счётчиком и возвращает (метка_значения, контейнер)."""
        box = tk.Frame(parent, bg=PANEL, padx=20, pady=12)
        box.grid(row=r, column=c, padx=6, pady=6, sticky="nsew")
        tk.Label(
            box, text=title, fg=SUB, bg=PANEL, font=("Segoe UI", 11),
        ).pack()
        val = tk.Label(
            box, text=value, fg=TEXT, bg=PANEL, font=("Segoe UI", 28, "bold"),
        )
        val.pack()
        return val, box

    # ---------- управление ----------
    def toggle(self):
        """Старт / стоп нагрузки."""
        if self.hammer.running:
            self.hammer.stop()
        else:
            self.hammer.start()
        self._refresh()

    def _refresh(self):
        """Обновляет состояние кнопки и статуса."""
        if self.hammer.running:
            self.btn.config(text="Стоп", bg="#c0392b",
                            activebackground="#e05c4a")
            self.lbl_status.config(text="Статус: работает", fg=GREEN)
        else:
            self.btn.config(text="Старт", bg=ACCENT,
                            activebackground="#ffa733")
            self.lbl_status.config(text="Статус: остановлен", fg=RED)

    def _tick(self):
        """Периодически обновляет счётчики в интерфейсе."""
        # если поток нагрузки завершился сам — вернём кнопку в исходное состояние
        if self.hammer.running and self.hammer.thread is not None \
                and not self.hammer.thread.is_alive():
            self.hammer.thread = None
            self.hammer.running = False
            self._refresh()

        sent, completed, errors, rps = self.hammer.snapshot()
        self.lbl_sent.config(text=f"{sent:,}".replace(",", " "))
        self.lbl_completed.config(text=f"{completed:,}".replace(",", " "))
        self.lbl_errors.config(text=f"{errors:,}".replace(",", " "))
        self.lbl_rps.config(text=f"{rps:,}".replace(",", " "))
        self.after(UPDATE_MS, self._tick)

    def on_close(self):
        """Останавливает нагрузчик перед закрытием окна."""
        self.hammer.stop()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
