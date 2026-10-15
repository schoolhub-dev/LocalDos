# -*- coding: utf-8 -*-
"""
Server Load Graph — живой монитор нагрузки на локальный сервер.

Приложение только наблюдает за 127.0.0.1:8000 и в реальном времени рисует
график времени ответа (как в диспетчере задач). Работает параллельно с
приложением Local Load Test, которое поднимает сервер и шлёт нагрузку.

Как устроено обновление:
  * перерисовка и опрос запускаются через tkinter after(1000, ...) — без while;
  * сам HTTP-запрос делается в короткоживущем фоновом потоке, чтобы НЕ
    блокировать главный поток: пока сервер молчит, окно остаётся отзывчивым;
  * главный поток раз в секунду забирает готовый результат, добавляет его в
    буфер, полностью перерисовывает Canvas и обновляет подписи.

ВНИМАНИЕ: мониторинг локального сервера 127.0.0.1:8000. Только для
локальных тестов.

Зависимости: tkinter (встроен) + requests.
Запуск:      python server_monitor.py
"""

import threading
import time

import tkinter as tk

import requests

# ---------------------------------------------------------------------------
# Жёстко зашитые константы (из интерфейса не меняются)
# ---------------------------------------------------------------------------
TARGET = "http://127.0.0.1:8000"   # единственная наблюдаемая цель
MAX_MS = 2000.0                    # верхняя граница шкалы, мс
HISTORY_LEN = 120                  # храним 120 значений (2 минуты)
POLL_MS = 1000                     # период опроса, мс
PING_TIMEOUT = 1.5                 # таймаут запроса, сек

# Геометрия графика
CANVAS_W = 1000
CANVAS_H = 300
MARGIN_LEFT = 60                   # место для подписей оси Y
MARGIN_RIGHT = 20
MARGIN_TOP = 15
MARGIN_BOTTOM = 25                 # место для подписей оси X

# Тёмная тема
BG = "#101012"                     # фон окна
PANEL = "#16161a"                  # фон области графика
GRID = "#2a2a30"                   # цвет сетки
AXIS_TEXT = "#8a8a94"              # цвет подписей осей
TEXT = "#e6e6ea"                   # основной текст
SUB = "#a0a0aa"                    # второстепенный текст
COLOR_OK = "#2ecc71"               # линия, когда сервер отвечает
COLOR_BAD = "#ff4136"              # линия при DOWN / TIMEOUT


class ServerMonitor(tk.Tk):
    """Главное окно монитора нагрузки."""

    def __init__(self):
        super().__init__()
        self.title("Server Load Graph")
        self.configure(bg=BG)
        self.resizable(False, False)
        # окно по ширине графика + отступы
        self.geometry(f"{CANVAS_W + 40}x600")

        # буфер последних значений времени ответа (мс), сдвигается влево
        self.values = []
        self.paused = False
        self.status = "—"
        self.last_ms = 0.0

        # обмен результатом между фоновым потоком и главным потоком
        self._lock = threading.Lock()
        self._result = None        # (мс, статус) от воркера
        self._measuring = False    # идёт ли прямо сейчас замер

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        # запускаем периодический цикл обновления
        self.after(POLL_MS, self._tick)

    # ---------- построение интерфейса ----------
    def _build_ui(self):
        # предупреждение сверху
        warn = tk.Label(
            self,
            text="Мониторинг локального сервера 127.0.0.1:8000. "
                 "Только для локальных тестов.",
            fg="white", bg="#b00020",
            font=("Segoe UI", 10), padx=8, pady=6,
        )
        warn.pack(fill="x")

        # заголовок
        tk.Label(
            self, text="Время ответа сервера (мс)",
            fg=TEXT, bg=BG, font=("Segoe UI", 12, "bold"),
        ).pack(anchor="w", padx=20, pady=(10, 4))

        # холст с графиком
        self.canvas = tk.Canvas(
            self, width=CANVAS_W, height=CANVAS_H,
            bg=PANEL, highlightthickness=0,
        )
        self.canvas.pack(padx=20, pady=4)

        # крупное текущее значение
        info = tk.Frame(self, bg=BG)
        info.pack(fill="x", padx=20, pady=(10, 0))

        self.lbl_response = tk.Label(
            info, text="Время ответа: — мс",
            fg=TEXT, bg=BG, font=("Segoe UI", 24, "bold"),
        )
        self.lbl_response.pack(anchor="w")

        self.lbl_status = tk.Label(
            info, text="Статус: —",
            fg=SUB, bg=BG, font=("Segoe UI", 16, "bold"),
        )
        self.lbl_status.pack(anchor="w", pady=(2, 0))

        # кнопка паузы
        self.btn_pause = tk.Button(
            self, text="Пауза", command=self.toggle_pause,
            bg="#2a2a30", fg=TEXT, activebackground="#3a3a42",
            activeforeground=TEXT, relief="flat", width=14,
            font=("Segoe UI", 11),
        )
        self.btn_pause.pack(pady=14)

        # первичная отрисовка пустой сетки
        self._draw_graph()

    # ---------- логика опроса ----------
    def _measure(self):
        """Блокирующий GET-запрос. Возвращает (время_мс, статус).

        Выполняется в фоновом потоке, поэтому не морозит интерфейс.
        """
        t0 = time.perf_counter()
        try:
            resp = requests.get(TARGET, timeout=PING_TIMEOUT)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            status = "OK" if resp.status_code == 200 else "ERROR"
        except requests.exceptions.Timeout:
            # сервер не успел ответить — показываем максимум шкалы
            return MAX_MS, "TIMEOUT"
        except requests.exceptions.ConnectionError:
            # сервер недоступен
            return MAX_MS, "DOWN"
        except Exception:
            return MAX_MS, "ERROR"

        # ограничиваем значение диапазоном графика
        dt_ms = min(max(dt_ms, 0.0), MAX_MS)
        return dt_ms, status

    def _worker_measure(self):
        """Тело фонового потока: делает замер и кладёт результат под замок."""
        ms, status = self._measure()
        with self._lock:
            self._result = (ms, status)
            self._measuring = False

    def toggle_pause(self):
        """Пауза / продолжение обновления графика."""
        self.paused = not self.paused
        self.btn_pause.config(text="Продолжить" if self.paused else "Пауза")

    def _tick(self):
        """Один такт цикла: забрать результат, перерисовать, запустить новый замер."""
        # try/finally гарантирует, что цикл перепланируется даже при ошибке:
        # иначе одно исключение навсегда «убивает» обновление графика
        try:
            # 1) забираем готовый результат фонового замера (если он есть)
            with self._lock:
                result = self._result
                self._result = None
                measuring = self._measuring

            if self.paused:
                # на паузе результат игнорируем и новые замеры не запускаем
                return

            # 2) если замер завершился — добавляем точку и перерисовываем график
            if result is not None:
                ms, status = result
                self.last_ms = ms
                self.status = status
                self.values.append(ms)
                # сдвиг влево: оставляем ровно HISTORY_LEN последних значений
                while len(self.values) > HISTORY_LEN:
                    self.values.pop(0)
                self._update_labels()
                self._draw_graph()

            # 3) запускаем следующий замер, если предыдущий уже отработал
            if not measuring:
                with self._lock:
                    self._measuring = True
                threading.Thread(target=self._worker_measure, daemon=True).start()
        finally:
            # 4) планируем следующий такт
            self.after(POLL_MS, self._tick)

    def _update_labels(self):
        """Обновляет крупные подписи под графиком."""
        self.lbl_response.config(text=f"Время ответа: {self.last_ms:.0f} мс")
        # цвет текста статуса совпадает с цветом линии
        color = COLOR_OK if self.status == "OK" else COLOR_BAD
        self.lbl_status.config(text=f"Статус: {self.status}", fg=color)

    # ---------- отрисовка ----------
    def _plot_bounds(self):
        """Возвращает координаты области построения графика."""
        x0 = MARGIN_LEFT
        y0 = MARGIN_TOP
        x1 = CANVAS_W - MARGIN_RIGHT
        y1 = CANVAS_H - MARGIN_BOTTOM
        return x0, y0, x1, y1

    def _y_max(self):
        """Автомасштаб по Y: максимум на 10% выше пика, минимум 0."""
        if not self.values:
            return 10.0
        peak = max(self.values)
        if peak <= 0:
            return 10.0
        return peak * 1.1

    def _draw_graph(self):
        """Полностью перерисовывает график: удаляет всё и рисует заново."""
        self.canvas.delete("all")
        x0, y0, x1, y1 = self._plot_bounds()
        plot_w = x1 - x0
        plot_h = y1 - y0
        y_max = self._y_max()

        # горизонтальная сетка и подписи Y (0, 25, 50, 75, 100 % от максимума)
        for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
            y = y1 - plot_h * frac
            self.canvas.create_line(x0, y, x1, y, fill=GRID)
            value = y_max * frac
            label = f"{value:.0f}" if y_max >= 10 else f"{value:.1f}"
            self.canvas.create_text(
                x0 - 8, y, text=label, anchor="e",
                fill=AXIS_TEXT, font=("Segoe UI", 9),
            )

        # вертикальная сетка и подписи X (каждые 30 секунд от «сейчас»)
        for i in range(0, HISTORY_LEN, 30):
            x = x0 + plot_w * i / (HISTORY_LEN - 1)
            self.canvas.create_line(x, y0, x, y1, fill=GRID)
            seconds = i - HISTORY_LEN
            self.canvas.create_text(
                x, y1 + 12, text=f"{seconds}с", anchor="n",
                fill=AXIS_TEXT, font=("Segoe UI", 9),
            )

        # если данных ещё нет — остаётся только сетка
        if not self.values:
            return

        # координаты точек: старые слева, новые справа
        pts = []
        for i, v in enumerate(self.values):
            x = x0 + plot_w * i / (HISTORY_LEN - 1)
            y = y1 - plot_h * (v / y_max)
            pts.extend((x, y))

        color = COLOR_OK if self.status == "OK" else COLOR_BAD

        # одиночная точка: линию из одной точки построить нельзя — рисуем маркер
        if len(self.values) == 1:
            self.canvas.create_oval(
                pts[0] - 3, pts[1] - 3, pts[0] + 3, pts[1] + 3,
                fill=color, outline="",
            )
            return

        # заливка под линией (штриховка создаёт эффект полупрозрачности)
        if len(pts) >= 4:
            polygon = pts + [pts[-2], y1, pts[0], y1]
            self.canvas.create_polygon(
                polygon, fill=color, stipple="gray25", outline="",
            )

        # сама линия
        self.canvas.create_line(*pts, fill=color, width=2)


if __name__ == "__main__":
    ServerMonitor().mainloop()
