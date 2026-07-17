"""Запуск веб-панели с автоматическим открытием браузера.

Использование:
    python run.py                 запустить панель и открыть вкладку браузера
    python run.py --no-browser    только запустить сервер (для сервера или отладки)

Адрес и порт берутся из WEB_HOST и WEB_PORT — тех же, что использует web_app.py,
чтобы запуск двумя способами не расходился.
"""

import os
import socket
import sys
import threading
import time
import webbrowser

HOST = os.environ.get('WEB_HOST', '0.0.0.0')
PORT = int(os.environ.get('WEB_PORT', '5000'))
BROWSER_URL = f"http://127.0.0.1:{PORT}/"

# Сколько ждать, пока сервер начнет принимать соединения.
STARTUP_TIMEOUT_SECONDS = 30


def port_is_open(host, port, timeout=0.3):
    """Проверяет, слушает ли кто-нибудь этот порт."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(timeout)
        return probe.connect_ex((host, port)) == 0


def open_browser_when_ready():
    """Ждет готовности сервера и открывает вкладку браузера.

    Опрос порта вместо паузы на удачу: холодный старт бывает дольше секунды
    из-за импорта зависимостей и инициализации планировщика.
    """
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if port_is_open('127.0.0.1', PORT):
            print(f"Открываю {BROWSER_URL}")
            webbrowser.open(BROWSER_URL)
            return
        time.sleep(0.3)
    print(f"Сервер не ответил за {STARTUP_TIMEOUT_SECONDS} с, браузер не открыт.")


def main():
    open_browser = '--no-browser' not in sys.argv

    # Если панель уже поднята, второй сервер на том же порту не встанет —
    # просто показываем существующий.
    if port_is_open('127.0.0.1', PORT):
        print(f"Панель уже запущена на порту {PORT}.")
        if open_browser:
            webbrowser.open(BROWSER_URL)
        return 0

    try:
        from waitress import serve
        from web_app import app
    except ImportError as e:
        print(f"Не хватает зависимости: {e}")
        print("Установите их: pip install -r requirements.txt")
        print("Если используется виртуальное окружение — запускайте .venv\\Scripts\\python.exe run.py")
        return 1

    if open_browser:
        threading.Thread(target=open_browser_when_ready, daemon=True).start()

    print(f"Панель запускается на http://{HOST}:{PORT}  (остановить: Ctrl+C)")
    try:
        serve(app, host=HOST, port=PORT, threads=4)
    except KeyboardInterrupt:
        print("\nОстановлено.")
    except OSError as e:
        print(f"Не удалось занять порт {PORT}: {e}")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
