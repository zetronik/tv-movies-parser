@echo off
rem Запуск веб-панели двойным кликом: поднимает сервер и открывает браузер.
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run.py %*
) else (
    echo Виртуальное окружение .venv не найдено, пробуем системный Python.
    python run.py %*
)

rem Окно остается открытым, чтобы была видна причина, если сервер упал.
if errorlevel 1 pause
