# Используем легкую версию Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Настраиваем правильный часовой пояс для корректной работы крона и логов
ENV TZ=Europe/Kyiv
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Копируем файл зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем production-сервер, который мы недавно добавили
RUN pip install waitress

# Копируем весь остальной код парсера
COPY . .

# Открываем порт 5000 наружу
EXPOSE 5000

# Команда для запуска веб-админки
CMD ["python", "web_app.py"]