import json
import re
from openai import OpenAI

# Настройка клиента для локальной LM Studio
client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

# Укажите identifier модели, которая загружена в LM Studio
MODEL_NAME = "local-model"

def _call_llm(system_prompt: str, user_content: str) -> dict:
    """Универсальный метод для вызова LLM без сохранения истории."""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            temperature=0.1
            # Убран response_format, так как LM Studio может его не поддерживать
        )

        result_text = response.choices[0].message.content.strip()

        # Очистка текста от возможных markdown-тегов (```json ... ```)
        # Ищем всё, что находится между фигурными скобками { ... }
        match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if match:
            clean_json_str = match.group(0)
            return json.loads(clean_json_str)
        else:
            print("Не удалось найти JSON в ответе модели.")
            print(f"Сырой ответ: {result_text}")
            return {}

    except Exception as e:
        print(f"Ошибка вызова LLM: {e}")
        return {}

def discover_categories(markdown_text: str) -> dict:
    """Стадия А: Поиск разделов на главной странице."""
    system_prompt = """Ты — интеллектуальный анализатор структуры сайтов.
    Твоя задача: найти в предоставленном Markdown-списке ссылок те, которые ведут в разделы "Фильмы", "Сериалы" и "Мультфильмы".
    Верни строгий JSON в формате:
    {
        "movies_url": "url или null",
        "series_url": "url или null",
        "cartoons_url": "url или null"
    }
    Отвечай ТОЛЬКО валидным JSON, без дополнительных комментариев и текста.
    """
    return _call_llm(system_prompt, markdown_text)

def extract_topic_links(markdown_text: str) -> dict:
    """Стадия Б: Поиск ссылок на раздачи, подразделы и пагинацию."""
    system_prompt = """Ты — интеллектуальный навигатор по форуму.
    Твоя задача — проанализировать список ссылок и распределить их по категориям.

    Ищи:
    1. "subforum_links": ссылки, ведущие в подразделы (например, "Отечественные фильмы", "Новинки кино", "Аниме").
    2. "movie_links": ссылки, ведущие на страницы конкретных фильмов (топики/раздачи с описанием и кнопкой скачать).
    3. "next_page": ссылка на следующую страницу списка (Next/След/Вперед).

    Верни строгий JSON в формате:
    {
        "subforum_links": ["url1", "url2"],
        "movie_links": ["url3", "url4"],
        "next_page": "url или null"
    }
    Игнорируй правила форума, профили пользователей, технические разделы и рекламу. Отвечай ТОЛЬКО валидным JSON.
    """
    return _call_llm(system_prompt, markdown_text)

def extract_torrent_data(markdown_text: str) -> dict:
    """Стадия В: Парсинг данных из конкретной раздачи."""
    system_prompt = """Ты — парсер данных о фильмах.
    Извлеки из текста информацию о раздаче.
    Верни строгий JSON в формате:
    {
        "title": "Название фильма",
        "original_title": "Оригинальное название",
        "year": "2024",
        "video_quality": "1080p",
        "size": "1.5 GB",
        "magnet_link": "magnet:?xt=urn:btih:..."
    }
    Отвечай ТОЛЬКО валидным JSON.
    """
    return _call_llm(system_prompt, markdown_text)