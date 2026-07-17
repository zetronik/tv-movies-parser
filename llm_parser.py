import json
import re
from urllib.parse import urlparse
from openai import OpenAI

# Настройка клиента для локального Lemonade / LM Studio
# Убедитесь, что порт соответствует запущенному инстансу Lemonade (обычно 8000 или 15400)
client = OpenAI(base_url="http://localhost:13305/api/v1", api_key="no_api_key")

# Укажите identifier модели
MODEL_NAME = "Gemma-4-E4B-it-GGUF"

def _clean_markdown_links(markdown_text: str) -> str:
    """
    Очищает Markdown от заведомо мусорных ссылок, чтобы сжать контекст.
    Удаляет ссылки на профили, правила, FAQ, и оставляет только потенциально полезные.
    """
    lines = markdown_text.split('\n')
    cleaned_lines = []

    # Регулярные выражения для фильтрации типичного мусора на форумах
    garbage_patterns = [
        r'profile\.php', r'memberlist\.php', r'privmsg\.php',
        r'faq\.php', r'rules\.php', r'search\.php',
        r'viewonline\.php', r'groupcp\.php', r'register\.php',
        r'user', r'profile', r'rules', r'advertising', r'reklama'
    ]
    garbage_rx = re.compile('|'.join(garbage_patterns), re.IGNORECASE)

    for line in lines:
        # Проверяем, содержит ли строка ссылку Markdown: [текст](ссылка)
        match = re.search(r'\[([^\]]+)\]\(([^)]+)\)', line)
        if match:
            text, url = match.group(1), match.group(2)
            # Если ссылка похожа на мусорную — игнорируем строку
            if garbage_rx.search(url) or garbage_rx.search(text):
                continue

            # Сохраняем только содержательную часть
            cleaned_lines.append(f"[{text.strip()}]({url.strip()})")
        elif line.strip().startswith('#') or 'magnet:?xt=' in line:
            # Оставляем заголовки и магнет-ссылки
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)

def _call_llm(system_prompt: str, user_content: str) -> dict:
    """Универсальный метод для вызова LLM без сохранения истории."""
    try:
        # Предварительная очистка входящего контента перед отправкой в модель
        optimized_content = _clean_markdown_links(user_content)

        # Если после очистки контент остался слишком большим, принудительно его обрезаем
        # (в среднем 1 токен ≈ 4 символа для английского, для русского — около 1.5-2 символов)
        # Ограничим лимит в 10 000 символов (~2500-3000 токенов)
        if len(optimized_content) > 12000:
            optimized_content = optimized_content[:12000] + "\n... [Часть текста обрезана для экономии контекста] ..."

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": optimized_content}
            ],
            temperature=0
        )

        result_text = response.choices[0].message.content.strip()

        # Очистка текста от возможных markdown-тегов (```json ... ```)
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
    Игнорируй правила форума, профили пользователей, технические разделы и рекламы. Отвечай ТОЛЬКО валидным JSON.
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