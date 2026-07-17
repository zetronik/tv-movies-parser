import json
import logging
import os
import re
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Настройка клиента для локального Lemonade / LM Studio.
# Адрес переопределяется через LLM_BASE_URL: в контейнере localhost указывает на
# сам контейнер, поэтому там нужен, например, http://host.docker.internal:13305.
# Устаревшая OLLAMA_BASE_URL намеренно не читается — она осталась в старых .env
# со ссылкой на Ollama, которая в этом проекте не используется.
BASE_URL = os.environ.get("LLM_BASE_URL") or "http://localhost:13305/api/v1"

# Идентификатор модели на этом сервере.
MODEL_NAME = os.environ.get("LLM_MODEL", "Gemma-4-E4B-it-GGUF")

# Таймаут одного запроса к модели. По умолчанию SDK ждет 600 секунд и делает
# два повтора — не отвечающий сервер подвесил бы обход на полчаса.
LLM_TIMEOUT_SECONDS = float(os.environ.get("LLM_TIMEOUT_SECONDS") or 180)

client = OpenAI(
    base_url=BASE_URL,
    api_key=os.environ.get("LLM_API_KEY", "no_api_key"),
    timeout=LLM_TIMEOUT_SECONDS,
)

# Максимальный размер одной части контента, отправляемой в модель, в символах.
MAX_CONTENT_CHARS = 12000

# Предохранитель: сколько частей максимум обрабатывать для одной страницы.
MAX_CHUNKS = 6


def check_llm_available() -> bool:
    """Проверяет, что локальный LLM-сервер отвечает и с моделью можно работать.

    Вызывать один раз перед началом парсинга: без работающей модели пайплайн
    не извлечет ни одной раздачи, но будет впустую обходить страницы трекера.

    Returns:
        True, если сервер ответил.
    """
    # Проверка должна отвечать быстро и без повторов, поэтому таймаут короче обычного.
    probe = client.with_options(timeout=10.0, max_retries=0)
    try:
        model_ids = [m.id for m in probe.models.list().data]
        if MODEL_NAME not in model_ids:
            logging.warning(
                f"Модель '{MODEL_NAME}' не значится в списке на {BASE_URL}. "
                f"Доступны: {model_ids or 'список пуст'}"
            )
        logging.info(f"LLM доступна: {BASE_URL}, модель {MODEL_NAME}")
        return True
    except Exception as e:
        logging.debug(f"Список моделей недоступен ({e}), пробуем тестовый запрос.")

    # Не все локальные серверы реализуют /models — проверяем реальным запросом.
    try:
        probe.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1,
            temperature=0,
        )
        logging.info(f"LLM доступна: {BASE_URL}, модель {MODEL_NAME}")
        return True
    except Exception as e:
        logging.error(
            f"LLM недоступна ({BASE_URL}, модель {MODEL_NAME}): {e}. "
            f"Запустите локальный сервер модели и повторите."
        )
        return False

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

def _split_into_chunks(text: str, max_chars: int) -> list:
    """Разбивает текст на части не длиннее max_chars, не разрывая строки.

    Args:
        text: Исходный текст.
        max_chars: Предельная длина одной части в символах.

    Returns:
        Список частей; для короткого текста — список из одного элемента.
    """
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current = []
    current_len = 0

    for line in text.split('\n'):
        # Одиночная строка длиннее лимита (например, склеенная таблица) режется принудительно.
        while len(line) > max_chars:
            if current:
                chunks.append('\n'.join(current))
                current, current_len = [], 0
            chunks.append(line[:max_chars])
            line = line[max_chars:]

        if current and current_len + len(line) + 1 > max_chars:
            chunks.append('\n'.join(current))
            current, current_len = [], 0

        current.append(line)
        current_len += len(line) + 1

    if current:
        chunks.append('\n'.join(current))

    return chunks

def _merge_first_non_empty(acc: dict, part: dict) -> dict:
    """Стратегия слияния: первое непустое значение для каждого ключа побеждает."""
    for key, value in part.items():
        if value in (None, '', [], {}):
            continue
        if not acc.get(key):
            acc[key] = value
    return acc

def _merge_link_lists(acc: dict, part: dict) -> dict:
    """Стратегия слияния для страниц-списков: ссылки объединяются без дублей."""
    for key in ('subforum_links', 'movie_links'):
        collected = acc.setdefault(key, [])
        for link in part.get(key) or []:
            if link and link not in collected:
                collected.append(link)
    if not acc.get('next_page') and part.get('next_page'):
        acc['next_page'] = part['next_page']
    return acc

def _request_json(system_prompt: str, content: str) -> dict:
    """Делает один запрос к модели и возвращает разобранный JSON или пустой dict."""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content}
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
            logging.warning("Не удалось найти JSON в ответе модели.")
            logging.warning(f"Сырой ответ: {result_text}")
            return {}

    except json.JSONDecodeError as e:
        logging.error(f"Модель вернула некорректный JSON: {e}")
        return {}
    except Exception as e:
        logging.error(f"Ошибка вызова LLM: {e}")
        return {}

def _call_llm(system_prompt: str, user_content: str, merge_fn=_merge_first_non_empty,
              stop_when=None, strip_to_links: bool = True) -> dict:
    """Отправляет контент в модель, разбивая его на части, и объединяет ответы.

    Раньше контент просто обрезался по лимиту, и все, что не поместилось, для
    модели не существовало. Теперь длинная страница обрабатывается по частям.

    Args:
        system_prompt: Системная инструкция для модели.
        user_content: Текст страницы в формате Markdown.
        merge_fn: Функция слияния ответа очередной части с накопленным результатом.
        stop_when: Предикат по накопленному результату; при True обход частей
            прекращается досрочно.
        strip_to_links: Оставить только ссылки, заголовки и magnet-строки. Годится
            для страниц-списков, но не для страницы раздачи, где нужные данные
            (год, качество, размер) лежат обычным текстом.

    Returns:
        Объединенный результат либо пустой dict.
    """
    content = _clean_markdown_links(user_content) if strip_to_links else user_content
    chunks = _split_into_chunks(content, MAX_CONTENT_CHARS)

    if len(chunks) > MAX_CHUNKS:
        logging.warning(
            f"Контент разбит на {len(chunks)} частей, будут обработаны первые {MAX_CHUNKS}."
        )
        chunks = chunks[:MAX_CHUNKS]
    elif len(chunks) > 1:
        logging.info(f"Контент ({len(content)} символов) разбит на {len(chunks)} частей.")

    result = {}
    for number, chunk in enumerate(chunks, start=1):
        part = _request_json(system_prompt, chunk)
        if not part:
            continue
        result = merge_fn(result, part)
        if stop_when and stop_when(result):
            logging.debug(f"Данные собраны на части {number} из {len(chunks)}.")
            break

    return result

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
    all_found = lambda r: all(r.get(k) for k in ('movies_url', 'series_url', 'cartoons_url'))
    return _call_llm(system_prompt, markdown_text, stop_when=all_found)

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
    # Ссылки собираются со всех частей страницы, поэтому досрочного выхода нет.
    return _call_llm(system_prompt, markdown_text, merge_fn=_merge_link_lists)

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
    # strip_to_links=False принципиально: на странице раздачи год, качество и
    # размер написаны обычным текстом, и фильтр по ссылкам их бы выбросил.
    complete = lambda r: all(r.get(k) for k in ('title', 'year', 'size', 'magnet_link'))
    return _call_llm(system_prompt, markdown_text, stop_when=complete, strip_to_links=False)