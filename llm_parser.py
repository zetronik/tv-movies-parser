import os
import re
import json
import logging
from typing import Optional
from openai import OpenAI
from schemas import TorrentExtraction

# Инициализируем клиент OpenAI для работы с Ollama.
# По умолчанию используем имя хоста контейнера 'ollama', но даем возможность переопределить через переменные окружения.
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gemma4:e2b")

client = OpenAI(
    base_url=OLLAMA_BASE_URL,
    api_key="nokey"  # Заглушка, так как Ollama не требует API-ключа
)

# Явный JSON-шаблон для подстановки в промпт
_JSON_TEMPLATE = '''{
  "ru_title": "Русское название фильма",
  "orig_title": "Original title or null",
  "year": 2024,
  "size_gb": 12.5,
  "quality": "BDRip / WEB-DL / 1080p / HDRip / ...",
  "magnet_link": "magnet:?xt=urn:btih:..."
}'''

_SYSTEM_PROMPT = f"""Ты — парсер данных о торрент-раздачах. Твоя единственная задача — извлечь из текста страницы торрент-трекера конкретные поля и вернуть их в виде JSON.

ВАЖНО: Верни ТОЛЬКО валидный JSON-объект, без пояснений, без markdown-блоков (не используй ```json).

Структура ответа (строго такая, все поля обязательны кроме orig_title):
{_JSON_TEMPLATE}

Правила извлечения:
- ru_title: русское название фильма/сериала из заголовка топика
- orig_title: оригинальное название (английское/латиница), или null если отсутствует
- year: год выпуска — четырёхзначное число
- size_gb: размер файла в ГБ (если указан в МБ — раздели на 1024)
- quality: тип качества из заголовка (BDRip, WEB-DL, HDRip, 1080p, 720p, 4K, UHD и т.п.)
- magnet_link: ссылка начинающаяся с "magnet:?xt="

Если поле не найдено в тексте — для строк используй "", для чисел 0, для magnet_link используй "".
"""


def _extract_json_from_text(text: str) -> Optional[dict]:
    """Извлекает JSON из текста, даже если модель обернула его в markdown-блок."""
    if not text:
        return None

    # Убираем markdown-блоки ```json ... ``` или ``` ... ```
    cleaned = re.sub(r'```(?:json)?\s*', '', text).strip()
    cleaned = re.sub(r'```\s*$', '', cleaned).strip()

    # Ищем первый JSON-объект в тексте
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Пробуем весь текст как JSON
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def extract_torrent_data(markdown_text: str) -> Optional[TorrentExtraction]:
    """Извлекает структурированную информацию о раздаче из markdown-текста с помощью LLM.

    Args:
        markdown_text: Очищенный текст страницы раздачи в формате Markdown.

    Returns:
        Объект TorrentExtraction, если извлечение и валидация прошли успешно, иначе None.
    """
    if not markdown_text:
        return None

    # Ограничиваем размер текста чтобы не перегружать контекст модели
    truncated_text = markdown_text[:8000]

    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": _SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": f"Извлеки данные из следующего текста страницы торрент-трекера:\n\n{truncated_text}"
                }
            ],
            temperature=0,  # Детерминированный вывод для структурированных данных
        )

        content = response.choices[0].message.content
        if not content:
            logging.warning("LLM вернула пустой ответ")
            return None

        data = _extract_json_from_text(content)
        if not data:
            logging.error(f"Не удалось извлечь JSON из ответа LLM. Ответ: {content[:200]}")
            return None

        # Валидируем полученный JSON через Pydantic-модель
        return TorrentExtraction.model_validate(data)

    except Exception as e:
        logging.error(f"Ошибка при работе LLM или парсинге JSON: {e}")
        return None
