"""Общий транспорт для трекеров: паузы между запросами, повторы и валидация ссылок.

Раньше каждый клиент ходил в сеть сам: без пауз, без повторов и без проверки
того, что вернула модель. Любой таймаут означал безвозвратно пропущенную
раздачу, а запросы шли вплотную — прямой путь к бану.
"""

import logging
import os
import random
import re
import time
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Паузы между запросами к трекеру, в секундах.
DEFAULT_DELAY_MIN = 1.0
DEFAULT_DELAY_MAX = 4.0

DEFAULT_TIMEOUT = 30
MAX_FETCH_ATTEMPTS = 3

# Коды, при которых имеет смысл повторить запрос: перегрузка или троттлинг.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

BACKOFF_BASE_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0

# Ссылки короче этого в результатах поиска — иконки и служебные пометки.
MIN_RESULT_TITLE_LENGTH = 5

# Запасное правило отбора строк, когда сиды и размер распознать не удалось.
MIN_RESULT_CELLS = 4

SIZE_IN_ROW_PATTERN = re.compile(
    r'(\d+(?:[.,]\d+)?)\s*(GB|MB|TB|KB|GiB|MiB|TiB|ГБ|МБ|ТБ|КБ)\b', re.IGNORECASE
)
MAGNET_PATTERN = re.compile(r'magnet:\?xt=urn:btih:[^"\'\s<>&]+')

SIZE_UNITS_GB = {
    'kb': 1 / 1048576, 'кб': 1 / 1048576,
    'mb': 1 / 1024, 'mib': 1 / 1024, 'мб': 1 / 1024,
    'gb': 1.0, 'gib': 1.0, 'гб': 1.0,
    'tb': 1024.0, 'tib': 1024.0, 'тб': 1024.0,
}


class TrackerFetchError(Exception):
    """Страницу трекера не удалось получить после всех попыток."""


def env_float(name: str, default: float) -> float:
    """Читает число с плавающей точкой из переменной окружения.

    Args:
        name: Имя переменной окружения.
        default: Значение, если переменная не задана или не разбирается.

    Returns:
        Значение переменной либо default.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        logging.warning(f"Некорректное значение {name}={raw!r}, используется {default}.")
        return default


class BaseTrackerClient:
    """Базовый клиент трекера на движке phpBB.

    Наследники задают name, base_domain и способ создания сессии. При
    необходимости переопределяют проверку истекшей сессии и повторный вход.
    """

    name = ''
    base_domain = ''

    # Формы ссылок, которым разрешено попадать в обход. Все, что модель вернет
    # помимо них, отбрасывается: она может выдумать ссылку или спутать разделы.
    TOPIC_URL_PATTERN = re.compile(r'viewtopic\.php\?\S*\bt=\d+')
    FORUM_URL_PATTERN = re.compile(r'viewforum\.php\?\S*\bf=\d+')

    # Штатный поиск phpBB-трекера и селекторы колонок в его результатах.
    SEARCH_PATH = '/forum/tracker.php'
    SEED_SELECTORS = ('.seedmed', '.seed', 'td.seedmed', 'b.seedmed')
    LEECH_SELECTORS = ('.leechmed', '.leech', 'td.leechmed', 'b.leechmed')

    def __init__(self, session):
        self.session = session
        prefix = self.name.upper()

        # Домен можно переопределить: у трекеров есть зеркала, и основной адрес
        # бывает недоступен.
        self.base_domain = os.environ.get(f'{prefix}_DOMAIN', self.base_domain).rstrip('/')

        user_agent = os.environ.get(f'{prefix}_USER_AGENT')
        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})

        self.delay_min = env_float(
            f'{prefix}_REQUEST_DELAY_MIN', env_float('REQUEST_DELAY_MIN', DEFAULT_DELAY_MIN)
        )
        self.delay_max = env_float(
            f'{prefix}_REQUEST_DELAY_MAX', env_float('REQUEST_DELAY_MAX', DEFAULT_DELAY_MAX)
        )
        if self.delay_max < self.delay_min:
            self.delay_max = self.delay_min
        self.timeout = int(env_float(f'{prefix}_REQUEST_TIMEOUT', DEFAULT_TIMEOUT))
        self._last_request_at = 0.0

    # --- Сеть ---

    def _respect_delay(self):
        """Выдерживает случайную паузу с момента предыдущего запроса."""
        if self._last_request_at <= 0:
            return
        target = random.uniform(self.delay_min, self.delay_max)
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < target:
            time.sleep(target - elapsed)

    def _fetch_with_retries(self, url: str) -> str:
        """Скачивает страницу, повторяя попытки при сетевых сбоях и 5xx.

        Raises:
            TrackerFetchError: Если все попытки исчерпаны или ответ не восстановим.
        """
        last_error = None

        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            self._respect_delay()
            try:
                response = self.session.get(url, timeout=self.timeout)
                self._last_request_at = time.monotonic()
            except requests.RequestException as e:
                last_error = e
                logging.warning(f"[{self.name}] Попытка {attempt}/{MAX_FETCH_ATTEMPTS} — сетевая ошибка: {e}")
                self._backoff(attempt)
                continue

            if response.status_code in RETRY_STATUS_CODES:
                last_error = f"HTTP {response.status_code}"
                logging.warning(
                    f"[{self.name}] Попытка {attempt}/{MAX_FETCH_ATTEMPTS} — {last_error} для {url}"
                )
                self._backoff(attempt, response.headers.get('Retry-After'))
                continue

            if response.status_code >= 400:
                # 403/404 повторять бессмысленно.
                raise TrackerFetchError(f"HTTP {response.status_code} для {url}")

            return response.text

        raise TrackerFetchError(f"Не удалось получить {url}: {last_error}")

    def _backoff(self, attempt: int, retry_after=None):
        """Ждет перед следующей попыткой: Retry-After или экспоненциальная пауза."""
        delay = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)
        if retry_after:
            try:
                delay = min(float(retry_after), MAX_BACKOFF_SECONDS)
            except (TypeError, ValueError):
                pass
        # Разброс, чтобы повторы не били в сервер синхронно.
        time.sleep(delay + random.uniform(0, 1))

    def fetch_page(self, url: str) -> str:
        """Возвращает HTML страницы, при необходимости восстановив сессию.

        Raises:
            TrackerFetchError: Страница недоступна или сессия не восстановлена.
        """
        html = self._fetch_with_retries(url)

        if self._looks_like_auth_wall(html):
            logging.warning(f"[{self.name}] Сессия истекла, пробуем авторизоваться заново.")
            if not self.relogin():
                raise TrackerFetchError(f"Не удалось восстановить сессию для {url}")
            html = self._fetch_with_retries(url)
            if self._looks_like_auth_wall(html):
                raise TrackerFetchError(f"Сессия недействительна после повторного входа: {url}")

        return html

    def apply_cookies_from_env(self) -> bool:
        """Подставляет cookies, скопированные из браузера.

        Единственный рабочий способ пройти защиту Cloudflare без headless-браузера:
        авторизоваться вручную в браузере и передать сюда его cookies строкой
        вида "bb_session=...; cf_clearance=...". Значение cf_clearance привязано
        к IP и точной строке User-Agent, поэтому вместе с ним нужно задать и
        {PREFIX}_USER_AGENT тем же, что в браузере.

        Returns:
            True, если cookies из окружения были применены.
        """
        raw = os.environ.get(f'{self.name.upper()}_COOKIES', '').strip()
        if not raw:
            return False

        host = urlparse(self.base_domain).hostname
        applied = 0
        for part in raw.split(';'):
            if '=' not in part:
                continue
            key, value = part.split('=', 1)
            self.session.cookies.set(key.strip(), value.strip(), domain=host)
            applied += 1

        logging.info(f"[{self.name}] Применены cookies из окружения: {applied} шт.")
        return applied > 0

    def _looks_like_auth_wall(self, html: str) -> bool:
        """Признак того, что вместо содержимого вернулась страница входа."""
        return False

    def relogin(self) -> bool:
        """Повторная авторизация. Для трекеров без входа всегда успешна."""
        return True

    # --- Поиск раздач ---

    def search(self, query, limit=50):
        """Ищет раздачи по названию через штатный поиск трекера.

        Страница результатов разбирается детерминированно, без LLM: она
        табличная, а результат нужен интерактивно, за секунды.

        Args:
            query: Название для поиска.
            limit: Максимальное число результатов.

        Returns:
            Список словарей: topic_id, url, title, size_gb, seeds, leeches.

        Raises:
            TrackerFetchError: Если страница поиска недоступна.
        """
        search_url = f"{self.base_domain}{self.SEARCH_PATH}?nm={quote_plus(query)}"
        logging.info(f"[{self.name}] Поиск: {query}")
        html = self.fetch_page(search_url)
        return self._parse_search_results(html, search_url, limit)

    def _row_topic_link(self, row, page_url):
        """Возвращает первую ссылку на раздачу внутри строки таблицы."""
        for link in row.find_all('a', href=True):
            if 'viewtopic.php' not in link['href']:
                continue
            topic_url = self.resolve_url(link['href'], page_url, 'topic')
            if topic_url and len(link.get_text(strip=True)) >= MIN_RESULT_TITLE_LENGTH:
                return link, topic_url
        return None, None

    def _parse_search_results(self, html, page_url, limit):
        """Извлекает строки результатов из HTML страницы поиска.

        Разбор идет по строкам таблиц, а не по таблицам целиком: страница
        трекера — это вложенные друг в друга таблицы верстки, и у внешней
        обертки ссылок на темы оказывается больше, чем у настоящих результатов,
        за счет бокового меню ("Правила", "Новости", "Помощь").

        Строку результата отличает наличие показателя раздачи — сидов или
        размера. Если таких строк не нашлось, применяется запасное правило: в
        строке есть ссылка на тему и хотя бы четыре ячейки.
        """
        soup = BeautifulSoup(html, 'lxml')
        candidates = []

        for row in soup.find_all('tr'):
            # Только листовые строки: строка верстки, внутри которой лежит вся
            # таблица результатов, иначе присвоила бы себе сиды и размер первой
            # раздачи из вложенной таблицы.
            if row.find('tr') is not None:
                continue

            link, topic_url = self._row_topic_link(row, page_url)
            if not link:
                continue

            seeds = self._extract_row_number(row, self.SEED_SELECTORS)
            leeches = self._extract_row_number(row, self.LEECH_SELECTORS)
            size_gb = self._extract_row_size(row)
            has_signal = bool(row.select_one(', '.join(self.SEED_SELECTORS))) or size_gb > 0

            candidates.append({
                'row': row,
                'has_signal': has_signal,
                'item': {
                    'tracker': self.name,
                    'topic_id': self.extract_topic_id(topic_url),
                    'url': topic_url,
                    'title': link.get_text(strip=True),
                    'size_gb': size_gb,
                    'seeds': seeds,
                    'leeches': leeches,
                },
            })

        rows = [c for c in candidates if c['has_signal']]
        if not rows:
            rows = [c for c in candidates if len(c['row'].find_all('td')) >= MIN_RESULT_CELLS]
            if rows:
                logging.warning(
                    f"[{self.name}] Показатели раздач не распознаны, "
                    f"результаты отобраны по структуре строки."
                )

        results = []
        seen = set()
        for candidate in rows:
            item = candidate['item']
            if not item['topic_id'] or item['topic_id'] in seen:
                continue
            seen.add(item['topic_id'])
            results.append(item)
            if len(results) >= limit:
                break

        logging.info(f"[{self.name}] Найдено результатов: {len(results)}")
        return results

    @staticmethod
    def _extract_row_size(row):
        """Достает размер раздачи из строки таблицы результатов."""
        if row is None:
            return 0.0
        match = SIZE_IN_ROW_PATTERN.search(row.get_text(' ', strip=True))
        if not match:
            return 0.0
        try:
            value = float(match.group(1).replace(',', '.'))
        except ValueError:
            return 0.0
        return value * SIZE_UNITS_GB.get(match.group(2).lower(), 1.0)

    @staticmethod
    def _extract_row_number(row, selectors):
        """Достает число сидов или личей по списку возможных селекторов.

        Разметка у трекеров разная, поэтому при неудаче возвращается 0:
        колонка останется пустой, но результат поиска не потеряется.
        """
        if row is None:
            return 0
        for selector in selectors:
            cell = row.select_one(selector)
            if cell is None:
                continue
            digits = re.sub(r'\D', '', cell.get_text(strip=True))
            if digits:
                return int(digits)
        return 0

    def fetch_magnet(self, topic_url):
        """Возвращает magnet-ссылку со страницы раздачи или пустую строку."""
        try:
            html = self.fetch_page(topic_url)
        except TrackerFetchError as e:
            logging.warning(f"[{self.name}] Не удалось открыть {topic_url}: {e}")
            return ''
        match = MAGNET_PATTERN.search(html)
        return match.group(0) if match else ''

    # --- Разбор ссылок ---

    def extract_topic_id(self, url: str):
        """Извлекает ID раздачи из URL.

        Граница слова обязательна: без нее 'start=50' в ссылке пагинации
        распознавалось как раздача с ID 50.
        """
        match = re.search(r'\bt=(\d+)', url)
        return int(match.group(1)) if match else None

    def resolve_url(self, link, current_page: str, kind: str):
        """Превращает ссылку от модели в абсолютный URL, если ей можно доверять.

        Args:
            link: Ссылка из ответа LLM, возможно относительная или выдуманная.
            current_page: Страница, на которой ссылка найдена.
            kind: 'topic' для раздачи или 'forum' для раздела и пагинации.

        Returns:
            Абсолютный URL или None, если ссылка не проходит проверку.
        """
        if not link or not isinstance(link, str):
            return None

        candidate = urljoin(current_page, link.strip())
        parsed = urlparse(candidate)

        if parsed.scheme not in ('http', 'https'):
            return None

        if parsed.netloc.lower() != urlparse(self.base_domain).netloc.lower():
            logging.debug(f"[{self.name}] Ссылка на чужой домен отброшена: {candidate}")
            return None

        pattern = self.TOPIC_URL_PATTERN if kind == 'topic' else self.FORUM_URL_PATTERN
        if not pattern.search(f"{parsed.path}?{parsed.query}"):
            logging.debug(f"[{self.name}] Ссылка не похожа на '{kind}', отброшена: {candidate}")
            return None

        return candidate
