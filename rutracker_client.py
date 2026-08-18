import logging
import os
import requests
from dotenv import load_dotenv

from tracker_client import BaseTrackerClient

load_dotenv()

class RutrackerClient(BaseTrackerClient):
    name = 'rutracker'
    base_domain = "https://rutracker.org"

    def __init__(self):
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })
        super().__init__(session)

    def login(self):
        """Проверяет доступ к Rutracker.

        Раздачи, включая magnet-ссылки, видны неавторизованным пользователям —
        аккаунт не нужен. Единственная преграда — защита Cloudflare: cookies из
        RUTRACKER_COOKIES (при необходимости) подставляются перед первым
        запросом, а этот метод проверяет, что сайт действительно отдает
        содержимое, а не challenge-страницу.

        Returns:
            True, если сайт доступен для запросов.
        """
        self.apply_cookies_from_env()
        return self._session_is_valid()

    def _session_is_valid(self):
        """Проверяет, что запрос к форуму возвращает содержимое, а не Cloudflare challenge."""
        try:
            html = self._fetch_with_retries(f"{self.base_domain}/forum/index.php")
        except Exception as e:
            logging.error(f"[{self.name}] Rutracker недоступен: {e}")
            return False

        if self._looks_like_auth_wall(html):
            logging.error(
                f"[{self.name}] Cloudflare блокирует запросы. Войдите в браузере и "
                f"скопируйте cf_clearance в RUTRACKER_COOKIES, а строку User-Agent "
                f"браузера — в RUTRACKER_USER_AGENT. Зеркало задается через RUTRACKER_DOMAIN."
            )
            return False

        return True

    def relogin(self):
        """Повторная проверка доступа после того, как страница на обходе оказалась Cloudflare-заглушкой."""
        try:
            return self.login()
        except Exception as e:
            logging.error(f"[{self.name}] Повторная проверка доступа не удалась: {e}")
            return False

    def _looks_like_auth_wall(self, html):
        """Отличает Cloudflare challenge-страницу от содержимого форума."""
        if not html:
            return False
        return 'just a moment' in html.lower()
