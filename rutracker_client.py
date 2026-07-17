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
        self.login_username = os.environ.get("RUTRACKER_LOGIN")
        self.login_password = os.environ.get("RUTRACKER_PASSWORD")

    def login(self):
        """Авторизация на Rutracker.

        Сначала пробует cookies из окружения: login.php закрыт проверкой
        Cloudflare, которую ни requests, ни cloudscraper не проходят, поэтому
        вход формой работает не везде.

        Returns:
            True, если сессия пригодна для запросов к трекеру.
        """
        if self.apply_cookies_from_env():
            return self._session_is_valid()

        if not self.login_username or not self.login_password:
            logging.error(
                f"[{self.name}] Не заданы RUTRACKER_LOGIN и RUTRACKER_PASSWORD, "
                f"а RUTRACKER_COOKIES пуст."
            )
            return False

        url = f"{self.base_domain}/forum/login.php"
        data = {
            "login_username": self.login_username,
            "login_password": self.login_password,
            "login": "Вход"
        }

        try:
            response = self.session.post(url, data=data, timeout=self.timeout)
        except Exception as e:
            logging.error(f"[{self.name}] Запрос входа не прошел: {e}")
            return False

        if response.status_code == 403 or 'just a moment' in response.text.lower():
            logging.error(
                f"[{self.name}] login.php вернул {response.status_code}: это защита Cloudflare, "
                f"а не неверный пароль. Войдите в браузере и скопируйте cookies "
                f"(bb_session и cf_clearance) в RUTRACKER_COOKIES, а строку User-Agent "
                f"браузера — в RUTRACKER_USER_AGENT. Зеркало задается через RUTRACKER_DOMAIN."
            )
            return False

        if response.status_code >= 400:
            logging.error(f"[{self.name}] login.php вернул HTTP {response.status_code}.")
            return False

        if 'bb_session' in self.session.cookies or 'profile.php?mode=viewprofile' in response.text:
            return True

        logging.error(f"[{self.name}] Вход отклонен: проверьте логин и пароль.")
        return False

    def _session_is_valid(self):
        """Проверяет, что с текущими cookies трекер отдает содержимое, а не форму входа."""
        try:
            html = self._fetch_with_retries(f"{self.base_domain}/forum/index.php")
        except Exception as e:
            logging.error(f"[{self.name}] Сессия из cookies не работает: {e}")
            return False

        if self._looks_like_auth_wall(html):
            logging.error(
                f"[{self.name}] Cookies не дают авторизованную сессию. "
                f"Они истекают: обновите bb_session и cf_clearance из браузера."
            )
            return False

        logging.info(f"[{self.name}] Сессия из cookies принята.")
        return True

    def relogin(self):
        """Повторный вход после того, как сессия протухла посреди обхода."""
        try:
            return self.login()
        except Exception as e:
            logging.error(f"[{self.name}] Повторная авторизация не удалась: {e}")
            return False

    def _looks_like_auth_wall(self, html):
        """Отличает страницу входа от содержимого форума.

        Без этой проверки истекшая сессия возвращала бы форму логина, а LLM
        честно пыталась бы извлечь из нее данные о фильме.
        """
        if not html:
            return False
        return 'login_username' in html and 'mode=viewprofile' not in html
