import os
import requests
import re
from dotenv import load_dotenv

load_dotenv()

class RutrackerClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })
        self.login_username = os.environ.get("RUTRACKER_LOGIN")
        self.login_password = os.environ.get("RUTRACKER_PASSWORD")
        self.base_domain = "https://rutracker.org"

    def login(self):
        """Авторизация на Rutracker"""
        if not self.login_username or not self.login_password:
            raise ValueError("Rutracker credentials are not set in the environment variables.")

        url = "https://rutracker.org/forum/login.php"
        data = {
            "login_username": self.login_username,
            "login_password": self.login_password,
            "login": "Вход"
        }

        response = self.session.post(url, data=data, timeout=30)
        response.raise_for_status()

        if 'bb_session' in self.session.cookies or 'profile.php?mode=viewprofile' in response.text:
            return True
        return False

    def fetch_page(self, url):
        """Скачивает сырой HTML страницы для передачи в LLM."""
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.text

    def extract_topic_id(self, url):
        """Извлекает ID раздачи из URL для проверки дубликатов в базе."""
        match = re.search(r't=(\d+)', url)
        return int(match.group(1)) if match else None