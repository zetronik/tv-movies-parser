import os
import re
import cloudscraper
from dotenv import load_dotenv

load_dotenv()

class NnmclubClient:
    def __init__(self):
        # Используем cloudscraper для автоматического обхода защиты Cloudflare
        self.session = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
        ua = os.environ.get("NNMCLUB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self.session.headers.update({
            "User-Agent": ua
        })
        self.base_domain = "https://nnmclub.to"

    def fetch_page(self, url):
        """Возвращает сырой HTML страницы для передачи в LLM."""
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.text

    def extract_topic_id(self, url):
        """Извлекает ID раздачи из URL."""
        match = re.search(r't=(\d+)', url)
        return int(match.group(1)) if match else None