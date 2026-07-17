import os
import cloudscraper
from dotenv import load_dotenv

from tracker_client import BaseTrackerClient

load_dotenv()

class NnmclubClient(BaseTrackerClient):
    name = 'nnmclub'
    base_domain = "https://nnmclub.to"

    def __init__(self):
        # Используем cloudscraper для автоматического обхода защиты Cloudflare
        session = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        ua = os.environ.get("NNMCLUB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        session.headers.update({
            "User-Agent": ua
        })
        super().__init__(session)
