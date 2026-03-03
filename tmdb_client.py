import os
import requests
import datetime
import gzip
import json
import io
from dotenv import load_dotenv

load_dotenv()

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"

    def __init__(self):
        self.api_key = os.environ.get("TMDB_API_KEY")
        self.read_token = os.environ.get("TMDB_READ_TOKEN")
        
        if not self.read_token:
             # Если v4 токен не задан, попробуем использовать v3 API Key в заголовках (хотя v4 предпочтительнее)
             self.headers = {
                "accept": "application/json"
            }
        else:
            self.headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {self.read_token}"
            }

    def download_daily_movie_ids(self):
        """
        Скачивает архив ID фильмов за вчерашний день и возвращает множество ID.
        """
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%m_%d_%Y")
        url = f"http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        movie_ids = set()
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            for line in f:
                data = json.loads(line)
                movie_ids.add(data.get("id"))
                
        return movie_ids

    def download_daily_tv_ids(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%m_%d_%Y")
        url = f"http://files.tmdb.org/p/exports/tv_series_ids_{date_str}.json.gz"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        tv_ids = set()
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            for line in f:
                tv_ids.add(json.loads(line).get("id"))
        return tv_ids

    def get_movie_details(self, movie_id):
        """
        Получает детальную информацию о конкретном фильме вместе с участниками (credits).
        """
        url = f"{self.BASE_URL}/movie/{movie_id}"
        params = {
            "language": "ru-RU",
            "append_to_response": "credits"
        }
        
        if not self.read_token and self.api_key:
            params["api_key"] = self.api_key

        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_tv_details(self, tv_id):
        url = f"{self.BASE_URL}/tv/{tv_id}"
        params = {"language": "ru-RU", "append_to_response": "credits"}
        if not self.read_token and self.api_key: params["api_key"] = self.api_key
        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_now_playing_movies(self):
        url = f"{self.BASE_URL}/movie/now_playing"
        params = {"language": "ru-RU", "page": 1}
        if not self.read_token and self.api_key: params["api_key"] = self.api_key
        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return [item['id'] for item in response.json().get('results', [])]

    def get_trending_tv_shows(self):
        url = f"{self.BASE_URL}/trending/tv/week"
        params = {"language": "ru-RU"}
        if not self.read_token and self.api_key: params["api_key"] = self.api_key
        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return [item['id'] for item in response.json().get('results', [])]

    def get_full_poster_url(self, poster_path):
        """
        Формирует полную ссылку на постер.
        """
        if not poster_path:
            return None
        return f"{self.IMAGE_BASE_URL}{poster_path}"
