import os
import requests
import datetime
import gzip
import json
import io
import logging
from dotenv import load_dotenv

from catalog import TV_ID_OFFSET

load_dotenv()

class TMDBClient:
    """Клиент TMDB.

    Пайплайн трекеров использует только search_movie, get_movie_details и
    get_full_poster_url. Остальные методы (дневные дампы ID, now_playing,
    trending, поиск сериалов) не подключены: они писались под режимы 'tmdb' и
    'trends', обработчиков которых в main.py нет. Оставлены как заготовка.
    """

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

    def search_movie(self, query, year=None):
        url = f"{self.BASE_URL}/search/movie"
        params = {"language": "ru-RU", "query": query, "page": 1}
        if year: params["primary_release_year"] = year
        if not self.read_token and self.api_key: params["api_key"] = self.api_key
        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        results = response.json().get("results", [])
        return results[0]['id'] if results else None

    def search_tv(self, query, year=None):
        url = f"{self.BASE_URL}/search/tv"
        params = {"language": "ru-RU", "query": query, "page": 1}
        if year: params["first_air_date_year"] = year
        if not self.read_token and self.api_key: params["api_key"] = self.api_key
        response = requests.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        results = response.json().get("results", [])
        return results[0]['id'] if results else None

    def search_candidates(self, query, year=None, limit=10):
        """Ищет фильмы и сериалы по названию и возвращает список кандидатов.

        В отличие от search_movie, который молча берет первый результат, здесь
        возвращаются варианты для ручного выбора в панели.

        Args:
            query: Название для поиска.
            year: Год выпуска, если известен.
            limit: Максимальное число кандидатов в ответе.

        Returns:
            Список словарей с ключами id, media_type, title, original_title,
            year, overview, poster_url, rating. Для сериалов id уже сдвинут
            на TV_ID_OFFSET, чтобы совпадать с идентификатором в каталоге.
        """
        candidates = []

        for media_type in ('movie', 'tv'):
            url = f"{self.BASE_URL}/search/{media_type}"
            params = {"language": "ru-RU", "query": query, "page": 1}
            if year:
                params["primary_release_year" if media_type == 'movie' else "first_air_date_year"] = year
            if not self.read_token and self.api_key:
                params["api_key"] = self.api_key

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=15)
                response.raise_for_status()
                results = response.json().get("results", [])
            except requests.RequestException as e:
                logging.error(f"Ошибка поиска в TMDB ({media_type}): {e}")
                continue

            for item in results:
                release = item.get("release_date") or item.get("first_air_date") or ""
                candidates.append({
                    "id": item["id"] + (TV_ID_OFFSET if media_type == 'tv' else 0),
                    "tmdb_id": item["id"],
                    "media_type": media_type,
                    "title": item.get("title") or item.get("name") or "",
                    "original_title": item.get("original_title") or item.get("original_name") or "",
                    "year": release[:4],
                    "overview": (item.get("overview") or "")[:300],
                    "poster_url": self.get_full_poster_url(item.get("poster_path")),
                    "rating": item.get("vote_average") or 0,
                    "popularity": item.get("popularity") or 0,
                })

        # Самые популярные вперед: так нужный вариант обычно оказывается сверху.
        candidates.sort(key=lambda c: c["popularity"], reverse=True)
        return candidates[:limit]

    def get_full_poster_url(self, poster_path):
        """
        Формирует полную ссылку на постер.
        """
        if not poster_path:
            return None
        return f"{self.IMAGE_BASE_URL}{poster_path}"
