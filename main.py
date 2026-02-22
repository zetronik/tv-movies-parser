import sys
import time
import requests
import json
import re
import logging
import zipfile
from tqdm import tqdm
from database import MovieDatabase
from tmdb_client import TMDBClient
from rutracker_client import RutrackerClient

# Настройка логирования
logging.basicConfig(
    filename='parser.log',
    filemode='w', # Очищаем файл при каждом запуске
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

import os

def update_progress(task_name, current, total):
    try:
        tmp_name = 'progress.tmp'
        with open(tmp_name, 'w', encoding='utf-8') as f:
            json.dump({'task': task_name, 'current': current, 'total': total, 'timestamp': time.time()}, f)
        os.replace(tmp_name, 'progress.json')
    except Exception as e:
        print(f"Progress error: {e}")

def get_config():
    try:
        with open('parser_config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"run_tmdb": True, "run_rutracker": True, "cron_time": "02:00"}

def main():
    config = get_config()
    update_progress("Инициализация", 0, 100)
    logging.info("--- Запуск парсера фильмов ---")
    
    # 1. Инициализация БД
    try:
        db = MovieDatabase()
        logging.info("[1/3] База данных инициализирована.")
    except Exception as e:
        logging.error(f"Ошибка при инициализации БД: {e}")
        sys.exit(1)

    # 2. Инициализация клиента TMDB (Если включено)
    if config.get("run_tmdb", True):
        tmdb_client = TMDBClient()
        
        if not tmdb_client.read_token and not tmdb_client.api_key:
            logging.error("Ошибка: API ключи TMDB не найдены в файле .env. Пожалуйста, заполните их.")
            sys.exit(1)

        logging.info("[2/3] Получение списков ID фильмов...")
        try:
            local_ids = db.get_existing_ids()
            logging.info(f"ID в локальной базе: {len(local_ids)}")
            
            tmdb_ids = tmdb_client.download_daily_movie_ids()
            logging.info(f"ID в базе TMDB (export): {len(tmdb_ids)}")
            
            ids_to_fetch = tmdb_ids - local_ids
            logging.info(f"Новых ID для скачивания: {len(ids_to_fetch)}")
        except Exception as e:
            logging.error(f"Ошибка при получении списков ID: {e}")
            sys.exit(1)

        # Обработка TMDB (ограничена 100 для скорости тестирования)
        if ids_to_fetch:
            limit = 100
            ids_to_process = list(ids_to_fetch)[:limit]
            logging.info(f"[3/3] Начинаем загрузку TMDB (взято {limit} ID для теста)...")

            saved_count = 0
            total_tmdb = len(ids_to_process)
            for i, movie_id in enumerate(tqdm(ids_to_process, desc="Парсинг TMDB", leave=True)):
                update_progress("Парсинг TMDB", i, total_tmdb)
                try:
                    movie = tmdb_client.get_movie_details(movie_id)
                    
                    title = movie.get("title")
                    original_title = movie.get("original_title")
                    overview = movie.get("overview")
                    rating = movie.get("vote_average")
                    release_date = movie.get("release_date")
                    poster_path = movie.get("poster_path")
                    
                    full_poster_url = tmdb_client.get_full_poster_url(poster_path)
                    
                    # Извлечение данных
                    genres = ", ".join([g.get("name", "") for g in movie.get("genres", []) if g.get("name")])
                    countries = ", ".join([c.get("name", "") for c in movie.get("production_countries", []) if c.get("name")])
                    
                    credits = movie.get("credits", {})
                    
                    # Режиссеры
                    directors = ", ".join([
                        crew_member.get("name", "") 
                        for crew_member in credits.get("crew", []) 
                        if crew_member.get("job") == "Director" and crew_member.get("name")
                    ])
                    
                    # Актёры
                    actors = ", ".join([
                        cast_member.get("name", "") 
                        for cast_member in credits.get("cast", [])[:10] 
                        if cast_member.get("name")
                    ])
                    
                    movie_data = (
                        movie_id,
                        title,
                        original_title,
                        overview,
                        rating,
                        release_date,
                        full_poster_url,
                        genres,
                        countries,
                        directors,
                        actors
                    )
                    
                    db.upsert_movie(movie_data)
                    logging.info(f"Сохранен фильм ID {movie_id}: {title}")
                    saved_count += 1
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        logging.info(f"ID {movie_id} не найден. Пропускаем.")
                    else:
                        logging.error(f"HTTP ошибка для ID {movie_id}: {e}")
                except Exception as e:
                    logging.error(f"Ошибка при обработке ID {movie_id}: {e}")
                    
                time.sleep(0.05)
        else:
            logging.info("База фильмов TMDB актуальна.")
    else:
        logging.info("Парсинг TMDB отключен в настройках.")

    # 3. Полный прогон парсера Рутрекера (Если включено)
    if config.get("run_rutracker", True):
        logging.info("--- Запуск парсера Rutracker ---")
    rutracker_client = RutrackerClient()
    
    logging.info("Авторизация на Rutracker...")
    try:
        if rutracker_client.login():
            logging.info("Успешная авторизация!")
        else:
            logging.error("Не удалось авторизоваться. Проверьте логин и пароль в .env.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Ошибка при авторизации: {e}")
        sys.exit(1)

    logging.info("Получение списка фильмов из базы данных...")
    with db.get_connection() as conn:
        cursor = conn.execute("SELECT id, title, original_title, release_date FROM movies WHERE original_title IS NOT NULL LIMIT 100")
        movies = cursor.fetchall()

    logging.info(f"Фильмов для парсинга раздач: {len(movies)}")

    total_rutracker = len(movies)
    for i, movie in enumerate(tqdm(movies, desc="Парсинг Rutracker", leave=True)):
        update_progress("Парсинг Rutracker", i, total_rutracker)
        movie_id, title, original_title, release_date = movie
        year = release_date[:4] if release_date else ""
        
        logging.info(f"Поиск: {title} ({original_title} {year})")
        
        try:
            results = rutracker_client.search_movie(title, original_title, year)
            topics_to_process = results[:5]  # ограничиваемся первыми 5
            
            logging.info(f"Найдено раздач: {len(results)}, скачиваем детали для первых {len(topics_to_process)}")
            
            for topic in topics_to_process:
                details = rutracker_client.get_topic_details(topic['topic_url'])
                
                magnet = details.get('magnet')
                seeds = topic.get('seeds', 0)
                leeches = topic.get('leeches', 0)
                
                if magnet:
                    size_gb = 0.0
                    size_str = details.get('size')
                    if size_str:
                        # Конвертируем строку веса в float gb
                        match = re.search(r"([\d\.]+)\s*(GB|MB|KB|ГБ|МБ|КБ)", size_str, re.IGNORECASE)
                        if match:
                            val = float(match.group(1))
                            unit = match.group(2).upper()
                            if unit in ['GB', 'ГБ']:
                                size_gb = val
                            elif unit in ['MB', 'МБ']:
                                size_gb = val / 1024.0
                            elif unit in ['KB', 'КБ']:
                                size_gb = val / (1024.0 * 1024.0)

                    db.insert_torrent(
                        movie_id=movie_id,
                        topic_title=topic['topic_title'],
                        size_gb=round(size_gb, 2),
                        quality=details.get('quality'),
                        file_format=details.get('format'),
                        translation=details.get('translation'),
                        magnet_link=magnet,
                        seeds=seeds,
                        leeches=leeches
                    )
            
            # Пауза между запросами к форуму для предотвращения бана
            time.sleep(1.5)
            
        except Exception as e:
             logging.error(f"Ошибка при парсинге раздач для фильма {title}: {e}")
    else:
        logging.info("Парсинг Rutracker отключен в настройках.")

    # 4. Архивация базы данных
    update_progress("Сжатие базы данных", 99, 100)
    logging.info("Сжатие базы данных...")
    try:
        with zipfile.ZipFile("movies.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(db.db_name)
        logging.info(f"База данных успешно сжата в movies.zip")
    except Exception as e:
        logging.error(f"Ошибка при сжатии базы данных: {e}")

    logging.info("--- Работа скрипта завершена ---")
    update_progress("Ожидание", 0, 0)

if __name__ == "__main__":
    main()
