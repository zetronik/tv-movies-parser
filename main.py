import sys
import time
import requests
import json
import re
import logging
import zipfile
import os
import argparse
from tqdm import tqdm
from database import MovieDatabase
from tmdb_client import TMDBClient
from rutracker_client import RutrackerClient
DATA_DIR = 'data/'
os.makedirs(DATA_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    filename=os.path.join(DATA_DIR, 'parser.log'),
    filemode='w', # Очищаем файл при каждом запуске
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def update_progress(task_name, current, total):
    try:
        tmp_name = os.path.join(DATA_DIR, 'progress.tmp')
        with open(tmp_name, 'w', encoding='utf-8') as f:
            json.dump({'task': task_name, 'current': current, 'total': total, 'timestamp': time.time()}, f)
        os.replace(tmp_name, os.path.join(DATA_DIR, 'progress.json'))
    except Exception as e:
        print(f"Progress error: {e}")

def get_config():
    try:
        with open(os.path.join(DATA_DIR, 'parser_config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"run_tmdb": True, "run_rutracker": True, "cron_time": "02:00"}

def create_zip(db_name="movies.db"):
    # Если db_name - это просто имя файла, то допишем DATA_DIR
    if not db_name.startswith(DATA_DIR):
        db_name = os.path.join(DATA_DIR, os.path.basename(db_name))
        
    update_progress("Сжатие базы данных", 99, 100)
    logging.info("Сжатие базы данных...")
    try:
        temp_zip = os.path.join(DATA_DIR, "movies_temp.zip")
        final_zip = os.path.join(DATA_DIR, "movies.zip")
        with zipfile.ZipFile(temp_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Кладем внутрь архива сам файл (чтобы внутри не было пути DATA_DIR)
            zipf.write(db_name, arcname=os.path.basename(db_name))
        
        # Безопасная замена файла (с попытками, если файл сейчас скачивают)
        for _ in range(5):
            try:
                os.replace(temp_zip, final_zip)
                break
            except PermissionError:
                time.sleep(2)
                
        logging.info("База данных успешно сжата в movies.zip")
    except Exception as e:
        logging.error(f"Ошибка при сжатии базы данных: {e}")

def main():
    parser = argparse.ArgumentParser(description="Movies Parser")
    parser.add_argument('--mode', choices=['tmdb', 'rutracker', 'cron'], required=True, help='Режим работы парсера')
    args = parser.parse_args()

    config = get_config()
    update_progress("Инициализация", 0, 100)
    logging.info(f"--- Запуск парсера фильмов (Режим: {args.mode}) ---")
    
    flag_path = os.path.join(DATA_DIR, 'stop.flag')
    if os.path.exists(flag_path):
        try:
            os.remove(flag_path)
        except OSError:
            pass

    try:
        # 1. Инициализация БД
        try:
            db = MovieDatabase()
            logging.info("[1/3] База данных инициализирована.")
        except Exception as e:
            logging.error(f"Ошибка при инициализации БД: {e}")
            sys.exit(1)

        run_tmdb = args.mode == 'tmdb' or (args.mode == 'cron' and config.get("run_tmdb", True))
        run_rutracker = args.mode == 'rutracker' or (args.mode == 'cron' and config.get("run_rutracker", True))

        # 2. Обработка TMDB
        if run_tmdb:
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
                    if os.path.exists(flag_path):
                        logging.info("Обнаружен сигнал остановки stop.flag. Прерывание цикла TMDB...")
                        break

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
            logging.info("Парсинг TMDB отключен или не запрошен в этом режиме.")

        # 3. Полный прогон парсера Рутрекера
        if run_rutracker and not os.path.exists(flag_path):
            update_progress("Парсинг Rutracker", 0, 100)
            logging.info("Запуск парсера Rutracker (режим сканирования форумов)...")
            rutracker = RutrackerClient()
            try:
                rutracker.login()
                target_categories = [2, 18] # 2 - Кино, 18 - Сериалы
                
                for cat_id in target_categories:
                    if os.path.exists(flag_path): break
                    
                    logging.info(f"Сбор форумов для категории {cat_id}...")
                    forum_ids = rutracker.get_forums_from_category(cat_id)
                    
                    for forum_id in forum_ids:
                        if os.path.exists(flag_path): break
                        logging.info(f"Сканирование подраздела f={forum_id}...")
                        
                        # Смотрим первые 2 страницы (свежие раздачи)
                        topics = rutracker.get_topics_from_forum(forum_id, pages=2)
                        
                        for topic in topics:
                            if os.path.exists(flag_path): break
                            
                            ru_title, orig_title, year = rutracker.parse_topic_title(topic['title'])
                            movie_id = db.find_movie_by_title_and_year(ru_title, orig_title, year)
                            
                            if movie_id:
                                logging.info(f"Найдено совпадение: {ru_title} ({year}) -> ID БД: {movie_id}")
                                details = rutracker.get_topic_details(topic['topic_id'])
                                
                                if details and details.get('magnet'):
                                    db.insert_torrent(
                                        movie_id=movie_id,
                                        topic_title=topic['title'],
                                        size_gb=round(details['size_gb'], 2),
                                        quality=details.get('quality', ''),
                                        file_format='', 
                                        translation='', 
                                        magnet_link=details['magnet'],
                                        seeds=details.get('seeds', 0),
                                        leeches=details.get('leeches', 0)
                                    )
                            time.sleep(1.5) # Пауза между раздачами, чтобы не получить бан
                            
            except Exception as e:
                logging.error(f"Ошибка в главном цикле парсинга Rutracker: {e}")
        else:
            if run_rutracker:
                logging.info("Парсинг Rutracker отменен из-за флага остановки.")
            else:
                logging.info("Парсинг Rutracker отключен или не запрошен в этом режиме.")

    finally:
        # 4. Архивация базы данных всегда выполняется
        create_zip(db.db_name if 'db' in locals() else "movies.db")
        if os.path.exists(flag_path):
            try:
                os.remove(flag_path)
            except OSError:
                pass
        logging.info("--- Работа скрипта завершена ---")
        update_progress("Ожидание", 0, 0)

if __name__ == "__main__":
    main()
