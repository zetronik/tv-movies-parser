import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import hashlib
from botocore.client import Config
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

def upload_to_r2(file_path):
    endpoint_url = os.environ.get('R2_ENDPOINT_URL')
    access_key = os.environ.get('R2_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
    bucket_name = os.environ.get('R2_BUCKET_NAME')
    
    if not all([endpoint_url, access_key, secret_key, bucket_name]):
        logging.warning("Ключи R2 не заданы. Пропуск загрузки в облако.")
        return
    try:
        update_progress("Выгрузка в облако", 99, 100)
        logging.info(f"Начало загрузки {file_path} в Cloudflare R2...")
        
        s3 = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        
        object_name = os.path.basename(file_path)
        s3.upload_file(file_path, bucket_name, object_name)
        logging.info("Успешная загрузка в Cloudflare R2!")
    except Exception as e:
        logging.error(f"Ошибка при загрузке в R2: {e}")

def create_zip(db_name="movies.db"):
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
        
        final_zip = os.path.join(DATA_DIR, "movies.zip")
        md5_file = os.path.join(DATA_DIR, "movies.md5")
        
        # Генерируем MD5 хеш
        md5_hash = hashlib.md5()
        with open(final_zip, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        hash_str = md5_hash.hexdigest()
        
        # Сохраняем в файл
        with open(md5_file, "w") as f:
            f.write(hash_str)
            
        logging.info(f"Сгенерирован MD5 хеш: {hash_str}")
        
        # Загружаем в облако оба файла
        upload_to_r2(final_zip)
        upload_to_r2(md5_file)
    except Exception as e:
        logging.error(f"Ошибка при сжатии базы данных: {e}")

def process_tmdb_movie(movie_id, db, tmdb_client):
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
            actors,
            'movie'
        )
        
        db.upsert_movie(movie_data)
        logging.info(f"Сохранен фильм ID {movie_id}: {title}")
        return True
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.info(f"ID {movie_id} не найден. Пропускаем.")
        else:
            logging.error(f"HTTP ошибка для ID {movie_id}: {e}")
    except Exception as e:
        logging.error(f"Ошибка при обработке ID {movie_id}: {e}")
    return False

def process_tmdb_tv(tv_id_shifted, db, tmdb_client):
    real_id = tv_id_shifted - 100000000
    try:
        tv = tmdb_client.get_tv_details(real_id)
        title = tv.get("name")
        original_title = tv.get("original_name")
        overview = tv.get("overview")
        rating = tv.get("vote_average")
        release_date = tv.get("first_air_date", "")
        poster_path = tv.get("poster_path")
        full_poster_url = tmdb_client.get_full_poster_url(poster_path)
        
        # Принудительно добавляем тег "Сериал", чтобы клиент Flutter его распознал
        genres_list = [g.get("name", "") for g in tv.get("genres", []) if g.get("name")]
        if "Сериал" not in genres_list: genres_list.append("Сериал")
        genres = ", ".join(genres_list)
        
        countries = ", ".join([c.get("name", "") for c in tv.get("production_countries", []) if c.get("name")])
        credits = tv.get("credits", {})
        directors = ", ".join([creator.get("name", "") for creator in tv.get("created_by", [])])
        actors = ", ".join([cast_member.get("name", "") for cast_member in credits.get("cast", [])[:10] if cast_member.get("name")])
        
        movie_data = (tv_id_shifted, title, original_title, overview, rating, release_date, full_poster_url, genres, countries, directors, actors, 'tv')
        db.upsert_movie(movie_data)
        logging.info(f"Сохранен сериал ID {real_id}: {title}")
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.info(f"Сериал ID {real_id} не найден. Пропускаем.")
        else:
            logging.error(f"HTTP ошибка для сериала ID {real_id}: {e}")
    except Exception as e:
        logging.error(f"Ошибка при обработке сериала ID {real_id}: {e}")
    return False

def main():
    parser = argparse.ArgumentParser(description="Movies Parser")
    parser.add_argument('--mode', choices=['tmdb', 'rutracker', 'cron', 'trends'], required=True, help='Режим работы парсера')
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
        run_trends = args.mode == 'trends' or run_tmdb

        # 1.5 Инициализация TMDB клиента если нужно
        if run_tmdb or run_trends:
            tmdb_client = TMDBClient()
            if not tmdb_client.read_token and not tmdb_client.api_key:
                logging.error("Ошибка: API ключи TMDB не найдены в файле .env. Пожалуйста, заполните их.")
                sys.exit(1)

        # 2. Обработка TMDB (полная база)
        if run_tmdb:
            logging.info("[2/3] Получение списков ID фильмов и сериалов...")
            try:
                local_ids = db.get_existing_ids()
                
                tmdb_movie_ids = tmdb_client.download_daily_movie_ids()
                ids_to_fetch_movies = tmdb_movie_ids - local_ids
                
                tmdb_tv_ids = tmdb_client.download_daily_tv_ids()
                shifted_tv_ids = {tid + 100000000 for tid in tmdb_tv_ids}
                ids_to_fetch_tv = shifted_tv_ids - local_ids
                
                ids_to_fetch = ids_to_fetch_movies.union(ids_to_fetch_tv)
                logging.info(f"Новых фильмов: {len(ids_to_fetch_movies)}, новых сериалов: {len(ids_to_fetch_tv)}")
            except Exception as e:
                logging.error(f"Ошибка при получении списков ID: {e}")
                sys.exit(1)

            # Обработка TMDB
            if ids_to_fetch:
                ids_to_process = list(ids_to_fetch)
                logging.info(f"[3/3] Начинаем загрузку TMDB (всего {len(ids_to_process)} новых ID)...")

                saved_count = 0
                total_tmdb = len(ids_to_process)
                
                max_workers = 15
                
                def process_item(item_id, db, tmdb_client):
                    if item_id > 100000000:
                        return process_tmdb_tv(item_id, db, tmdb_client)
                    else:
                        return process_tmdb_movie(item_id, db, tmdb_client)
                        
                import concurrent.futures
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    active_tasks = set()
                    id_iterator = iter(ids_to_process)
                    
                    # Пул начинается с небольшого запаса задач
                    for _ in range(max_workers * 2):
                        try:
                            item_id = next(id_iterator)
                            active_tasks.add(executor.submit(process_item, item_id, db, tmdb_client))
                        except StopIteration:
                            break
                    
                    with tqdm(total=total_tmdb, desc="Парсинг TMDB") as pbar:
                        while active_tasks:
                            if os.path.exists(flag_path):
                                logging.info("Получен сигнал остановки, прерываем парсинг TMDB.")
                                try:
                                    executor.shutdown(wait=False, cancel_futures=True)
                                except TypeError:
                                    executor.shutdown(wait=False) # Для старых версий Python
                                break
                            
                            # Ждем завершения хотя бы одной задачи, или просыпаемся раз в секунду
                            done, active_tasks = concurrent.futures.wait(active_tasks, timeout=1.0, return_when=concurrent.futures.FIRST_COMPLETED)
                            
                            for future in done:
                                try:
                                    if future.result():
                                        saved_count += 1
                                except Exception as e:
                                    logging.error(f"Ошибка в потоке при обработке фильма/сериала: {e}")
                                
                                pbar.update(1)
                                update_progress("Парсинг TMDB", pbar.n, total_tmdb)
                                
                                # Добавляем новую задачу в пул взамен завершенной
                                try:
                                    next_item_id = next(id_iterator)
                                    active_tasks.add(executor.submit(process_item, next_item_id, db, tmdb_client))
                                except StopIteration:
                                    pass
            else:
                logging.info("База фильмов TMDB актуальна.")
        else:
            if args.mode != 'trends':
                logging.info("Парсинг TMDB отключен (работает другой режим).")

        # 2.5 Обработка трендов "Сейчас смотрят"
        if run_trends:
            if not os.path.exists(flag_path):
                update_progress("Обновление 'Сейчас смотрят'", 0, 100)
                logging.info("Получение списка 'Сейчас смотрят' (фильмы и сериалы)...")
                try:
                    now_playing_m = tmdb_client.get_now_playing_movies()
                    trending_tv = tmdb_client.get_trending_tv_shows()
                    
                    # Сдвигаем ID сериалов
                    shifted_tv_ids = [tid + 100000000 for tid in trending_tv]
                    all_trending_ids = now_playing_m + shifted_tv_ids
                    
                    # Проверяем, есть ли эти фильмы в нашей базе, если нет - докачиваем
                    local_ids = db.get_existing_ids()
                    missing_ids = [mid for mid in all_trending_ids if mid not in local_ids]
                    
                    if missing_ids:
                        logging.info(f"Докачиваем {len(missing_ids)} недостающих фильмов/сериалов для раздела трендов...")
                        for mid in missing_ids:
                            if mid > 100000000:
                                process_tmdb_tv(mid, db, tmdb_client)
                            else:
                                process_tmdb_movie(mid, db, tmdb_client)
                                
                    # Обновляем таблицу
                    db.update_now_playing_list(all_trending_ids)
                    logging.info(f"Раздел 'Сейчас смотрят' обновлен. Всего: {len(all_trending_ids)} элементов.")
                except Exception as e:
                    logging.error(f"Ошибка при обновлении 'Сейчас смотрят': {e}")

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
