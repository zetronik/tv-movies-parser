import sys
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
from urllib.parse import urljoin

from database import MovieDatabase
from tmdb_client import TMDBClient
from rutracker_client import RutrackerClient
from nnmclub_client import NnmclubClient
from content_cleaner import clean_html_to_markdown

# Импортируем методы для работы с локальной LLM
from llm_parser import discover_categories, extract_topic_links, extract_torrent_data

DATA_DIR = 'data/'
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'parser.log'), mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Эта строка дублирует вывод в консоль
    ]
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
    try:
        temp_zip = os.path.join(DATA_DIR, "movies_temp.zip")
        final_zip = os.path.join(DATA_DIR, "movies.zip")
        with zipfile.ZipFile(temp_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(db_name, arcname=os.path.basename(db_name))

        for _ in range(5):
            try:
                os.replace(temp_zip, final_zip)
                break
            except PermissionError:
                time.sleep(2)

        final_zip = os.path.join(DATA_DIR, "movies.zip")
        md5_file = os.path.join(DATA_DIR, "movies.md5")

        md5_hash = hashlib.md5()
        with open(final_zip, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        hash_str = md5_hash.hexdigest()
        with open(md5_file, "w") as f:
            f.write(hash_str)

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
        genres = ", ".join([g.get("name", "") for g in movie.get("genres", []) if g.get("name")])
        countries = ", ".join([c.get("name", "") for c in movie.get("production_countries", []) if c.get("name")])
        credits = movie.get("credits", {})
        directors = ", ".join([crew.get("name", "") for crew in credits.get("crew", []) if crew.get("job") == "Director" and crew.get("name")])
        actors = ", ".join([cast.get("name", "") for cast in credits.get("cast", [])[:10] if cast.get("name")])

        movie_data = (movie_id, title, original_title, overview, rating, release_date, full_poster_url, genres, countries, directors, actors, 'movie')
        db.upsert_movie(movie_data)
        return True
    except Exception as e:
        logging.error(f"Ошибка при обработке TMDB ID {movie_id}: {e}")
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
        genres_list = [g.get("name", "") for g in tv.get("genres", []) if g.get("name")]
        if "Сериал" not in genres_list: genres_list.append("Сериал")
        genres = ", ".join(genres_list)
        countries = ", ".join([c.get("name", "") for c in tv.get("production_countries", []) if c.get("name")])
        credits = tv.get("credits", {})
        directors = ", ".join([creator.get("name", "") for creator in tv.get("created_by", [])])
        actors = ", ".join([cast.get("name", "") for cast in credits.get("cast", [])[:10] if cast.get("name")])

        movie_data = (tv_id_shifted, title, original_title, overview, rating, release_date, full_poster_url, genres, countries, directors, actors, 'tv')
        db.upsert_movie(movie_data)
        return True
    except Exception as e:
        logging.error(f"Ошибка при обработке TMDB ID сериала {real_id}: {e}")
    return False

def run_tracker_pipeline(tracker_name, start_url, client, db, tmdb_client, flag_path):
    """
    Основной конечный автомат для парсинга трекера через локальную LLM с поддержкой подразделов.
    """
    logging.info(f"--- Запуск LLM-парсинга: {tracker_name} ---")
    topology = db.get_tracker_topology(tracker_name)

    # СТАДИЯ А: Исследователь (Поиск корневых разделов)
    if not topology or not topology.get('movies_url'):
        logging.info(f"[{tracker_name}] Структура неизвестна. Анализ главной страницы: {start_url}")
        try:
            html = client.fetch_page(start_url)
            md_text = clean_html_to_markdown(html)
            urls_json = discover_categories(md_text)
            logging.info(f"[{tracker_name}] LLM нашла категории: {urls_json}")
            db.save_tracker_topology(tracker_name, urls_json)
            movies_url = urls_json.get('movies_url')
        except Exception as e:
            logging.error(f"[{tracker_name}] Ошибка на стадии Discovery: {e}")
            return
    else:
        movies_url = topology.get('movies_url')
        logging.info(f"[{tracker_name}] Раздел фильмов загружен из БД: {movies_url}")

    if not movies_url:
        logging.error(f"[{tracker_name}] URL раздела фильмов не определен. Парсинг остановлен.")
        return

    # Очередь для обхода: сюда попадают и корневые разделы, и подразделы, и страницы пагинации
    root_movies_url = urljoin(start_url, movies_url)
    pages_queue = [root_movies_url]
    visited_pages = set() # Множество для защиты от бесконечных циклов

    # СТАДИЯ Б: Навигатор (Проход по очереди страниц)
    while pages_queue:
        if os.path.exists(flag_path):
            logging.info(f"[{tracker_name}] Получен сигнал остановки.")
            break

        current_page = pages_queue.pop(0) # Берем первую ссылку из очереди

        if current_page in visited_pages:
            continue

        visited_pages.add(current_page)
        logging.info(f"[{tracker_name}] Навигация. Очередь: {len(pages_queue)} | Обработка: {current_page}")

        try:
            html = client.fetch_page(current_page)
            md_text = clean_html_to_markdown(html)

            page_data = extract_topic_links(md_text)
            movie_links = page_data.get('movie_links') or []
            subforum_links = page_data.get('subforum_links') or []
            next_page = page_data.get('next_page')

            logging.info(f"[{tracker_name}] Найдено: {len(subforum_links)} подразделов, {len(movie_links)} раздач.")

            # Добавляем найденные подразделы в конец очереди
            for sf_link in subforum_links:
                if sf_link:
                    full_sf_link = urljoin(current_page, sf_link)
                    if full_sf_link not in visited_pages:
                        pages_queue.append(full_sf_link)

            # Добавляем следующую страницу (пагинацию) в очередь
            if next_page:
                full_next_page = urljoin(current_page, next_page)
                if full_next_page not in visited_pages:
                    pages_queue.append(full_next_page)

        except Exception as e:
            logging.error(f"[{tracker_name}] Ошибка на стадии Навигации ({current_page}): {e}")
            continue

        # СТАДИЯ В: Экстрактор (Сбор данных с конкретной раздачи)
        for link in movie_links:
            if not link: continue
            if os.path.exists(flag_path): break

            full_link = urljoin(current_page, link)
            topic_id = client.extract_topic_id(full_link)

            if not topic_id:
                continue

            if db.is_torrent_exists(tracker_name, topic_id):
                logging.info(f"  -> Пропуск: Раздача {topic_id} уже в базе.")
                continue

            logging.info(f"  -> Анализ раздачи: {full_link}")
            try:
                topic_html = client.fetch_page(full_link)
                topic_md = clean_html_to_markdown(topic_html)

                extracted = extract_torrent_data(topic_md)
                if not extracted:
                    logging.warning(f"  [!] LLM не смогла извлечь данные.")
                    continue

                ru_title = extracted.get('title') or extracted.get('ru_title', '')
                orig_title = extracted.get('original_title') or extracted.get('orig_title', '')
                year_raw = extracted.get('year', '')
                year = str(year_raw) if year_raw else ''

                if not ru_title:
                    continue

                movie_id = db.find_movie_by_title_and_year(ru_title, orig_title, year)

                if not movie_id:
                    search_title = orig_title if orig_title else ru_title
                    if search_title:
                        tmdb_id = tmdb_client.search_movie(search_title, year)
                        if tmdb_id:
                            if process_tmdb_movie(tmdb_id, db, tmdb_client):
                                movie_id = tmdb_id

                if movie_id:
                    size_str = str(extracted.get('size', '0'))
                    size_match = re.search(r'[\d\.]+', size_str.replace(',', '.'))
                    size_val = float(size_match.group()) if size_match else 0.0

                    db.insert_torrent(
                        tracker=tracker_name,
                        topic_id=topic_id,
                        movie_id=movie_id,
                        topic_title=ru_title,
                        size_gb=size_val,
                        quality=extracted.get('video_quality') or extracted.get('quality', ''),
                        file_format='',
                        translation='',
                        magnet_link=extracted.get('magnet_link', ''),
                        seeds=0,
                        leeches=0
                    )
                    logging.info(f"  [+] Добавлено: {ru_title} -> ID фильма: {movie_id}")
                else:
                    logging.warning(f"  [-] Не удалось привязать к фильму: {ru_title} ({year})")

            except Exception as e:
                logging.error(f"  [!] Ошибка при извлечении раздачи {full_link}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Movies Parser")
    parser.add_argument('--mode', choices=['tmdb', 'rutracker', 'nnmclub', 'cron', 'trends'], required=True, help='Режим работы парсера')
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
        try:
            db = MovieDatabase()
        except Exception as e:
            logging.error(f"Ошибка при инициализации БД: {e}")
            sys.exit(1)

        run_rutracker = args.mode == 'rutracker' or (args.mode == 'cron' and config.get("run_rutracker", True))
        run_nnmclub = args.mode == 'nnmclub' or (args.mode == 'cron' and config.get("run_nnmclub", True))

        tmdb_client = TMDBClient()
        if not tmdb_client.read_token and not tmdb_client.api_key:
            logging.error("Ошибка: API ключи TMDB не найдены в файле .env.")
            sys.exit(1)

        # 1. Запуск пайплайна Rutracker
        if run_rutracker and not os.path.exists(flag_path):
            update_progress("Парсинг Rutracker", 0, 100)
            rutracker = RutrackerClient()
            if rutracker.login():
                run_tracker_pipeline('rutracker', f"{rutracker.base_domain}/forum/index.php", rutracker, db, tmdb_client, flag_path)
            else:
                logging.error("Не удалось авторизоваться на Rutracker.")

        # 2. Запуск пайплайна NNM-Club
        if run_nnmclub and not os.path.exists(flag_path):
            update_progress("Парсинг NNM-Club", 0, 100)
            nnm = NnmclubClient()
            run_tracker_pipeline('nnmclub', f"{nnm.base_domain}/forum/index.php", nnm, db, tmdb_client, flag_path)

    finally:
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