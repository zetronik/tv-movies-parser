import sys
import threading
import queue
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
from nnmclub_client import NnmclubClient
from content_cleaner import clean_html_to_markdown
from llm_parser import extract_torrent_data

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

html_queue = queue.Queue()
db_lock = threading.Lock()
topic_metadata = {}
metadata_lock = threading.Lock()

def producer_task(url, tracker_type, client):
    """Скачивает HTML-код топика, очищает его и добавляет в очередь html_queue."""
    import random
    # Случайная задержка для имитации естественного поведения браузера
    # и предотвращения блокировки по rate-limit (503 от NNM-Club)
    delay_min = float(os.environ.get("NNMCLUB_REQUEST_DELAY_MIN", 1.0))
    delay_max = float(os.environ.get("NNMCLUB_REQUEST_DELAY_MAX", 4.0))
    time.sleep(random.uniform(delay_min, delay_max))
    try:
        topic_id_match = re.search(r't=(\d+)', url)
        if not topic_id_match:
            logging.error(f"Не удалось извлечь topic_id из URL: {url}")
            return
        topic_id = int(topic_id_match.group(1))
        
        details = client.get_topic_details(topic_id)
        if details and 'html' in details:
            markdown_text = clean_html_to_markdown(details['html'])
            html_queue.put((url, markdown_text))
            logging.info(f"Producer: добавлен {url} в очередь")
    except Exception as e:
        logging.error(f"Ошибка в Producer для {url}: {e}")

def consumer_loop(db, tmdb_client, nnm_tv_forums):
    """В бесконечном цикле обрабатывает очередь html_queue и сохраняет данные в БД."""
    while True:
        item = html_queue.get()
        if item is None:
            html_queue.task_done()
            break
        
        url, markdown_text = item
        try:
            extracted = extract_torrent_data(markdown_text)
            if not extracted:
                logging.warning(f"LLM не смогла извлечь данные для {url}")
                continue
            
            with metadata_lock:
                meta = topic_metadata.get(url)
            
            if not meta:
                logging.warning(f"Метаданные для URL не найдены: {url}")
                continue
            
            ru_title = extracted.ru_title
            orig_title = extracted.orig_title
            year = str(extracted.year)
            
            # Поиск фильма в локальной БД
            movie_id = db.find_movie_by_title_and_year(ru_title, orig_title, year)
            
            if not movie_id:
                search_title = orig_title if orig_title else ru_title
                if search_title:
                    logging.info(f"Фильм не найден в БД. Поиск в TMDB: {search_title} ({year})")
                    try:
                        is_tv = False
                        if meta['tracker'] == 'rutracker' and meta['cat_id'] == 18:
                            is_tv = True
                        elif meta['tracker'] == 'nnmclub' and meta['cat_id'] in nnm_tv_forums:
                            is_tv = True
                        
                        if is_tv:
                            tmdb_id = tmdb_client.search_tv(search_title, year)
                            if tmdb_id:
                                shifted_id = tmdb_id + 100000000
                                if process_tmdb_tv(shifted_id, db, tmdb_client):
                                    movie_id = shifted_id
                        else:
                            tmdb_id = tmdb_client.search_movie(search_title, year)
                            if tmdb_id:
                                if process_tmdb_movie(tmdb_id, db, tmdb_client):
                                    movie_id = tmdb_id
                    except Exception as e:
                        logging.error(f"Ошибка TMDB поиска для {search_title} ({year}): {e}")
            
            if movie_id:
                logging.info(f"Запись раздачи в БД: {ru_title} ({year}) -> ID фильма: {movie_id}")
                with db_lock:
                    db.insert_torrent(
                        tracker=meta['tracker'],
                        topic_id=meta['topic_id'],
                        movie_id=movie_id,
                        topic_title=meta['topic_title'],
                        size_gb=round(extracted.size_gb, 2),
                        quality=extracted.quality,
                        file_format='',
                        translation='',
                        magnet_link=extracted.magnet_link,
                        seeds=meta['seeds'],
                        leeches=meta['leeches']
                    )
            else:
                logging.warning(f"Не удалось привязать раздачу к фильму: {ru_title} ({year})")
                
        except Exception as e:
            logging.error(f"Ошибка в Consumer при обработке {url}: {e}")
        finally:
            html_queue.task_done()

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
            zipf.write(db_name, arcname=os.path.basename(db_name))
        
        for _ in range(5):
            try:
                os.replace(temp_zip, final_zip)
                break
            except PermissionError:
                time.sleep(2)
                
        logging.info("База данных успешно сжата in movies.zip")
        
        final_zip = os.path.join(DATA_DIR, "movies.zip")
        md5_file = os.path.join(DATA_DIR, "movies.md5")
        
        md5_hash = hashlib.md5()
        with open(final_zip, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        hash_str = md5_hash.hexdigest()
        
        with open(md5_file, "w") as f:
            f.write(hash_str)
            
        logging.info(f"Сгенерирован MD5 хеш: {hash_str}")
        
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
        
        directors = ", ".join([
            crew_member.get("name", "") 
            for crew_member in credits.get("crew", []) 
            if crew_member.get("job") == "Director" and crew_member.get("name")
        ])
        
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
            logging.info("[1/3] База данных инициализирована.")
        except Exception as e:
            logging.error(f"Ошибка при инициализации БД: {e}")
            sys.exit(1)

        run_tmdb = args.mode == 'tmdb' or (args.mode == 'cron' and config.get("run_tmdb", True))
        run_rutracker = args.mode == 'rutracker' or (args.mode == 'cron' and config.get("run_rutracker", True))
        run_nnmclub = args.mode == 'nnmclub' or (args.mode == 'cron' and config.get("run_nnmclub", True))
        run_trends = args.mode == 'trends' or run_tmdb

        tmdb_client = TMDBClient()
        if not tmdb_client.read_token and not tmdb_client.api_key:
            logging.error("Ошибка: API ключи TMDB не найдены в файле .env. Пожалуйста, заполните их.")
            sys.exit(1)

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
                                    executor.shutdown(wait=False)
                                break
                            
                            done, active_tasks = concurrent.futures.wait(active_tasks, timeout=1.0, return_when=concurrent.futures.FIRST_COMPLETED)
                            
                            for future in done:
                                try:
                                    if future.result():
                                        saved_count += 1
                                except Exception as e:
                                    logging.error(f"Ошибка в потоке при обработке фильма/сериала: {e}")
                                
                                pbar.update(1)
                                update_progress("Парсинг TMDB", pbar.n, total_tmdb)
                                
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

        if run_trends:
            if not os.path.exists(flag_path):
                update_progress("Обновление 'Сейчас смотрят'", 0, 100)
                logging.info("Получение списка 'Сейчас смотрят' (фильмы и сериалы)...")
                try:
                    now_playing_m = tmdb_client.get_now_playing_movies()
                    trending_tv = tmdb_client.get_trending_tv_shows()
                    
                    shifted_tv_ids = [tid + 100000000 for tid in trending_tv]
                    all_trending_ids = now_playing_m + shifted_tv_ids
                    
                    local_ids = db.get_existing_ids()
                    missing_ids = [mid for mid in all_trending_ids if mid not in local_ids]
                    
                    if missing_ids:
                        logging.info(f"Докачиваем {len(missing_ids)} недостающих фильмов/сериалов для раздела трендов...")
                        for mid in missing_ids:
                            if mid > 100000000:
                                process_tmdb_tv(mid, db, tmdb_client)
                            else:
                                process_tmdb_movie(mid, db, tmdb_client)
                                
                    db.update_now_playing_list(all_trending_ids)
                    logging.info(f"Раздел 'Сейчас смотрят' обновлен. Всего: {len(all_trending_ids)} элементов.")
                except Exception as e:
                    logging.error(f"Ошибка при обновлении 'Сейчас смотрят': {e}")

        # Инициализация пулов потоков для Producer-Consumer архитектуры
        # Параметры читаются из .env:
        # PRODUCER_MAX_WORKERS — кол-во параллельных потоков (дефолт: 3)
        # NNMCLUB_REQUEST_DELAY_MIN/MAX — диапазон задержки в секундах (дефолт: 1.0–4.0)
        _max_workers = int(os.environ.get("PRODUCER_MAX_WORKERS", 3))
        producers_executor = ThreadPoolExecutor(max_workers=_max_workers)
        consumers_executor = ThreadPoolExecutor(max_workers=1)
        
        producer_futures = []
        
        NNM_TV_FORUMS = [
            # Зарубежные сериалы
            1344, 779, 1288, 787, 1141, 777, 786, 776, 785, 775, 
            1265, 1242, 1140, 782, 773, 1142, 772, 771, 783, 1144, 
            804, 1290, 1300, 784, 774, 922, 770, 780
        ]
        
        # Запуск фонового потребителя (Consumer)
        consumers_executor.submit(consumer_loop, db, tmdb_client, NNM_TV_FORUMS)

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
                        
                        try:
                            topics = rutracker.get_topics_from_forum(forum_id, pages=2)
                        except Exception as e:
                            logging.error(f"Ошибка при получении топиков форума {forum_id}: {e}")
                            time.sleep(2)
                            continue
                        
                        for topic in topics:
                            if os.path.exists(flag_path): break
                            
                            topic_id = topic['topic_id']
                            
                            if db.is_torrent_exists("rutracker", topic_id):
                                db.update_torrent_seeds("rutracker", topic_id, topic['seeds'], topic['leeches'])
                                continue
                            
                            url = f"https://rutracker.org/forum/viewtopic.php?t={topic_id}"
                            with metadata_lock:
                                topic_metadata[url] = {
                                    'tracker': 'rutracker',
                                    'topic_id': topic_id,
                                    'topic_title': topic['title'],
                                    'seeds': topic['seeds'],
                                    'leeches': topic['leeches'],
                                    'cat_id': cat_id
                                }
                            
                            logging.info(f"Добавление задачи в Producers для Rutracker URL: {url}")
                            producer_futures.append(
                                producers_executor.submit(producer_task, url, 'rutracker', rutracker)
                            )
                            
            except Exception as e:
                logging.error(f"Ошибка в главном цикле парсинга Rutracker: {e}")
        else:
            if run_rutracker:
                logging.info("Парсинг Rutracker отменен из-за флага остановки.")
            else:
                logging.info("Парсинг Rutracker отключен или не запрошен в этом режиме.")

        # --- Парсинг NNM-Club ---
        if run_nnmclub and not os.path.exists(flag_path):
            update_progress("Парсинг NNM-Club", 0, 100)
            nnm = NnmclubClient()
            logging.info("Авторизация отключена: парсинг в гостевом режиме.")
            NNM_FORUMS = [
                # Горячие новинки
                218, 954,
                # Классика кино и Старые фильмы до 90-х
                319, 885, 910, 912,
                # Зарубежное кино
                225, 227, 1296, 1299, 682, 884
            ]
            all_nnm_forums = NNM_FORUMS + NNM_TV_FORUMS
            
            for idx, f_id in enumerate(all_nnm_forums):
                if os.path.exists(flag_path): break
                update_progress(f"NNM-Club: Форум {f_id}", idx, len(all_nnm_forums))
                
                try:
                    topics = nnm.get_topics_from_forum(f_id, pages=2)
                except Exception as e:
                    logging.error(f"Ошибка при получении топиков NNM-Club форума {f_id}: {e}")
                    time.sleep(2)
                    continue
                    
                for topic in topics:
                    if os.path.exists(flag_path): break
                    
                    try:
                        topic_id = topic['topic_id']
                        if db.is_torrent_exists("nnmclub", topic_id):
                            db.update_torrent_seeds("nnmclub", topic_id, topic['seeds'], topic['leeches'])
                            continue
                            
                        url = f"https://nnmclub.to/forum/viewtopic.php?t={topic_id}"
                        with metadata_lock:
                            topic_metadata[url] = {
                                'tracker': 'nnmclub',
                                'topic_id': topic_id,
                                'topic_title': topic['title'],
                                'seeds': topic['seeds'],
                                'leeches': topic['leeches'],
                                'cat_id': f_id
                            }
                        
                        logging.info(f"Добавление задачи в Producers для NNM-Club URL: {url}")
                        producer_futures.append(
                            producers_executor.submit(producer_task, url, 'nnmclub', nnm)
                        )
                    except Exception as e:
                        logging.error(f"Ошибка на NNM-Club при обработке топика {topic.get('topic_id', 'Unknown')}: {e}")
        else:
            if run_nnmclub:
                logging.info("Парсинг NNM-Club отменен из-за флага остановки.")
            else:
                logging.info("Парсинг NNM-Club отключен или не запрошен в этом режиме.")

        # Ожидание завершения всех продюсеров
        logging.info("Ожидание завершения работы всех Producers...")
        producers_executor.shutdown(wait=True)
        
        # Ждем, пока Consumer обработает все элементы из очереди
        logging.info("Ожидание завершения обработки очереди Consumer...")
        html_queue.join()
        
        # Останавливаем Consumer
        html_queue.put(None)
        consumers_executor.shutdown(wait=True)
        logging.info("Все потоки Producer-Consumer успешно завершили работу.")

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
