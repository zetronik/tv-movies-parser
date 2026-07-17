import sys
import boto3
import hashlib
from botocore.client import Config
import time
import json
import re
import logging
from logging.handlers import RotatingFileHandler
import zipfile
import os
import argparse
from urllib.parse import urljoin, urlparse

from database import MovieDatabase
from tmdb_client import TMDBClient
from content_cleaner import clean_html_to_markdown
from tracker_client import env_float
from catalog import TV_ID_OFFSET, save_tmdb_movie, save_tmdb_tv

# Импортируем методы для работы с локальной LLM
from llm_parser import (
    check_llm_available,
    discover_categories,
    extract_topic_links,
    extract_torrent_data,
)

DATA_DIR = 'data/'
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Лог накапливается между запусками и ротируется, чтобы не терять историю
        # предыдущих прогонов и при этом не расти бесконечно.
        RotatingFileHandler(
            os.path.join(DATA_DIR, 'parser.log'),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8',
        ),
        logging.StreamHandler(sys.stdout) # Эта строка дублирует вывод в консоль
    ]
)

# Смещение блокируемого байта в файле замка: за пределами записанного PID.
LOCK_BYTE_OFFSET = 1024

def acquire_run_lock():
    """Берет эксклюзивную блокировку на запуск парсера.

    Веб-панель отслеживает только тот процесс, который запустила сама: после ее
    перезапуска кнопка спокойно поднимала второй парсер поверх работающего, и оба
    писали в одну базу. Блокировка файловая, поэтому ОС снимает ее сама, если
    процесс упал — зависших замков не остается.

    Returns:
        Файловый объект блокировки (держать до конца работы) или None, если
        парсер уже запущен.
    """
    lock_path = os.path.join(DATA_DIR, 'parser.lock')
    lock_file = open(lock_path, 'a+')
    try:
        # Блокируем байт далеко за концом текста: на Windows блокировка
        # обязательная, и замок на нулевом байте сделал бы PID нечитаемым.
        lock_file.seek(LOCK_BYTE_OFFSET)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_file.close()
        return None

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file

def release_run_lock(lock_file):
    """Снимает блокировку запуска."""
    if not lock_file:
        return
    try:
        lock_file.seek(LOCK_BYTE_OFFSET)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except OSError as e:
        logging.warning(f"Не удалось снять блокировку запуска: {e}")
    finally:
        lock_file.close()

def load_tracker_client(tracker_name):
    """Возвращает класс клиента трекера или None, если зависимости не установлены.

    Клиенты импортируются отложенно и по одному: отсутствие зависимости одного
    трекера (например, cloudscraper для NNM-Club) не должно ронять весь парсер
    на этапе импорта, до того как настроено логирование.

    Args:
        tracker_name: 'rutracker' или 'nnmclub'.

    Returns:
        Класс клиента либо None, если импорт не удался.
    """
    try:
        if tracker_name == 'rutracker':
            from rutracker_client import RutrackerClient
            return RutrackerClient
        if tracker_name == 'nnmclub':
            from nnmclub_client import NnmclubClient
            return NnmclubClient
        logging.error(f"Неизвестный трекер: {tracker_name}")
    except ImportError as e:
        logging.error(
            f"Клиент '{tracker_name}' недоступен, не установлена зависимость: {e}. "
            f"Выполните: pip install -r requirements.txt"
        )
    return None

# Сколько раз пытаться разобрать раздачу, прежде чем перестать тратить на нее LLM.
MAX_UNMATCHED_ATTEMPTS = 3

# Предохранители обхода. Форум по построению бесконечен, поэтому прогон
# ограничен по числу страниц, глубине вложенности разделов и времени.
# Очередь хранится в БД, так что следующий запуск продолжит с того же места.
MAX_PAGES_PER_RUN = int(env_float('MAX_PAGES_PER_RUN', 200))
MAX_CRAWL_DEPTH = int(env_float('MAX_CRAWL_DEPTH', 5))
MAX_RUNTIME_SECONDS = int(env_float('MAX_RUNTIME_SECONDS', 3600))

# Дневной дамп TMDB содержит около миллиона позиций, и на каждую новую нужен
# отдельный запрос за карточкой. Поэтому за один прогон догружается ограниченная
# порция, начиная с самых свежих id; остальное доберется следующими запусками.
MAX_TMDB_ITEMS_PER_RUN = int(env_float('MAX_TMDB_ITEMS_PER_RUN', 500))
TMDB_REQUEST_DELAY = env_float('TMDB_REQUEST_DELAY', 0.05)

# Множители перевода в гигабайты. Модель возвращает размер как есть, поэтому
# единица измерения может быть любой и на любом языке.
SIZE_UNITS_GB = {
    'kb': 1 / 1048576, 'kib': 1 / 1048576, 'кб': 1 / 1048576,
    'mb': 1 / 1024, 'mib': 1 / 1024, 'мб': 1 / 1024,
    'gb': 1.0, 'gib': 1.0, 'гб': 1.0,
    'tb': 1024.0, 'tib': 1024.0, 'тб': 1024.0,
}

def parse_size_gb(raw_size):
    """Переводит размер раздачи в гигабайты.

    Раньше из строки просто выдиралось первое число, из-за чего "1500 MB"
    превращалось в 1500 гигабайт.

    Args:
        raw_size: Размер в свободной форме, например "1.5 GB", "700 МБ", "2,3 TB".

    Returns:
        Размер в гигабайтах; 0.0, если распознать не удалось.

    Example:
        >>> parse_size_gb("1500 MB")
        1.46484375
    """
    if raw_size is None:
        return 0.0
    if isinstance(raw_size, (int, float)):
        return float(raw_size)

    match = re.search(r'(\d+(?:[.,]\d+)?)\s*([A-Za-zА-Яа-я]*)', str(raw_size))
    if not match:
        return 0.0

    try:
        value = float(match.group(1).replace(',', '.'))
    except ValueError:
        return 0.0

    unit = match.group(2).lower()
    if not unit:
        # Без единицы измерения считаем, что это уже гигабайты.
        return value
    if unit not in SIZE_UNITS_GB:
        logging.warning(f"Неизвестная единица размера '{match.group(2)}' в '{raw_size}'.")
        return value
    return value * SIZE_UNITS_GB[unit]

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
        return {"run_rutracker": True, "run_nnmclub": True, "cron_time": "02:00"}

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

def create_zip(db_name="movies.db", db=None):
    if not db_name.startswith(DATA_DIR):
        db_name = os.path.join(DATA_DIR, os.path.basename(db_name))
    update_progress("Сжатие базы данных", 99, 100)

    # В режиме WAL свежие транзакции лежат в отдельном файле -wal, поэтому без
    # чекпоинта в архив попала бы база без последних изменений.
    if db is not None:
        db.checkpoint()

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

def run_tmdb_catalog_update(db, tmdb_client, flag_path):
    """Догружает в каталог карточки, которых еще нет, по дневным дампам TMDB.

    TMDB публикует ежедневные выгрузки всех идентификаторов. Сравниваем их с
    тем, что уже лежит в базе, и добираем недостающее — начиная с самых больших
    id, то есть с самых новых поступлений.

    Returns:
        Количество сохраненных карточек.
    """
    logging.info("--- Обновление каталога TMDB ---")
    update_progress("TMDB: загрузка списка id", 0, 100)

    existing = db.get_existing_ids()
    logging.info(f"В каталоге сейчас карточек: {len(existing)}")

    pending = []
    try:
        movie_ids = tmdb_client.download_daily_movie_ids()
        pending += [(mid, 'movie') for mid in movie_ids if mid not in existing]
        logging.info(f"В дампе фильмов TMDB: {len(movie_ids)}")
    except Exception as e:
        logging.error(f"Не удалось скачать дамп фильмов TMDB: {e}")

    if os.path.exists(flag_path):
        return 0

    try:
        tv_ids = tmdb_client.download_daily_tv_ids()
        pending += [(tid + TV_ID_OFFSET, 'tv') for tid in tv_ids
                    if tid + TV_ID_OFFSET not in existing]
        logging.info(f"В дампе сериалов TMDB: {len(tv_ids)}")
    except Exception as e:
        logging.error(f"Не удалось скачать дамп сериалов TMDB: {e}")

    if not pending:
        logging.info("Каталог уже содержит все позиции из дампов TMDB.")
        return 0

    # Самые свежие поступления имеют наибольшие id.
    pending.sort(reverse=True)
    total_new = len(pending)
    batch = pending[:MAX_TMDB_ITEMS_PER_RUN]
    logging.info(
        f"Отсутствует карточек: {total_new}. За этот прогон загрузим {len(batch)}."
    )

    saved = 0
    for number, (item_id, kind) in enumerate(batch, start=1):
        if os.path.exists(flag_path):
            logging.info("TMDB: получен сигнал остановки.")
            break

        if kind == 'tv':
            ok = save_tmdb_tv(item_id, db, tmdb_client)
        else:
            ok = save_tmdb_movie(item_id, db, tmdb_client)
        if ok:
            saved += 1

        if number % 20 == 0 or number == len(batch):
            update_progress("TMDB: загрузка карточек", number, len(batch))
            logging.info(f"TMDB: обработано {number} из {len(batch)}, сохранено {saved}.")

        time.sleep(TMDB_REQUEST_DELAY)

    remaining = total_new - saved
    logging.info(
        f"TMDB: сохранено {saved} карточек. Осталось загрузить примерно {remaining}."
    )
    return saved


def run_trends_update(db, tmdb_client, flag_path):
    """Обновляет подборку «сейчас смотрят»: премьеры кино и популярные сериалы.

    Это единственное, что наполняет таблицу now_playing — до сих пор она всегда
    оставалась пустой, хотя счетчик и страница в панели существуют.

    Returns:
        Количество сохраненных карточек.
    """
    logging.info("--- Обновление подборки now_playing ---")
    update_progress("TMDB: премьеры и тренды", 0, 100)

    saved = 0
    featured = []

    try:
        movie_ids = tmdb_client.get_now_playing_movies()
        logging.info(f"Премьер в прокате: {len(movie_ids)}")
        for movie_id in movie_ids:
            if os.path.exists(flag_path):
                break
            if save_tmdb_movie(movie_id, db, tmdb_client):
                featured.append(movie_id)
                saved += 1
            time.sleep(TMDB_REQUEST_DELAY)
    except Exception as e:
        logging.error(f"Не удалось получить премьеры TMDB: {e}")

    try:
        tv_ids = tmdb_client.get_trending_tv_shows()
        logging.info(f"Популярных сериалов: {len(tv_ids)}")
        for tv_id in tv_ids:
            if os.path.exists(flag_path):
                break
            shifted = tv_id + TV_ID_OFFSET
            if save_tmdb_tv(shifted, db, tmdb_client):
                featured.append(shifted)
                saved += 1
            time.sleep(TMDB_REQUEST_DELAY)
    except Exception as e:
        logging.error(f"Не удалось получить тренды сериалов TMDB: {e}")

    if featured:
        db.update_now_playing_list(featured)
        logging.info(f"Подборка now_playing обновлена: {len(featured)} позиций.")
    else:
        logging.warning("Подборка now_playing не обновлена: получить нечего.")

    return saved


def run_tracker_pipeline(tracker_name, start_url, client, db, tmdb_client, flag_path):
    """
    Основной конечный автомат для парсинга трекера через локальную LLM с поддержкой подразделов.

    Returns:
        Количество раздач, добавленных в базу за прогон.
    """
    logging.info(f"--- Запуск LLM-парсинга: {tracker_name} ---")
    inserted_count = 0
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
            return inserted_count
    else:
        movies_url = topology.get('movies_url')
        logging.info(f"[{tracker_name}] Раздел фильмов загружен из БД: {movies_url}")

    if not movies_url:
        logging.error(f"[{tracker_name}] URL раздела фильмов не определен. Парсинг остановлен.")
        return inserted_count

    # Корневой раздел тоже приходит от модели. Форму URL здесь только проверяем
    # предупреждением: раздел мог называться иначе, а жесткий отказ остановил бы
    # весь обход. А вот уход на чужой домен недопустим в любом случае.
    root_movies_url = client.resolve_url(movies_url, start_url, 'forum')
    if not root_movies_url:
        root_movies_url = urljoin(start_url, movies_url)
        if urlparse(root_movies_url).netloc.lower() != urlparse(client.base_domain).netloc.lower():
            logging.error(
                f"[{tracker_name}] Раздел фильмов ведет на чужой домен: {root_movies_url}. Обход отменен."
            )
            return inserted_count
        logging.warning(
            f"[{tracker_name}] Раздел фильмов не похож на страницу форума: {root_movies_url}. Пробуем как есть."
        )

    if db.count_frontier(tracker_name, 'pending') == 0:
        restored = db.restart_frontier(tracker_name)
        if restored:
            logging.info(f"[{tracker_name}] Новый проход: возвращено в очередь страниц — {restored}.")
    db.enqueue_page(tracker_name, root_movies_url, depth=0)

    started_at = time.monotonic()
    pages_processed = 0

    # СТАДИЯ Б: Навигатор (Проход по очереди страниц)
    while True:
        if os.path.exists(flag_path):
            logging.info(f"[{tracker_name}] Получен сигнал остановки.")
            break

        if pages_processed >= MAX_PAGES_PER_RUN:
            logging.info(
                f"[{tracker_name}] Достигнут лимит страниц за прогон ({MAX_PAGES_PER_RUN}). "
                f"Очередь сохранена, обход продолжится в следующий раз."
            )
            break

        elapsed = time.monotonic() - started_at
        if elapsed > MAX_RUNTIME_SECONDS:
            logging.info(
                f"[{tracker_name}] Достигнут лимит времени ({MAX_RUNTIME_SECONDS} с). "
                f"Очередь сохранена."
            )
            break

        page = db.next_pending_page(tracker_name)
        if not page:
            logging.info(f"[{tracker_name}] Очередь обхода пуста.")
            break

        current_page, depth = page
        pages_processed += 1

        done_count = db.count_frontier(tracker_name, 'done')
        pending_count = db.count_frontier(tracker_name, 'pending')
        update_progress(f"Обход {tracker_name}", done_count, done_count + pending_count)
        logging.info(
            f"[{tracker_name}] Навигация. Глубина: {depth} | В очереди: {pending_count} | "
            f"Обработка: {current_page}"
        )

        try:
            html = client.fetch_page(current_page)
            md_text = clean_html_to_markdown(html)

            page_data = extract_topic_links(md_text)
            movie_links = page_data.get('movie_links') or []
            subforum_links = page_data.get('subforum_links') or []
            next_page = page_data.get('next_page')
        except Exception as e:
            logging.error(f"[{tracker_name}] Ошибка на стадии Навигации ({current_page}): {e}")
            db.mark_page(tracker_name, current_page, 'failed')
            continue

        # Ссылки от модели проверяются на домен и форму: она может выдумать URL
        # или отнести раздачу к подразделам.
        queued_subforums = 0
        if depth < MAX_CRAWL_DEPTH:
            for sf_link in subforum_links:
                resolved = client.resolve_url(sf_link, current_page, 'forum')
                if resolved and db.enqueue_page(tracker_name, resolved, depth + 1):
                    queued_subforums += 1
        elif subforum_links:
            logging.info(f"[{tracker_name}] Глубина {depth} — подразделы дальше не раскрываем.")

        # Пагинация остается на той же глубине: это продолжение того же раздела.
        if next_page:
            resolved_next = client.resolve_url(next_page, current_page, 'forum')
            if resolved_next:
                db.enqueue_page(tracker_name, resolved_next, depth)

        valid_topics = []
        for link in movie_links:
            resolved = client.resolve_url(link, current_page, 'topic')
            if resolved:
                valid_topics.append(resolved)

        rejected = len(movie_links) - len(valid_topics)
        logging.info(
            f"[{tracker_name}] Найдено: {len(subforum_links)} подразделов "
            f"(в очередь {queued_subforums}), {len(valid_topics)} раздач"
            + (f", отброшено ссылок: {rejected}" if rejected else "")
        )

        # СТАДИЯ В: Экстрактор (Сбор данных с конкретной раздачи)
        for topic_url in valid_topics:
            if os.path.exists(flag_path):
                break
            if process_topic(tracker_name, topic_url, client, db, tmdb_client):
                inserted_count += 1

        db.mark_page(tracker_name, current_page, 'done')

    logging.info(
        f"[{tracker_name}] Обработано страниц: {pages_processed}, добавлено раздач: {inserted_count}."
    )
    return inserted_count

def process_topic(tracker_name, topic_url, client, db, tmdb_client):
    """Разбирает страницу раздачи и привязывает ее к фильму.

    Args:
        tracker_name: Имя трекера.
        topic_url: Абсолютная ссылка на страницу раздачи.
        client: Клиент трекера.
        db: Экземпляр MovieDatabase.
        tmdb_client: Клиент TMDB для поиска отсутствующих фильмов.

    Returns:
        True, если раздача добавлена в базу.
    """
    topic_id = client.extract_topic_id(topic_url)
    if not topic_id:
        return False

    if db.is_torrent_exists(tracker_name, topic_id):
        logging.info(f"  -> Пропуск: Раздача {topic_id} уже в базе.")
        return False

    # Раздачи, которые уже несколько раз не поддались разбору, не стоят
    # очередного похода в LLM — они лежат в unmatched_torrents для разбора.
    attempts = db.get_unmatched_attempts(tracker_name, topic_id)
    if attempts >= MAX_UNMATCHED_ATTEMPTS:
        logging.info(f"  -> Пропуск: Раздача {topic_id} не разобрана за {attempts} попыток.")
        return False

    logging.info(f"  -> Анализ раздачи: {topic_url}")
    try:
        topic_html = client.fetch_page(topic_url)
        topic_md = clean_html_to_markdown(topic_html)

        extracted = extract_torrent_data(topic_md)
        if not extracted:
            logging.warning(f"  [!] LLM не смогла извлечь данные.")
            db.save_unmatched(tracker_name, topic_id, topic_url, 'llm_empty')
            return False

        ru_title = extracted.get('title') or extracted.get('ru_title', '')
        orig_title = extracted.get('original_title') or extracted.get('orig_title', '')
        year_raw = extracted.get('year', '')
        year = str(year_raw) if year_raw else ''
        size_val = parse_size_gb(extracted.get('size'))
        quality = extracted.get('video_quality') or extracted.get('quality', '')
        magnet_link = extracted.get('magnet_link', '')
        raw_json = json.dumps(extracted, ensure_ascii=False)

        if not ru_title:
            logging.warning(f"  [!] В ответе LLM нет названия раздачи.")
            db.save_unmatched(
                tracker_name, topic_id, topic_url, 'no_title',
                original_title=orig_title, year=year, size_gb=size_val,
                quality=quality, magnet_link=magnet_link, raw_json=raw_json
            )
            return False

        movie_id = db.find_movie_by_title_and_year(ru_title, orig_title, year)

        if not movie_id:
            search_title = orig_title if orig_title else ru_title
            if search_title:
                tmdb_id = tmdb_client.search_movie(search_title, year)
                if tmdb_id:
                    if save_tmdb_movie(tmdb_id, db, tmdb_client):
                        movie_id = tmdb_id

        if movie_id:
            db.insert_torrent(
                tracker=tracker_name,
                topic_id=topic_id,
                movie_id=movie_id,
                topic_title=ru_title,
                size_gb=size_val,
                quality=quality,
                file_format='',
                translation='',
                magnet_link=magnet_link,
                seeds=0,
                leeches=0
            )
            # Раздача могла лежать в очереди непривязанных с прошлых прогонов.
            if attempts:
                db.delete_unmatched(tracker_name, topic_id)
            logging.info(f"  [+] Добавлено: {ru_title} -> ID фильма: {movie_id}")
            return True

        logging.warning(f"  [-] Не удалось привязать к фильму: {ru_title} ({year})")
        db.save_unmatched(
            tracker_name, topic_id, topic_url, 'no_tmdb_match',
            ru_title=ru_title, original_title=orig_title, year=year,
            size_gb=size_val, quality=quality, magnet_link=magnet_link,
            raw_json=raw_json
        )
        return False

    except Exception as e:
        logging.error(f"  [!] Ошибка при извлечении раздачи {topic_url}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Movies Parser")
    parser.add_argument(
        '--mode',
        choices=['tmdb', 'rutracker', 'nnmclub', 'cron', 'trends', 'publish'],
        required=True,
        help='Режим работы парсера. publish — только упаковка базы и выгрузка в облако.'
    )
    args = parser.parse_args()

    config = get_config()
    update_progress("Инициализация", 0, 100)
    logging.info(f"--- Запуск парсера фильмов (Режим: {args.mode}) ---")

    run_lock = acquire_run_lock()
    if not run_lock:
        logging.error("Парсер уже запущен (блокировка data/parser.lock). Запуск отменен.")
        sys.exit(3)

    flag_path = os.path.join(DATA_DIR, 'stop.flag')
    if os.path.exists(flag_path):
        try:
            os.remove(flag_path)
        except OSError:
            pass

    # Режим publish ничего не парсит: он нужен, чтобы выгрузить базу после
    # ручных правок из веб-панели, не запуская обход трекеров.
    if args.mode == 'publish':
        try:
            db = MovieDatabase()
            logging.info("Публикация базы по запросу.")
            create_zip(db.db_name, db)
        except Exception as e:
            logging.error(f"Ошибка при публикации базы: {e}")
            sys.exit(1)
        finally:
            release_run_lock(run_lock)
            logging.info("--- Работа скрипта завершена ---")
            update_progress("Ожидание", 0, 0)
        return

    run_rutracker = args.mode == 'rutracker' or (args.mode == 'cron' and config.get("run_rutracker", True))
    run_nnmclub = args.mode == 'nnmclub' or (args.mode == 'cron' and config.get("run_nnmclub", True))
    # Оба режима ходят в TMDB, поэтому в ночном прогоне участвуют только когда
    # включены явно: молча начать сетевую работу за пользователя неправильно.
    run_tmdb = args.mode == 'tmdb' or (args.mode == 'cron' and config.get("run_tmdb", False))
    run_trends = args.mode == 'trends' or (args.mode == 'cron' and config.get("run_trends", False))

    # Проверки, не требующие БД, выполняются до блока try: при неудаче нет смысла
    # заходить в finally и упаковывать базу впустую.
    tmdb_client = TMDBClient()
    if not tmdb_client.read_token and not tmdb_client.api_key:
        logging.error("Ошибка: API ключи TMDB не найдены в файле .env.")
        sys.exit(1)

    if (run_rutracker or run_nnmclub) and not check_llm_available():
        logging.error("Парсинг трекеров невозможен без локальной LLM. Запуск отменен.")
        sys.exit(1)

    db = None
    total_inserted = 0
    try:
        try:
            db = MovieDatabase()
        except Exception as e:
            logging.error(f"Ошибка при инициализации БД: {e}")
            sys.exit(1)

        # 1. Обновление каталога из TMDB
        if run_tmdb and not os.path.exists(flag_path):
            total_inserted += run_tmdb_catalog_update(db, tmdb_client, flag_path)

        # 2. Премьеры и популярные сериалы
        if run_trends and not os.path.exists(flag_path):
            total_inserted += run_trends_update(db, tmdb_client, flag_path)

        # 3. Запуск пайплайна Rutracker
        if run_rutracker and not os.path.exists(flag_path):
            update_progress("Парсинг Rutracker", 0, 100)
            rutracker_cls = load_tracker_client('rutracker')
            if rutracker_cls:
                rutracker = rutracker_cls()
                try:
                    authorized = rutracker.login()
                except Exception as e:
                    logging.error(f"Ошибка авторизации на Rutracker: {e}")
                    authorized = False
                if authorized:
                    total_inserted += run_tracker_pipeline('rutracker', f"{rutracker.base_domain}/forum/index.php", rutracker, db, tmdb_client, flag_path)
                else:
                    logging.error("Не удалось авторизоваться на Rutracker.")

        # 4. Запуск пайплайна NNM-Club
        if run_nnmclub and not os.path.exists(flag_path):
            update_progress("Парсинг NNM-Club", 0, 100)
            nnm_cls = load_tracker_client('nnmclub')
            if nnm_cls:
                nnm = nnm_cls()
                total_inserted += run_tracker_pipeline('nnmclub', f"{nnm.base_domain}/forum/index.php", nnm, db, tmdb_client, flag_path)

    finally:
        # Архив весит сотни мегабайт и уезжает в облако — упаковываем только если
        # база действительно изменилась либо архива еще нет.
        archive_exists = os.path.exists(os.path.join(DATA_DIR, "movies.zip"))
        if total_inserted > 0 or not archive_exists:
            logging.info(f"Изменений за прогон: {total_inserted}. Упаковка базы.")
            create_zip(db.db_name if db else "movies.db", db)
        else:
            logging.info("Изменений нет — архивация и выгрузка в облако пропущены.")
        if os.path.exists(flag_path):
            try:
                os.remove(flag_path)
            except OSError:
                pass
        release_run_lock(run_lock)
        logging.info("--- Работа скрипта завершена ---")
        update_progress("Ожидание", 0, 0)

if __name__ == "__main__":
    main()