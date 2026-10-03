from flask import Flask, render_template, request, jsonify, abort, send_file, Response
import sqlite3
import hmac
import logging
import math
import os
import re
import sys
import json
import subprocess
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from catalog import save_tmdb_candidate
from database import MovieDatabase
from tmdb_client import TMDBClient
from tracker_client import DEFAULT_SEARCH_LIMIT, env_int

load_dotenv()

app = Flask(__name__)

# Панель умеет запускать процессы и останавливать сервер, поэтому без пароля
# пускаем только с самой машины. Архив базы отдается без пароля: его же раздает
# публичный бакет R2, и на него могут ходить клиентские приложения.
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
PUBLIC_ENDPOINTS = {'download_db'}
LOCAL_ADDRESSES = {'127.0.0.1', '::1', 'localhost'}

if not ADMIN_PASSWORD:
    logging.warning(
        "ADMIN_PASSWORD не задан: панель доступна только с localhost. "
        "Задайте пароль в .env, если открываете порт наружу."
    )


def _password_matches(supplied):
    """Сравнивает пароль за постоянное время, чтобы не подсказывать его перебором."""
    return hmac.compare_digest(supplied or '', ADMIN_PASSWORD or '')


@app.before_request
def require_auth():
    """Закрывает панель паролем; без пароля пускает только локальные запросы."""
    if request.endpoint in PUBLIC_ENDPOINTS or request.endpoint == 'static':
        return None

    if not ADMIN_PASSWORD:
        if request.remote_addr in LOCAL_ADDRESSES:
            return None
        logging.warning(f"Отклонен внешний запрос с {request.remote_addr}: пароль не задан.")
        return abort(403)

    auth = request.authorization
    if auth and auth.username == ADMIN_USER and _password_matches(auth.password):
        return None

    return Response(
        'Требуется авторизация.', 401,
        {'WWW-Authenticate': 'Basic realm="Movies Parser"'}
    )


DATA_DIR = 'data/'
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, "movies.db")

# Глобальное состояние для процесса
parser_process = None
# Клиенты трекеров для страницы поиска; создаются при первом обращении.
tracker_clients = None
# Причины, по которым какой-то трекер не поднялся, — показываем их в панели.
tracker_errors = []
# APScheduler на уровне INFO рапортует о старте и о каждой постановке задачи.
# В окне логов на дашборде это вытесняет полезные строки парсера.
logging.getLogger('apscheduler').setLevel(logging.WARNING)

scheduler = BackgroundScheduler()
scheduler.start()

def get_parser_config():
    try:
        with open(os.path.join(DATA_DIR, 'parser_config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"run_rutracker": True, "run_nnmclub": True, "cron_time": "02:00"}

def save_parser_config(data):
    with open(os.path.join(DATA_DIR, 'parser_config.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f)
        
def start_parser_task(mode):
    global parser_process
    flag_path = os.path.join(DATA_DIR, 'stop.flag')
    if parser_process is None or parser_process.poll() is not None:
        if os.path.exists(flag_path):
            try: os.remove(flag_path)
            except OSError: pass
        # Процесс не запущен или уже завершился.
        # sys.executable, а не "python": в venv и в контейнере из PATH может
        # взяться другой интерпретатор, без установленных зависимостей.
        parser_process = subprocess.Popen([sys.executable, "main.py", "--mode", mode])

def update_cron_job(cron_time_str):
    try:
        hour, minute = map(int, cron_time_str.split(':'))
        scheduler.reschedule_job('parser_job', trigger=CronTrigger(hour=hour, minute=minute))
    except Exception as e:
        print(f"Error updating cron: {e}")

# Добавляем задачу при старте
initial_config = get_parser_config()
h, m = map(int, initial_config.get("cron_time", "02:00").split(':'))
scheduler.add_job(id='parser_job', func=start_parser_task, args=['cron'], trigger=CronTrigger(hour=h, minute=m))

def make_searchable(text):
    if not text:
        return ""
    return str(text).lower().replace('ё', 'е')

def get_db_connection():
    # timeout нужен, потому что панель читает базу, пока парсер (отдельный процесс)
    # в нее пишет. Без него любое пересечение дает "database is locked".
    conn = sqlite3.connect(DB_NAME, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.create_function("searchable", 1, make_searchable)
    return conn

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        # Статистика: фильмы
        movies_count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        # Статистика: раздачи
        torrents_count = conn.execute("SELECT COUNT(*) FROM torrents").fetchone()[0]
        # Фильмы без раздач
        movies_without_torrents = conn.execute("""
            SELECT COUNT(*) FROM movies 
            WHERE id NOT IN (SELECT DISTINCT movie_id FROM torrents)
        """).fetchone()[0]
        now_playing_count = conn.execute("SELECT COUNT(*) FROM now_playing").fetchone()[0]
        try:
            unmatched_count = conn.execute("SELECT COUNT(*) FROM unmatched_torrents").fetchone()[0]
        except sqlite3.OperationalError:
            # Таблица появляется при первом запуске парсера после обновления.
            unmatched_count = 0
        conn.close()
    except sqlite3.OperationalError:
        movies_count, torrents_count, movies_without_torrents = 0, 0, 0
        now_playing_count, unmatched_count = 0, 0

    return render_template(
        'index.html',
        movies_count=movies_count,
        torrents_count=torrents_count,
        movies_without_torrents=movies_without_torrents,
        now_playing_count=now_playing_count,
        unmatched_count=unmatched_count
    )

@app.route('/movies')
def movies():
    search_query = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page

    conn = get_db_connection()
    
    if search_query:
        # Поиск по названию ИЛИ оригинальному названию
        query_sql = """
            SELECT * FROM movies 
            WHERE searchable(title) LIKE ? OR searchable(original_title) LIKE ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """
        count_sql = "SELECT COUNT(*) FROM movies WHERE searchable(title) LIKE ? OR searchable(original_title) LIKE ?"
        like_term = f"%{make_searchable(search_query)}%"
        
        movies_list = conn.execute(query_sql, (like_term, like_term, per_page, offset)).fetchall()
        total_movies = conn.execute(count_sql, (like_term, like_term)).fetchone()[0]
    else:
        query_sql = "SELECT * FROM movies ORDER BY id DESC LIMIT ? OFFSET ?"
        count_sql = "SELECT COUNT(*) FROM movies"
        movies_list = conn.execute(query_sql, (per_page, offset)).fetchall()
        total_movies = conn.execute(count_sql).fetchone()[0]

    conn.close()

    total_pages = math.ceil(total_movies / per_page)
    
    return render_template(
        'movies.html', 
        movies=movies_list, 
        search_query=search_query, 
        page=page, 
        total_pages=total_pages
    )

@app.route('/movie/<int:movie_id>')
def movie_detail(movie_id):
    conn = get_db_connection()
    movie = conn.execute("SELECT * FROM movies WHERE id = ?", (movie_id,)).fetchone()
    
    if movie is None:
        conn.close()
        abort(404)
        
    torrents = conn.execute("SELECT * FROM torrents WHERE movie_id = ? ORDER BY size_gb DESC", (movie_id,)).fetchall()
    conn.close()

    # dict, а не sqlite3.Row: карточка уходит в шаблон еще и через tojson,
    # чтобы виджет подбора раздач знал, для какого фильма искать.
    return render_template('movie_detail.html', movie=dict(movie), torrents=torrents)

@app.route('/now_playing')
def now_playing():
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page

    conn = get_db_connection()
    
    query_sql = """
        SELECT m.*, np.added_at 
        FROM now_playing np 
        JOIN movies m ON np.movie_id = m.id 
        ORDER BY np.added_at DESC 
        LIMIT ? OFFSET ?
    """
    count_sql = "SELECT COUNT(*) FROM now_playing"
    
    try:
        items_list = conn.execute(query_sql, (per_page, offset)).fetchall()
        total_items = conn.execute(count_sql).fetchone()[0]
    except sqlite3.OperationalError:
        items_list = []
        total_items = 0

    conn.close()

    total_pages = math.ceil(total_items / per_page) if total_items > 0 else 0
    
    return render_template(
        'now_playing.html', 
        items=items_list, 
        page=page, 
        total_pages=total_pages
    )

@app.route('/movies.zip')
def download_db():
    try:
        return send_file(os.path.join(DATA_DIR, 'movies.zip'), as_attachment=True)
    except FileNotFoundError:
        return abort(404)

@app.route('/search')
def search_page():
    return render_template('search.html')


def _get_tracker_clients():
    """Лениво создает и переиспользует клиентов трекеров.

    Клиенты живут между запросами: проверка доступности Rutracker стоит
    запроса, а сессия внутри клиента сама переустанавливается, когда истекает.
    """
    global tracker_clients
    if tracker_clients is not None:
        return tracker_clients

    global tracker_errors
    clients, problems = [], []

    try:
        from rutracker_client import RutrackerClient
        rutracker = RutrackerClient()
        if rutracker.login():
            clients.append(rutracker)
        else:
            problems.append(
                "Rutracker: сайт недоступен (подробности в логе). Скорее всего защита "
                "Cloudflare — задайте RUTRACKER_COOKIES и RUTRACKER_USER_AGENT в .env."
            )
    except Exception as e:
        logging.error(f"Поиск: клиент Rutracker недоступен: {e}")
        problems.append(f"Rutracker: {e}")

    try:
        from nnmclub_client import NnmclubClient
        clients.append(NnmclubClient())
    except Exception as e:
        logging.error(f"Поиск: клиент NNM-Club недоступен: {e}")
        problems.append(f"NNM-Club: {e}")

    tracker_clients, tracker_errors = clients, problems
    return clients


def _parser_is_running():
    """Идет ли сейчас фоновый обход: во время него в трекеры лучше не ходить."""
    return parser_process is not None and parser_process.poll() is None


# "Мятеж 2026", "Мятеж (2026)", "Мятеж [2026]" — год в конце запроса.
YEAR_IN_QUERY_RE = re.compile(r'^(?P<title>.+?)[\s(\[]+(?P<year>(?:19|20)\d{2})[)\]\s]*$')


def split_query_year(query):
    """Отделяет год в конце запроса от названия.

    Args:
        query: Строка из поля ввода.

    Returns:
        Кортеж (название, год) — год строкой или None, если его нет.
    """
    match = YEAR_IN_QUERY_RE.match(query)
    if not match:
        return query, None
    return match.group('title').strip(), match.group('year')


@app.route('/api/search/catalog', methods=['POST'])
def api_search_catalog():
    """Шаг 1: ищет карточку в локальном каталоге, при промахе — в TMDB."""
    payload = request.json or {}
    query = payload.get('query', '').strip()
    # Оператор может принудительно спросить TMDB, если в базе нашлось не то:
    # локальный каталог содержит миллионы карточек, и по общему слову вроде
    # "Мятеж" он всегда что-нибудь возвращает, раньше намертво закрывая
    # дорогу к TMDB.
    force_tmdb = bool(payload.get('force_tmdb'))
    if not query:
        return jsonify({"error": "Пустой запрос"}), 400

    title, year = split_query_year(query)

    if not force_tmdb:
        db = MovieDatabase(db_name=DB_NAME)
        local = db.find_movies_by_title(title, year=year)
        if local:
            return jsonify({"source": "database", "candidates": local})

    tmdb_client = TMDBClient()
    if not tmdb_client.read_token and not tmdb_client.api_key:
        return jsonify({"error": "Ключи TMDB не заданы в .env"}), 500

    candidates = tmdb_client.search_candidates(title, year=year)
    if not candidates and year:
        # Год мог относиться к релизу на трекере, а не к дате TMDB.
        candidates = tmdb_client.search_candidates(title)
    return jsonify({"source": "tmdb", "candidates": candidates})


@app.route('/api/search/save_candidate', methods=['POST'])
def api_save_candidate():
    """Сохраняет выбранного кандидата TMDB в каталог и возвращает карточку."""
    candidate = (request.json or {}).get('candidate')
    if not candidate or 'id' not in candidate:
        return jsonify({"error": "Кандидат не передан"}), 400

    db = MovieDatabase(db_name=DB_NAME)
    movie_id = save_tmdb_candidate(candidate, db, TMDBClient())
    if not movie_id:
        return jsonify({"error": "Не удалось получить карточку из TMDB"}), 502

    return jsonify({"movie": db.get_movie(movie_id)})


@app.route('/api/search/torrents', methods=['POST'])
def api_search_torrents():
    """Шаг 2: ищет раздачи на трекерах по названию выбранной карточки."""
    payload = request.json or {}
    movie_id = payload.get('movie_id')
    query = (payload.get('query') or '').strip()

    if not query:
        return jsonify({"error": "Пустой поисковый запрос"}), 400

    if _parser_is_running():
        return jsonify({
            "error": "Идет фоновый обход трекеров. Остановите его, чтобы не удваивать нагрузку на трекер."
        }), 409

    db = MovieDatabase(db_name=DB_NAME)
    known = {(t['tracker'], t['topic_id']) for t in db.get_torrents_for_movie(movie_id)} if movie_id else set()

    clients = _get_tracker_clients()
    results, errors = [], list(tracker_errors)

    if not clients:
        return jsonify({"error": "Ни один трекер недоступен. " + " ".join(errors)}), 503

    limit = env_int('SEARCH_RESULT_LIMIT', DEFAULT_SEARCH_LIMIT)
    for client in clients:
        try:
            found = client.search(query, limit)
            for item in found:
                item['already_linked'] = (item['tracker'], item['topic_id']) in known
                results.append(item)
            # Выдача уперлась в потолок — раздач на трекере больше, чем показано.
            if len(found) >= limit:
                errors.append(
                    f"{client.name}: показаны первые {limit} раздач, уточните запрос."
                )
        except Exception as e:
            logging.error(f"Поиск на {client.name} не удался: {e}")
            errors.append(f"{client.name}: {e}")

    results.sort(key=lambda r: (r.get('seeds') or 0), reverse=True)
    return jsonify({"results": results, "errors": errors})


@app.route('/api/search/attach', methods=['POST'])
def api_attach_torrents():
    """Шаг 3: привязывает отмеченные раздачи к карточке фильма."""
    payload = request.json or {}
    movie_id = payload.get('movie_id')
    selected = payload.get('items') or []

    if not movie_id:
        return jsonify({"error": "Не выбран фильм"}), 400
    if not selected:
        return jsonify({"error": "Не отмечено ни одной раздачи"}), 400

    db = MovieDatabase(db_name=DB_NAME)
    if not db.get_movie(movie_id):
        return jsonify({"error": "Карточка не найдена в каталоге"}), 404

    clients = {client.name: client for client in _get_tracker_clients()}
    added, skipped = 0, []

    for item in selected:
        tracker = item.get('tracker')
        topic_id = item.get('topic_id')
        if not tracker or not topic_id:
            continue
        if db.is_torrent_exists(tracker, topic_id):
            skipped.append(f"{tracker}#{topic_id} уже в базе")
            continue

        # Magnet есть только на странице раздачи, поэтому качаем ее лишь для
        # подтвержденных вручную позиций.
        magnet = ''
        client = clients.get(tracker)
        if client and item.get('url'):
            magnet = client.fetch_magnet(item['url'])

        db.insert_torrent(
            tracker=tracker,
            topic_id=int(topic_id),
            movie_id=int(movie_id),
            topic_title=item.get('title', ''),
            size_gb=float(item.get('size_gb') or 0),
            quality='',
            file_format='',
            translation='',
            magnet_link=magnet,
            seeds=int(item.get('seeds') or 0),
            leeches=int(item.get('leeches') or 0),
        )
        db.delete_unmatched(tracker, int(topic_id))
        added += 1

    logging.info(f"Ручная привязка: добавлено {added} раздач к фильму {movie_id}.")
    return jsonify({
        "added": added,
        "skipped": skipped,
        "torrents": db.get_torrents_for_movie(movie_id),
    })


@app.route('/api/publish', methods=['POST'])
def api_publish():
    """Запускает упаковку базы и выгрузку в Cloudflare R2 фоновым процессом."""
    if _parser_is_running():
        return jsonify({"error": "Парсер уже занят, дождитесь завершения"}), 409
    start_parser_task('publish')
    return jsonify({"status": "started"})


@app.route('/api/status')
def api_status():
    global parser_process
    
    is_running = parser_process is not None and parser_process.poll() is None
    is_stopping = os.path.exists(os.path.join(DATA_DIR, 'stop.flag'))
    status = {'task': 'Idle', 'current': 0, 'total': 0, 'logs': [], 'is_running': is_running, 'is_stopping': is_stopping}
    
    # Считывание статистики БД
    try:
        conn = get_db_connection()
        status['movies_count'] = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        status['torrents_count'] = conn.execute("SELECT COUNT(*) FROM torrents").fetchone()[0]
        status['movies_without_torrents'] = conn.execute("SELECT COUNT(*) FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM torrents)").fetchone()[0]
        status['now_playing_count'] = conn.execute("SELECT COUNT(*) FROM now_playing").fetchone()[0]
        try:
            status['unmatched_count'] = conn.execute("SELECT COUNT(*) FROM unmatched_torrents").fetchone()[0]
        except sqlite3.OperationalError:
            # Таблица появляется при первом запуске парсера после обновления.
            status['unmatched_count'] = 0
        conn.close()
    except:
        status['movies_count'] = 0
        status['torrents_count'] = 0
        status['movies_without_torrents'] = 0
        status['now_playing_count'] = 0
        status['unmatched_count'] = 0
        
    # Считывание прогресса
    try:
        progress_path = os.path.join(DATA_DIR, 'progress.json')
        if os.path.exists(progress_path):
            with open(progress_path, 'r', encoding='utf-8') as f:
                prog = json.load(f)
                status.update(prog)
    except:
        pass
        
    # Считывание логов
    try:
        log_path = os.path.join(DATA_DIR, 'parser.log')
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # Берем последние 50 строк лога
                status['logs'] = [line.strip() for line in lines[-50:]]
    except:
        pass
        
    return jsonify(status)

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    if request.method == 'POST':
        data = request.json
        save_parser_config(data)
        if 'cron_time' in data:
            update_cron_job(data['cron_time'])
        return jsonify({"status": "success"})
    else:
        return jsonify(get_parser_config())

@app.route('/api/action', methods=['POST'])
def api_action():
    global parser_process
    action = request.json.get('action')
    
    if action == 'start_tmdb':
        start_parser_task('tmdb')
        return jsonify({"status": "started"})
    elif action == 'start_trends':
        start_parser_task('trends')
        return jsonify({"status": "started"})
    elif action == 'start_rutracker':
        start_parser_task('rutracker')
        return jsonify({"status": "started"})
    elif action == 'start_nnmclub':
        start_parser_task('nnmclub')
        return jsonify({"status": "started"})
    elif action == 'stop':
        # БЕЗУСЛОВНО создаем флаг остановки, даже если процесс - "сирота"
        with open(os.path.join(DATA_DIR, 'stop.flag'), 'w') as f:
            f.write('stop')
            
        # Если мы все еще отслеживаем процесс - можем попытаться его мягко завершить
        if parser_process is not None and parser_process.poll() is None:
            pass # Процесс сам завершится, прочитав stop.flag
            
        return jsonify({"status": "stopping"})
    elif action == 'clear_lock':
        flag_path = os.path.join(DATA_DIR, 'stop.flag')
        if os.path.exists(flag_path):
            try: os.remove(flag_path)
            except OSError: pass
        return jsonify({"status": "cleared"})
        
    return abort(400)

@app.route('/api/shutdown', methods=['POST'])
def api_shutdown():
    global parser_process
    
    # 1. Создаем флаг остановки для любых зависших фоновых скриптов
    with open(os.path.join(DATA_DIR, 'stop.flag'), 'w') as f:
        f.write('stop')
        
    # 2. Принудительно убиваем отслеживаемый процесс, если он жив
    if parser_process is not None and parser_process.poll() is None:
        try:
            parser_process.terminate()
        except:
            pass
            
    # 3. Убиваем сам веб-сервер через 1 секунду (чтобы успеть отдать ответ)
    import threading
    def kill_process():
        time.sleep(1)
        os._exit(0)
    threading.Thread(target=kill_process, daemon=True).start()
    
    return jsonify({"status": "shutting_down"})

if __name__ == '__main__':
    from waitress import serve
    # Те же переменные читает run.py, чтобы два способа запуска не разошлись.
    host = os.environ.get('WEB_HOST', '0.0.0.0')
    port = int(os.environ.get('WEB_PORT', '5000'))
    print(f"Запуск production-сервера Waitress на {host}:{port}...")
    serve(app, host=host, port=port, threads=4)
