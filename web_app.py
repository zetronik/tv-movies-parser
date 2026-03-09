from flask import Flask, render_template, request, jsonify, abort
import sqlite3
import math
import os
import json
import subprocess
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)
DATA_DIR = 'data/'
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, "movies.db")

# Глобальное состояние для процесса
parser_process = None
scheduler = BackgroundScheduler()
scheduler.start()

def get_parser_config():
    try:
        with open(os.path.join(DATA_DIR, 'parser_config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"run_tmdb": True, "run_rutracker": True, "run_nnmclub": True, "cron_time": "02:00"}

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
        # Процесс не запущен или уже завершился
        parser_process = subprocess.Popen(["python", "main.py", "--mode", mode])

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
    conn = sqlite3.connect(DB_NAME)
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
        conn.close()
    except sqlite3.OperationalError:
        movies_count, torrents_count, movies_without_torrents, now_playing_count = 0, 0, 0, 0

    return render_template(
        'index.html', 
        movies_count=movies_count, 
        torrents_count=torrents_count,
        movies_without_torrents=movies_without_torrents,
        now_playing_count=now_playing_count
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
    
    return render_template('movie_detail.html', movie=movie, torrents=torrents)

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

import os
import json

from flask import send_file

@app.route('/movies.zip')
def download_db():
    try:
        return send_file(os.path.join(DATA_DIR, 'movies.zip'), as_attachment=True)
    except FileNotFoundError:
        return abort(404)

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
        conn.close()
    except:
        status['movies_count'] = 0
        status['torrents_count'] = 0
        status['movies_without_torrents'] = 0
        status['now_playing_count'] = 0
        
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
    print("Запуск production-сервера Waitress на порту 5000...")
    serve(app, host='0.0.0.0', port=5000, threads=4)
