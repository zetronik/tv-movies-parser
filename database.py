import logging
import sqlite3
import threading

db_lock = threading.Lock()

# Сколько ждать освобождения базы, прежде чем отдать "database is locked".
# Парсер и веб-панель — разные процессы, поэтому db_lock между ними не работает,
# и единственная реальная защита от гонок — таймаут самого SQLite.
DB_TIMEOUT_SECONDS = 30

class MovieDatabase:
    def __init__(self, db_name="data/movies.db"):
        self.db_name = db_name
        self._enable_wal()
        self._create_tables()
        self._run_migrations()

    def get_connection(self):
        return sqlite3.connect(self.db_name, timeout=DB_TIMEOUT_SECONDS)

    def _enable_wal(self):
        """Включает журнал WAL, чтобы панель могла читать базу во время записи.

        Режим журнала — постоянное свойство файла базы, поэтому достаточно
        выставить его один раз при инициализации.
        """
        try:
            with self.get_connection() as conn:
                mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode.lower() != 'wal':
                    logging.warning(f"Не удалось включить WAL, режим журнала: {mode}")
        except sqlite3.Error as e:
            logging.warning(f"Не удалось включить WAL: {e}")

    def checkpoint(self):
        """Сбрасывает содержимое WAL-журнала в основной файл базы.

        Обязательно вызывать перед архивацией movies.db: в режиме WAL свежие
        транзакции какое-то время живут в отдельном файле -wal, и без чекпоинта
        в архив попадет база без последних изменений.
        """
        try:
            with self.get_connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as e:
            logging.warning(f"Не удалось выполнить чекпоинт WAL: {e}")

    def _create_tables(self):
        """Создает таблицы, если они еще не существуют."""
        movies_query = """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            overview TEXT,
            rating REAL,
            release_date TEXT,
            poster_url TEXT,
            genres TEXT,
            countries TEXT,
            directors TEXT,
            actors TEXT,
            media_type TEXT DEFAULT 'movie'
        )
        """

        torrents_query = """
        CREATE TABLE IF NOT EXISTS torrents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker TEXT,
            topic_id INTEGER,
            movie_id INTEGER,
            topic_title TEXT,
            size_gb REAL,
            quality TEXT,
            file_format TEXT,
            translation TEXT,
            magnet_link TEXT,
            seeds INTEGER,
            leeches INTEGER,
            UNIQUE(tracker, topic_id),
            FOREIGN KEY(movie_id) REFERENCES movies(id)
        )
        """
        now_playing_query = """
        CREATE TABLE IF NOT EXISTS now_playing (
            movie_id INTEGER PRIMARY KEY,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(movie_id) REFERENCES movies(id)
        )
        """

        # Новая таблица для сохранения структуры трекеров (память LLM)
        tracker_topology_query = """
        CREATE TABLE IF NOT EXISTS tracker_topology (
            tracker_name TEXT PRIMARY KEY,
            movies_url TEXT,
            series_url TEXT,
            cartoons_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # Раздачи, которые не удалось привязать к фильму. Раньше они молча
        # выбрасывались вместе со всей работой LLM по странице; теперь это
        # очередь на повтор и материал для отладки сопоставления.
        unmatched_query = """
        CREATE TABLE IF NOT EXISTS unmatched_torrents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker TEXT,
            topic_id INTEGER,
            topic_url TEXT,
            ru_title TEXT,
            original_title TEXT,
            year TEXT,
            size_gb REAL,
            quality TEXT,
            magnet_link TEXT,
            raw_json TEXT,
            reason TEXT,
            attempts INTEGER DEFAULT 1,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tracker, topic_id)
        )
        """

        # Очередь обхода. Раньше она жила только в памяти процесса, поэтому любая
        # остановка отбрасывала парсер к корню раздела.
        frontier_query = """
        CREATE TABLE IF NOT EXISTS crawl_frontier (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker TEXT NOT NULL,
            url TEXT NOT NULL,
            depth INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            UNIQUE(tracker, url)
        )
        """

        with self.get_connection() as conn:
            conn.execute(movies_query)
            conn.execute(torrents_query)
            conn.execute(now_playing_query)
            conn.execute(tracker_topology_query)
            conn.execute(unmatched_query)
            conn.execute(frontier_query)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_frontier_lookup "
                "ON crawl_frontier(tracker, status, depth, id)"
            )
            conn.commit()

    def _run_migrations(self):
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.execute("PRAGMA table_info(movies)")
                columns = [info[1] for info in cursor.fetchall()]
                if 'media_type' not in columns:
                    conn.execute("ALTER TABLE movies ADD COLUMN media_type TEXT DEFAULT 'movie'")
                    conn.commit()

                # Добавление колонки tracker в torrents.
                # Раньше здесь стоял DROP TABLE — теперь колонка добавляется на
                # месте, а уникальность обеспечивается отдельным индексом
                # (ALTER TABLE не умеет добавлять табличные ограничения).
                cursor = conn.execute("PRAGMA table_info(torrents)")
                columns = [info[1] for info in cursor.fetchall()]
                if 'tracker' not in columns:
                    logging.info("Миграция: добавляем колонку 'tracker' в torrents.")
                    conn.execute("ALTER TABLE torrents ADD COLUMN tracker TEXT")
                    # До появления мультитрекерности источник был только один.
                    conn.execute("UPDATE torrents SET tracker = 'rutracker' WHERE tracker IS NULL")
                    conn.commit()

                # Безопасное создание индексов для ускорения поиска на клиенте
                indexes_query = """
                CREATE INDEX IF NOT EXISTS idx_movies_release_date ON movies(release_date);
                CREATE INDEX IF NOT EXISTS idx_torrents_movie_id ON torrents(movie_id);
                CREATE INDEX IF NOT EXISTS idx_movies_media_type ON movies(media_type);
                """
                conn.executescript(indexes_query)
                conn.commit()

                # Уникальность пары (tracker, topic_id) для баз, прошедших миграцию
                # выше: в них таблица создавалась без этого ограничения.
                try:
                    conn.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_torrents_tracker_topic "
                        "ON torrents(tracker, topic_id)"
                    )
                    conn.commit()
                except sqlite3.IntegrityError:
                    logging.warning(
                        "В torrents есть дубликаты (tracker, topic_id) — "
                        "уникальный индекс не создан, требуется ручная чистка."
                    )

    def get_existing_ids(self):
        """Возвращает множество id карточек, которые уже есть в каталоге.

        Нужен режиму tmdb, чтобы из дневного дампа TMDB отобрать только то,
        чего в базе еще нет.
        """
        with self.get_connection() as conn:
            try:
                return {row[0] for row in conn.execute("SELECT id FROM movies")}
            except sqlite3.OperationalError:
                return set()

    def find_movies_by_title(self, query, limit=10):
        """Ищет карточки в локальном каталоге по части названия.

        Args:
            query: Часть русского или оригинального названия.
            limit: Максимальное число результатов.

        Returns:
            Список словарей с полями карточки.
        """
        sql = """
        SELECT id, title, original_title, release_date, poster_url, rating, media_type, overview
        FROM movies
        WHERE title LIKE ? OR original_title LIKE ?
        ORDER BY rating DESC
        LIMIT ?
        """
        like = f"%{query}%"
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, (like, like, limit)).fetchall()
            return [dict(row) for row in rows]

    def get_movie(self, movie_id):
        """Возвращает карточку каталога по id или None."""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM movies WHERE id = ?", (movie_id,)).fetchone()
            return dict(row) if row else None

    def get_torrents_for_movie(self, movie_id):
        """Возвращает раздачи, уже привязанные к карточке."""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM torrents WHERE movie_id = ? ORDER BY seeds DESC, size_gb DESC",
                (movie_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def upsert_movie(self, movie_data):
        """
        Вставляет или обновляет данные о фильме.
        movie_data ожидает кортеж: (id, title, original_title, overview, rating, release_date, poster_url, genres, countries, directors, actors)
        """
        query = """
        INSERT OR REPLACE INTO movies (
            id, title, original_title, overview, rating, release_date, poster_url,
            genres, countries, directors, actors, media_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, movie_data)
                conn.commit()

    def insert_torrent(self, tracker, topic_id, movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches):
        query = """
        INSERT OR IGNORE INTO torrents (
            tracker, topic_id, movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (tracker, topic_id, movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches))
                conn.commit()

    def is_torrent_exists(self, tracker, topic_id):
        query = "SELECT 1 FROM torrents WHERE tracker = ? AND topic_id = ?"
        with self.get_connection() as conn:
            return conn.execute(query, (tracker, topic_id)).fetchone() is not None

    def update_torrent_seeds(self, tracker, topic_id, seeds, leeches):
        query = "UPDATE torrents SET seeds = ?, leeches = ? WHERE tracker = ? AND topic_id = ?"
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (seeds, leeches, tracker, topic_id))
                conn.commit()

    def find_movie_by_title_and_year(self, title, original_title, year):
        """
        Ищет фильм в базе по названию и году.
        Возвращает ID фильма или None.
        """
        if not year:
            return None

        query = """
        SELECT id FROM movies
        WHERE (title LIKE ? OR original_title LIKE ?)
        AND release_date LIKE ?
        LIMIT 1
        """
        # Ищем год в начале release_date (формат YYYY-MM-DD)
        year_pattern = f"{year}-%"

        with self.get_connection() as conn:
            # Пробуем найти по оригинальному названию, если оно есть
            if original_title:
                cursor = conn.execute(query, (f"%{original_title}%", f"%{original_title}%", year_pattern))
                result = cursor.fetchone()
                if result: return result[0]

            # Пробуем найти по русскому названию
            cursor = conn.execute(query, (f"%{title}%", f"%{title}%", year_pattern))
            result = cursor.fetchone()
            if result: return result[0]

        return None

    def update_now_playing_list(self, movie_ids):
        """Очищает старый список 'Сейчас смотрят' и вставляет новый"""
        with db_lock:
            with self.get_connection() as conn:
                conn.execute("DELETE FROM now_playing")
                for mid in movie_ids:
                    conn.execute("INSERT OR IGNORE INTO now_playing (movie_id) VALUES (?)", (mid,))
                conn.commit()

    # --- Очередь обхода страниц (crawl frontier) ---

    def enqueue_page(self, tracker, url, depth=0):
        """Добавляет страницу в очередь обхода.

        Уникальность (tracker, url) заменяет прежнее множество visited_pages:
        уже виденная страница просто не добавится повторно.

        Returns:
            True, если страница действительно добавлена.
        """
        query = "INSERT OR IGNORE INTO crawl_frontier (tracker, url, depth) VALUES (?, ?, ?)"
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.execute(query, (tracker, url, depth))
                conn.commit()
                return cursor.rowcount > 0

    def next_pending_page(self, tracker):
        """Возвращает следующую страницу очереди в порядке обхода в ширину.

        Returns:
            Кортеж (url, depth) или None, если необработанных страниц нет.
        """
        query = """
        SELECT url, depth FROM crawl_frontier
        WHERE tracker = ? AND status = 'pending'
        ORDER BY depth ASC, id ASC
        LIMIT 1
        """
        with self.get_connection() as conn:
            row = conn.execute(query, (tracker,)).fetchone()
            return (row[0], row[1]) if row else None

    def mark_page(self, tracker, url, status):
        """Отмечает страницу как обработанную ('done') или сбойную ('failed')."""
        query = """
        UPDATE crawl_frontier
        SET status = ?, attempts = attempts + 1, processed_at = CURRENT_TIMESTAMP
        WHERE tracker = ? AND url = ?
        """
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (status, tracker, url))
                conn.commit()

    def count_frontier(self, tracker, status=None):
        """Считает страницы в очереди, при необходимости с фильтром по статусу."""
        with self.get_connection() as conn:
            try:
                if status:
                    return conn.execute(
                        "SELECT COUNT(*) FROM crawl_frontier WHERE tracker = ? AND status = ?",
                        (tracker, status)
                    ).fetchone()[0]
                return conn.execute(
                    "SELECT COUNT(*) FROM crawl_frontier WHERE tracker = ?", (tracker,)
                ).fetchone()[0]
            except sqlite3.OperationalError:
                return 0

    def restart_frontier(self, tracker, max_attempts=3):
        """Открывает новый проход: обработанные страницы снова становятся в очередь.

        Сбойные страницы возвращаются, только если лимит попыток не исчерпан —
        иначе намертво недоступная страница блокировала бы каждый прогон.

        Returns:
            Количество страниц, возвращенных в очередь.
        """
        query = """
        UPDATE crawl_frontier
        SET status = 'pending'
        WHERE tracker = ? AND (status = 'done' OR (status = 'failed' AND attempts < ?))
        """
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.execute(query, (tracker, max_attempts))
                conn.commit()
                return cursor.rowcount

    # --- Раздачи, не привязанные к фильму ---

    def save_unmatched(self, tracker, topic_id, topic_url, reason,
                       ru_title='', original_title='', year='', size_gb=0.0,
                       quality='', magnet_link='', raw_json=''):
        """Сохраняет раздачу, которую не удалось привязать к фильму.

        Повторная встреча той же раздачи увеличивает счетчик попыток, а не
        плодит дубликаты.

        Args:
            tracker: Имя трекера.
            topic_id: ID раздачи на трекере.
            topic_url: Полная ссылка на страницу раздачи.
            reason: Причина: 'llm_empty', 'no_title' или 'no_tmdb_match'.
            raw_json: Сырой ответ LLM для последующего разбора.
        """
        query = """
        INSERT INTO unmatched_torrents (
            tracker, topic_id, topic_url, ru_title, original_title, year,
            size_gb, quality, magnet_link, raw_json, reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tracker, topic_id) DO UPDATE SET
            attempts = attempts + 1,
            last_attempt = CURRENT_TIMESTAMP,
            reason = excluded.reason,
            raw_json = excluded.raw_json
        """
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (tracker, topic_id, topic_url, ru_title,
                                     original_title, year, size_gb, quality,
                                     magnet_link, raw_json, reason))
                conn.commit()

    def get_unmatched_attempts(self, tracker, topic_id):
        """Возвращает число неудачных попыток разбора раздачи (0, если их не было)."""
        query = "SELECT attempts FROM unmatched_torrents WHERE tracker = ? AND topic_id = ?"
        with self.get_connection() as conn:
            row = conn.execute(query, (tracker, topic_id)).fetchone()
            return row[0] if row else 0

    def delete_unmatched(self, tracker, topic_id):
        """Убирает раздачу из очереди непривязанных после успешной привязки."""
        query = "DELETE FROM unmatched_torrents WHERE tracker = ? AND topic_id = ?"
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (tracker, topic_id))
                conn.commit()

    def count_unmatched(self):
        """Возвращает количество непривязанных раздач."""
        with self.get_connection() as conn:
            try:
                return conn.execute("SELECT COUNT(*) FROM unmatched_torrents").fetchone()[0]
            except sqlite3.OperationalError:
                return 0

    # --- Новые методы для управления картой структуры трекеров (Tracker Topology) ---

    def get_tracker_topology(self, tracker_name: str):
        """Получает структуру (URL разделов) для конкретного трекера."""
        query = "SELECT movies_url, series_url, cartoons_url FROM tracker_topology WHERE tracker_name = ?"
        with self.get_connection() as conn:
            cursor = conn.execute(query, (tracker_name,))
            row = cursor.fetchone()
            if row:
                return {
                    'movies_url': row[0],
                    'series_url': row[1],
                    'cartoons_url': row[2]
                }
            return None

    def save_tracker_topology(self, tracker_name: str, topology_data: dict):
        """Сохраняет или обновляет структуру ссылок разделов трекера."""
        query = """
        INSERT INTO tracker_topology (tracker_name, movies_url, series_url, cartoons_url)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(tracker_name) DO UPDATE SET
            movies_url = excluded.movies_url,
            series_url = excluded.series_url,
            cartoons_url = excluded.cartoons_url,
            updated_at = CURRENT_TIMESTAMP
        """
        with db_lock:
            with self.get_connection() as conn:
                conn.execute(query, (
                    tracker_name,
                    topology_data.get('movies_url'),
                    topology_data.get('series_url'),
                    topology_data.get('cartoons_url')
                ))
                conn.commit()