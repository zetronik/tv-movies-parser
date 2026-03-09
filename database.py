import sqlite3
import threading

db_lock = threading.Lock()
class MovieDatabase:
    def __init__(self, db_name="data/movies.db"):
        self.db_name = db_name
        self._create_tables()
        self._run_migrations()

    def get_connection(self):
        return sqlite3.connect(self.db_name)

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
        with self.get_connection() as conn:
            conn.execute(movies_query)
            conn.execute(torrents_query)
            conn.execute(now_playing_query)
            conn.commit()

    def _run_migrations(self):
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.execute("PRAGMA table_info(movies)")
                columns = [info[1] for info in cursor.fetchall()]
                if 'media_type' not in columns:
                    conn.execute("ALTER TABLE movies ADD COLUMN media_type TEXT DEFAULT 'movie'")
                    conn.commit()

                # Migration for torrents table to add tracker
                cursor = conn.execute("PRAGMA table_info(torrents)")
                columns = [info[1] for info in cursor.fetchall()]
                if 'tracker' not in columns:
                    conn.execute("DROP TABLE torrents")
                    conn.commit()
                    self._create_tables()

                # Безопасное создание индексов для ускорения поиска на клиенте
                indexes_query = """
                CREATE INDEX IF NOT EXISTS idx_movies_release_date ON movies(release_date);
                CREATE INDEX IF NOT EXISTS idx_torrents_movie_id ON torrents(movie_id);
                CREATE INDEX IF NOT EXISTS idx_movies_media_type ON movies(media_type);
                """
                conn.executescript(indexes_query)
                conn.commit()

    def get_existing_ids(self):
        """Возвращает множество ID фильмов, которые уже есть в базе."""
        query = "SELECT id FROM movies"
        with self.get_connection() as conn:
            try:
                cursor = conn.execute(query)
                return {row[0] for row in cursor.fetchall()}
            except sqlite3.OperationalError:
                # В случае если таблица еще не создана
                return set()

    def get_movie_for_search(self):
        """Возвращает один фильм для тестирования поиска на Rutracker."""
        query = "SELECT id, title, original_title, release_date FROM movies WHERE original_title IS NOT NULL LIMIT 1"
        with self.get_connection() as conn:
            try:
                cursor = conn.execute(query)
                row = cursor.fetchone()
                if row:
                    return {
                        "id": row[0],
                        "title": row[1],
                        "original_title": row[2],
                        "year": row[3][:4] if row[3] else ""
                    }
                return None
            except sqlite3.OperationalError:
                return None

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
