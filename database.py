import sqlite3

class MovieDatabase:
    def __init__(self, db_name="movies.db"):
        self.db_name = db_name
        self._create_tables()

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
            actors TEXT
        )
        """
        
        torrents_query = """
        CREATE TABLE IF NOT EXISTS torrents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id INTEGER,
            topic_title TEXT,
            size_gb REAL,
            quality TEXT,
            file_format TEXT,
            translation TEXT,
            magnet_link TEXT,
            seeds INTEGER,
            leeches INTEGER,
            UNIQUE(movie_id, magnet_link),
            FOREIGN KEY(movie_id) REFERENCES movies(id)
        )
        """
        with self.get_connection() as conn:
            conn.execute(movies_query)
            conn.execute(torrents_query)
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
            genres, countries, directors, actors
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self.get_connection() as conn:
            conn.execute(query, movie_data)
            conn.commit()

    def insert_torrent(self, movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches):
        """
        Добавляет информацию о торрент-раздаче в БД.
        Использует INSERT OR IGNORE для избежания дубликатов по movie_id и magnet_link.
        """
        query = """
        INSERT OR IGNORE INTO torrents (
            movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self.get_connection() as conn:
            conn.execute(query, (movie_id, topic_title, size_gb, quality, file_format, translation, magnet_link, seeds, leeches))
            conn.commit()
