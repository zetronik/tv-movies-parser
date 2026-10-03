"""Сборка публичной копии базы для клиентских приложений.

Локальная база — полный каталог TMDB: в ней лежат и карточки без единой
раздачи, из-за чего файл разрастается до сотен мегабайт. Клиентскому
приложению такие карточки бесполезны: показать по ним нечего. Поэтому в облако
уезжает не рабочая база, а её срез — только фильмы и сериалы, у которых есть
хотя бы один торрент, плюс сами раздачи.

Служебные таблицы парсера (очередь обхода, непривязанные раздачи, топология
трекеров) в публичную копию не попадают вовсе.
"""

import logging
import os
import sqlite3

# Таблицы, которые видит клиент. Остальное — внутренняя кухня парсера.
PUBLIC_TABLES = ("movies", "torrents", "now_playing")

MOVIES_DDL = """
CREATE TABLE movies (
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

TORRENTS_DDL = """
CREATE TABLE torrents (
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

NOW_PLAYING_DDL = """
CREATE TABLE now_playing (
    movie_id INTEGER PRIMARY KEY,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(movie_id) REFERENCES movies(id)
)
"""

INDEXES_DDL = """
CREATE INDEX idx_movies_release_date ON movies(release_date);
CREATE INDEX idx_movies_media_type ON movies(media_type);
CREATE INDEX idx_torrents_movie_id ON torrents(movie_id);
"""


def public_export_enabled():
    """Включён ли срез базы для облака (по умолчанию — да).

    Отключается через PUBLIC_DB_ONLY_WITH_TORRENTS=0, если клиенту вдруг
    понадобится полный каталог.
    """
    value = os.environ.get("PUBLIC_DB_ONLY_WITH_TORRENTS", "1").strip().lower()
    return value not in ("0", "false", "no", "off")


def build_public_db(source_db, target_db):
    """Собирает урезанную копию базы: только фильмы с раздачами.

    Копия создаётся с нуля рядом с рабочей базой, поэтому исходный файл
    открывается только на чтение и никак не меняется.

    Args:
        source_db: Путь к рабочей базе парсера.
        target_db: Путь, по которому будет создана публичная копия.

    Returns:
        Путь к собранной копии (совпадает с target_db).

    Raises:
        FileNotFoundError: Если рабочей базы нет.
        sqlite3.Error: При ошибке чтения исходной базы или записи копии.
    """
    if not os.path.exists(source_db):
        raise FileNotFoundError(f"Не найдена база {source_db}")

    # Остатки предыдущей сборки: и сам файл, и возможные журналы.
    for suffix in ("", "-wal", "-shm", "-journal"):
        stale = target_db + suffix
        if os.path.exists(stale):
            os.remove(stale)

    logging.info("Сборка публичной базы: только фильмы с раздачами.")

    conn = sqlite3.connect(target_db)
    try:
        conn.executescript(MOVIES_DDL + ";" + TORRENTS_DDL + ";" + NOW_PLAYING_DDL)
        conn.execute("ATTACH DATABASE ? AS src", (os.path.abspath(source_db),))

        # Раздачи-сироты (movie_id пустой или указывает на удалённую карточку)
        # в копию не берём: клиенту нечего с ними делать.
        conn.execute("""
            INSERT INTO movies (id, title, original_title, overview, rating, release_date,
                                poster_url, genres, countries, directors, actors, media_type)
            SELECT m.id, m.title, m.original_title, m.overview, m.rating, m.release_date,
                   m.poster_url, m.genres, m.countries, m.directors, m.actors, m.media_type
            FROM src.movies m
            WHERE EXISTS (
                SELECT 1 FROM src.torrents t
                WHERE t.movie_id = m.id AND t.magnet_link IS NOT NULL AND t.magnet_link != ''
            )
        """)
        conn.execute("""
            INSERT INTO torrents (id, tracker, topic_id, movie_id, topic_title, size_gb,
                                  quality, file_format, translation, magnet_link, seeds, leeches)
            SELECT t.id, t.tracker, t.topic_id, t.movie_id, t.topic_title, t.size_gb,
                   t.quality, t.file_format, t.translation, t.magnet_link, t.seeds, t.leeches
            FROM src.torrents t
            WHERE t.magnet_link IS NOT NULL AND t.magnet_link != ''
              AND EXISTS (SELECT 1 FROM movies m WHERE m.id = t.movie_id)
        """)
        # now_playing ссылается на movies, поэтому оставляем только те строки,
        # чьи карточки пережили фильтр.
        conn.execute("""
            INSERT INTO now_playing (movie_id, added_at)
            SELECT n.movie_id, n.added_at FROM src.now_playing n
            WHERE EXISTS (SELECT 1 FROM movies m WHERE m.id = n.movie_id)
        """)

        conn.executescript(INDEXES_DDL)
        conn.commit()

        stats = {}
        for table in PUBLIC_TABLES:
            stats[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total_movies = conn.execute("SELECT COUNT(*) FROM src.movies").fetchone()[0]

        conn.execute("DETACH DATABASE src")
        # Копия наполняется одним заходом, но VACUUM убирает хвосты страниц —
        # ради этого файла всё и затевалось.
        conn.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()

    size_mb = os.path.getsize(target_db) / (1024 * 1024)
    source_mb = os.path.getsize(source_db) / (1024 * 1024)
    logging.info(
        f"Публичная база готова: фильмов {stats['movies']} из {total_movies}, "
        f"раздач {stats['torrents']}, в прокате {stats['now_playing']}. "
        f"Размер {size_mb:.1f} МБ против {source_mb:.1f} МБ у рабочей базы."
    )
    return target_db
