"""Запись карточек TMDB в локальный каталог.

Вынесено из main.py, чтобы веб-панель могла добавлять фильмы, не импортируя
парсер целиком (импорт main.py настраивает логирование парсера и перехватил бы
логи веб-приложения).
"""

import logging

# Сериалы живут в той же таблице movies, что и фильмы, поэтому их TMDB-id
# сдвигается на эту константу, чтобы не столкнуться с id фильмов.
TV_ID_OFFSET = 100000000


def save_tmdb_movie(movie_id, db, tmdb_client):
    """Скачивает карточку фильма из TMDB и сохраняет ее в каталог.

    Args:
        movie_id: Идентификатор фильма в TMDB.
        db: Экземпляр MovieDatabase.
        tmdb_client: Экземпляр TMDBClient.

    Returns:
        True, если карточка сохранена.
    """
    try:
        movie = tmdb_client.get_movie_details(movie_id)
        credits = movie.get("credits", {})

        movie_data = (
            movie_id,
            movie.get("title"),
            movie.get("original_title"),
            movie.get("overview"),
            movie.get("vote_average"),
            movie.get("release_date"),
            tmdb_client.get_full_poster_url(movie.get("poster_path")),
            ", ".join(g.get("name", "") for g in movie.get("genres", []) if g.get("name")),
            ", ".join(c.get("name", "") for c in movie.get("production_countries", []) if c.get("name")),
            ", ".join(
                crew.get("name", "") for crew in credits.get("crew", [])
                if crew.get("job") == "Director" and crew.get("name")
            ),
            ", ".join(cast.get("name", "") for cast in credits.get("cast", [])[:10] if cast.get("name")),
            'movie',
        )
        db.upsert_movie(movie_data)
        return True
    except Exception as e:
        logging.error(f"Ошибка при обработке TMDB ID {movie_id}: {e}")
    return False


def save_tmdb_tv(tv_id_shifted, db, tmdb_client):
    """Скачивает карточку сериала из TMDB и сохраняет ее в каталог.

    Args:
        tv_id_shifted: Идентификатор сериала, уже сдвинутый на TV_ID_OFFSET.
        db: Экземпляр MovieDatabase.
        tmdb_client: Экземпляр TMDBClient.

    Returns:
        True, если карточка сохранена.
    """
    real_id = tv_id_shifted - TV_ID_OFFSET
    try:
        tv = tmdb_client.get_tv_details(real_id)
        credits = tv.get("credits", {})

        genres_list = [g.get("name", "") for g in tv.get("genres", []) if g.get("name")]
        if "Сериал" not in genres_list:
            genres_list.append("Сериал")

        movie_data = (
            tv_id_shifted,
            tv.get("name"),
            tv.get("original_name"),
            tv.get("overview"),
            tv.get("vote_average"),
            tv.get("first_air_date", ""),
            tmdb_client.get_full_poster_url(tv.get("poster_path")),
            ", ".join(genres_list),
            ", ".join(c.get("name", "") for c in tv.get("production_countries", []) if c.get("name")),
            ", ".join(creator.get("name", "") for creator in tv.get("created_by", [])),
            ", ".join(cast.get("name", "") for cast in credits.get("cast", [])[:10] if cast.get("name")),
            'tv',
        )
        db.upsert_movie(movie_data)
        return True
    except Exception as e:
        logging.error(f"Ошибка при обработке TMDB ID сериала {real_id}: {e}")
    return False


def save_tmdb_candidate(candidate, db, tmdb_client):
    """Сохраняет выбранного кандидата поиска TMDB в каталог.

    Args:
        candidate: Словарь с ключами 'id' (уже сдвинутый для сериалов) и 'media_type'.
        db: Экземпляр MovieDatabase.
        tmdb_client: Экземпляр TMDBClient.

    Returns:
        Локальный id карточки в каталоге либо None.
    """
    catalog_id = int(candidate['id'])
    if candidate.get('media_type') == 'tv':
        saved = save_tmdb_tv(catalog_id, db, tmdb_client)
    else:
        saved = save_tmdb_movie(catalog_id, db, tmdb_client)
    return catalog_id if saved else None
