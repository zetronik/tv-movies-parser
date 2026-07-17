# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Russian-language movie/torrent aggregator. It builds a SQLite catalog by combining TMDB metadata with torrent releases scraped from Rutracker and NNM-Club, then zips the DB and uploads it to Cloudflare R2 for client apps to download. A Flask admin panel drives and monitors the parser. Code comments, log messages, and UI text are in Russian — keep new ones in Russian to match.

## Commands

```powershell
.venv\Scripts\Activate.ps1          # local venv already exists in .venv/
pip install -r requirements.txt
pip install cloudscraper             # NOT in requirements.txt but imported by nnmclub_client.py

python run.py                        # start the panel and open a browser tab (--no-browser to skip)
start.bat                            # same, double-clickable; picks .venv\Scripts\python.exe
python web_app.py                    # panel only, no browser
# WEB_HOST / WEB_PORT (default 0.0.0.0:5000) are read by both run.py and web_app.py
python main.py --mode rutracker      # run one parser pipeline directly
python main.py --mode nnmclub
python main.py --mode cron           # respects run_* flags in data/parser_config.json
python main.py --mode tmdb           # top up the catalog from TMDB's daily id dumps
python main.py --mode trends         # refresh now_playing: cinema releases + trending TV
python main.py --mode publish        # zip data/movies.db + upload to R2, no crawling
# exit 3 = another parser already holds data/parser.lock

docker compose up --build            # dev: mounts repo into /app, reads .env.local
docker compose -f docker-compose.prod.yml up -d   # prod: named volume for /app/data, reads .env.prod
```

There is no test suite, linter config, or formatter config in this repo.

## Architecture

Two processes that communicate only through files in `data/`:

- **`web_app.py`** — Flask admin panel + APScheduler cron. It never parses in-process; `POST /api/action` shells out with `subprocess.Popen([sys.executable, "main.py", "--mode", …])` and keeps the handle in the module-global `parser_process`. Restarting the web app orphans that handle, but `main.py` holds an OS-level lock on `data/parser.lock`, so a second parser cannot start on top of a running one. A `before_request` hook gates every route behind HTTP Basic auth (`ADMIN_USER` / `ADMIN_PASSWORD`); with no password set it serves localhost only and 403s everything else. `/movies.zip` stays public — client apps fetch it.
- **`main.py`** — the parser CLI. Writes `data/progress.json` (atomic rename from `progress.tmp`), appends to `data/parser.log` (a `RotatingFileHandler`, 5 MB × 3, so history survives across runs), and polls for `data/stop.flag`.

The stop mechanism is cooperative: the panel writes `stop.flag`, and `main.py` checks `os.path.exists(flag_path)` at the top of the page queue loop and per-torrent loop. `/api/shutdown` additionally `terminate()`s the child and `os._exit(0)`s the server.

`GET /api/status` is what the dashboard polls — it merges live DB counts, `progress.json`, and the last 50 log lines into one payload.

### Tracker pipeline (`run_tracker_pipeline` in main.py)

The trackers are parsed with a local LLM rather than CSS selectors, so the same function handles any phpBB-style forum. Three stages:

- **A — Discovery**: fetch the forum index → `clean_html_to_markdown` → `discover_categories()` asks the LLM for the movies/series/cartoons section URLs → persisted in the `tracker_topology` table. Skipped on later runs (the table is the LLM's memory).
- **B — Navigation**: BFS over the `crawl_frontier` table (`ORDER BY depth, id`), not an in-memory queue — a run stopped by the stop flag, a limit, or a crash resumes where it left off. `UNIQUE(tracker, url)` replaces the old `visited_pages` set. `extract_topic_links()` classifies each page's links into `subforum_links` (queued at `depth+1`), `movie_links`, and `next_page` (pagination, queued at the *same* depth). When no `pending` rows remain, `restart_frontier()` reopens `done` pages for a fresh sweep, leaving pages that failed 3+ times behind.
- **C — Extraction**: for each unseen `topic_id`, `extract_torrent_data()` pulls title/year/quality/size/magnet, then the release is matched to a movie — first `db.find_movie_by_title_and_year` (LIKE on title + `YYYY-%` on release_date), falling back to `tmdb_client.search_movie` + `process_tmdb_movie` to create the row. Releases that fail go to `unmatched_torrents` with a `reason` (`llm_empty` / `no_title` / `no_tmdb_match`), the raw LLM JSON, and an `attempts` counter; after `MAX_UNMATCHED_ATTEMPTS` (3) a topic is skipped before any fetch. A later successful match deletes the row.

`_call_llm` chunks long pages instead of truncating them, and `strip_to_links` controls preprocessing: list pages are reduced to links/headings/magnets, but topic pages are sent as full text — year, quality and size are plain prose there, and the link filter used to delete them before the model ever saw them.

### TMDB modes

`run_tmdb_catalog_update()` diffs TMDB's daily id exports (`download_daily_movie_ids` / `download_daily_tv_ids`) against `db.get_existing_ids()` and fetches the missing cards **highest id first** — newest releases — capped at `MAX_TMDB_ITEMS_PER_RUN` (500) per run with a `TMDB_REQUEST_DELAY` pause between calls, since each card is one API request and the backlog can be tens of thousands. It honours `stop.flag` and reports how much is left, so repeated runs drain the queue.

`run_trends_update()` pulls `/movie/now_playing` and `/trending/tv/week`, saves the cards, and is the **only** caller of `db.update_now_playing_list()` — the `now_playing` table and its dashboard tile stay empty unless this mode runs.

Both are opt-in for `--mode cron` (`run_tmdb`, `run_trends` default to `False`): an unattended job should not start making network calls the operator did not enable.

### Manual search (`/search`)

An operator-driven alternative to the crawl, in three steps backed by four endpoints:

1. `POST /api/search/catalog` — looks in `movies` first (`find_movies_by_title`); only on a miss does it call `TMDBClient.search_candidates()`, which queries both `/search/movie` and `/search/tv` and returns ranked candidates. `POST /api/search/save_candidate` writes the chosen one via `catalog.save_tmdb_candidate`.
2. `POST /api/search/torrents` — `BaseTrackerClient.search()` hits each tracker's own `tracker.php?nm=` and parses the result table **deterministically with BeautifulSoup, no LLM**: an interactive page cannot wait for a model call per row. Parsing walks *leaf* `<tr>` elements (a row containing no nested `<tr>`) and keeps those carrying a seed marker or a parsable size. Both rules are load-bearing on real markup: these trackers nest layout tables, so an outer wrapper row otherwise inherits the first result's seeds and size, and sidebar links ("Правила", "Новости") point at `viewtopic.php` too and appear before the results. Results carry `already_linked` so rows already in `torrents` are shown disabled.
3. `POST /api/search/attach` — inserts the checked rows against the movie id, fetching the magnet from each topic page only for confirmed selections. This is the one path that populates `seeds`/`leeches`, which the crawl leaves at 0.

`POST /api/publish` then runs `main.py --mode publish` (zip + R2 upload, no crawling) as a background process.

Steps 2 and 3 live in `static/torrent_picker.js` as a mountable widget, because `/movie/<id>` offers the same flow — there step 1 is already answered, so the button jumps straight to searching trackers for that card and reloads the page after attaching. `TorrentPicker.mount(containerId, movie, {autoSearch, onAttached})` is the whole interface; `movie_detail` passes its row as `dict(...)` so the template can hand it over via `tojson`.

Both the torrent search and publish refuse to run while `parser_process` is alive — two sources of tracker requests at once is how you get banned. The tracker clients used by the panel are cached in the module-global `tracker_clients` so Rutracker login happens once per web-app lifetime.

`catalog.py` holds `save_tmdb_movie` / `save_tmdb_tv` / `TV_ID_OFFSET`, extracted from `main.py` precisely so `web_app.py` can reuse them — importing `main` into the web app would attach the parser's rotating log handler to the panel's root logger.

### Tracker clients

`tracker_client.py` holds `BaseTrackerClient`, the shared transport: randomized delays between requests, retries with exponential backoff on network errors / 429 / 5xx (never on 4xx), `Retry-After` support, and auth-wall recovery. `fetch_page()` raises `TrackerFetchError` when a page is unrecoverable, and the pipeline marks that frontier row `failed` rather than losing the whole run.

`resolve_url(link, current_page, kind)` is the guard on LLM output: it resolves relative links, rejects other domains, and requires the URL to match `viewtopic.php?…t=N` (`kind='topic'`) or `viewforum.php?…f=N` (`kind='forum'`). Nothing from the model enters the crawl without passing it.

**Rutracker is behind Cloudflare as of 2026-08-16.** `login.php` and `tracker.php` return 403 with a "Just a moment" challenge to both `requests` and `cloudscraper` (cloudscraper cannot solve current Cloudflare versions); only the cached `index.php` passes. `login()` therefore tries `RUTRACKER_COOKIES` first — a raw cookie string copied from a logged-in browser (`bb_session=…; cf_clearance=…`) — and validates the session against `index.php`. `cf_clearance` is bound to IP *and* the exact User-Agent, so `RUTRACKER_USER_AGENT` must match the browser too, and it expires within hours. A form login is still attempted when no cookies are set, and a 403 there is reported as a Cloudflare problem rather than "wrong password". `RUTRACKER_DOMAIN` switches to a mirror. **NNM-Club is unaffected and works without auth** — verified live end to end (search → sizes/seeds → magnet).

Every subclass accepts `{PREFIX}_DOMAIN`, `{PREFIX}_USER_AGENT`, and `{PREFIX}_COOKIES` from the environment.

Subclasses (`rutracker_client.py`, `nnmclub_client.py`) supply only `name`, `base_domain`, and a session — Rutracker `requests` + form login, NNM-Club `cloudscraper` for Cloudflare. Rutracker also overrides `_looks_like_auth_wall()` and `relogin()` so an expired session is detected and re-established mid-crawl instead of feeding the login page to the LLM. To add a tracker, subclass `BaseTrackerClient` and register it in `main.load_tracker_client()`.

Delays and limits come from env vars (`REQUEST_DELAY_MIN/MAX`, overridable per tracker as `RUTRACKER_…`/`NNMCLUB_…`, plus `MAX_PAGES_PER_RUN`, `MAX_CRAWL_DEPTH`, `MAX_RUNTIME_SECONDS`) — see `.env.example`. The crawl is deliberately sequential; there is no concurrency, since parallel requests raise the ban risk.

### LLM layer (`llm_parser.py`)

Talks to a local OpenAI-compatible server (`check_llm_available()` probes it once at startup; `main()` aborts if it is down). Every call goes through `_call_llm`, which:
1. optionally runs `_clean_markdown_links` (see `strip_to_links` above) — drops profile/FAQ/rules links and keeps only markdown links, headings, and `magnet:` lines;
2. splits the result into ≤12 000-char chunks on line boundaries, capped at `MAX_CHUNKS` (6);
3. calls each chunk with `temperature=0`, regex-extracts the first `{…}`, and folds the results together with a merge strategy — `_merge_first_non_empty` by default, `_merge_link_lists` for list pages — stopping early when `stop_when` is satisfied.

Failures return `{}` rather than raising, so callers must check for empty dicts. Prompt keys are inconsistent with what `main.py` reads, which is why main.py accepts both spellings (`title`/`ru_title`, `original_title`/`orig_title`, `video_quality`/`quality`).

### Data layer (`database.py`)

`MovieDatabase` opens a fresh `sqlite3.connect` per call (WAL, `timeout=30`) and serializes writes behind a module-level `threading.Lock` (`db_lock`); reads are unlocked. Note that `db_lock` is a *thread* lock while the parser and panel are separate *processes* — the real cross-process protection is WAL plus the busy timeout. Because WAL keeps recent commits in a side file, `create_zip()` calls `db.checkpoint()` before archiving.

Tables: `movies` (PK is the TMDB id), `torrents` (`UNIQUE(tracker, topic_id)`), `unmatched_torrents`, `crawl_frontier`, `now_playing`, `tracker_topology`. TV shows share the `movies` table with their id shifted by `+100000000` (see `process_tmdb_tv`) and `media_type='tv'`.

### Distribution

`main()`'s `finally` block zips `data/movies.db` → `data/movies.zip`, writes an MD5 sidecar, and uploads both to R2 via boto3 (`s3v4` signature) — but only when the run actually inserted torrents or the archive is missing, since it is a ~200 MB upload. Missing R2 env vars log a warning and skip silently. The panel also serves the zip at `/movies.zip`.

## Gotchas

- **`schemas.py` and `TMDBClient.search_tv` are still unused** — kept as scaffolding, not live code.
- `exit 2` no longer exists: every `--mode` value now has a handler.
- **`llm_parser.py` reads `LLM_BASE_URL`, `LLM_MODEL`, `LLM_TIMEOUT_SECONDS`, `LLM_API_KEY`** and falls back to `http://localhost:13305/api/v1` + `Gemma-4-E4B-it-GGUF`. The legacy `OLLAMA_BASE_URL` is intentionally *not* read — stale copies of it in existing `.env` files point at an Ollama instance this project does not use, and honoring it silently redirects the parser to a dead port. The `ollama` service in `docker-compose.yml` is likewise unused. `PRODUCER_MAX_WORKERS` has been dropped from `.env.example` — the crawl is intentionally single-threaded.
- **`data/` is the only source of truth for runtime files.** The stale `parser_config.json` and empty `movies.db` that used to sit in the repo root have been removed; code reads `data/parser_config.json` and `data/movies.db`.
- `docker-compose.prod.yml` expects a `.env.prod` that is not in the repo — create it before `up`.
- `data/` is gitignored and holds a ~500 MB DB and ~200 MB zip. Don't read or copy them wholesale.
- `.env`, `.env.local`, and `.env.prod` are gitignored; `.env.example` is the tracked template.
- `web_app.py` and several `main.py`/`database.py` handlers use bare `except:` / `except Exception` to swallow errors. When touching those paths, prefer keeping the existing shape unless the user asks for a cleanup.

## Conventions

`.claude/skills/python-expert/` holds a vendored generic Python style guide (type hints, Google-style docstrings, PEP 8) — `SKILL.md` is the entry point, `AGENTS.md` the full rule set. It is not enforced by any tooling, and the existing code largely predates it and does not follow it.