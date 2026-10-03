/*
 * Виджет подбора раздач: поиск на трекерах, ручная проверка, привязка к фильму.
 *
 * Используется и на странице ручного поиска, и на карточке фильма, поэтому
 * живет отдельным файлом: две копии этой логики неизбежно разошлись бы.
 *
 * Применение:
 *   TorrentPicker.mount('containerId', movie, { onAttached: () => ... });
 * где movie — объект карточки с полями id, title, original_title, release_date.
 */
const TorrentPicker = {
  // Сдвиг идентификаторов сериалов в каталоге (см. TV_ID_OFFSET в catalog.py).
  TV_ID_OFFSET: 100000000,
  container: null,
  movie: null,
  results: [],
  options: {},

  mount(containerId, movie, options) {
    this.container = document.getElementById(containerId);
    this.movie = movie;
    this.options = options || {};
    this.results = [];

    const title = movie.original_title || movie.title || "";
    const year = (movie.release_date || "").slice(0, 4);
    // У сериала год карточки — это год первого сезона, и поиск на трекере с
    // ним отсекает все остальные сезоны. Для фильмов год, наоборот, отсеивает
    // однофамильцев; если он не совпадет с трекером, клиент повторит запрос
    // без года сам.
    const isSeries = movie.media_type === "tv" || Number(movie.id) >= TorrentPicker.TV_ID_OFFSET;
    const query = year && !isSeries ? `${title} ${year}` : title;

    this.container.innerHTML = `
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-dark border-secondary">
          <i class="bi bi-search"></i> Подбор раздач на трекерах
        </div>
        <div class="card-body">
          <label class="form-label text-muted mb-1">Поисковый запрос</label>
          <div class="input-group mb-3">
            <input type="text" id="tpQuery"
                   class="form-control bg-dark text-light border-secondary"
                   value="${this._attr(query)}">
            <button class="btn btn-primary" id="tpSearch">Искать раздачи</button>
          </div>
          <div id="tpStatus" class="text-muted"></div>
          <div id="tpResults"></div>
          <div id="tpActions" class="mt-3 d-none">
            <button class="btn btn-success" id="tpAttach">Добавить отмеченные в каталог</button>
            <button class="btn btn-outline-info ms-2 d-none" id="tpPublish">
              Опубликовать базу в Cloudflare
            </button>
          </div>
          <div id="tpAttachStatus" class="mt-3"></div>
        </div>
      </div>`;

    this._byId("tpSearch").onclick = () => this.search();
    this._byId("tpAttach").onclick = () => this.attach();
    this._byId("tpPublish").onclick = () => this.publish();
    this._byId("tpQuery").onkeydown = (e) => {
      if (e.key === "Enter") this.search();
    };

    if (this.options.autoSearch) this.search();
  },

  // --- Поиск ---
  search() {
    const query = this._byId("tpQuery").value.trim();
    if (!query) return;

    this._byId("tpResults").innerHTML = "";
    this._byId("tpActions").classList.add("d-none");
    this._byId("tpSearch").disabled = true;
    this._note("tpStatus", "Опрашиваем трекеры, это занимает несколько секунд...");

    this._post("/api/search/torrents", { query: query, movie_id: this.movie.id })
      .then((data) => {
        this._byId("tpSearch").disabled = false;
        if (data.error) return this._note("tpStatus", this._esc(data.error), "danger");

        this.results = data.results || [];
        let message = `Найдено раздач: ${this.results.length}.`;
        if (data.errors && data.errors.length) {
          message += ` <span class="text-warning">${this._esc(data.errors.join("; "))}</span>`;
        }
        this._note("tpStatus", message);

        if (this.results.length) {
          this._renderTable();
          this._byId("tpActions").classList.remove("d-none");
        }
      })
      .catch((e) => {
        this._byId("tpSearch").disabled = false;
        this._note("tpStatus", "Ошибка запроса: " + this._esc(e), "danger");
      });
  },

  _renderTable() {
    const rows = this.results
      .map((item, index) => {
        const linked = item.already_linked
          ? '<span class="badge bg-secondary ms-1">уже в базе</span>'
          : "";
        return `
        <tr>
          <td><input type="checkbox" class="form-check-input" data-index="${index}"
                     ${item.already_linked ? "disabled" : ""}></td>
          <td><span class="badge bg-dark border">${this._esc(item.tracker)}</span></td>
          <td>
            <a href="${this._attr(item.url)}" target="_blank" rel="noopener">${this._esc(item.title)}</a>
            ${linked}
          </td>
          <td class="text-nowrap">${(item.size_gb || 0).toFixed(2)} ГБ</td>
          <td class="text-success">${this._esc(item.seeds)}</td>
          <td class="text-danger">${this._esc(item.leeches)}</td>
        </tr>`;
      })
      .join("");

    this._byId("tpResults").innerHTML = `
      <div class="table-responsive mt-3">
        <table class="table table-sm table-hover align-middle">
          <thead>
            <tr>
              <th><input type="checkbox" class="form-check-input" id="tpAll"></th>
              <th>Трекер</th>
              <th>Название (ссылка для проверки)</th>
              <th>Размер</th>
              <th>Сиды</th>
              <th>Личи</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;

    this._byId("tpAll").onclick = (e) => {
      this._boxes().forEach((box) => (box.checked = e.target.checked));
    };
  },

  // --- Привязка ---
  attach() {
    const chosen = this._boxes()
      .filter((box) => box.checked)
      .map((box) => this.results[parseInt(box.dataset.index, 10)]);

    if (!chosen.length) return this._note("tpAttachStatus", "Ничего не отмечено.", "warning");

    this._byId("tpAttach").disabled = true;
    this._note("tpAttachStatus", "Добавляем и забираем magnet-ссылки со страниц раздач...");

    this._post("/api/search/attach", { movie_id: this.movie.id, items: chosen })
      .then((data) => {
        this._byId("tpAttach").disabled = false;
        if (data.error) return this._note("tpAttachStatus", this._esc(data.error), "danger");

        let message = `Добавлено раздач: ${data.added}.`;
        if (data.skipped && data.skipped.length) {
          message += ` Пропущено: ${this._esc(data.skipped.join(", "))}.`;
        }
        message += ` Всего у карточки: ${data.torrents.length}.`;
        this._note("tpAttachStatus", message, "success");

        if (data.added > 0) {
          this._byId("tpPublish").classList.remove("d-none");
          if (this.options.onAttached) this.options.onAttached(data);
          else this.search();
        }
      });
  },

  publish() {
    this._byId("tpPublish").disabled = true;
    this._post("/api/publish", {}).then((data) => {
      if (data.error) {
        this._byId("tpPublish").disabled = false;
        return this._note("tpAttachStatus", this._esc(data.error), "danger");
      }
      this._note(
        "tpAttachStatus",
        'Упаковка базы и выгрузка запущены. Прогресс виден на <a href="/">дашборде</a>.',
        "info"
      );
    });
  },

  // --- Вспомогательное ---
  _byId(id) {
    return document.getElementById(id);
  },
  _boxes() {
    return Array.from(
      this.container.querySelectorAll("input[type=checkbox][data-index]:not(:disabled)")
    );
  },
  _post(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then((r) => r.json());
  },
  _esc(text) {
    const div = document.createElement("div");
    div.textContent = text === null || text === undefined ? "" : text;
    return div.innerHTML;
  },
  _attr(text) {
    return this._esc(text).replace(/"/g, "&quot;");
  },
  _note(id, message, kind) {
    this._byId(id).innerHTML = kind
      ? `<div class="alert alert-${kind} py-2 mb-0">${message}</div>`
      : message;
  },
};
