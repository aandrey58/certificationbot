# certificationbot

## О проекте

Система подготовки к аттестации с динамическими тестами, PostgreSQL-хранилищем и поиском по базе вопросов (интерфейс — Telegram-бот).

Решает задачу: централизованное хранение вопросов, прохождение тестов, отслеживание прогресса и быстрый поиск по базе вопросов.

Проект включает backend-логику, работу с БД и обработку пользовательских сценариев.

## Стек

- Python, aiogram
- PostgreSQL, SQLAlchemy (async)
- FSM (состояния пользователей)
- Excel → PostgreSQL парсинг
- pg_trgm (поиск с опечатками)

## Ключевая архитектура

- **Вопросы из Excel → PostgreSQL**: каждый лист Excel становится отдельной таблицей вопросов (гибко под разные области аттестации).
- **Прогресс пользователей в БД**: результаты ответов сохраняются и доступны для статистики/матрицы прогресса.
- **Сценарии (FSM) с сохранением**: состояния пользователей хранятся в PostgreSQL, не теряются при перезапуске.
- **Поиск по базе вопросов**: нормализация текста + (если доступно) **`pg_trgm`** для поиска с опечатками (`SEARCH_TRGM_THRESHOLD`).

## Возможности

- **Тестирование**: вопросы по одной области, множественный выбор, проверка и переход к следующему вопросу.
- **Прогресс**: статистика и список/матрица вопросов с отметками ✅/❌.
- **Помощник**: просмотр всех вопросов и поиск по вопросам.
- **Администрирование**: загрузка нового Excel через Telegram (с полной перезагрузкой банка вопросов).

## Скриншоты

<p><b>Главное меню при запуске бота. В помощнике можно выполнить поиск по вопросам и посмотреть их все.</b></p>
<p>
  <img src="screenshots/main_menu.png" alt="Главное меню" width="100%">
  <br>
  <img src="screenshots/helper.png" alt="Помощник" width="100%">
</p>

<p><b>Выбор области аттестации или переход к прогрессу.</b></p>
<p>
  <img src="screenshots/area_of_certification.png" alt="Выбор области аттестации" width="100%">
</p>

<p><b>Оценка результатов прохождения во вкладке «Прогресс». При нажатии на вопрос можно подробнее ознакомиться с вопросом или с выбранным ответом.</b></p>
<p>
  <img src="screenshots/result.png" alt="Результат ответа" width="100%">
  <br>
  <img src="screenshots/result2.png" alt="Результат ответа (вариант)" width="100%">
</p>

## Требования

- **Python**: 3.10+ (рекомендуется 3.11)
- **PostgreSQL**: 13+ (рекомендуется 14+)
- **Windows / Linux / macOS**: без ограничений (ниже примеры для PowerShell)

## Быстрый старт

1. **Зависимости**: `python -m venv .venv` → активировать venv → `pip install -r requirements.txt` (пример для PowerShell: `.\.venv\Scripts\Activate.ps1`).
2. **`.env`** в корне (см. `config.py`): **`BOT_TOKEN`**, **`DATABASE_URL`** в формате `postgresql+asyncpg://user:pass@host:port/db`. Опционально **`SEARCH_TRGM_THRESHOLD`** (по умолчанию `0.25`).

   ```env
   BOT_TOKEN=123456:ABCDEF...
   DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/testingbot
   SEARCH_TRGM_THRESHOLD=0.25
   ```

3. **PostgreSQL**: создать БД и указать её в `DATABASE_URL`. Бот при старте пытается включить **`pg_trgm`**; без прав на `CREATE EXTENSION` fuzzy-поиск будет ограничен (в лог — предупреждение).
4. **Данные**: положить `.xlsx` в **`data/`** (`DATA_DIR` в `config.py`). Временные файлы `~$*.xlsx` игнорируются.
5. **Запуск**: `python bot.py` — инициализация схемы БД, при необходимости парсинг Excel из `data/`, затем polling.

### Запуск через Docker

Сборка образа:

```bash
docker build -t certificationbot .
```

Запуск (подхватить переменные из `.env`, а `data/` примонтировать как volume):

```bash
docker run --rm -it \
  --env-file .env \
  -v "./data:/app/data" \
  certificationbot
```

Важно: если PostgreSQL запущен на хосте, в `DATABASE_URL` внутри Docker **нельзя** использовать `localhost` (это будет сам контейнер). Используйте IP/hostname хоста (например, `host.docker.internal` на Windows) или вынесите Postgres в отдельный контейнер/compose.

## Админы и загрузка Excel через Telegram

Кнопка **«📤 Загрузить Excel»** доступна только администраторам.

Список админов берётся из файла `admins.txt` в корне проекта (по одному `user_id` Telegram на строку). Можно добавлять комментарии строками, начинающимися с `#`.

Пример `admins.txt`:

```text
# Telegram user_id админов
123456789
987654321
```

При загрузке нового Excel бот:
- удаляет прогресс всех пользователей (`user_results`)
- очищает FSM-состояния (`fsm_states`)
- удаляет таблицы вопросов
- удаляет старые Excel файлы из `data/`
- скачивает новый файл в `data/` и парсит его

## Поиск по вопросам

Реализация описана в **Ключевая архитектура** (нормализация текста + при наличии **`pg_trgm`** — similarity / word_similarity с порогом `SEARCH_TRGM_THRESHOLD`).

## Структура проекта (кратко)

- `bot.py` — точка входа, инициализация бота/БД/FSM storage
- `config.py` — конфигурация и загрузка `.env`
- `parser.py` — парсинг Excel → таблицы вопросов в PostgreSQL
- `database/models.py` — модели и класс `Database` (динамические таблицы под листы)
- `services/database_service.py` — бизнес-логика работы с вопросами/прогрессом/поиском
- `handlers/start_handler.py` — обработчики `/start`, меню, подготовка, прогресс, помощник, загрузка Excel
- `keyboards/inline_keyboards.py` — inline-клавиатуры + проверка админов через `admins.txt`
- `storage/postgresql_storage.py` — FSM storage в PostgreSQL
- `excel_creator/` — отдельный инструмент/утилита (GUI) со своими зависимостями (`excel_creator/requirements.txt`)

## Excel Creator (опционально)

В папке `excel_creator/` лежит отдельная утилита с GUI (PyQt5) для работы с Excel (судя по зависимостям).  
Пошаговая инструкция (JSON, запуск, `.exe`): `excel_creator/README.md`.

Зависимости для неё устанавливаются отдельно:

```bash
pip install -r excel_creator/requirements.txt
```

### Сборка `excel_creator` в `.exe` (Windows)

В `excel_creator/requirements.txt` есть `pyinstaller`, утилиту можно собрать в один исполняемый файл.

Пример (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r excel_creator\requirements.txt
pyinstaller --onefile --noconsole -n excel_creator excel_creator\main_window.py
```

Готовый файл появится в `dist\excel_creator.exe`.

Примечание: `excel_creator/main_window.py` использует Qt-ресурс `icon_rc.py` (`":/icon.ico"`). Если при сборке иконка не подхватится, потребуется добавить ресурс в параметры PyInstaller или завести `.spec` файл.

## Типовые проблемы

- **Бот не стартует и пишет про токен**: проверьте `BOT_TOKEN` в `.env`.
- **Нет доступных областей/листов**: проверьте, что `.xlsx` лежат в `data/`.
- **Ошибка подключения к БД**: проверьте `DATABASE_URL`, доступность PostgreSQL и права пользователя.
- **Поиск по опечаткам не работает**: проверьте права на `CREATE EXTENSION pg_trgm` (без него поиск остаётся, но без fuzzy-матчинга).

