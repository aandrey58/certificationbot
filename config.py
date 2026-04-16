"""
Конфигурация приложения
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Базовая директория проекта
BASE_DIR = Path(__file__).parent

# Загружаем переменные окружения из .env файла
load_dotenv(BASE_DIR / ".env")

# Токен бота (будет взят из переменной окружения или .env файла)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Путь к папке с данными
DATA_DIR = BASE_DIR / "data"

# URL базы данных PostgreSQL
# Формат: postgresql+asyncpg://user:password@host:port/database
# Для локальной разработки: postgresql+asyncpg://postgres:postgres@localhost:5432/testingbot
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
)

# Порог для поиска с опечатками (pg_trgm similarity).
# Чем выше значение, тем строже поиск (меньше ложных совпадений).
SEARCH_TRGM_THRESHOLD = float(os.getenv("SEARCH_TRGM_THRESHOLD", "0.25"))

