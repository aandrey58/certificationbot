"""
Основной файл запуска бота
"""
import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
import config
from handlers import start_router
from storage.postgresql_storage import PostgreSQLStorage

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Основная функция запуска бота"""
    # Проверка токена
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен! Установите переменную окружения BOT_TOKEN")
        return
    
    # Инициализация глобального экземпляра DatabaseService
    from services.database_service import DatabaseService, set_db_service
    db_service = DatabaseService()
    await db_service.init_db()  # Инициализация БД
    set_db_service(db_service)  # Устанавливаем глобальный экземпляр
    
    # Создаем PostgreSQL storage для FSM состояний
    storage = PostgreSQLStorage(db_service.db)
    logger.info("PostgreSQL storage для FSM инициализирован")
    
    # Инициализация бота и диспетчера с PostgreSQL storage
    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=storage)
    
    # Регистрация роутеров
    dp.include_router(start_router)
    
    # Парсинг Excel файлов при запуске (опционально, можно закомментировать)
    logger.info("Начинаю парсинг Excel файлов...")
    await db_service.parse_excel_files()  # Парсинг файлов (async)
    logger.info("Парсинг завершен")
    
    # Запуск бота
    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

