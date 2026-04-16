"""
Модели базы данных для PostgreSQL (async)
"""
from sqlalchemy import Column, Integer, BigInteger, String, Text, Index, DateTime, ARRAY
from sqlalchemy.ext.asyncio import (
    create_async_engine, 
    AsyncSession, 
    async_sessionmaker,
    AsyncSession as AsyncSessionType
)
from sqlalchemy.orm import declarative_base
from sqlalchemy import exc as sa_exc
from datetime import datetime
import config
import logging
import warnings

logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore', category=sa_exc.SAWarning, message='.*already contains a class with the same class name and module name.*')

Base = declarative_base()

# Глобальный кэш для классов таблиц
_global_table_classes = {}


class Question(Base):
    """Модель вопроса"""
    __tablename__ = 'questions_template'
    
    id = Column(Integer, primary_key=True, index=True)
    number = Column(String, nullable=False)  # № п.п.
    question = Column(Text, nullable=False)  # Вопрос
    answer1 = Column(Text)  # Ответ 1
    check1 = Column(String)  # проверка (+, -)
    answer2 = Column(Text)  # Ответ 2
    check2 = Column(String)  # проверка2
    answer3 = Column(Text)  # Ответ 3
    check3 = Column(String)  # проверка3
    answer4 = Column(Text)  # Ответ 4
    check4 = Column(String)  # проверка4
    answer5 = Column(Text)  # Ответ 5
    check5 = Column(String)  # проверка5
    answer6 = Column(Text)  # Ответ 6
    check6 = Column(String)  # проверка6
    normative_basis = Column(Text)  # Нормативная основа вопроса


class UserResult(Base):
    """Модель результатов пользователя"""
    __tablename__ = 'user_results'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, nullable=False, index=True)  # ID пользователя Telegram
    sheet_name = Column(String, nullable=False, index=True)  # Имя листа (область аттестации)
    question_id = Column(Integer, nullable=False)  # ID вопроса
    selected_answer = Column(Integer, nullable=False)  # Выбранный ответ (1, 2, 3...) - для совместимости
    selected_answers = Column(ARRAY(Integer), nullable=True)  # Все выбранные ответы (массив целых чисел)
    is_correct = Column(String, nullable=False)  # Правильность ответа ('+' или '-')
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)  # Время ответа
    
    __table_args__ = (
        Index('idx_user_results_user_sheet', 'user_id', 'sheet_name'),
        Index('idx_user_results_question', 'question_id'),
    )


class FSMState(Base):
    """Модель для хранения FSM состояний пользователей"""
    __tablename__ = 'fsm_states'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, nullable=False, index=True)  # ID пользователя Telegram
    chat_id = Column(BigInteger, nullable=False, index=True)  # ID чата
    state = Column(String, nullable=True)  # Текущее состояние FSM
    data = Column(Text, nullable=True)  # Данные состояния в формате JSON
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)  # Время создания
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)  # Время обновления
    
    __table_args__ = (
        Index('idx_fsm_states_user_chat', 'user_id', 'chat_id', unique=True),
    )


class Database:
    """Класс для работы с базой данных PostgreSQL (async)"""
    
    def __init__(self, database_url: str):
        self.engine = create_async_engine(
            database_url,
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        self.async_session_maker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        self.created_tables = {}  # Кэш созданных таблиц
        self._table_exists_cache = set()  # Кэш существующих таблиц в БД
    
    def clear_table_exists_cache(self):
        """Очистить кэш существования таблиц"""
        self._table_exists_cache.clear()
        logger.debug("Кэш существования таблиц очищен")
    
    def remove_from_table_exists_cache(self, table_name: str):
        """Удалить конкретную таблицу из кэша существования"""
        self._table_exists_cache.discard(table_name)
        logger.debug(f"Таблица '{table_name}' удалена из кэша существования")
    
    async def init_db(self):
        """Инициализация базы данных - создание всех таблиц"""
        # Пытаемся включить расширения для более умного поиска (опечатки и т.п.)
        await self._ensure_postgres_extensions()

        async with self.engine.begin() as conn:
            # Для async engine используем sync_to_async или создаем через SQL
            await conn.run_sync(lambda sync_conn: Base.metadata.create_all(bind=sync_conn))
        
        # Миграция: добавляем колонку selected_answers если её нет
        await self._migrate_add_selected_answers_column()
        await self._migrate_user_id_to_bigint()

    async def _ensure_postgres_extensions(self):
        """
        Попытаться включить нужные расширения PostgreSQL.

        Важно: создание расширений требует прав. Если прав нет — просто логируем и продолжаем.
        """
        from sqlalchemy import text

        async with self.engine.begin() as conn:
            try:
                # Для поиска с опечатками (trigram similarity)
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                logger.info("PostgreSQL extension pg_trgm is available")
            except Exception as e:
                logger.warning(f"Не удалось включить расширение pg_trgm (поиск по опечаткам будет ограничен): {e}")

            # unaccent можно подключить позже при необходимости (тоже требует прав)
    
    async def _migrate_add_selected_answers_column(self):
        """Миграция: добавляет колонку selected_answers в таблицу user_results если её нет"""
        from sqlalchemy import text
        
        async with self.engine.begin() as conn:
            # Проверяем, существует ли колонка
            result = await conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = 'user_results'
                        AND column_name = 'selected_answers'
                    )
                """)
            )
            column_exists = result.scalar()
            
            if not column_exists:
                logger.info("Добавляю колонку selected_answers в таблицу user_results...")
                await conn.execute(
                    text("""
                        ALTER TABLE user_results 
                        ADD COLUMN selected_answers INTEGER[]
                    """)
                )
                logger.info("Колонка selected_answers успешно добавлена")
            else:
                logger.debug("Колонка selected_answers уже существует")
    
    async def _migrate_user_id_to_bigint(self):
        """Миграция: изменяет тип колонки user_id с INTEGER на BIGINT если нужно"""
        from sqlalchemy import text
        
        async with self.engine.begin() as conn:
            # Проверяем текущий тип колонки user_id
            result = await conn.execute(
                text("""
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = 'user_results'
                    AND column_name = 'user_id'
                """)
            )
            row = result.fetchone()
            
            if row and row[0] == 'integer':
                logger.info("Изменяю тип колонки user_id с INTEGER на BIGINT...")
                try:
                    # Изменяем тип колонки на BIGINT
                    await conn.execute(
                        text("""
                            ALTER TABLE user_results 
                            ALTER COLUMN user_id TYPE BIGINT
                        """)
                    )
                    logger.info("Колонка user_id успешно изменена на BIGINT")
                except Exception as e:
                    logger.error(f"Ошибка при изменении типа колонки user_id: {e}")
            else:
                logger.debug("Колонка user_id уже имеет тип BIGINT или не существует")
    
    def get_session(self):
        """Получить асинхронную сессию базы данных (async context manager)"""
        # async_session_maker() уже возвращает async context manager
        return self.async_session_maker()
    
    def get_safe_table_name(self, sheet_name: str) -> str:
        """Получить безопасное имя таблицы из имени листа"""
        import re
        
        # Делаем безопасное имя таблицы для PostgreSQL
        # Заменяем все не-буквенно-цифровые символы на подчеркивания
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', sheet_name)
        # Убираем множественные подчеркивания
        safe_name = re.sub(r'_+', '_', safe_name)
        # Убираем подчеркивания в начале и конце
        safe_name = safe_name.strip('_')
        # Если имя пустое или начинается с цифры, добавляем префикс
        if not safe_name or (safe_name and safe_name[0].isdigit()):
            safe_name = 'sheet_' + safe_name if safe_name else 'sheet_unnamed'
        
        return safe_name
    
    async def create_table_for_sheet(self, sheet_name: str):
        """Создать таблицу для конкретного листа Excel (async)"""
        from sqlalchemy import inspect as sql_inspect
        
        safe_name = self.get_safe_table_name(sheet_name)
        
        # Проверяем глобальный кэш
        if safe_name in _global_table_classes:
            table_class = _global_table_classes[safe_name]
            self.created_tables[safe_name] = table_class
            return table_class
        
        # Проверяем локальный кэш
        if safe_name in self.created_tables:
            return self.created_tables[safe_name]
        
        # Проверяем, существует ли класс в реестре
        class_name = f'Question_{safe_name}'
        if class_name in Base.registry._class_registry:
            table_class = Base.registry._class_registry[class_name]
            _global_table_classes[safe_name] = table_class
            self.created_tables[safe_name] = table_class
            return table_class
        
        # Проверяем, существует ли таблица в metadata и есть ли для неё класс
        if safe_name in Base.metadata.tables:
            # Попробуем найти существующий класс в registry по имени таблицы
            for reg_class in Base.registry._class_registry.values():
                if hasattr(reg_class, '__tablename__') and reg_class.__tablename__ == safe_name:
                    _global_table_classes[safe_name] = reg_class
                    self.created_tables[safe_name] = reg_class
                    return reg_class
        
        # Проверяем, существует ли таблица в базе данных (async)
        # Используем кэш для избежания лишних SQL запросов
        table_exists_in_db = False
        if safe_name in self._table_exists_cache:
            table_exists_in_db = True
            logger.debug(f"Использован кэш существования таблицы для '{safe_name}'")
        else:
            from sqlalchemy import text
            async with self.engine.connect() as conn:
                # Проверяем существование таблицы через SQL запрос
                result = await conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = :table_name
                        )
                    """),
                    {"table_name": safe_name}
                )
                table_exists_in_db = result.scalar()
                if table_exists_in_db:
                    # Сохраняем в кэш
                    self._table_exists_cache.add(safe_name)
        
        # Если таблица уже существует, пытаемся найти существующий класс
        if table_exists_in_db:
            # Попробуем найти существующий класс в registry
            for reg_class in Base.registry._class_registry.values():
                if (hasattr(reg_class, '__tablename__') and 
                    getattr(reg_class, '__tablename__', None) == safe_name):
                    _global_table_classes[safe_name] = reg_class
                    self.created_tables[safe_name] = reg_class
                    logger.debug(f"Использован существующий класс для таблицы '{safe_name}'")
                    return reg_class
        
        # Если таблица уже в metadata, удаляем её, чтобы создать новый класс
        if safe_name in Base.metadata.tables:
            existing_table = Base.metadata.tables[safe_name]
            Base.metadata.remove(existing_table)
        
        # Тщательно удаляем класс из всех возможных мест в реестре SQLAlchemy
        # SQLAlchemy использует комбинацию (module, class_name) для идентификации
        class_keys_to_remove = []
        
        # Собираем все возможные ключи, по которым класс может быть зарегистрирован
        for key in list(Base.registry._class_registry.keys()):
            if key == class_name or key.endswith(f".{class_name}"):
                class_keys_to_remove.append(key)
            # Также проверяем по имени класса в значениях
            try:
                class_obj = Base.registry._class_registry[key]
                if (hasattr(class_obj, '__name__') and class_obj.__name__ == class_name and
                    hasattr(class_obj, '__tablename__') and getattr(class_obj, '__tablename__', None) == safe_name):
                    if key not in class_keys_to_remove:
                        class_keys_to_remove.append(key)
            except:
                pass
        
        # Удаляем все найденные вхождения
        for key in class_keys_to_remove:
            try:
                class_obj = Base.registry._class_registry.get(key)
                if class_obj:
                    # Удаляем из основного реестра
                    del Base.registry._class_registry[key]
                    logger.debug(f"Удален класс {key} из реестра перед созданием нового")
            except Exception as e:
                logger.debug(f"Не удалось удалить класс {key} из реестра: {e}")
        
        # Создаем класс через type() с __tablename__ и индексами
        table_class = type(
            class_name,
            (Base,),
            {
                '__tablename__': safe_name,
                '__table_args__': (
                    # Индекс на id для быстрой сортировки и фильтрации (primary key уже имеет индекс, но явно указываем)
                    Index(f'idx_{safe_name}_id', 'id'),
                ),
                'id': Column(Integer, primary_key=True, index=True),
                'number': Column(String, nullable=False, index=True),  # Индекс на number для быстрого поиска
                'question': Column(Text, nullable=False),
                'answer1': Column(Text),
                'check1': Column(String),
                'answer2': Column(Text),
                'check2': Column(String),
                'answer3': Column(Text),
                'check3': Column(String),
                'answer4': Column(Text),
                'check4': Column(String),
                'answer5': Column(Text),
                'check5': Column(String),
                'answer6': Column(Text),
                'check6': Column(String),
                'normative_basis': Column(Text),
            }
        )
        
        # Создаем таблицу в базе данных - используем CREATE IF NOT EXISTS вместо DROP
        async with self.engine.begin() as conn:
            # Создаем таблицу только если её нет (checkfirst=True)
            await conn.run_sync(lambda sync_conn: table_class.__table__.create(bind=sync_conn, checkfirst=True))
            
            # Создаем индексы для таблицы (IF NOT EXISTS уже обрабатывается SQLAlchemy через checkfirst)
            from sqlalchemy import text
            # Создаем индекс на id (primary key уже имеет индекс, но для уверенности)
            await conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_id ON \"{safe_name}\"(id)"))
            # Создаем индекс на number для быстрой сортировки
            await conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_number ON \"{safe_name}\"(number)"))
            
            # Помечаем таблицу как существующую в кэше
            self._table_exists_cache.add(safe_name)
        
        # Сохраняем в кэши
        _global_table_classes[safe_name] = table_class
        self.created_tables[safe_name] = table_class
        
        logger.info(f"Таблица {safe_name} создана/проверена в базе данных")
        
        return table_class
    
    async def get_table_for_sheet(self, sheet_name: str):
        """Получить класс таблицы для листа (async)"""
        import logging
        logger = logging.getLogger(__name__)
        
        safe_name = self.get_safe_table_name(sheet_name)
        logger.debug(f"get_table_for_sheet: лист='{sheet_name}', safe_name='{safe_name}'")
        
        if safe_name in self.created_tables:
            logger.debug(f"Таблица найдена в локальном кэше: {safe_name}")
            return self.created_tables[safe_name]
        
        logger.debug(f"Создание таблицы для: {safe_name}")
        # Создаем модель таблицы (таблица создается только если её нет)
        table_class = await self.create_table_for_sheet(sheet_name)
        logger.debug(f"Таблица создана: {table_class.__name__ if table_class else None}")
        return table_class
    
    async def close(self):
        """Закрыть соединение с базой данных"""
        await self.engine.dispose()
