"""
PostgreSQL storage для FSM состояний aiogram
"""
import json
import logging
import time
from typing import Optional, Any, Dict, Tuple
from datetime import datetime
from collections import OrderedDict
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from sqlalchemy import select
from database.models import FSMState, Database

logger = logging.getLogger(__name__)


class LRUCache:
    """Простой LRU кэш для FSM состояний"""
    
    def __init__(self, maxsize: int = 1000, ttl: float = 300.0):
        """
        Args:
            maxsize: максимальное количество элементов в кэше
            ttl: время жизни элемента в секундах (5 минут по умолчанию)
        """
        self.cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl
    
    def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            # Проверяем TTL
            if time.time() - timestamp < self.ttl:
                # Перемещаем в конец (самый свежий)
                self.cache.move_to_end(key)
                return value
            else:
                # Удаляем устаревший элемент
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Установить значение в кэш"""
        if key in self.cache:
            # Обновляем существующий
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.maxsize:
            # Удаляем самый старый элемент (первый)
            self.cache.popitem(last=False)
        
        self.cache[key] = (value, time.time())
    
    def delete(self, key: str):
        """Удалить значение из кэша"""
        if key in self.cache:
            del self.cache[key]
    
    def clear(self):
        """Очистить весь кэш"""
        self.cache.clear()


class PostgreSQLStorage(BaseStorage):
    """Хранилище FSM состояний в PostgreSQL с кэшированием"""
    
    def __init__(self, database: Database, cache_size: int = 1000, cache_ttl: float = 300.0):
        """
        Инициализация хранилища
        
        Args:
            database: Экземпляр Database для работы с БД
            cache_size: размер кэша (количество элементов)
            cache_ttl: время жизни элемента в кэше в секундах
        """
        self.database = database
        # Кэш для состояний: {cache_key: state}
        self._state_cache = LRUCache(maxsize=cache_size, ttl=cache_ttl)
        # Кэш для данных: {cache_key: data_dict}
        self._data_cache = LRUCache(maxsize=cache_size, ttl=cache_ttl)
    
    def _get_cache_key(self, user_id: int, chat_id: int) -> str:
        """Получить ключ кэша для пользователя и чата"""
        return f"{user_id}:{chat_id}"
    
    async def set_state(
        self,
        key: StorageKey,
        state: StateType = None,
    ) -> None:
        """Установить состояние FSM для ключа"""
        chat_id = key.chat_id
        user_id = key.user_id
        cache_key = self._get_cache_key(user_id, chat_id)
        
        # Преобразуем state в строку
        if state is None:
            state_str = None
        elif isinstance(state, str):
            state_str = state
        else:
            # Если это объект State, берем его имя
            state_str = state.state if hasattr(state, 'state') else str(state)
        
        # Обновляем кэш
        self._state_cache.set(cache_key, state_str)
        
        async with self.database.get_session() as session:
            # Проверяем, существует ли запись
            stmt = select(FSMState).where(
                FSMState.user_id == user_id,
                FSMState.chat_id == chat_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            
            if existing:
                # Обновляем существующую запись
                existing.state = state_str
                existing.updated_at = datetime.utcnow()
            else:
                # Создаем новую запись
                new_state = FSMState(
                    user_id=user_id,
                    chat_id=chat_id,
                    state=state_str
                )
                session.add(new_state)
            
            await session.commit()
    
    async def get_state(
        self,
        key: StorageKey,
    ) -> Optional[str]:
        """Получить состояние FSM для ключа"""
        chat_id = key.chat_id
        user_id = key.user_id
        cache_key = self._get_cache_key(user_id, chat_id)
        
        # Проверяем кэш
        cached_state = self._state_cache.get(cache_key)
        if cached_state is not None:
            logger.debug(f"Использован кэш состояния для user_id={user_id}, chat_id={chat_id}")
            return cached_state
        
        # Загружаем из БД
        async with self.database.get_session() as session:
            stmt = select(FSMState).where(
                FSMState.user_id == user_id,
                FSMState.chat_id == chat_id
            )
            result = await session.execute(stmt)
            fsm_state = result.scalar_one_or_none()
            
            state_str = fsm_state.state if fsm_state else None
            
            # Сохраняем в кэш
            self._state_cache.set(cache_key, state_str)
            
            return state_str
    
    async def set_data(
        self,
        key: StorageKey,
        data: Dict[str, Any],
    ) -> None:
        """Установить данные FSM для ключа"""
        chat_id = key.chat_id
        user_id = key.user_id
        cache_key = self._get_cache_key(user_id, chat_id)
        
        # Обновляем кэш
        self._data_cache.set(cache_key, data.copy() if data else {})
        
        data_json = json.dumps(data, ensure_ascii=False) if data else None
        
        async with self.database.get_session() as session:
            # Проверяем, существует ли запись
            stmt = select(FSMState).where(
                FSMState.user_id == user_id,
                FSMState.chat_id == chat_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            
            if existing:
                # Обновляем существующую запись
                existing.data = data_json
                existing.updated_at = datetime.utcnow()
            else:
                # Создаем новую запись
                new_state = FSMState(
                    user_id=user_id,
                    chat_id=chat_id,
                    data=data_json
                )
                session.add(new_state)
            
            await session.commit()
    
    async def get_data(
        self,
        key: StorageKey,
    ) -> Dict[str, Any]:
        """Получить данные FSM для ключа"""
        chat_id = key.chat_id
        user_id = key.user_id
        cache_key = self._get_cache_key(user_id, chat_id)
        
        # Проверяем кэш
        cached_data = self._data_cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Использован кэш данных для user_id={user_id}, chat_id={chat_id}")
            return cached_data.copy() if cached_data else {}
        
        # Загружаем из БД
        async with self.database.get_session() as session:
            stmt = select(FSMState).where(
                FSMState.user_id == user_id,
                FSMState.chat_id == chat_id
            )
            result = await session.execute(stmt)
            fsm_state = result.scalar_one_or_none()
            
            if fsm_state and fsm_state.data:
                try:
                    data = json.loads(fsm_state.data)
                    # Сохраняем в кэш
                    self._data_cache.set(cache_key, data)
                    return data
                except json.JSONDecodeError:
                    logger.error(f"Ошибка декодирования JSON для user_id={user_id}, chat_id={chat_id}")
                    return {}
            
            # Сохраняем пустой словарь в кэш
            self._data_cache.set(cache_key, {})
            return {}
    
    async def update_data(
        self,
        key: StorageKey,
        data: Dict[str, Any],
    ) -> None:
        """Обновить данные FSM для ключа (объединить с существующими)"""
        current_data = await self.get_data(key)
        current_data.update(data)
        await self.set_data(key, current_data)
    
    async def clear_state(
        self,
        key: StorageKey,
    ) -> None:
        """Очистить состояние FSM для ключа (переопределяем для очистки кэша)"""
        chat_id = key.chat_id
        user_id = key.user_id
        cache_key = self._get_cache_key(user_id, chat_id)
        
        # Очищаем кэш
        self._state_cache.delete(cache_key)
        self._data_cache.delete(cache_key)
        
        # Вызываем базовую реализацию (если есть) или очищаем в БД
        await self.set_state(key, None)
        await self.set_data(key, {})
    
    async def close(self) -> None:
        """Закрыть хранилище (ничего не делаем, т.к. используем connection pool)"""
        pass

