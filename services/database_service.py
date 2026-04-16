"""
Сервис для работы с базой данных (async)
"""
from pathlib import Path
from database.models import Database
from parser import ExcelParser
import config
import logging
import asyncio
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Глобальный экземпляр DatabaseService
_db_service = None


class DatabaseService:
    """Сервис для управления базой данных (async)"""
    
    def __init__(self):
        self.db = Database(config.DATABASE_URL)
        self.parser = ExcelParser(self.db)
        # Кэш для списков ID вопросов: {sheet_name: (question_ids, timestamp)}
        self._question_ids_cache: dict[str, tuple[list[int], float]] = {}
        # Время жизни кэша в секундах (5 минут)
        self._cache_ttl = 300
    
    async def init_db(self):
        """Инициализация базы данных"""
        await self.db.init_db()
    
    async def get_available_sheets(self) -> list[str]:
        """Получить список доступных листов из Excel файлов (async метод)"""
        data_dir = Path(config.DATA_DIR)
        # Выполняем glob в отдельном потоке
        excel_files = await asyncio.to_thread(lambda: list(data_dir.glob("*.xlsx")))
        
        all_sheets = []
        for excel_file in excel_files:
            if excel_file.name.startswith("~$"):
                continue
            sheets = await self.parser.get_available_sheets(excel_file)
            all_sheets.extend(sheets)
        
        # Убираем дубликаты, сохраняя порядок
        return list(dict.fromkeys(all_sheets))
    
    async def table_exists_and_has_data(self, sheet_name: str) -> bool:
        """Проверить, существует ли таблица и содержит ли она данные (async)"""
        try:
            QuestionModel = await self.db.get_table_for_sheet(sheet_name)
            if QuestionModel is None:
                return False
            
            async with self.db.get_session() as session:
                from sqlalchemy import select, func
                result = await session.execute(select(func.count(QuestionModel.id)))
                count = result.scalar()
                return count > 0
        except Exception as e:
            logger.error(f"Ошибка при проверке таблицы для листа '{sheet_name}': {e}")
            return False
    
    async def parse_excel_files(self):
        """Парсить все Excel файлы в базу данных (async)"""
        data_dir = Path(config.DATA_DIR)
        # Выполняем glob в отдельном потоке
        excel_files = await asyncio.to_thread(lambda: list(data_dir.glob("*.xlsx")))
        
        success_count = 0
        parsed_sheets = []
        
        for excel_file in excel_files:
            if excel_file.name.startswith("~$"):
                continue
            
            # Проверяем, нужно ли парсить этот файл
            sheets = await self.parser.get_available_sheets(excel_file)
            needs_parsing = False
            
            for sheet_name in sheets:
                if not await self.table_exists_and_has_data(sheet_name):
                    needs_parsing = True
                    parsed_sheets.append(sheet_name)
                    break
            
            if needs_parsing:
                logger.info(f"Парсинг файла {excel_file.name}...")
                if await self.parser.parse_excel_file(excel_file):
                    success_count += 1
            else:
                logger.info(f"Файл {excel_file.name} уже обработан, пропускаем парсинг")
                success_count += 1  # Считаем как успешный, так как данные уже есть
        
        # Инвалидируем кэш для загруженных листов
        for sheet_name in parsed_sheets:
            self._invalidate_question_ids_cache(sheet_name)
        
        return success_count == len([f for f in excel_files if not f.name.startswith("~$")])
    
    async def get_question_by_number(self, sheet_name: str, question_number: int = 1):
        """
        Получить вопрос по номеру из указанного листа (async)
        question_number: номер вопроса (начиная с 1)
        """
        logger.info(f"get_question_by_number: начало, лист='{sheet_name}', номер={question_number}")
        try:
            logger.info(f"Получение таблицы для листа: {sheet_name}")
            QuestionModel = await self.db.get_table_for_sheet(sheet_name)
            if QuestionModel is None:
                logger.error(f"Не удалось получить модель таблицы для листа: {sheet_name}")
                return None
            
            logger.info(f"Модель таблицы получена: {QuestionModel.__name__}")
            async with self.db.get_session() as session:
                from sqlalchemy import select
                
                logger.info(f"Запрос вопроса с offset={question_number - 1}")
                # Получаем вопрос по порядковому номеру (id или по номеру п.п.)
                # Используем limit(1) для оптимизации - получаем только одну запись
                # Убрали count() для ускорения - проверяем наличие данных через сам запрос
                stmt = select(QuestionModel).order_by(QuestionModel.id).offset(question_number - 1).limit(1)
                result = await session.execute(stmt)
                question = result.scalar_one_or_none()
                
                logger.info(f"Вопрос получен: {question is not None}")
                
                if not question:
                    logger.warning("Вопрос не найден в результате запроса")
                    return None
                
                logger.info("Форматирование вопроса")
                formatted = self._format_question(question)
                logger.info(f"Вопрос отформатирован, количество ответов: {len(formatted.get('answers', []))}")
                return formatted
                
        except Exception as e:
            logger.error(f"Ошибка при получении вопроса из листа '{sheet_name}': {e}", exc_info=True)
            return None
    
    async def get_question_by_id(self, sheet_name: str, question_id: int):
        """
        Получить вопрос по ID из указанного листа (async)
        question_id: ID вопроса в базе данных
        """
        try:
            QuestionModel = await self.db.get_table_for_sheet(sheet_name)
            if QuestionModel is None:
                logger.warning(f"Таблица для листа '{sheet_name}' не найдена в базе данных")
                return None
            
            async with self.db.get_session() as session:
                from sqlalchemy import select
                stmt = select(QuestionModel).filter(QuestionModel.id == question_id)
                result = await session.execute(stmt)
                question = result.scalar_one_or_none()
                
                if not question:
                    return None
                
                return self._format_question(question)
                
        except Exception as e:
            logger.error(f"Ошибка при получении вопроса по ID {question_id} из листа '{sheet_name}': {e}", exc_info=True)
            return None
    
    async def get_next_question_by_id(self, sheet_name: str, question_id: int):
        """
        Получить следующий вопрос по ID из указанного листа (async)
        question_id: ID текущего вопроса
        Возвращает следующий вопрос или None, если это был последний вопрос
        """
        try:
            QuestionModel = await self.db.get_table_for_sheet(sheet_name)
            if QuestionModel is None:
                logger.warning(f"Таблица для листа '{sheet_name}' не найдена в базе данных")
                return None
            
            async with self.db.get_session() as session:
                from sqlalchemy import select
                # Получаем следующий вопрос с ID больше текущего
                stmt = select(QuestionModel).filter(QuestionModel.id > question_id).order_by(QuestionModel.id).limit(1)
                result = await session.execute(stmt)
                question = result.scalar_one_or_none()
                
                if not question:
                    return None  # Это был последний вопрос
                
                return self._format_question(question)
                
        except Exception as e:
            logger.error(f"Ошибка при получении следующего вопроса после ID {question_id} из листа '{sheet_name}': {e}", exc_info=True)
            return None
    
    def _format_question(self, question):
        """Форматировать вопрос в словарь"""
        # Формируем словарь с ответами
        answers = []
        checks = []
        
        for i in range(1, 7):  # От 1 до 6
            answer = getattr(question, f'answer{i}', None)
            check = getattr(question, f'check{i}', None)
            
            if answer and str(answer).strip():  # Если ответ не пустой
                answers.append(str(answer).strip())
                checks.append(str(check).strip() if check else '-')
        
        return {
            'id': question.id,
            'number': question.number,
            'question': question.question,
            'answers': answers,
            'checks': checks,
            'normative_basis': question.normative_basis if hasattr(question, 'normative_basis') else ''
        }
    
    async def save_user_result(self, user_id: int, sheet_name: str, question_id: int, 
                               selected_answer: int, is_correct: str, selected_answers: set[int] = None):
        """
        Сохранить результат ответа пользователя (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
            question_id: ID вопроса
            selected_answer: выбранный ответ (1, 2, 3...) - для совместимости
            is_correct: правильность ответа ('+' или '-')
            selected_answers: множество всех выбранных ответов (опционально)
        """
        from database.models import UserResult
        from datetime import datetime
        
        async with self.db.get_session() as session:
            # Проверяем, есть ли уже результат для этого вопроса
            from sqlalchemy import select
            stmt = select(UserResult).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name,
                UserResult.question_id == question_id
            )
            result = await session.execute(stmt)
            existing_result = result.scalar_one_or_none()
            
            # Подготавливаем массив для всех выбранных ответов
            selected_answers_array = None
            if selected_answers is not None:
                selected_answers_array = sorted(list(selected_answers))
            
            if existing_result:
                # Обновляем существующий результат
                existing_result.selected_answer = selected_answer
                existing_result.is_correct = is_correct
                existing_result.created_at = datetime.utcnow()
                if selected_answers_array is not None:
                    existing_result.selected_answers = selected_answers_array
            else:
                # Создаем новый результат
                new_result = UserResult(
                    user_id=user_id,
                    sheet_name=sheet_name,
                    question_id=question_id,
                    selected_answer=selected_answer,
                    is_correct=is_correct,
                    selected_answers=selected_answers_array,
                    created_at=datetime.utcnow()
                )
                session.add(new_result)
            
            await session.commit()
    
    async def get_user_results_for_sheet(self, user_id: int, sheet_name: str) -> dict[int, str]:
        """
        Получить результаты пользователя для конкретной области (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
        
        Returns:
            Словарь {question_id: 'correct'/'incorrect'/'not_answered'}
        """
        from database.models import UserResult
        from sqlalchemy import select
        
        results = {}
        
        async with self.db.get_session() as session:
            stmt = select(UserResult).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name
            )
            result = await session.execute(stmt)
            user_results = result.scalars().all()
            
            for user_result in user_results:
                if user_result.is_correct == '+':
                    results[user_result.question_id] = 'correct'
                else:
                    results[user_result.question_id] = 'incorrect'
        
        return results
    
    async def get_user_answer_for_question(self, user_id: int, sheet_name: str, question_id: int) -> dict:
        """
        Получить ответ пользователя на конкретный вопрос (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
            question_id: ID вопроса
        
        Returns:
            Словарь с информацией об ответе: {'selected_answer': int, 'is_correct': str} или None
        """
        from database.models import UserResult
        from sqlalchemy import select
        
        async with self.db.get_session() as session:
            stmt = select(UserResult).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name,
                UserResult.question_id == question_id
            )
            result = await session.execute(stmt)
            user_result = result.scalar_one_or_none()
            
            if user_result:
                result = {
                    'selected_answer': user_result.selected_answer,
                    'is_correct': user_result.is_correct
                }
                # Добавляем все выбранные ответы, если они есть
                if user_result.selected_answers:
                    result['selected_answers'] = set(user_result.selected_answers)
                else:
                    result['selected_answers'] = set()
                return result
            return None
    
    async def get_user_progress_stats(self, user_id: int, sheet_name: str) -> dict:
        """
        Получить статистику прогресса пользователя для области (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
        
        Returns:
            Словарь с ключами: correct_count, incorrect_count, total_answered, percentage
        """
        from database.models import UserResult
        from sqlalchemy import select, func
        
        async with self.db.get_session() as session:
            # Получаем количество правильных ответов
            stmt_correct = select(func.count(UserResult.id)).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name,
                UserResult.is_correct == '+'
            )
            result_correct = await session.execute(stmt_correct)
            correct_count = result_correct.scalar() or 0
            
            # Получаем количество неправильных ответов
            stmt_incorrect = select(func.count(UserResult.id)).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name,
                UserResult.is_correct == '-'
            )
            result_incorrect = await session.execute(stmt_incorrect)
            incorrect_count = result_incorrect.scalar() or 0
            
            total_answered = correct_count + incorrect_count
            
            # Вычисляем процент правильности
            if total_answered > 0:
                percentage = round((correct_count / total_answered) * 100, 1)
            else:
                percentage = 0.0
            
            return {
                'correct_count': correct_count,
                'incorrect_count': incorrect_count,
                'total_answered': total_answered,
                'percentage': percentage
            }
    
    def _invalidate_question_ids_cache(self, sheet_name: str):
        """Инвалидировать кэш для конкретного листа"""
        if sheet_name in self._question_ids_cache:
            del self._question_ids_cache[sheet_name]
            logger.debug(f"Кэш ID вопросов для листа '{sheet_name}' инвалидирован")
    
    def _invalidate_all_question_ids_cache(self):
        """Инвалидировать весь кэш ID вопросов"""
        self._question_ids_cache.clear()
        logger.debug("Весь кэш ID вопросов инвалидирован")
    
    async def get_all_question_ids_for_sheet(self, sheet_name: str, use_cache: bool = True) -> list[int]:
        """
        Получить все ID вопросов для области (async) с кэшированием
        
        Args:
            sheet_name: имя листа (область аттестации)
            use_cache: использовать кэш (по умолчанию True)
        
        Returns:
            Список ID вопросов, отсортированный по id
        """
        # Проверяем кэш
        if use_cache and sheet_name in self._question_ids_cache:
            question_ids, timestamp = self._question_ids_cache[sheet_name]
            if time.time() - timestamp < self._cache_ttl:
                logger.debug(f"Использован кэш ID вопросов для листа '{sheet_name}'")
                return question_ids
            else:
                # Кэш устарел, удаляем
                del self._question_ids_cache[sheet_name]
        
        # Загружаем из БД
        QuestionModel = await self.db.get_table_for_sheet(sheet_name)
        if QuestionModel is None:
            return []
        
        async with self.db.get_session() as session:
            from sqlalchemy import select
            stmt = select(QuestionModel.id).order_by(QuestionModel.id)
            result = await session.execute(stmt)
            question_ids = list(result.scalars().all())
            
            # Сохраняем в кэш
            if use_cache:
                self._question_ids_cache[sheet_name] = (question_ids, time.time())
                logger.debug(f"ID вопросов для листа '{sheet_name}' закэшированы ({len(question_ids)} вопросов)")
            
            return question_ids
    
    async def get_total_questions_count(self, sheet_name: str) -> int:
        """
        Получить общее количество вопросов в области (async)
        
        Args:
            sheet_name: имя листа (область аттестации)
        
        Returns:
            Общее количество вопросов
        """
        question_ids = await self.get_all_question_ids_for_sheet(sheet_name)
        return len(question_ids)
    
    async def get_question_number(self, sheet_name: str, question_id: int) -> Optional[int]:
        """
        Получить порядковый номер вопроса по его ID (async)
        
        Args:
            sheet_name: имя листа (область аттестации)
            question_id: ID вопроса
        
        Returns:
            Порядковый номер вопроса (начиная с 1) или None, если вопрос не найден
        """
        question_ids = await self.get_all_question_ids_for_sheet(sheet_name)
        try:
            # Находим индекс вопроса в отсортированном списке
            index = question_ids.index(question_id)
            return index + 1  # Нумерация с 1
        except ValueError:
            return None
    
    async def get_question_number_and_total(self, sheet_name: str, question_id: int) -> Tuple[Optional[int], int]:
        """
        Получить порядковый номер вопроса и общее количество вопросов за один запрос (async)
        Оптимизированный метод для избежания дублирования запросов
        
        Args:
            sheet_name: имя листа (область аттестации)
            question_id: ID вопроса
        
        Returns:
            Кортеж (номер_вопроса, общее_количество_вопросов)
            Если вопрос не найден, номер_вопроса будет None
        """
        question_ids = await self.get_all_question_ids_for_sheet(sheet_name)
        total = len(question_ids)
        
        try:
            index = question_ids.index(question_id)
            question_number = index + 1
        except ValueError:
            question_number = None
        
        return question_number, total
    
    async def has_user_progress(self, user_id: int, sheet_name: str) -> bool:
        """
        Проверить, есть ли у пользователя прогресс по области (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
        
        Returns:
            True, если есть хотя бы один ответ по этой области
        """
        from database.models import UserResult
        from sqlalchemy import select, func
        
        async with self.db.get_session() as session:
            stmt = select(func.count(UserResult.id)).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name
            )
            result = await session.execute(stmt)
            count = result.scalar() or 0
            return count > 0
    
    async def get_last_unanswered_question_id(self, user_id: int, sheet_name: str) -> int:
        """
        Получить ID последнего неотвеченного вопроса (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
        
        Returns:
            ID последнего неотвеченного вопроса или None, если все вопросы отвечены
        """
        from database.models import UserResult
        from sqlalchemy import select
        
        # Получаем все ID вопросов для области
        all_question_ids = await self.get_all_question_ids_for_sheet(sheet_name)
        
        if not all_question_ids:
            return None
        
        # Получаем ID отвеченных вопросов
        async with self.db.get_session() as session:
            stmt = select(UserResult.question_id).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name
            )
            result = await session.execute(stmt)
            answered_ids = set(result.scalars().all())
        
        # Находим первый неотвеченный вопрос
        for question_id in all_question_ids:
            if question_id not in answered_ids:
                return question_id
        
        # Если все вопросы отвечены, возвращаем None
        return None
    
    async def search_questions(self, search_query: str, sheet_name: Optional[str] = None) -> list[dict]:
        """
        Поиск вопросов по тексту во всех областях или в указанной области (async)
        Улучшен:
        - игнорирует регистр
        - игнорирует пунктуацию
        - поддерживает опечатки (если доступно PostgreSQL расширение pg_trgm)
        
        Args:
            search_query: текст для поиска
            sheet_name: опционально - имя области для поиска только в этой области
        
        Returns:
            Список словарей с информацией о найденных вопросах:
            [{'sheet_name': str, 'question_id': int, 'question': str, 'question_number': int}, ...]
        """
        results = []
        if sheet_name:
            # Поиск только в указанной области
            sheet_names = [sheet_name]
        else:
            # Поиск во всех областях
            sheet_names = await self.get_available_sheets()

        # Нормализуем запрос: lower + удаляем пунктуацию
        import re
        normalized_query = re.sub(r"[^\w\s]+", " ", search_query, flags=re.UNICODE).lower()
        normalized_query = re.sub(r"\s+", " ", normalized_query).strip()
        
        for sheet_name in sheet_names:
            try:
                QuestionModel = await self.db.get_table_for_sheet(sheet_name)
                if QuestionModel is None:
                    continue
                
                # Получаем список всех ID вопросов для вычисления номеров
                all_question_ids = await self.get_all_question_ids_for_sheet(sheet_name)
                if not all_question_ids:
                    continue
                
                # Создаем словарь для быстрого поиска номера по ID
                id_to_number = {qid: idx + 1 for idx, qid in enumerate(all_question_ids)}
                
                async with self.db.get_session() as session:
                    from sqlalchemy import select, func

                    # Нормализуем текст вопроса на стороне БД:
                    # lower(regexp_replace(question, punctuation, ' ', 'g'))
                    normalized_question_expr = func.lower(
                        func.regexp_replace(
                            QuestionModel.question,
                            r"[^\w\s]+",
                            " ",
                            "g",
                        )
                    )

                    # 1) базовый поиск: contains по нормализованному тексту (регистр/пунктуация игнорируются)
                    base_filter = (
                        normalized_question_expr.contains(normalized_query)
                        if normalized_query and len(normalized_query) >= 2
                        else None
                    )

                    # 2) поиск с опечатками через pg_trgm similarity (если доступно).
                    # Используем только если запрос не пустой и достаточно длинный (>= 3 символов).
                    trgm_filter = None
                    if normalized_query and len(normalized_query) >= 3:
                        try:
                            # similarity(text, query) > threshold
                            threshold = getattr(config, "SEARCH_TRGM_THRESHOLD", 0.25)
                            # Для фраз лучше использовать word_similarity, но не во всех версиях PG она есть.
                            # Пытаемся сначала word_similarity, если её нет — падаем обратно на similarity.
                            try:
                                trgm_filter = func.word_similarity(normalized_question_expr, normalized_query) > threshold
                            except Exception:
                                trgm_filter = func.similarity(normalized_question_expr, normalized_query) > threshold
                        except Exception:
                            trgm_filter = None

                    if base_filter is None and trgm_filter is None:
                        continue

                    if base_filter is not None and trgm_filter is not None:
                        stmt = (
                            select(QuestionModel)
                            .filter(base_filter | trgm_filter)
                            .order_by(QuestionModel.id)
                        )
                    elif base_filter is not None:
                        stmt = select(QuestionModel).filter(base_filter).order_by(QuestionModel.id)
                    else:
                        stmt = select(QuestionModel).filter(trgm_filter).order_by(QuestionModel.id)

                    result = await session.execute(stmt)
                    questions = result.scalars().all()
                    
                    for question in questions:
                        # Получаем номер вопроса из словаря (O(1) вместо отдельного запроса)
                        question_number = id_to_number.get(question.id)
                        
                        results.append({
                            'sheet_name': sheet_name,
                            'question_id': question.id,
                            'question': question.question,
                            'question_number': question_number
                        })
            except Exception as e:
                logger.error(f"Ошибка при поиске в области '{sheet_name}': {e}", exc_info=True)
                continue
        
        return results

    async def get_questions_by_ids(self, sheet_name: str, question_ids: list[int]) -> dict[int, dict]:
        """
        Получить несколько вопросов по списку ID одной пачкой (async).

        Используется для формирования текстовых результатов поиска без N+1 запросов.

        Args:
            sheet_name: имя листа (область аттестации)
            question_ids: список ID вопросов

        Returns:
            Словарь {question_id: formatted_question_dict}
        """
        if not question_ids:
            return {}

        QuestionModel = await self.db.get_table_for_sheet(sheet_name)
        if QuestionModel is None:
            return {}

        async with self.db.get_session() as session:
            from sqlalchemy import select

            stmt = select(QuestionModel).where(QuestionModel.id.in_(question_ids))
            result = await session.execute(stmt)
            questions = result.scalars().all()

            return {q.id: self._format_question(q) for q in questions}
    
    async def delete_user_progress(self, user_id: int, sheet_name: str):
        """
        Удалить весь прогресс пользователя по области (async)
        
        Args:
            user_id: ID пользователя Telegram
            sheet_name: имя листа (область аттестации)
        """
        from database.models import UserResult
        from sqlalchemy import delete
        
        async with self.db.get_session() as session:
            stmt = delete(UserResult).filter(
                UserResult.user_id == user_id,
                UserResult.sheet_name == sheet_name
            )
            await session.execute(stmt)
            await session.commit()
        
        # Инвалидируем кэш, так как данные могли измениться
        # (хотя это не влияет на ID вопросов, но на всякий случай)
    
    async def delete_all_user_progress(self):
        """
        Удалить весь прогресс всех пользователей из базы данных (async)
        """
        from database.models import UserResult
        from sqlalchemy import delete
        
        async with self.db.get_session() as session:
            # Удаляем все записи прогресса
            stmt = delete(UserResult)
            result = await session.execute(stmt)
            deleted_count = result.rowcount
            await session.commit()
            logger.info(f"Удалено записей прогресса пользователей: {deleted_count}")
            
            # Проверяем, что все записи действительно удалены
            from sqlalchemy import select, func
            check_stmt = select(func.count(UserResult.id))
            check_result = await session.execute(check_stmt)
            remaining_count = check_result.scalar() or 0
            
            if remaining_count > 0:
                logger.warning(f"После удаления осталось {remaining_count} записей прогресса! Повторная попытка удаления...")
                # Повторно удаляем оставшиеся записи
                stmt2 = delete(UserResult)
                await session.execute(stmt2)
                await session.commit()
                logger.info("Повторное удаление выполнено")
            
            return deleted_count

    async def delete_all_fsm_states(self) -> int:
        """
        Удалить все FSM-состояния пользователей (очистить таблицу fsm_states).
        Используется при полной перезагрузке вопросов из Excel.
        """
        from database.models import FSMState
        from sqlalchemy import delete

        async with self.db.get_session() as session:
            stmt = delete(FSMState)
            result = await session.execute(stmt)
            deleted_count = result.rowcount or 0
            await session.commit()
            logger.info(f"Удалено FSM-состояний пользователей: {deleted_count}")
            return deleted_count
    async def delete_all_question_tables(self):
        """
        Удалить все таблицы вопросов из базы данных (async)
        """
        from sqlalchemy import text
        
        async with self.db.engine.begin() as conn:
            # Получаем список всех таблиц вопросов (исключая системные таблицы и служебные таблицы бота)
            result = await conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name NOT IN ('user_results', 'questions_template', 'fsm_states')
                    AND table_name NOT LIKE 'pg_%'
                    AND table_name NOT LIKE 'sql_%'
                """)
            )
            tables = result.fetchall()
            
            # Удаляем каждую таблицу
            for table in tables:
                table_name = table[0]
                try:
                    await conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    logger.info(f"Таблица {table_name} удалена")
                except Exception as e:
                    logger.error(f"Ошибка при удалении таблицы {table_name}: {e}")
        
        # Очищаем кэш созданных таблиц и метаданные
        self.db.created_tables.clear()
        # Очищаем кэш существования таблиц
        self.db.clear_table_exists_cache()
        from database.models import _global_table_classes, Base
        _global_table_classes.clear()
        
        # Инвалидируем весь кэш ID вопросов
        self._invalidate_all_question_ids_cache()
        
        # Удаляем все таблицы вопросов из метаданных SQLAlchemy
        tables_to_remove = []
        for table_name in list(Base.metadata.tables.keys()):
            if table_name not in ('user_results', 'questions_template'):
                tables_to_remove.append(table_name)
        
        for table_name in tables_to_remove:
            try:
                Base.metadata.remove(Base.metadata.tables[table_name])
                logger.info(f"Таблица {table_name} удалена из метаданных SQLAlchemy")
            except Exception as e:
                logger.debug(f"Не удалось удалить таблицу {table_name} из метаданных: {e}")
        
        # Очищаем реестр классов
        classes_to_remove = []
        for class_name, class_obj in list(Base.registry._class_registry.items()):
            if (hasattr(class_obj, '__tablename__') and 
                class_obj.__tablename__ not in ('user_results', 'questions_template')):
                classes_to_remove.append(class_name)
        
        for class_name in classes_to_remove:
            try:
                del Base.registry._class_registry[class_name]
                logger.info(f"Класс {class_name} удален из реестра")
            except Exception as e:
                logger.debug(f"Не удалось удалить класс {class_name} из реестра: {e}")
    
    async def close(self):
        """Закрыть соединение с базой данных"""
        await self.db.close()


def get_db_service() -> DatabaseService:
    """Получить глобальный экземпляр DatabaseService"""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service


def set_db_service(db_service: DatabaseService):
    """Установить глобальный экземпляр DatabaseService (для инициализации в bot.py)"""
    global _db_service
    _db_service = db_service
