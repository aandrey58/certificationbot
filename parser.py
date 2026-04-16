"""
Парсер Excel файлов в PostgreSQL базу данных (async)
"""
import pandas as pd
from pathlib import Path
from database.models import Database
import config
import logging
import asyncio

logger = logging.getLogger(__name__)


class ExcelParser:
    """Класс для парсинга Excel файлов (async)"""
    
    def __init__(self, db: Database):
        self.db = db
    
    async def parse_excel_file(self, file_path: Path):
        """
        Парсить Excel файл и загрузить данные в базу данных (async)
        Каждый лист будет сохранен в отдельную таблицу
        """
        try:
            # Читаем все листы Excel файла (асинхронно через to_thread)
            excel_file = await asyncio.to_thread(pd.ExcelFile, file_path)
            sheet_names = excel_file.sheet_names.copy()  # Копируем список листов
            # Закрываем файл, чтобы он не блокировал доступ
            await asyncio.to_thread(excel_file.close)
            
            for sheet_name in sheet_names:
                logger.info(f"Обработка листа: {sheet_name}")
                
                # Проверяем, существует ли таблица и содержит ли она данные
                safe_name = self.db.get_safe_table_name(sheet_name)
                
                # Проверяем наличие данных в таблице (async)
                from sqlalchemy import select, func, text
                
                # Проверяем существование таблицы через SQL запрос
                async with self.db.engine.connect() as conn:
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
                    table_exists = result.scalar()
                
                if table_exists:
                    # Проверяем, есть ли данные в таблице (async)
                    QuestionModel = await self.db.get_table_for_sheet(sheet_name)
                    if QuestionModel:
                        async with self.db.get_session() as session:
                            stmt = select(func.count(QuestionModel.id))
                            result = await session.execute(stmt)
                            count = result.scalar()
                            if count > 0:
                                logger.info(f"Лист '{sheet_name}' уже обработан ({count} записей), пропускаем")
                                continue
                
                # Читаем данные листа (асинхронно через to_thread)
                df = await asyncio.to_thread(pd.read_excel, file_path, sheet_name=sheet_name, header=0)
                
                # Нормализуем названия столбцов: убираем пробелы
                df.columns = df.columns.str.strip()
                
                # Пропускаем пустые строки (где нет номера и вопроса)
                required_cols = ['№ п.п.', 'Вопрос']
                available_required = [col for col in required_cols if col in df.columns]
                if available_required:
                    df = df.dropna(subset=available_required, how='all')
                else:
                    # Если нужные столбцы не найдены, пропускаем лист
                    logger.warning(f"Не найдены обязательные столбцы в листе {sheet_name}: {required_cols}")
                    continue
                
                # Получаем или создаем таблицу для этого листа (async)
                QuestionModel = await self.db.create_table_for_sheet(sheet_name)
                
                async with self.db.get_session() as session:
                    try:
                        # Очищаем таблицу перед загрузкой новых данных (если она существует)
                        if table_exists:
                            from sqlalchemy import delete
                            stmt = delete(QuestionModel)
                            await session.execute(stmt)
                            await session.commit()
                        
                        # Обрабатываем каждую строку данных (данные начинаются со строки 2 в Excel)
                        questions_to_add = []
                        for idx, row in df.iterrows():
                            try:
                                # Пропускаем пустые строки
                                if pd.isna(row.get('№ п.п.')) and pd.isna(row.get('Вопрос')):
                                    continue
                                
                                # Извлекаем данные из строки, обрабатывая NaN значения
                                def safe_str(value):
                                    if pd.isna(value):
                                        return ''
                                    return str(value).strip()
                                
                                # Функция для безопасного получения значения из строки
                                def get_value(row, possible_names):
                                    for name in possible_names:
                                        if name in row.index:
                                            return safe_str(row[name])
                                    return ''
                                
                                question_data = {
                                    'number': get_value(row, ['№ п.п.', 'номер', '№']),
                                    'question': get_value(row, ['Вопрос', 'вопрос']),
                                    'answer1': get_value(row, ['1']),
                                    'check1': get_value(row, ['проверка', 'проверка1']),
                                    'answer2': get_value(row, ['2']),
                                    'check2': get_value(row, ['проверка2']),
                                    'answer3': get_value(row, ['3']),
                                    'check3': get_value(row, ['проверка3']),
                                    'answer4': get_value(row, ['4']),
                                    'check4': get_value(row, ['проверка4']),
                                    'answer5': get_value(row, ['5']),
                                    'check5': get_value(row, ['проверка5']),
                                    'answer6': get_value(row, ['6']),
                                    'check6': get_value(row, ['проверка6']),
                                    'normative_basis': get_value(row, ['Нормативная основа вопроса', 'Нормативная основа']),
                                }
                                
                                # Пропускаем строки без вопроса
                                if not question_data['question']:
                                    continue
                                
                                # Создаем объект вопроса
                                question = QuestionModel(**question_data)
                                questions_to_add.append(question)
                                
                            except Exception as e:
                                logger.error(f"Ошибка при обработке строки {idx + 2} листа {sheet_name}: {e}")
                                continue
                        
                        # Добавляем все вопросы одним запросом (batch insert)
                        if questions_to_add:
                            session.add_all(questions_to_add)
                            await session.commit()
                            logger.info(f"Лист {sheet_name} успешно загружен в базу данных ({len(questions_to_add)} записей)")
                        
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"Ошибка при загрузке листа {sheet_name}: {e}")
                        raise
            
            logger.info(f"Файл {file_path} успешно обработан")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге файла {file_path}: {e}", exc_info=True)
            return False
    
    async def get_available_sheets(self, file_path: Path):
        """Получить список доступных листов из Excel файла (async метод)"""
        try:
            excel_file = await asyncio.to_thread(pd.ExcelFile, file_path)
            sheet_names = excel_file.sheet_names.copy()  # Копируем список листов
            # Закрываем файл, чтобы он не блокировал доступ
            await asyncio.to_thread(excel_file.close)
            return sheet_names
        except Exception as e:
            logger.error(f"Ошибка при чтении листов файла {file_path}: {e}")
            return []


async def parse_data_directory():
    """Парсить все Excel файлы из папки data (async)"""
    db = Database(config.DATABASE_URL)
    parser = ExcelParser(db)
    
    await db.init_db()
    
    data_dir = Path(config.DATA_DIR)
    # Выполняем glob в отдельном потоке
    excel_files = await asyncio.to_thread(lambda: list(data_dir.glob("*.xlsx")))
    
    for excel_file in excel_files:
        if excel_file.name.startswith("~$"):
            continue  # Пропускаем временные файлы Excel
        await parser.parse_excel_file(excel_file)
    
    await db.close()
    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(parse_data_directory())
