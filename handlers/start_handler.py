"""
Обработчик команды /start
"""
import logging
import asyncio
from pathlib import Path
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from keyboards.inline_keyboards import (
    get_main_menu_keyboard,
    get_preparation_keyboard,
    get_question_keyboard,
    get_question_keyboard_with_status,
    get_back_keyboard,
    get_progress_keyboard,
    get_questions_progress_keyboard,
    get_continue_or_reset_keyboard,
    get_progress_question_back_keyboard,
    get_assistant_keyboard,
    get_assistant_all_questions_keyboard,
    get_assistant_questions_keyboard,
    get_assistant_question_back_keyboard,
    get_search_results_keyboard,
    get_search_result_back_keyboard,
    get_search_sheet_keyboard
)
from services.database_service import get_db_service

router = Router()
logger = logging.getLogger(__name__)

# Флаг, что FSM-состояния были глобально сброшены (при загрузке нового Excel)
_FSM_RESET_OCCURRED = False


# Вспомогательные функции для работы с выбранными ответами в FSM state
async def get_selected_answers(state: FSMContext, sheet_name: str, question_id: int) -> set[int]:
    """Получить выбранные ответы для вопроса из FSM state"""
    data = await state.get_data()
    selected = data.get("selected_answers_per_question", {})
    sheet_answers = selected.get(sheet_name, {})
    answer_list = sheet_answers.get(question_id, [])
    return set(answer_list)


async def set_selected_answers(state: FSMContext, sheet_name: str, question_id: int, answers: set[int]):
    """Установить выбранные ответы для вопроса в FSM state"""
    data = await state.get_data()
    selected = data.get("selected_answers_per_question", {})
    if sheet_name not in selected:
        selected[sheet_name] = {}
    selected[sheet_name][question_id] = list(answers)
    await state.update_data(selected_answers_per_question=selected)


async def toggle_answer_selection(state: FSMContext, sheet_name: str, question_id: int, answer_number: int) -> set[int]:
    """Переключить выбор ответа (добавить/убрать) и вернуть обновленное множество"""
    current_answers = await get_selected_answers(state, sheet_name, question_id)
    if answer_number in current_answers:
        current_answers.discard(answer_number)
    else:
        current_answers.add(answer_number)
    await set_selected_answers(state, sheet_name, question_id, current_answers)
    return current_answers


async def clear_selected_answers_for_sheet(state: FSMContext, sheet_name: str):
    """Очистить все выбранные ответы для указанного листа"""
    data = await state.get_data()
    selected = data.get("selected_answers_per_question", {})
    if sheet_name in selected:
        del selected[sheet_name]
        await state.update_data(selected_answers_per_question=selected)


class SearchStates(StatesGroup):
    """Состояния для поиска по вопросам"""
    selecting_sheet = State()  # Состояние выбора области аттестации для поиска
    waiting_for_search_query = State()
    search_results = State()  # Состояние для хранения результатов поиска


class UploadStates(StatesGroup):
    """Состояния для загрузки Excel файла"""
    waiting_for_excel_file = State()


def mark_fsm_reset_occurred():
    """Пометить, что все FSM-состояния были сброшены (после загрузки Excel)."""
    global _FSM_RESET_OCCURRED
    _FSM_RESET_OCCURRED = True


def _maybe_prepend_fsm_reset_notice(text: str) -> str:
    """
    Если недавно была полная перезагрузка вопросов (сброс FSM),
    один раз добавляем пользователю предупреждение в начало текста.
    """
    global _FSM_RESET_OCCURRED
    if _FSM_RESET_OCCURRED:
        _FSM_RESET_OCCURRED = False
        notice = (
            "⚠️ Вопросы и прогресс были обновлены.\n"
            "Ваши предыдущие состояния и прогресс сброшены.\n\n"
        )
        return notice + text
    return text


async def _redirect_if_fsm_reset(callback: CallbackQuery, state: FSMContext) -> bool:
    """
    Если во время работы бота была выполнена полная перезагрузка вопросов (сброс FSM),
    при любом взаимодействии в процессе подготовки пользователя сразу выкидывает в меню
    с предупреждением.

    Returns:
        True, если был выполнен редирект в меню и обработчик дальше выполнять не нужно.
    """
    global _FSM_RESET_OCCURRED
    if _FSM_RESET_OCCURRED:
        # Используем уже существующий обработчик возврата в меню, чтобы не дублировать логику
        await back_to_main(callback, state)
        # back_to_main сам вызовет _maybe_prepend_fsm_reset_notice и тем самым сбросит флаг
        return True
    return False
def _append_normative_basis(text: str, question: dict, max_message_length: int = 4000) -> str:
    """
    Добавить к тексту ссылку на нормативное требование из поля 'normative_basis', если оно есть.

    Формат: внизу вопроса отдельной строкой: "Нормативное требование: ..."
    """
    normative = (question.get("normative_basis") or "").strip()
    if not normative:
        return text

    law_line = f"\n\nНормативное требование: {normative}"

    # Следим за ограничением длины сообщения Telegram
    if len(text) + len(law_line) > max_message_length:
        # Немного обрезаем нормативку, чтобы влезло
        remaining = max_message_length - len(text) - len("\n\nНормативное требование: ...")
        if remaining > 0:
            law_line = f"\n\nНормативное требование: {normative[:remaining]}..."
        else:
            # Совсем нет места – не добавляем, чтобы не сломать сообщение
            return text

    return text + law_line

def _format_correct_answers_for_search(question: dict) -> str:
    """Сформировать строки '✅ Правильный ответ: ...' для текстовых результатов поиска."""
    answers = question.get("answers", [])
    checks = question.get("checks", [])

    correct_texts: list[str] = []
    for i, check in enumerate(checks, 1):
        if check and str(check).strip() == "+" and i <= len(answers):
            correct_texts.append(answers[i - 1])

    if not correct_texts:
        return "✅ Правильный ответ: (не указан)"

    # Если правильных ответов несколько — выводим каждый на отдельной строке
    return "\n".join([f"✅ Правильный ответ: {t}" for t in correct_texts])


def _format_answers_with_checks_for_search(question: dict) -> str:
    """
    Вернуть строки ответов с отметками правильных/неправильных для выдачи поиска.
    Формат: "1. ✅ текст" или "2. ❌ текст".
    """
    answers = question.get("answers", [])
    checks = question.get("checks", [])

    lines = []
    for i, answer in enumerate(answers, 1):
        mark = "✅" if i <= len(checks) and str(checks[i - 1]).strip() == "+" else "❌"
        lines.append(f"{i}. {mark} {answer}")
    return "\n".join(lines)


async def _render_search_results_page_text(
    db_service,
    search_query: str,
    results: list[dict],
    page: int,
    items_per_page: int = 3,
) -> str:
    """Сформировать текст результатов поиска (по умолчанию 3 вопроса на страницу)."""
    total_found = len(results)
    total_pages = (total_found + items_per_page - 1) // items_per_page if results else 1
    page = max(0, min(page, max(total_pages - 1, 0)))

    header = f"🔍 Поиск по запросу: <b>'{search_query}'</b>\n\nНайдено: {total_found}"
    header += f"\nСтраница: {page + 1}/{max(total_pages, 1)}\n\n"

    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_found)
    page_results = results[start_idx:end_idx]

    if not page_results:
        return header + "❌ Ничего не найдено. Попробуйте другой запрос."

    # Группируем по sheet_name, чтобы загрузить вопросы пачками
    sheet_to_ids: dict[str, list[int]] = {}
    for r in page_results:
        sheet_to_ids.setdefault(r["sheet_name"], []).append(r["question_id"])

    # Загружаем вопросы из БД пачками
    questions_map: dict[tuple[str, int], dict] = {}
    for sheet_name, ids in sheet_to_ids.items():
        fetched = await db_service.get_questions_by_ids(sheet_name, ids)
        for qid, q in fetched.items():
            questions_map[(sheet_name, qid)] = q

    lines: list[str] = [header]
    for r in page_results:
        qnum = r.get("question_number") or "?"
        sheet_name = r["sheet_name"]
        qid = r["question_id"]
        question = questions_map.get((sheet_name, qid))

        question_text = question["question"] if question else r.get("question", "")
        lines.append(f"[{qnum}] {question_text}")
        if question:
            lines.append(_format_answers_with_checks_for_search(question))
        else:
            lines.append("✅ Правильный ответ: (не удалось загрузить)")
        lines.append("")  # пустая строка между вопросами

    return "\n".join(lines).strip()


@router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    # Сбрасываем состояние при старте
    await state.clear()
    base_text = "👋 Добро пожаловать в бота для подготовки к аттестации!\n\nВыберите действие:"
    text = _maybe_prepend_fsm_reset_notice(base_text)
    await message.answer(text, reply_markup=get_main_menu_keyboard(user_id=message.from_user.id))


@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    # Сбрасываем состояние при возврате в главное меню
    await state.clear()
    base_text = "👋 Добро пожаловать в бота для подготовки к аттестации!\n\nВыберите действие:"
    text = _maybe_prepend_fsm_reset_notice(base_text)
    await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard(user_id=callback.from_user.id))
    await callback.answer()


@router.callback_query(F.data == "preparation")
async def show_preparation_menu(callback: CallbackQuery):
    """Показать меню выбора области аттестации (первая страница)"""
    await callback.answer()
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        text = "❌ Не найдено доступных областей аттестации.\nУбедитесь, что Excel файлы загружены в папку data."
        await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard(user_id=callback.from_user.id))
    else:
        text = "📚 Выберите область аттестации:"
        keyboard = get_preparation_keyboard(sheet_names, page=0)
        await callback.message.edit_text(text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("prep_page_"))
async def handle_preparation_pagination(callback: CallbackQuery):
    """Обработчик пагинации меню подготовки"""
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        await callback.answer("❌ Не найдено доступных областей аттестации", show_alert=True)
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 7
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Проверяем специальные случаи для неактивных кнопок
    if callback.data == "prep_page_back_disabled":
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if callback.data == "prep_page_forward_disabled":
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Формат: "prep_page_0", "prep_page_1", etc.
    page_str = callback.data.replace("prep_page_", "")
    try:
        page = int(page_str)
    except ValueError:
        await callback.answer()
        return
    
    # Проверяем границы
    if page < 0:
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if page >= total_pages:
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    await callback.answer()
    
    text = "📚 Выберите область аттестации:"
    keyboard = get_preparation_keyboard(sheet_names, page=page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            # Сообщение не изменилось - это нормально, игнорируем
            logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
        else:
            raise


@router.callback_query(F.data.startswith("sheet_"))
async def handle_sheet_selection(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора листа - показывает первый вопрос"""
    logger.info(f"Начало обработки выбора листа: {callback.data}")
    
    # Сразу отвечаем на callback, чтобы не истекал таймаут
    await callback.answer()

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return
    
    # Извлекаем имя листа из callback_data
    # Формат: "sheet_название_листа"
    sheet_name = callback.data.replace("sheet_", "", 1)
    logger.info(f"Имя листа: {sheet_name}")
    
    # Очищаем выбранные ответы для этого листа при начале новой сессии
    await clear_selected_answers_for_sheet(state, sheet_name)
    
    try:
        db_service = get_db_service()
        logger.info("DatabaseService создан")
        user_id = callback.from_user.id
        
        # Проверяем, есть ли у пользователя прогресс по этой области
        has_progress = await db_service.has_user_progress(user_id, sheet_name)
        
        if has_progress:
            logger.info(f"У пользователя {user_id} есть прогресс по области '{sheet_name}'")
            text = "🤔 Вы уже проходили подготовку по этой области.\nПродолжить или начать сначала?"
            keyboard = get_continue_or_reset_keyboard(sheet_name)
            try:
                await callback.message.edit_text(text, reply_markup=keyboard)
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e).lower():
                    raise
            return
        
        # Если прогресса нет, начинаем с первого вопроса
        logger.info(f"У пользователя {user_id} нет прогресса по области '{sheet_name}', начинаем с первого вопроса")
        
        # Получаем первый вопрос из выбранного листа
        logger.info(f"Запрос первого вопроса из листа: {sheet_name}")
        question = await db_service.get_question_by_number(sheet_name, question_number=1)
        logger.info(f"Получен вопрос: {question is not None}")
        
        if not question:
            logger.warning(f"Вопрос не найден для листа: {sheet_name}")
            text = (
                f"❌ В области аттестации '{sheet_name}' не найдено вопросов.\n\n"
                "Возможные причины:\n"
                "• Данные не были загружены из Excel файла\n"
                "• Файл не содержит данных для этого листа\n\n"
                "Попробуйте перезапустить бота для повторной загрузки данных."
            )
            await callback.message.edit_text(text, reply_markup=get_back_keyboard())
            return
        
        logger.info(f"Формирование сообщения для вопроса ID: {question['id']}")
        
        # Получаем номер вопроса и общее количество вопросов (одним запросом)
        question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question['id'])
        
        # Формируем текст вопроса с номером (ограничение Telegram - 4096 символов)
        question_header = f"[{question_number}/{total_questions}]" if question_number else ""
        text = f"{question_header} ❓ {question['question']}\n\n"
        
        # Добавляем нумерованные ответы
        MAX_MESSAGE_LENGTH = 4000  # Оставляем запас для эмодзи и форматирования
        for i, answer in enumerate(question['answers'], 1):
            answer_line = f"{i}. {answer}\n"
            if len(text) + len(answer_line) > MAX_MESSAGE_LENGTH:
                text += "...\n(Текст обрезан из-за ограничения длины сообщения)"
                break
            text += answer_line

        # Добавляем ссылку на закон (если есть)
        text = _append_normative_basis(text, question, max_message_length=MAX_MESSAGE_LENGTH)
        
        logger.info(f"Длина текста сообщения: {len(text)} символов")
        
        # Создаем клавиатуру с кнопками ответов
        logger.info("Создание клавиатуры")
        keyboard = get_question_keyboard(
            sheet_name=sheet_name,
            question_id=question['id'],
            answers_count=len(question['answers'])
        )
        
        logger.info("Отправка сообщения")
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
            logger.info("Сообщение успешно отправлено")
        except TelegramBadRequest as e:
            # Ошибка "message is not modified" - нормальная ситуация, если пользователь быстро нажимает кнопку
            if "message is not modified" in str(e).lower():
                logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
                await callback.answer()
            else:
                raise  # Если другая ошибка - пробрасываем дальше
        
    except Exception as e:
        logger.error(f"Критическая ошибка в handle_sheet_selection: {e}", exc_info=True)
        try:
            text = f"❌ Произошла ошибка при загрузке вопроса.\n\nПопробуйте позже."
            await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        except:
            pass


@router.callback_query(F.data == "assistant")
async def handle_assistant(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Помощник'"""
    await callback.answer()
    # Сбрасываем состояние при возврате в подменю помощника
    await state.clear()
    text = "🤖 Помощник\n\nВыберите действие:"
    
    try:
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_assistant")
        else:
            raise


@router.callback_query(F.data == "assistant_all_questions")
async def handle_assistant_all_questions(callback: CallbackQuery):
    """Обработчик кнопки 'Все вопросы' - показывает список областей"""
    await callback.answer()
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        text = "❌ Не найдено доступных областей аттестации.\nУбедитесь, что Excel файлы загружены в папку data."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
    else:
        text = "📋 Все вопросы\n\nВыберите область:"
        keyboard = get_assistant_all_questions_keyboard(sheet_names, page=0)
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                logger.debug("Message is not modified in handle_assistant_all_questions")
            else:
                raise


@router.callback_query(F.data.startswith("assistant_all_page_"))
async def handle_assistant_all_pagination(callback: CallbackQuery):
    """Обработчик пагинации списка областей в 'Все вопросы'"""
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        await callback.answer("❌ Не найдено доступных областей аттестации", show_alert=True)
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 7
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Проверяем специальные случаи для неактивных кнопок
    if callback.data == "assistant_all_page_back_disabled":
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if callback.data == "assistant_all_page_forward_disabled":
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Формат: "assistant_all_page_0", "assistant_all_page_1", etc.
    page_str = callback.data.replace("assistant_all_page_", "")
    try:
        page = int(page_str)
    except ValueError:
        await callback.answer()
        return
    
    # Проверяем границы
    if page < 0:
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if page >= total_pages:
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    await callback.answer()
    text = "📋 Все вопросы\n\nВыберите область:"
    keyboard = get_assistant_all_questions_keyboard(sheet_names, page=page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_assistant_all_pagination")
        else:
            raise


@router.callback_query(F.data.startswith("assistant_sheet_"))
async def handle_assistant_sheet_selection(callback: CallbackQuery):
    """Обработчик выбора области в 'Все вопросы' - показывает матрицу вопросов"""
    await callback.answer()
    
    # Формат: "assistant_sheet_Б 2.2."
    sheet_name = callback.data.replace("assistant_sheet_", "", 1)
    
    db_service = get_db_service()
    
    # Получаем все ID вопросов для области
    question_ids = await db_service.get_all_question_ids_for_sheet(sheet_name)
    
    if not question_ids:
        text = f"❌ В области '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
        return
    
    text = f"📋 {sheet_name}\n\nВсего вопросов: {len(question_ids)}"
    
    # Создаем клавиатуру с вопросами (без эмодзи)
    keyboard = get_assistant_questions_keyboard(
        sheet_name=sheet_name,
        question_ids=question_ids,
        page=0
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_assistant_sheet_selection")
        else:
            raise


@router.callback_query(F.data.startswith("assistant_questions_page_"))
async def handle_assistant_questions_pagination(callback: CallbackQuery):
    """Обработчик пагинации матрицы вопросов в 'Все вопросы'"""
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
            logger.debug("Callback query is too old in handle_assistant_questions_pagination")
            return
        raise
    
    # Формат: "assistant_questions_page_{sheet_name}_{page}" или "assistant_questions_page_back_disabled_{sheet_name}"
    data = callback.data.replace("assistant_questions_page_", "")
    
    # Проверяем специальные случаи для неактивных кнопок
    if data.startswith("back_disabled_"):
        sheet_name = data.replace("back_disabled_", "", 1)
        try:
            await callback.answer("Это первая страница", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (back_disabled)")
        return
    
    if data.startswith("forward_disabled_"):
        sheet_name = data.replace("forward_disabled_", "", 1)
        try:
            await callback.answer("Это последняя страница", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (forward_disabled)")
        return
    
    # Обычная пагинация: формат "{sheet_name}_{page}"
    # Находим последнее подчеркивание, чтобы разделить sheet_name и page
    last_underscore_idx = data.rfind("_")
    if last_underscore_idx == -1:
        try:
            await callback.answer("Ошибка обработки", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (error)")
        return
    
    sheet_name = data[:last_underscore_idx]
    try:
        page = int(data[last_underscore_idx + 1:])
    except ValueError:
        try:
            await callback.answer("Ошибка обработки", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (value error)")
        return
    
    db_service = get_db_service()
    
    # Получаем все ID вопросов для области
    question_ids = await db_service.get_all_question_ids_for_sheet(sheet_name)
    
    if not question_ids:
        text = f"❌ В области '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 25
    total_pages = (len(question_ids) + items_per_page - 1) // items_per_page if question_ids else 1
    
    # Проверяем границы
    if page < 0:
        try:
            await callback.answer("Это первая страница", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (page < 0)")
        return
    
    if page >= total_pages:
        try:
            await callback.answer("Это последняя страница", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "query id is invalid" in str(e).lower():
                logger.debug("Callback query is too old in handle_assistant_questions_pagination (page >= total_pages)")
        return
    
    text = f"📋 {sheet_name}\n\nВсего вопросов: {len(question_ids)}"
    
    # Создаем клавиатуру с вопросами
    keyboard = get_assistant_questions_keyboard(
        sheet_name=sheet_name,
        question_ids=question_ids,
        page=page
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_assistant_questions_pagination")
        else:
            raise


@router.callback_query(F.data.startswith("assistant_question|"))
async def handle_assistant_question_click(callback: CallbackQuery):
    """Обработчик клика на вопрос в 'Все вопросы' - показывает карточку вопроса с правильными/неправильными ответами"""
    await callback.answer()
    
    # Формат: "assistant_question|Б 2.2.|123|0" (sheet_name|question_id|page)
    parts = callback.data.split("|")
    
    if len(parts) < 4:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    sheet_name = parts[1]
    try:
        question_id = int(parts[2])
        page = int(parts[3])
    except ValueError:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    db_service = get_db_service()
    
    # Получаем вопрос из базы данных
    question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        text = f"❌ Вопрос не найден."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
        return
    
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question_id)
    
    # Формируем текст карточки вопроса
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Определяем правильные ответы
    correct_answer_numbers = set()
    for i, check in enumerate(question['checks'], 1):
        if i <= len(question['checks']) and check.strip() == '+':
            correct_answer_numbers.add(i)
    
    # Добавляем варианты ответов с отметками правильных/неправильных
    for i, answer in enumerate(question['answers'], 1):
        if i in correct_answer_numbers:
            text += f"{i}. ✅ {answer}\n"
        else:
            text += f"{i}. ❌ {answer}\n"

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question)
    
    # Создаем клавиатуру с кнопкой "Назад"
    keyboard = get_assistant_question_back_keyboard(sheet_name, page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@router.callback_query(F.data == "assistant_search")
async def handle_assistant_search(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Поиск по вопросам' - показывает список областей для выбора"""
    await callback.answer()
    # Очищаем предыдущее состояние поиска перед новым поиском
    await state.clear()
    
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        text = "❌ Не найдено доступных областей аттестации.\nУбедитесь, что Excel файлы загружены в папку data."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
    else:
        text = "🔍 Поиск по вопросам\n\nВыберите область аттестации:"
        # Используем ту же клавиатуру, что и для подготовки, но с другим префиксом callback_data
        keyboard = get_search_sheet_keyboard(sheet_names, page=0)
        await state.set_state(SearchStates.selecting_sheet)
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                logger.debug("Message is not modified in handle_assistant_search")
            else:
                raise


@router.callback_query(F.data.startswith("search_sheet_"))
async def handle_search_sheet_selection(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора области для поиска - запрашивает текст для поиска"""
    await callback.answer()
    
    # Формат: "search_sheet_Б 2.2."
    sheet_name = callback.data.replace("search_sheet_", "", 1)
    
    # Сохраняем выбранную область в состоянии
    await state.update_data(search_sheet_name=sheet_name)
    
    text = f"🔍 Поиск по вопросам\n\nОбласть: <b>{sheet_name}</b>\n\nВведите текст для поиска:"
    
    # Устанавливаем состояние ожидания ввода текста поиска
    await state.set_state(SearchStates.waiting_for_search_query)
    
    try:
        await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode="HTML")
        # Сохраняем message_id сообщения с запросом поиска для последующего удаления
        await state.update_data(search_request_message_id=callback.message.message_id)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_search_sheet_selection")
        else:
            raise


@router.callback_query(F.data.startswith("search_sheet_page_"))
async def handle_search_sheet_pagination(callback: CallbackQuery):
    """Обработчик пагинации списка областей для поиска"""
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        await callback.answer("❌ Не найдено доступных областей аттестации", show_alert=True)
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 7
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Проверяем специальные случаи для неактивных кнопок
    if callback.data == "search_sheet_page_back_disabled":
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if callback.data == "search_sheet_page_forward_disabled":
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Формат: "search_sheet_page_0", "search_sheet_page_1", etc.
    page_str = callback.data.replace("search_sheet_page_", "")
    try:
        page = int(page_str)
    except ValueError:
        await callback.answer()
        return
    
    # Проверяем границы
    if page < 0:
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if page >= total_pages:
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    await callback.answer()
    
    text = "🔍 Поиск по вопросам\n\nВыберите область аттестации:"
    keyboard = get_search_sheet_keyboard(sheet_names, page=page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_search_sheet_pagination")
        else:
            raise


@router.message(SearchStates.waiting_for_search_query)
async def handle_search_query(message: Message, state: FSMContext):
    """Обработчик текстового запроса для поиска"""
    # Получаем message_id сообщения с запросом поиска и удаляем его
    data = await state.get_data()
    search_request_message_id = data.get('search_request_message_id')
    if search_request_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=search_request_message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение с запросом поиска: {e}")

    # Удаляем предыдущие результаты поиска, если они существуют
    previous_search_results_message_id = data.get('previous_search_results_message_id')
    if previous_search_results_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=previous_search_results_message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить предыдущие результаты поиска: {e}")

    # Удаляем предыдущее сообщение об ошибке (если было)
    previous_error_message_id = data.get('previous_error_message_id')
    if previous_error_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=previous_error_message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить предыдущее сообщение об ошибке: {e}")

    # Удаляем сообщение пользователя с текстом поиска
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение пользователя: {e}")
    
    search_query = message.text.strip()
    
    if not search_query:
        error_message = await message.answer("❌ Пожалуйста, введите текст для поиска.")
        # Сохраняем message_id сообщения об ошибке для последующего удаления
        await state.update_data(
            search_request_message_id=None,
            previous_error_message_id=error_message.message_id
        )
        return
    
    db_service = get_db_service()
    
    # Получаем выбранную область из состояния
    search_sheet_name = data.get('search_sheet_name')
    
    # Выполняем поиск с обработкой ошибок (только в выбранной области)
    try:
        all_results = await db_service.search_questions(search_query, sheet_name=search_sheet_name)
    except Exception as e:
        logger.error(f"Ошибка при выполнении поиска: {e}", exc_info=True)
        error_text = "❌ Произошла ошибка при выполнении поиска. Попробуйте позже."
        error_message = await message.answer(error_text, reply_markup=get_back_keyboard())
        await state.update_data(
            search_request_message_id=None,
            previous_error_message_id=error_message.message_id
        )
        return
    
    # Сохраняем все результаты поиска (не ограничиваем, пагинация будет в клавиатуре)
    total_found = len(all_results)

    if not all_results:
        # Ничего не найдено, остаемся в режиме поиска и предлагаем только завершить поиск
        sheet_info = f" в области <b>'{search_sheet_name}'</b>" if search_sheet_name else ""
        text = f"🔍 Поиск по запросу: <b>'{search_query}'</b>{sheet_info}\n\n❌ Ничего не найдено. Попробуйте другой запрос."
        await state.update_data(
            search_query=search_query,
            search_results=[],
            total_found=0,
            search_request_message_id=None,
            previous_error_message_id=None,
            search_results_page=0,
        )
        await state.set_state(SearchStates.search_results)
        keyboard = get_search_results_keyboard([], page=0, items_per_page=3)
        sent_message = await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        # Сохраняем id сообщения с результатом "ничего не найдено", чтобы удалить его при следующем поиске
        await state.update_data(previous_search_results_message_id=sent_message.message_id)
        return
    
    # Формируем текст первой страницы (по 3 вопроса)
    # Добавляем информацию об области в заголовок, если область выбрана
    search_query_with_sheet = search_query
    if search_sheet_name:
        search_query_with_sheet = f"{search_query} (область: {search_sheet_name})"
    
    text = await _render_search_results_page_text(
        db_service=db_service,
        search_query=search_query_with_sheet,
        results=all_results,
        page=0,
        items_per_page=3,
    )
    
    # Сохраняем все результаты поиска и запрос в состояние FSM
    # Удаляем search_request_message_id и previous_error_message_id, так как сообщения уже удалены
    # Сохраняем исходный запрос (без области) и область отдельно
    await state.update_data(
        search_query=search_query,  # Исходный запрос без области
        search_sheet_name=search_sheet_name,  # Сохраняем область для повторного поиска
        search_results=all_results,
        total_found=total_found,
        search_request_message_id=None,
        previous_error_message_id=None,
        search_results_page=0  # Начинаем с первой страницы
    )
    
    # Устанавливаем состояние поиска с результатами
    await state.set_state(SearchStates.search_results)
    
    # Создаем клавиатуру с результатами поиска (первая страница)
    keyboard = get_search_results_keyboard(all_results, page=0, items_per_page=3)
    
    sent_message = await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await state.update_data(previous_search_results_message_id=sent_message.message_id)


@router.message(SearchStates.search_results)
async def handle_search_query_in_results(message: Message, state: FSMContext):
    """
    Повторный поиск по вопросам: в режиме результатов любой новый текст запускает новый поиск.
    Сохраняем выбранную область из предыдущего поиска.
    """
    # Сохраняем выбранную область из предыдущего поиска перед новым поиском
    data = await state.get_data()
    search_sheet_name = data.get('search_sheet_name')
    
    # Переходим в состояние ожидания ввода текста
    await state.set_state(SearchStates.waiting_for_search_query)
    
    # Вызываем обработчик поиска (он использует сохраненную область)
    await handle_search_query(message, state)


@router.callback_query(F.data.startswith("search_result|"))
async def handle_search_result_click(callback: CallbackQuery, state: FSMContext):
    """Обработчик клика на результат поиска - показывает карточку вопроса с правильными/неправильными ответами"""
    await callback.answer()
    
    # Формат: "search_result|Б 2.2.|123" (sheet_name|question_id)
    parts = callback.data.split("|")
    
    if len(parts) < 3:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    sheet_name = parts[1]
    try:
        question_id = int(parts[2])
    except ValueError:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    db_service = get_db_service()
    
    # Получаем вопрос из базы данных
    question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        text = f"❌ Вопрос не найден."
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
        return
    
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question_id)
    
    # Формируем текст карточки вопроса
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Определяем правильные ответы
    correct_answer_numbers = set()
    for i, check in enumerate(question['checks'], 1):
        if i <= len(question['checks']) and check.strip() == '+':
            correct_answer_numbers.add(i)
    
    # Добавляем варианты ответов с отметками правильных/неправильных
    for i, answer in enumerate(question['answers'], 1):
        if i in correct_answer_numbers:
            text += f"{i}. ✅ {answer}\n"
        else:
            text += f"{i}. ❌ {answer}\n"

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question)
    
    # Убеждаемся, что состояние поиска сохранено (результаты уже должны быть в состоянии из handle_search_query)
    current_state = await state.get_state()
    if current_state != SearchStates.search_results:
        # Если состояние потеряно, пытаемся восстановить из данных
        data = await state.get_data()
        if not data.get('search_results'):
            # Если данных нет, возвращаемся в меню помощника
            await state.clear()
            text = "❌ Результаты поиска не найдены."
            await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
            return
        await state.set_state(SearchStates.search_results)
    
    # Создаем клавиатуру с кнопкой "Назад" к результатам поиска
    keyboard = get_search_result_back_keyboard()
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@router.callback_query(F.data.startswith("search_page_"))
async def handle_search_results_pagination(callback: CallbackQuery, state: FSMContext):
    """Обработчик пагинации результатов поиска"""
    await callback.answer()
    
    # Получаем сохраненные результаты поиска из состояния
    data = await state.get_data()
    search_query = data.get('search_query', '')
    results = data.get('search_results', [])
    total_found = data.get('total_found', len(results))
    
    if not results:
        await callback.answer("❌ Результаты поиска не найдены", show_alert=True)
        return
    
    # Проверяем специальные случаи для неактивных кнопок
    if callback.data == "search_page_back_disabled":
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if callback.data == "search_page_forward_disabled":
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Формат: "search_page_0", "search_page_1", etc.
    page_str = callback.data.replace("search_page_", "")
    try:
        page = int(page_str)
    except ValueError:
        await callback.answer()
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 3
    total_pages = (len(results) + items_per_page - 1) // items_per_page if results else 1
    
    # Проверяем границы
    if page < 0:
        page = 0
    elif page >= total_pages:
        page = total_pages - 1
    
    # Формируем search_query с информацией об области для отображения
    search_sheet_name = data.get('search_sheet_name')
    search_query_display = search_query
    if search_sheet_name:
        search_query_display = f"{search_query} (область: {search_sheet_name})"
    
    # Формируем текст страницы результатов (по 3 вопроса)
    db_service = get_db_service()
    text = await _render_search_results_page_text(
        db_service=db_service,
        search_query=search_query_display,
        results=results,
        page=page,
        items_per_page=items_per_page,
    )
    
    # Создаем клавиатуру с результатами поиска для текущей страницы
    keyboard = get_search_results_keyboard(results, page=page, items_per_page=items_per_page)
    
    # Сохраняем текущую страницу в состоянии
    await state.update_data(search_results_page=page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_search_results_pagination")
        else:
            raise


@router.callback_query(F.data == "back_to_search_results")
async def handle_back_to_search_results(callback: CallbackQuery, state: FSMContext):
    """Обработчик возврата к результатам поиска"""
    await callback.answer()
    
    # Получаем сохраненные результаты поиска из состояния
    data = await state.get_data()
    search_query = data.get('search_query', '')
    search_sheet_name = data.get('search_sheet_name')
    results = data.get('search_results', [])
    total_found = data.get('total_found', len(results))
    page = data.get('search_results_page', 0)  # Получаем сохраненную страницу
    
    if not results:
        # Если результатов нет, возвращаемся в меню помощника
        await state.clear()
        text = "🤖 Помощник\n\nВыберите действие:"
        await callback.message.edit_text(text, reply_markup=get_assistant_keyboard())
        return
    
    # Формируем search_query с информацией об области для отображения
    search_query_display = search_query
    if search_sheet_name:
        search_query_display = f"{search_query} (область: {search_sheet_name})"
    
    # Формируем текст с результатами поиска (по 3 вопроса)
    db_service = get_db_service()
    text = await _render_search_results_page_text(
        db_service=db_service,
        search_query=search_query_display,
        results=results,
        page=page,
        items_per_page=3,
    )
    
    # Создаем клавиатуру с результатами поиска для сохраненной страницы
    keyboard = get_search_results_keyboard(results, page=page, items_per_page=3)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_back_to_search_results")
        else:
            raise


@router.callback_query(F.data == "finish_search")
async def handle_finish_search(callback: CallbackQuery, state: FSMContext):
    """Завершить поиск и вернуться в главное меню."""
    await callback.answer()
    # Переиспользуем логику возврата в главное меню
    await back_to_main(callback, state)


async def show_question(message, db_service, sheet_name: str, question: dict, state: FSMContext = None):
    """Вспомогательная функция для отображения вопроса"""
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question['id'])
    
    # Формируем текст вопроса с номером (ограничение Telegram - 4096 символов)
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Добавляем нумерованные ответы
    MAX_MESSAGE_LENGTH = 4000  # Оставляем запас для эмодзи и форматирования
    for i, answer in enumerate(question['answers'], 1):
        answer_line = f"{i}. {answer}\n"
        if len(text) + len(answer_line) > MAX_MESSAGE_LENGTH:
            text += "...\n(Текст обрезан из-за ограничения длины сообщения)"
            break
        text += answer_line

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question, max_message_length=MAX_MESSAGE_LENGTH)
    
    # Получаем выбранные ответы для этого вопроса (если есть state)
    if state:
        selected_answers_set = await get_selected_answers(state, sheet_name, question['id'])
    else:
        selected_answers_set = set()
    
    # Создаем клавиатуру с кнопками ответов
    keyboard = get_question_keyboard(
        sheet_name=sheet_name,
        question_id=question['id'],
        answers_count=len(question['answers']),
        selected_answers=selected_answers_set
    )
    
    try:
        await message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        # Ошибка "message is not modified" - нормальная ситуация
        if "message is not modified" not in str(e).lower():
            raise  # Если другая ошибка - пробрасываем дальше


@router.callback_query(F.data.startswith("select_answer|"))
async def handle_answer_selection(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора варианта ответа - отмечает/снимает отметку с варианта"""
    logger.info(f"Начало обработки выбора варианта ответа: {callback.data}")

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return
    
    # Формат callback_data: "select_answer|sheet_name|question_id|answer_number"
    parts = callback.data.split("|")
    
    if len(parts) < 4:
        logger.error(f"Неверный формат callback_data: {callback.data}")
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    sheet_name = parts[1]  # Имя листа
    question_id = int(parts[2])  # ID вопроса
    answer_number = int(parts[3])  # Номер ответа
    
    logger.info(f"Парсинг данных: лист='{sheet_name}', question_id={question_id}, answer_number={answer_number}")
    
    # СРАЗУ отвечаем на callback, чтобы не истекал таймаут
    await callback.answer()
    
    # Переключаем выбор варианта ответа в FSM state
    selected_answers_set = await toggle_answer_selection(state, sheet_name, question_id, answer_number)
    logger.info(f"Вариант {answer_number} {'добавлен в' if answer_number in selected_answers_set else 'снят с'} выбор")
    
    db_service = get_db_service()
    
    # Получаем вопрос из базы данных
    question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        logger.error(f"Вопрос не найден: лист='{sheet_name}', question_id={question_id}")
        return
    
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question_id)
    
    # Формируем текст вопроса с номером
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Добавляем нумерованные ответы
    MAX_MESSAGE_LENGTH = 4000
    for i, answer in enumerate(question['answers'], 1):
        answer_line = f"{i}. {answer}\n"
        if len(text) + len(answer_line) > MAX_MESSAGE_LENGTH:
            text += "...\n(Текст обрезан из-за ограничения длины сообщения)"
            break
        text += answer_line

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question, max_message_length=MAX_MESSAGE_LENGTH)
    
    # Создаем клавиатуру с обновленными выбранными ответами
    keyboard = get_question_keyboard(
        sheet_name=sheet_name,
        question_id=question_id,
        answers_count=len(question['answers']),
        selected_answers=selected_answers_set
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@router.callback_query(F.data.startswith("submit_answer|"))
async def handle_submit_answer(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Ответить' - проверяет выбранные ответы и показывает результат"""
    logger.info(f"Начало обработки отправки ответа: {callback.data}")

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return
    
    # Формат callback_data: "submit_answer|sheet_name|question_id"
    parts = callback.data.split("|")
    
    if len(parts) < 3:
        logger.error(f"Неверный формат callback_data: {callback.data}")
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    sheet_name = parts[1]  # Имя листа
    question_id = int(parts[2])  # ID вопроса
    
    # СРАЗУ отвечаем на callback
    await callback.answer()
    
    # Получаем выбранные ответы из FSM state (если ничего не выбрано - пустое множество)
    selected_answer_numbers = await get_selected_answers(state, sheet_name, question_id)
    
    db_service = get_db_service()
    
    # Получаем вопрос из базы данных
    question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        logger.error(f"Вопрос не найден: лист='{sheet_name}', question_id={question_id}")
        return
    
    logger.info(f"Вопрос получен. Количество ответов: {len(question['answers'])}")
    
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question_id)
    
    # Формируем текст вопроса с номером
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Проверяем правильность ответов
    # Правильный ответ - если выбраны все правильные варианты и только они
    correct_answer_numbers = set()
    for i, check in enumerate(question['checks'], 1):
        if i <= len(question['checks']) and check.strip() == '+':
            correct_answer_numbers.add(i)
    
    # Сравниваем множества выбранных и правильных ответов
    is_correct = selected_answer_numbers == correct_answer_numbers
    
    # Добавляем нумерованные ответы с пометками правильности
    MAX_MESSAGE_LENGTH = 4000
    for i, answer in enumerate(question['answers'], 1):
        # Статус варианта: правильный или нет (зависит только от checks)
        if i in correct_answer_numbers:
            status_emoji = "✅"
        else:
            status_emoji = "❌"

        answer_line = f"{status_emoji} {i}. {answer}\n"
        if len(text) + len(answer_line) > MAX_MESSAGE_LENGTH:
            text += "...\n(Текст обрезан из-за ограничения длины сообщения)"
            break
        text += answer_line

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question, max_message_length=MAX_MESSAGE_LENGTH)

    logger.info(f"Текст сообщения сформирован. Длина: {len(text)} символов")
    logger.info(f"Выбранные ответы: {selected_answer_numbers}, Правильные ответы: {correct_answer_numbers}, Правильно: {is_correct}")
    
    # Сохраняем результат в базу данных (сохраняем первый выбранный ответ для совместимости и все выбранные ответы)
    user_id = callback.from_user.id
    first_selected = min(selected_answer_numbers) if selected_answer_numbers else 1
    try:
        await db_service.save_user_result(
            user_id=user_id,
            sheet_name=sheet_name,
            question_id=question_id,
            selected_answer=first_selected,
            is_correct='+' if is_correct else '-',
            selected_answers=selected_answer_numbers
        )
        logger.info(f"Результат сохранен: user_id={user_id}, question_id={question_id}, is_correct={is_correct}, selected_answers={selected_answer_numbers}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении результата: {e}", exc_info=True)
    
    # Добавляем текст о правильности ответа внизу сообщения
    if is_correct:
        text += "\n\n✅ ПРАВИЛЬНО!"
    else:
        text += "\n\n❌ НЕПРАВИЛЬНО!"
    
    # Создаем клавиатуру с отображением статуса ответа
    logger.info("Создание клавиатуры с отображением статуса ответа")
    keyboard = get_question_keyboard_with_status(
        sheet_name=sheet_name,
        question_id=question_id,
        answers_count=len(question['answers']),
        selected_answers=selected_answer_numbers,
        checks=question['checks']
    )
    
    try:
        logger.info("Обновление сообщения с текстом вопроса и клавиатурой")
        await callback.message.edit_text(text, reply_markup=keyboard)
        logger.info("Сообщение успешно обновлено")
    except TelegramBadRequest as e:
        # Ошибка "message is not modified" - нормальная ситуация
        if "message is not modified" in str(e).lower():
            logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
        else:
            logger.error(f"TelegramBadRequest (не message is not modified): {e}")
            raise
    except TelegramNetworkError as e:
        logger.error(f"Сетевая ошибка при обновлении сообщения: {e}")
        raise
    
    logger.info(f"Ответ {'правильный' if is_correct else 'неправильный'}")
    
    # Очищаем выбранные ответы для этого вопроса после проверки
    await set_selected_answers(state, sheet_name, question_id, set())



@router.callback_query(F.data.startswith("finish|"))
async def handle_finish(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Завершить'"""
    logger.info(f"Начало обработки кнопки 'Завершить': {callback.data}")
    await callback.answer()
    
    # Формат: "finish|sheet_name"
    parts = callback.data.split("|", 1)
    sheet_name = parts[1] if len(parts) > 1 else ""
    
    # Очищаем выбранные ответы для этого листа
    await clear_selected_answers_for_sheet(state, sheet_name)
    logger.info(f"Очищены выбранные ответы для листа '{sheet_name}'.")
    
    text = "🏠 Главное меню"
    await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard(user_id=callback.from_user.id))
    logger.info(f"Возврат в главное меню после завершения подготовки по листу '{sheet_name}'.")


@router.callback_query(F.data == "progress")
async def handle_progress(callback: CallbackQuery):
    """Обработчик кнопки 'Прогресс' - показывает список областей"""
    await callback.answer()
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        text = "❌ Не найдено доступных областей аттестации.\nУбедитесь, что Excel файлы загружены в папку data."
        try:
            await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
    else:
        text = "📊 Здесь Ваш прогресс"
        keyboard = get_progress_keyboard(sheet_names, page=0)
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
            else:
                raise


@router.callback_query(F.data.startswith("progress_page_"))
async def handle_progress_pagination(callback: CallbackQuery):
    """Обработчик пагинации меню прогресса"""
    db_service = get_db_service()
    sheet_names = await db_service.get_available_sheets()
    
    if not sheet_names:
        await callback.answer("❌ Не найдено доступных областей аттестации", show_alert=True)
        return
    
    # Вычисляем общее количество страниц
    items_per_page = 7
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Проверяем специальные случаи для неактивных кнопок
    if callback.data == "progress_page_back_disabled":
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if callback.data == "progress_page_forward_disabled":
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Формат: "progress_page_0", "progress_page_1", etc.
    page_str = callback.data.replace("progress_page_", "")
    try:
        page = int(page_str)
    except ValueError:
        await callback.answer()
        return
    
    # Проверяем границы
    if page < 0:
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if page >= total_pages:
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    await callback.answer()
    
    text = "📊 Здесь Ваш прогресс"
    keyboard = get_progress_keyboard(sheet_names, page=page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
        else:
            raise


@router.callback_query(F.data.startswith("progress_sheet_"))
async def handle_progress_sheet_selection(callback: CallbackQuery):
    """Обработчик выбора области для просмотра прогресса"""
    await callback.answer()
    
    # Формат: "progress_sheet_Б 2.2."
    sheet_name = callback.data.replace("progress_sheet_", "", 1)
    
    db_service = get_db_service()
    user_id = callback.from_user.id
    
    # Получаем статистику прогресса
    stats = await db_service.get_user_progress_stats(user_id, sheet_name)
    
    # Получаем все ID вопросов для области
    question_ids = await db_service.get_all_question_ids_for_sheet(sheet_name)
    
    if not question_ids:
        text = f"❌ В области '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        return
    
    # Получаем результаты пользователя
    results = await db_service.get_user_results_for_sheet(user_id, sheet_name)
    
    # Формируем текст с информацией о прогрессе
    text = f"📊 {sheet_name}\n\n"
    text += f"✅ <b>Правильных:</b> {stats['correct_count']}\n"
    text += f"❌ <b>Неправильных:</b> {stats['incorrect_count']}\n"
    text += f"📝 <b>Всего отвечено:</b> {stats['total_answered']}\n"
    text += f"📈 <b>Процент правильности:</b> {stats['percentage']}%"
    
    # Создаем клавиатуру с вопросами (матрица 5x5, первая страница)
    keyboard = get_questions_progress_keyboard(
        sheet_name=sheet_name,
        question_ids=question_ids,
        results=results,
        page=0
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@router.callback_query(F.data.startswith("progress_questions_page_"))
async def handle_progress_questions_pagination(callback: CallbackQuery):
    """Обработчик пагинации списка вопросов в прогрессе"""
    # Формат: "progress_questions_page_Б 2.2._0" или "progress_questions_page_back_disabled_Б 2.2."
    data = callback.data.replace("progress_questions_page_", "")
    
    # Проверяем специальные случаи для неактивных кнопок
    if data.startswith("back_disabled_"):
        await callback.answer("Это первая страница", show_alert=True)
        return
    
    if data.startswith("forward_disabled_"):
        await callback.answer("Это последняя страница", show_alert=True)
        return
    
    # Извлекаем sheet_name и page
    # Формат: "Б 2.2._0" - нужно разделить по последнему подчеркиванию
    try:
        # Находим последнее подчеркивание перед номером страницы
        last_underscore_idx = data.rfind("_")
        if last_underscore_idx == -1:
            await callback.answer()
            return
        
        sheet_name = data[:last_underscore_idx]
        page_str = data[last_underscore_idx + 1:]
        page = int(page_str)
    except (ValueError, IndexError):
        await callback.answer()
        return
    
    await callback.answer()
    
    db_service = get_db_service()
    user_id = callback.from_user.id
    
    # Получаем статистику прогресса
    stats = await db_service.get_user_progress_stats(user_id, sheet_name)
    
    # Получаем все ID вопросов для области
    question_ids = await db_service.get_all_question_ids_for_sheet(sheet_name)
    
    if not question_ids:
        text = f"❌ В области '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        return
    
    # Получаем результаты пользователя
    results = await db_service.get_user_results_for_sheet(user_id, sheet_name)
    
    # Формируем текст с информацией о прогрессе
    text = f"📊 {sheet_name}\n\n"
    text += f"✅ <b>Правильных:</b> {stats['correct_count']}\n"
    text += f"❌ <b>Неправильных:</b> {stats['incorrect_count']}\n"
    text += f"📝 <b>Всего отвечено:</b> {stats['total_answered']}\n"
    text += f"📈 <b>Процент правильности:</b> {stats['percentage']}%"
    
    # Создаем клавиатуру с вопросами
    keyboard = get_questions_progress_keyboard(
        sheet_name=sheet_name,
        question_ids=question_ids,
        results=results,
        page=page
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug(f"Сообщение не было изменено (нормальная ситуация): {e}")
        else:
            raise


@router.callback_query(F.data.startswith("progress_question|"))
async def handle_progress_question_click(callback: CallbackQuery):
    """Обработчик клика на вопрос в прогрессе - показывает карточку вопроса"""
    await callback.answer()
    
    # Формат: "progress_question|Б 2.2.|123|0" (sheet_name|question_id|page)
    parts = callback.data.split("|")
    
    if len(parts) < 4:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    sheet_name = parts[1]
    try:
        question_id = int(parts[2])
        page = int(parts[3])
    except ValueError:
        await callback.answer("Ошибка обработки", show_alert=True)
        return
    
    db_service = get_db_service()
    user_id = callback.from_user.id
    
    # Получаем вопрос из базы данных
    question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        text = f"❌ Вопрос не найден."
        await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        return
    
    # Получаем номер вопроса и общее количество вопросов (одним запросом)
    question_number, total_questions = await db_service.get_question_number_and_total(sheet_name, question_id)
    
    # Получаем ответ пользователя на этот вопрос
    user_answer_data = await db_service.get_user_answer_for_question(user_id, sheet_name, question_id)
    
    # Формируем текст карточки вопроса
    question_header = f"[{question_number}/{total_questions}]" if question_number else ""
    text = f"{question_header} ❓ {question['question']}\n\n"
    
    # Определяем правильные ответы
    correct_answer_numbers = set()
    for i, check in enumerate(question['checks'], 1):
        if i <= len(question['checks']) and check.strip() == '+':
            correct_answer_numbers.add(i)
    
    # Добавляем варианты ответов с отметками правильных/неправильных
    for i, answer in enumerate(question['answers'], 1):
        if i in correct_answer_numbers:
            text += f"{i}. ✅ {answer}\n"
        else:
            text += f"{i}. ❌ {answer}\n"

    # Добавляем ссылку на закон (если есть)
    text = _append_normative_basis(text, question)
    
    text += "\n" + "─" * 20 + "\n\n"
    
    # Добавляем информацию о выбранном ответе
    if user_answer_data:
        is_correct = user_answer_data['is_correct'] == '+'
        
        # Получаем все выбранные ответы
        selected_answers = user_answer_data.get('selected_answers', set())
        
        if selected_answers:
            # Формируем список выбранных ответов
            selected_texts = []
            for answer_num in sorted(selected_answers):
                if answer_num <= len(question['answers']):
                    answer_text = question['answers'][answer_num - 1]
                    selected_texts.append(f"{answer_num}. {answer_text}")
            
            if len(selected_texts) == 1:
                text += f"📝 <b>Ваш ответ:</b> {selected_texts[0]}\n"
            else:
                text += f"📝 <b>Ваши ответы:</b>\n"
                for selected_text in selected_texts:
                    text += f"   • {selected_text}\n"
        else:
            text += "📝 <b>Ваш ответ:</b> Варианты не выбраны\n"
        
        if is_correct:
            text += "✅ Правильно!\n"
        else:
            text += "❌ Неправильно!\n"
            
            # Находим все правильные ответы
            correct_answer_nums = []
            for i, check in enumerate(question['checks'], 1):
                if i <= len(question['checks']) and check.strip() == '+':
                    correct_answer_nums.append(i)
            
            if correct_answer_nums:
                if len(correct_answer_nums) == 1:
                    correct_answer_text = question['answers'][correct_answer_nums[0] - 1] if correct_answer_nums[0] <= len(question['answers']) else "Неизвестно"
                    text += f"✅ <b>Правильный ответ:</b> {correct_answer_nums[0]}. {correct_answer_text}\n"
                else:
                    text += f"✅ <b>Правильные ответы:</b>\n"
                    for correct_num in correct_answer_nums:
                        correct_answer_text = question['answers'][correct_num - 1] if correct_num <= len(question['answers']) else "Неизвестно"
                        text += f"   • {correct_num}. {correct_answer_text}\n"
    else:
        text += "❓ Вопрос не был отвечен\n"
    
    # Создаем клавиатуру с кнопкой "Назад"
    keyboard = get_progress_question_back_keyboard(sheet_name, page)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@router.callback_query(F.data.startswith("disabled_answer|"))
async def handle_disabled_answer(callback: CallbackQuery):
    """Обработчик для неактивных кнопок после отправки ответа - просто отвечаем на callback без действий"""
    await callback.answer()


@router.callback_query(F.data.startswith("disabled_finish|"))
async def handle_disabled_finish(callback: CallbackQuery):
    """Обработчик для неактивной кнопки 'Завершить' после отправки ответа - просто отвечаем на callback без действий"""
    await callback.answer()


@router.callback_query(F.data.startswith("next_question|"))
async def handle_next_question(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Следующий вопрос' - открывает следующий вопрос только по нажатию"""
    logger.info(f"Начало обработки кнопки 'Следующий вопрос': {callback.data}")

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return

    # Формат callback_data: "next_question|sheet_name|question_id"
    parts = callback.data.split("|")

    if len(parts) < 3:
        logger.error(f"Неверный формат callback_data: {callback.data}")
        await callback.answer("Ошибка обработки", show_alert=True)
        return

    sheet_name = parts[1]
    try:
        question_id = int(parts[2])
    except ValueError:
        logger.error(f"Не удалось преобразовать question_id: {callback.data}")
        await callback.answer("Ошибка обработки", show_alert=True)
        return

    # Сразу отвечаем на callback
    await callback.answer()

    db_service = get_db_service()

    # Получаем следующий вопрос
    logger.info(f"Запрос следующего вопроса после question_id={question_id}")
    next_question = await db_service.get_next_question_by_id(sheet_name, question_id)

    if next_question:
        logger.info(f"Следующий вопрос найден. ID: {next_question['id']}")
        # Показываем следующий вопрос
        await show_question(callback.message, db_service, sheet_name, next_question, state)
    else:
        logger.info("Это был последний вопрос")
        # Это был последний вопрос - показываем сообщение о завершении
        finish_text = f"✅ Все вопросы по области '{sheet_name}' пройдены!\n\nВыберите действие:"
        try:
            await callback.message.edit_text(
                finish_text,
                reply_markup=get_main_menu_keyboard(user_id=callback.from_user.id)
            )
            logger.info("Сообщение о завершении отправлено")
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                logger.error(f"Ошибка при отправке сообщения о завершении: {e}")


@router.callback_query(F.data.startswith("continue_"))
async def handle_continue(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Продолжить' - открывает последний неотвеченный вопрос"""
    await callback.answer()

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return
    
    # Формат: "continue_Б 2.2."
    sheet_name = callback.data.replace("continue_", "", 1)
    logger.info(f"Продолжение подготовки по области '{sheet_name}'")
    
    db_service = get_db_service()
    user_id = callback.from_user.id
    
    # Получаем ID последнего неотвеченного вопроса
    question_id = await db_service.get_last_unanswered_question_id(user_id, sheet_name)
    
    if question_id is None:
        # Все вопросы отвечены, начинаем сначала
        logger.info(f"Все вопросы по области '{sheet_name}' уже отвечены, начинаем сначала")
        question = await db_service.get_question_by_number(sheet_name, question_number=1)
    else:
        # Получаем неотвеченный вопрос
        logger.info(f"Найден неотвеченный вопрос с ID {question_id}")
        question = await db_service.get_question_by_id(sheet_name, question_id)
    
    if not question:
        logger.error(f"Вопрос не найден для листа '{sheet_name}'")
        text = f"❌ В области аттестации '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        return
    
    # Показываем вопрос
    await show_question(callback.message, db_service, sheet_name, question, state)


@router.callback_query(F.data.startswith("reset_"))
async def handle_reset(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Сначала' - сбрасывает прогресс и начинает сначала"""
    await callback.answer()

    # Если во время прохождения теста произошла полная перезагрузка вопросов,
    # сразу выкидываем пользователя в главное меню с предупреждением.
    if await _redirect_if_fsm_reset(callback, state):
        return
    
    # Формат: "reset_Б 2.2."
    sheet_name = callback.data.replace("reset_", "", 1)
    logger.info(f"Сброс прогресса по области '{sheet_name}'")
    
    db_service = get_db_service()
    user_id = callback.from_user.id
    
    # Удаляем весь прогресс пользователя по этой области
    await db_service.delete_user_progress(user_id, sheet_name)
    logger.info(f"Прогресс пользователя {user_id} по области '{sheet_name}' удален")
    
    # Очищаем выбранные ответы для этого листа
    await clear_selected_answers_for_sheet(state, sheet_name)
    
    # Получаем первый вопрос из базы данных
    question = await db_service.get_question_by_number(sheet_name, question_number=1)
    
    if not question:
        logger.error(f"Вопросы не найдены для листа '{sheet_name}'")
        text = f"❌ В области аттестации '{sheet_name}' не найдено вопросов."
        await callback.message.edit_text(text, reply_markup=get_back_keyboard())
        return
    
    logger.info(f"Первый вопрос найден. ID: {question['id']}")
    
    # Показываем первый вопрос
    await show_question(callback.message, db_service, sheet_name, question, state)


@router.callback_query(F.data == "upload_excel")
async def handle_upload_excel(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Загрузить Excel' - запрашивает файл"""
    from keyboards.inline_keyboards import is_admin
    
    # Проверяем, является ли пользователь администратором
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У вас нет прав для выполнения этого действия", show_alert=True)
        return
    
    await callback.answer()
    text = "📤 Загрузка Excel файла\n\n⚠️ <b>ВНИМАНИЕ!</b> Загрузка нового файла сбросит весь прогресс всех пользователей.\n\nПожалуйста, отправьте Excel файл (.xlsx) для загрузки в базу данных."
    
    # Устанавливаем состояние ожидания файла
    await state.set_state(UploadStates.waiting_for_excel_file)
    
    try:
        await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode="HTML")
        # Сохраняем message_id сообщения с запросом загрузки для последующего удаления
        await state.update_data(upload_request_message_id=callback.message.message_id)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message is not modified in handle_upload_excel")
        else:
            raise


@router.message(UploadStates.waiting_for_excel_file, F.document)
async def handle_excel_file_upload(message: Message, state: FSMContext):
    """Обработчик загрузки Excel файла"""
    from keyboards.inline_keyboards import is_admin
    import config
    import shutil
    
    # Проверяем, является ли пользователь администратором
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для выполнения этого действия")
        await state.clear()
        return
    
    # Проверяем, что это Excel файл
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        await message.answer("❌ Пожалуйста, отправьте файл с расширением .xlsx или .xls")
        return
    
    # Получаем message_id сообщения с запросом загрузки и удаляем его
    data = await state.get_data()
    upload_request_message_id = data.get('upload_request_message_id')
    if upload_request_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=upload_request_message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение с запросом загрузки: {e}")
    
    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer("⏳ Начинаю обработку файла...")
    
    try:
        # Используем глобальный экземпляр DatabaseService
        db_service = get_db_service()
        
        # Шаг 1: Удаляем весь прогресс пользователей
        await processing_msg.edit_text("🗑️ Удаляю прогресс всех пользователей...")
        deleted_progress = await db_service.delete_all_user_progress()
        logger.info(f"Удалено записей прогресса: {deleted_progress}")

        # Шаг 1.1: Очищаем FSM-состояния (чтобы все сценарии начались заново)
        await processing_msg.edit_text("🗑️ Очищаю состояния пользователей (FSM)...")
        deleted_fsm = await db_service.delete_all_fsm_states()
        logger.info(f"Удалено FSM-состояний: {deleted_fsm}")
        # Помечаем, что после этой операции пользователям нужно показать уведомление
        if deleted_fsm:
            mark_fsm_reset_occurred()
        
        # Шаг 2: Удаляем все таблицы вопросов из базы данных
        await processing_msg.edit_text("🗑️ Удаляю старые данные из базы данных...")
        await db_service.delete_all_question_tables()
        logger.info("Все таблицы вопросов удалены из базы данных")
        
        # Шаг 3: Удаляем все старые Excel файлы из папки data
        await processing_msg.edit_text("🗑️ Удаляю старые файлы из папки data...")
        data_dir = Path(config.DATA_DIR)
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Удаляем все .xlsx и .xls файлы (кроме временных файлов Excel)
        deleted_files = []
        # Выполняем glob в отдельном потоке
        excel_files_xlsx = await asyncio.to_thread(lambda: list(data_dir.glob("*.xlsx")))
        for excel_file in excel_files_xlsx:
            if not excel_file.name.startswith("~$"):
                try:
                    excel_file.unlink()
                    deleted_files.append(excel_file.name)
                    logger.info(f"Удален файл: {excel_file.name}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить файл {excel_file.name} (возможно, занят): {e}")
        
        excel_files_xls = await asyncio.to_thread(lambda: list(data_dir.glob("*.xls")))
        for excel_file in excel_files_xls:
            if not excel_file.name.startswith("~$"):
                try:
                    excel_file.unlink()
                    deleted_files.append(excel_file.name)
                    logger.info(f"Удален файл: {excel_file.name}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить файл {excel_file.name} (возможно, занят): {e}")
        
        logger.info(f"Удалено файлов: {len(deleted_files)}")
        
        # Шаг 4: Скачиваем новый файл
        await processing_msg.edit_text("📥 Скачиваю новый файл...")
        file_info = await message.bot.get_file(message.document.file_id)
        file_path = Path(config.DATA_DIR) / message.document.file_name
        
        # Скачиваем файл
        await message.bot.download_file(file_info.file_path, destination=file_path)
        logger.info(f"Файл сохранен: {file_path}")
        
        # Шаг 5: Парсим новый файл
        await processing_msg.edit_text("📥 Файл загружен. Начинаю парсинг...")
        
        # Парсим файл
        success = await db_service.parser.parse_excel_file(file_path)
        
        if success:
            # Получаем список загруженных листов
            from parser import ExcelParser
            parser = ExcelParser(db_service.db)
            sheets = await parser.get_available_sheets(file_path)
            
            sheets_text = "\n".join([f"  • {sheet}" for sheet in sheets])
            text = f"✅ Файл успешно загружен и обработан!\n\nЗагружены листы:\n{sheets_text}"
            await processing_msg.edit_text(text, reply_markup=get_main_menu_keyboard(user_id=message.from_user.id))
        else:
            await processing_msg.edit_text("❌ Ошибка при обработке файла. Проверьте формат файла и попробуйте снова.", reply_markup=get_main_menu_keyboard(user_id=message.from_user.id))
        
        # Очищаем состояние (включая upload_request_message_id)
        await state.update_data(upload_request_message_id=None)
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке Excel файла: {e}", exc_info=True)
        await processing_msg.edit_text(f"❌ Ошибка при обработке файла: {str(e)}", reply_markup=get_main_menu_keyboard(user_id=message.from_user.id))
        await state.update_data(upload_request_message_id=None)
        await state.clear()


@router.message(UploadStates.waiting_for_excel_file)
async def handle_non_excel_file(message: Message, state: FSMContext):
    """Обработчик для сообщений, которые не являются Excel файлами"""
    # Получаем message_id сообщения с запросом загрузки и удаляем его
    data = await state.get_data()
    upload_request_message_id = data.get('upload_request_message_id')
    if upload_request_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=upload_request_message_id)
            await state.update_data(upload_request_message_id=None)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение с запросом загрузки: {e}")
    
    await message.answer("❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)")
