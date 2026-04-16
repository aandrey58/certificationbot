"""
Инлайн-клавиатуры для бота
"""
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором"""
    try:
        with open("admins.txt", "r", encoding="utf-8") as f:
            admin_ids = []
            for line in f:
                line = line.strip()
                # Пропускаем пустые строки и комментарии
                if line and not line.startswith("#"):
                    try:
                        admin_ids.append(int(line))
                    except ValueError:
                        continue
            return user_id in admin_ids
    except FileNotFoundError:
        return False


def get_main_menu_keyboard(user_id: int = None) -> InlineKeyboardMarkup:
    """Главное меню с кнопками 'Подготовка' и 'Помощник'"""
    buttons = [
        [
            InlineKeyboardButton(text="📚 Подготовка", callback_data="preparation")
        ],
        [
            InlineKeyboardButton(text="🤖 Помощник", callback_data="assistant")
        ]
    ]
    
    # Добавляем кнопку "Загрузить Excel" только для администраторов
    if user_id and is_admin(user_id):
        buttons.append([
            InlineKeyboardButton(text="📤 Загрузить Excel", callback_data="upload_excel")
        ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_search_sheet_keyboard(sheet_names: list[str], page: int = 0, items_per_page: int = 7) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора области аттестации для поиска с пагинацией
    
    Args:
        sheet_names: список всех имен листов
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (по умолчанию 7)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(sheet_names))
    
    # Добавляем кнопки для листов текущей страницы
    for sheet_name in sheet_names[start_idx:end_idx]:
        # Ограничиваем длину текста кнопки (Telegram лимит - 64 символа)
        display_name = sheet_name[:60] + "..." if len(sheet_name) > 60 else sheet_name
        buttons.append([
            InlineKeyboardButton(text=f"📄 {display_name}", callback_data=f"search_sheet_{sheet_name}")
        ])
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"search_sheet_page_{page - 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data="search_sheet_page_back_disabled")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"search_sheet_page_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"search_sheet_page_{page + 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data="search_sheet_page_forward_disabled")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Назад" для возврата в меню помощника
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="assistant")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_preparation_keyboard(sheet_names: list[str], page: int = 0, items_per_page: int = 7) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора области аттестации (листов Excel) с пагинацией
    
    Args:
        sheet_names: список всех имен листов
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (по умолчанию 7)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(sheet_names))
    
    # Добавляем кнопки для листов текущей страницы
    for sheet_name in sheet_names[start_idx:end_idx]:
        # Ограничиваем длину текста кнопки (Telegram лимит - 64 символа)
        display_name = sheet_name[:60] + "..." if len(sheet_name) > 60 else sheet_name
        buttons.append([
            InlineKeyboardButton(text=f"📄 {display_name}", callback_data=f"sheet_{sheet_name}")
        ])
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        # Если не первая страница, показываем кнопку для перехода на предыдущую
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"prep_page_{page - 1}")
        )
    else:
        # Если первая страница, показываем неактивную кнопку с специальным callback_data
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data="prep_page_back_disabled")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"prep_page_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        # Если есть следующая страница, показываем активную кнопку
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"prep_page_{page + 1}")
        )
    else:
        # Если последняя страница, показываем неактивную кнопку с специальным callback_data
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data="prep_page_forward_disabled")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Прогресс" - ПОД пагинацией
    buttons.append([
        InlineKeyboardButton(text="📊 Прогресс", callback_data="progress")
    ])
    
    # Отдельная кнопка "Назад" для возврата в главное меню - ПОД кнопкой "Прогресс"
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_question_keyboard(sheet_name: str, question_id: int, answers_count: int, selected_answers: set = None) -> InlineKeyboardMarkup:
    """
    Клавиатура для вопроса с кнопками ответов (множественный выбор)
    
    Args:
        sheet_name: имя листа (область аттестации)
        question_id: ID вопроса в базе данных
        answers_count: количество ответов
        selected_answers: множество выбранных ответов (для отображения эмодзи)
    """
    if selected_answers is None:
        selected_answers = set()
    
    buttons = []
    
    # Создаем кнопки для каждого ответа в два столбика
    row = []
    for i in range(1, answers_count + 1):
        # Определяем, выбран ли этот ответ
        is_selected = i in selected_answers
        
        # Формируем текст кнопки: номер ответа с эмодзи отметки
        if is_selected:
            button_text = f"⭕ {i}"  # Эмодзи для выбранного варианта
        else:
            button_text = str(i)
        
        # Формируем callback_data: используем разделитель "|" для избежания проблем с подчеркиваниями
        # Формат: select_answer|sheet_name|question_id|answer_number
        callback_data = f"select_answer|{sheet_name}|{question_id}|{i}"
        
        row.append(InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        # Если набрали 2 кнопки в строку или это последний ответ
        if len(row) == 2 or i == answers_count:
            buttons.append(row)
            row = []
    
    # Добавляем кнопку "Ответить" (всегда видна)
    buttons.append([
        InlineKeyboardButton(text="📤 Ответить", callback_data=f"submit_answer|{sheet_name}|{question_id}")
    ])
    
    # Добавляем кнопку "Завершить"
    buttons.append([
        InlineKeyboardButton(text="✅ Завершить", callback_data=f"finish|{sheet_name}")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_question_keyboard_with_status(sheet_name: str, question_id: int, answers_count: int, 
                                     selected_answers: set[int], checks: list[str]) -> InlineKeyboardMarkup:
    """
    Клавиатура для вопроса с отображением статуса ответов (эмодзи) после проверки
    
    Args:
        sheet_name: имя листа
        question_id: ID вопроса
        answers_count: количество ответов
        selected_answers: множество выбранных ответов (номера)
        checks: список проверок для каждого ответа (['+', '-', ...])
    """
    buttons = []
    
    # Определяем правильные ответы
    correct_answer_numbers = set()
    for i, check in enumerate(checks, 1):
        if i <= len(checks) and check.strip() == '+':
            correct_answer_numbers.add(i)
    
    # Проверяем правильность выбранных ответов
    is_correct = selected_answers == correct_answer_numbers
    
    # Создаем кнопки для каждого ответа в два столбика
    row = []
    for i in range(1, answers_count + 1):
        # Определяем, был ли этот ответ выбран
        was_selected = i in selected_answers
        is_correct_answer = i in correct_answer_numbers
        
        # Определяем статус ответа
        if was_selected and is_correct_answer:
            # Выбран правильный ответ
            button_text = f"✅ {i}"
        elif was_selected and not is_correct_answer:
            # Выбран неправильный ответ
            button_text = f"❌ {i}"
        elif not was_selected and is_correct_answer:
            # Не выбран, но это правильный ответ (показываем для информации)
            button_text = f"✅ {i}"
        else:
            # Не выбран и не правильный
            button_text = str(i)
        
        # Формируем callback_data (кнопки неактивны после проверки - используем специальный callback)
        callback_data = f"disabled_answer|{sheet_name}|{question_id}|{i}"
        
        row.append(InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        # Если набрали 2 кнопки в строку или это последний ответ
        if len(row) == 2 or i == answers_count:
            buttons.append(row)
            row = []
    
    # Добавляем кнопку "Следующий вопрос" (активна) и "Завершить" (неактивна)
    buttons.append([
        InlineKeyboardButton(
            text="➡️ Следующий вопрос",
            callback_data=f"next_question|{sheet_name}|{question_id}"
        )
    ])
    buttons.append([
        InlineKeyboardButton(
            text="✅ Завершить",
            callback_data=f"finish|{sheet_name}"
        )
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой 'В главное меню'"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")
        ]
    ])
    return keyboard


def get_progress_keyboard(sheet_names: list[str], page: int = 0, items_per_page: int = 7) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора области для просмотра прогресса с пагинацией
    
    Args:
        sheet_names: список всех имен листов
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (по умолчанию 7)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(sheet_names))
    
    # Добавляем кнопки для листов текущей страницы
    for sheet_name in sheet_names[start_idx:end_idx]:
        # Ограничиваем длину текста кнопки (Telegram лимит - 64 символа)
        display_name = sheet_name[:60] + "..." if len(sheet_name) > 60 else sheet_name
        buttons.append([
            InlineKeyboardButton(text=f"📄 {display_name}", callback_data=f"progress_sheet_{sheet_name}")
        ])
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"progress_page_{page - 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data="progress_page_back_disabled")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"progress_page_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"progress_page_{page + 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data="progress_page_forward_disabled")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Назад" для возврата к меню подготовки
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="preparation")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_questions_progress_keyboard(sheet_name: str, question_ids: list[int], results: dict[int, str], 
                                     page: int = 0, items_per_page: int = 25) -> InlineKeyboardMarkup:
    """
    Клавиатура для отображения прогресса по вопросам (матрица 5x5 с пагинацией)
    
    Args:
        sheet_name: имя листа (область аттестации)
        question_ids: список ID вопросов
        results: словарь {question_id: 'correct'/'incorrect'/'not_answered'}
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (25 = 5x5)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(question_ids) + items_per_page - 1) // items_per_page if question_ids else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(question_ids))
    
    # Получаем вопросы для текущей страницы
    current_questions = question_ids[start_idx:end_idx]
    
    # Создаем матрицу 5x5
    for row in range(5):
        row_buttons = []
        for col in range(5):
            idx = row * 5 + col
            if idx < len(current_questions):
                question_id = current_questions[idx]
                question_number = start_idx + idx + 1
                
                # Определяем эмодзи в зависимости от результата
                result = results.get(question_id, 'not_answered')
                if result == 'correct':
                    emoji = '✅'
                elif result == 'incorrect':
                    emoji = '❌'
                else:
                    emoji = ''
                
                button_text = f"{emoji} {question_number}" if emoji else str(question_number)
                row_buttons.append(
                    InlineKeyboardButton(
                        text=button_text,
                        callback_data=f"progress_question|{sheet_name}|{question_id}|{page}"
                    )
                )
        
        if row_buttons:
            buttons.append(row_buttons)
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"progress_questions_page_{sheet_name}_{page - 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"progress_questions_page_back_disabled_{sheet_name}")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"progress_questions_page_{sheet_name}_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"progress_questions_page_{sheet_name}_{page + 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"progress_questions_page_forward_disabled_{sheet_name}")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Назад" для возврата к выбору области
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="progress")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_progress_question_back_keyboard(sheet_name: str, page: int) -> InlineKeyboardMarkup:
    """
    Клавиатура с кнопкой "Назад" для возврата к прогрессу вопросов
    
    Args:
        sheet_name: имя листа (область аттестации)
        page: номер страницы прогресса
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"progress_questions_page_{sheet_name}_{page}")
        ]
    ])
    return keyboard


def get_continue_or_reset_keyboard(sheet_name: str) -> InlineKeyboardMarkup:
    """
    Клавиатура с кнопками "Продолжить" и "Сначала"
    
    Args:
        sheet_name: имя листа (область аттестации)
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Продолжить", callback_data=f"continue_{sheet_name}"),
            InlineKeyboardButton(text="🔄 Сначала", callback_data=f"reset_{sheet_name}")
        ]
    ])
    return keyboard


def get_assistant_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подменю помощника"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Все вопросы", callback_data="assistant_all_questions")
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск по вопросам", callback_data="assistant_search")
        ],
        [
            InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")
        ]
    ])
    return keyboard


def get_assistant_all_questions_keyboard(sheet_names: list[str], page: int = 0, items_per_page: int = 7) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора области для просмотра всех вопросов с пагинацией
    
    Args:
        sheet_names: список всех имен листов
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (по умолчанию 7)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(sheet_names) + items_per_page - 1) // items_per_page if sheet_names else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(sheet_names))
    
    # Добавляем кнопки для листов текущей страницы
    for sheet_name in sheet_names[start_idx:end_idx]:
        # Ограничиваем длину текста кнопки (Telegram лимит - 64 символа)
        display_name = sheet_name[:60] + "..." if len(sheet_name) > 60 else sheet_name
        buttons.append([
            InlineKeyboardButton(text=f"📄 {display_name}", callback_data=f"assistant_sheet_{sheet_name}")
        ])
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"assistant_all_page_{page - 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data="assistant_all_page_back_disabled")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"assistant_all_page_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"assistant_all_page_{page + 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data="assistant_all_page_forward_disabled")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Назад" для возврата в подменю помощника
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="assistant")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_assistant_questions_keyboard(sheet_name: str, question_ids: list[int], 
                                     page: int = 0, items_per_page: int = 25) -> InlineKeyboardMarkup:
    """
    Клавиатура для отображения всех вопросов (матрица 5x5 с пагинацией, без эмодзи)
    
    Args:
        sheet_name: имя листа (область аттестации)
        question_ids: список ID вопросов
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (25 = 5x5)
    """
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(question_ids) + items_per_page - 1) // items_per_page if question_ids else 1
    
    # Определяем диапазон элементов для текущей страницы
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(question_ids))
    
    # Получаем вопросы для текущей страницы
    current_questions = question_ids[start_idx:end_idx]
    
    # Создаем матрицу 5x5
    for row in range(5):
        row_buttons = []
        for col in range(5):
            idx = row * 5 + col
            if idx < len(current_questions):
                question_id = current_questions[idx]
                question_number = start_idx + idx + 1
                
                # Без эмодзи, только номер вопроса
                button_text = str(question_number)
                row_buttons.append(
                    InlineKeyboardButton(
                        text=button_text,
                        callback_data=f"assistant_question|{sheet_name}|{question_id}|{page}"
                    )
                )
        
        if row_buttons:
            buttons.append(row_buttons)
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"assistant_questions_page_{sheet_name}_{page - 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"assistant_questions_page_back_disabled_{sheet_name}")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"assistant_questions_page_{sheet_name}_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"assistant_questions_page_{sheet_name}_{page + 1}")
        )
    else:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"assistant_questions_page_forward_disabled_{sheet_name}")
        )
    
    # Добавляем кнопки пагинации в одну строку
    buttons.append(pagination_buttons)
    
    # Кнопка "Назад" для возврата к выбору области
    buttons.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="assistant_all_questions")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_assistant_question_back_keyboard(sheet_name: str, page: int) -> InlineKeyboardMarkup:
    """
    Клавиатура с кнопкой "Назад" для возврата к матрице вопросов в помощнике
    
    Args:
        sheet_name: имя листа (область аттестации)
        page: номер страницы матрицы вопросов
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"assistant_questions_page_{sheet_name}_{page}")
        ]
    ])
    return keyboard


def get_search_results_keyboard(results: list[dict], page: int = 0, items_per_page: int = 3) -> InlineKeyboardMarkup:
    """
    Клавиатура с результатами поиска с пагинацией
    
    Args:
        results: список всех результатов поиска [{'sheet_name': str, 'question_id': int, 'question': str, 'question_number': int}, ...]
        page: номер страницы (начинается с 0)
        items_per_page: количество элементов на странице (по умолчанию 5)
    """
    # Если результатов нет — показываем только кнопку "Завершить поиск"
    if not results:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить поиск", callback_data="finish_search")]
        ])
        return keyboard
    
    buttons = []
    
    # Вычисляем общее количество страниц
    total_pages = (len(results) + items_per_page - 1) // items_per_page if results else 1
    
    # Добавляем кнопки пагинации: [◀️ Назад] [1/2] [Вперёд ▶️] - ВСЕГДА видны
    pagination_buttons = []
    
    # Кнопка "Назад" для пагинации (предыдущая страница) - ВСЕГДА видна
    if page > 0:
        # Если не первая страница, показываем кнопку для перехода на предыдущую
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data=f"search_page_{page - 1}")
        )
    else:
        # Если первая страница, показываем неактивную кнопку с специальным callback_data
        pagination_buttons.append(
            InlineKeyboardButton(text="◀️ Назад", callback_data="search_page_back_disabled")
        )
    
    # Кнопка с номером страницы [1/2] - ВСЕГДА видна
    pagination_buttons.append(
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"search_page_{page}")
    )
    
    # Кнопка "Вперёд" (следующая страница) - ВСЕГДА видна
    if page < total_pages - 1:
        # Если есть следующая страница, показываем активную кнопку
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"search_page_{page + 1}")
        )
    else:
        # Если последняя страница, показываем неактивную кнопку с специальным callback_data
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперёд ▶️", callback_data="search_page_forward_disabled")
        )
    
    buttons.append(pagination_buttons)
    
    # Добавляем кнопку "Завершить поиск"
    buttons.append([
        InlineKeyboardButton(text="✅ Завершить поиск", callback_data="finish_search")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard


def get_search_result_back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопками 'Поиск по вопросам' и 'Назад' для карточки вопроса из поиска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 Поиск по вопросам", callback_data="assistant_search")
        ],
        [
            InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_search_results")
        ]
    ])
    return keyboard
