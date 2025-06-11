"""Inline клавиатуры для бота"""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def disclaimer_kb() -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения условий использования"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Я согласен", callback_data="accept_disclaimer"),
                InlineKeyboardButton(text="❌ Отказаться", callback_data="reject_disclaimer")
            ]
        ]
    )


def main_menu_kb(user_id: int, admin_ids: list[int]) -> InlineKeyboardMarkup:
    """Главное меню бота с кнопкой Баланс"""
    keyboard = [
        [
            InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file"),
            InlineKeyboardButton(text="🔗 Отправить ссылки", callback_data="send_links")
        ],
        [
            InlineKeyboardButton(text="📊 Мои результаты", callback_data="my_results"),
            InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance")
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск по телефону", callback_data="search_phone"),
            InlineKeyboardButton(text="📈 Статистика", callback_data="user_stats")
        ],
        [
            InlineKeyboardButton(text="📚 Помощь", callback_data="help")
        ]
    ]

    # Добавляем кнопку загрузки БД только для админов
    if user_id in admin_ids:
        keyboard.append([
            InlineKeyboardButton(text="🗄 Загрузить БД ВК", callback_data="load_database")
        ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)



def back_to_menu_kb() -> InlineKeyboardMarkup:
    """Клавиатура для возврата в главное меню"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ]
    )


def processing_menu_kb() -> InlineKeyboardMarkup:
    """Меню во время обработки с кнопкой Баланс"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏸ Пауза", callback_data="pause"),
                InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance_processing")
            ],
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
                InlineKeyboardButton(text="🚫 Отменить", callback_data="cancel")
            ]
        ]
    )


def continue_kb() -> InlineKeyboardMarkup:
    """Кнопка продолжения после лимита"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Продолжить обработку", callback_data="continue")
            ],
            [
                InlineKeyboardButton(text="📊 Получить текущие результаты", callback_data="export_current"),
                InlineKeyboardButton(text="🚫 Отменить", callback_data="cancel")
            ]
        ]
    )


def finish_kb() -> InlineKeyboardMarkup:
    """Меню завершения обработки"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Скачать результаты", callback_data="download_results")
            ],
            [
                InlineKeyboardButton(text="➕ Добавить еще ссылки", callback_data="add_more"),
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="main_menu")
            ]
        ]
    )


def duplicate_actions_kb() -> InlineKeyboardMarkup:
    """Клавиатура для работы с дубликатами"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🗑 Удалить дубликаты", callback_data="remove_duplicates"),
                InlineKeyboardButton(text="📋 Оставить все", callback_data="keep_all")
            ],
            [
                InlineKeyboardButton(text="📊 Обновить данные дубликатов", callback_data="update_duplicates")
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_processing")
            ]
        ]
    )


def file_action_menu_kb() -> InlineKeyboardMarkup:
    """Меню действий с файлом"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔍 Анализировать", callback_data="analyze_only"),
                InlineKeyboardButton(text="📤 Обработать", callback_data="process_only")
            ],
            [
                InlineKeyboardButton(text="📊 Анализ + Обработка", callback_data="analyze_and_process")
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
            ]
        ]
    )


def analysis_results_kb() -> InlineKeyboardMarkup:
    """Меню после анализа файла"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📤 Обработать файл", callback_data="process_after_analysis"),
                InlineKeyboardButton(text="📋 Детали", callback_data="analysis_details")
            ],
            [
                InlineKeyboardButton(text="💾 Скачать отчет", callback_data="export_analysis"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
            ]
        ]
    )


def db_load_menu_kb() -> InlineKeyboardMarkup:
    """Меню режима загрузки БД"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="❌ Отменить загрузку", callback_data="cancel_db_load")
            ]
        ]
    )


def confirm_kb(yes_callback: str, no_callback: str) -> InlineKeyboardMarkup:
    """Универсальная клавиатура подтверждения"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=yes_callback),
                InlineKeyboardButton(text="❌ Нет", callback_data=no_callback)
            ]
        ]
    )


def cancel_only_kb() -> InlineKeyboardMarkup:
    """Клавиатура только с кнопкой отмены"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")]
        ]
    )