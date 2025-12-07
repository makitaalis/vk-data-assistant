"""Inline клавиатуры для бота с поддержкой обработки дубликатов"""

from typing import Optional

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

    # Добавляем кнопки админа
    if user_id in admin_ids:
        keyboard.append([
            InlineKeyboardButton(text="────────────", callback_data="main_menu_separator")
        ])
        keyboard.append([
            InlineKeyboardButton(text="📡 Сессии", callback_data="admin_session_status"),
            InlineKeyboardButton(text="🤖 VK‑пул", callback_data="vk_pool")
        ])
        keyboard.append([
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_settings"),
            InlineKeyboardButton(text="🛠 Обслуживание", callback_data="admin_maintenance")
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
                InlineKeyboardButton(text="🛑 Отменить", callback_data="cancel_all_tasks"),
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
                InlineKeyboardButton(text="📊 Получить текущие результаты", callback_data="export_current")
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


def duplicate_actions_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Клавиатура для работы с дубликатами в БД"""
    keyboard = []

    if is_admin:
        keyboard.append([
            InlineKeyboardButton(text="🗑 Удалить дубликаты", callback_data="remove_duplicates"),
            InlineKeyboardButton(text="📋 Оставить все", callback_data="keep_all")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="📋 Оставить все", callback_data="keep_all")
        ])

    keyboard.append([
        InlineKeyboardButton(text="📊 Обновить данные дубликатов", callback_data="update_duplicates")
    ])
    keyboard.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_processing")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


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


def file_duplicates_menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Меню для работы с дубликатами внутри файла"""
    keyboard = []

    if is_admin:
        keyboard.append([
            InlineKeyboardButton(text="🗑 Удалить дубликаты", callback_data="process_unique_only"),
            InlineKeyboardButton(text="📋 Обработать все", callback_data="process_with_duplicates")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="📋 Обработать все", callback_data="process_with_duplicates")
        ])

    if is_admin:
        keyboard.append([
            InlineKeyboardButton(text="📊 Детали дубликатов", callback_data="show_duplicate_details"),
            InlineKeyboardButton(text="🔍 Полный анализ", callback_data="analyze_only")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="🔍 Полный анализ", callback_data="analyze_only")
        ])
    keyboard.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def analysis_results_kb(is_admin: bool = False, has_duplicates: bool = False) -> InlineKeyboardMarkup:
    """Меню после анализа файла"""
    keyboard = []

    if is_admin and has_duplicates:
        keyboard.append([
            InlineKeyboardButton(text="🗑 Удалить дубликаты (БД)", callback_data="remove_duplicates"),
            InlineKeyboardButton(text="📤 Обработать все", callback_data="process_after_analysis")
        ])
        keyboard.append([
            InlineKeyboardButton(text="🧹 Только уникальные (файл)", callback_data="process_unique_only"),
            InlineKeyboardButton(text="📋 Детали", callback_data="analysis_details")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="📤 Обработать файл", callback_data="process_after_analysis")
        ])
        keyboard.append([
            InlineKeyboardButton(text="📋 Детали", callback_data="analysis_details")
        ])

    keyboard.append([
        InlineKeyboardButton(text="💾 Обработать с кешем", callback_data="process_with_cache"),
        InlineKeyboardButton(text="🆕 Только новые", callback_data="process_only_new")
    ])

    keyboard.append([
        InlineKeyboardButton(text="💾 Скачать отчет", callback_data="export_analysis"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def insufficient_balance_kb() -> InlineKeyboardMarkup:
    """Клавиатура при недостатке поисков"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Продолжить принудительно",
                    callback_data="force_continue_processing"
                )
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_processing")
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


def all_cached_menu_kb() -> InlineKeyboardMarkup:
    """Меню когда обработка завершена"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Скачать результаты", callback_data="download_results")
            ],
            [
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="main_menu")
            ]
        ]
    )


def mixed_cache_menu_kb() -> InlineKeyboardMarkup:
    """Меню когда обработка завершена"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Скачать результаты", callback_data="download_results")
            ],
            [
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="main_menu")
            ]
        ]
    )


def settings_kb(use_cache: bool, admin_use_cache: bool, enable_duplicate_removal: bool) -> InlineKeyboardMarkup:
    """
    Клавиатура настроек бота для администраторов

    Args:
        use_cache: Текущее состояние USE_CACHE
        admin_use_cache: Текущее состояние ADMIN_USE_CACHE
        enable_duplicate_removal: Текущее состояние ENABLE_DUPLICATE_REMOVAL
    """
    def status_text(enabled: bool) -> str:
        return "✅" if enabled else "❌"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{status_text(use_cache)} Кеш для всех",
                    callback_data="toggle_use_cache"
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"{status_text(admin_use_cache)} Кеш для админов",
                    callback_data="toggle_admin_use_cache"
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"{status_text(enable_duplicate_removal)} Проверка дубликатов",
                    callback_data="toggle_duplicate_removal"
                )
            ],
            [
                InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_settings"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
            ]
        ]
    )


from bot.config import TELEGRAM_SESSIONS


def session_control_kb(
    current_mode: str,
    sessions: list[dict],
    slots: Optional[dict] = None,
    protected_sessions: Optional[set[str]] = None,
) -> InlineKeyboardMarkup:
    """Клавиатура управления Telegram-сессиями"""

    def mode_button(label: str, mode: str) -> InlineKeyboardButton:
        prefix = "✅ " if current_mode == mode else ""
        return InlineKeyboardButton(text=f"{prefix}{label}", callback_data=f"session_mode:{mode}")

    keyboard: list[list[InlineKeyboardButton]] = [
        [
            mode_button("Primary", "primary"),
            mode_button("Secondary", "secondary"),
            mode_button("Both", "both"),
        ]
    ]

    slot_a_label = (slots or {}).get("slot_a") or "не выбрано"
    slot_b_label = (slots or {}).get("slot_b") or "не выбрано"
    keyboard.append([
        InlineKeyboardButton(text=f"🎯 Slot A: {slot_a_label}", callback_data="session_slot:slot_a"),
        InlineKeyboardButton(text=f"🎯 Slot B: {slot_b_label}", callback_data="session_slot:slot_b"),
    ])

    total_bots = sum(session.get("bots_total", 0) for session in sessions or [])
    keyboard.append([
        InlineKeyboardButton(
            text=f"🤖 VK боты ({total_bots or '0'})",
            callback_data="vk_pool"
        ),
        InlineKeyboardButton(
            text="➕ Сессия",
            callback_data="admin_session_auth"
        ),
    ])

    for session in sessions or []:
        status_icon = "🟢" if session.get("enabled") else "⚪️"
        name = session.get("name", "unknown")
        availability = f"{session.get('bots_available', 0)}/{session.get('bots_total', 0)}"
        requests = session.get("requests", 0)
        errors = session.get("errors", 0)
        is_protected = protected_sessions and name in protected_sessions
        label_prefix = "🛡 " if is_protected else ""
        extras = []
        if session.get("bots_on_hold"):
            extras.append(f"H{session['bots_on_hold']}")
        if session.get("bots_limited"):
            extras.append(f"L{session['bots_limited']}")
        if session.get("bots_removed"):
            extras.append(f"R{session['bots_removed']}")
        extras_text = f" [{' '.join(extras)}]" if extras else ""
        button_label = f"{status_icon} {label_prefix}{name} ({availability}) • {requests}/{errors}{extras_text}"
        row = [
            InlineKeyboardButton(
                text=button_label,
                callback_data=f"session_toggle:{name}"
            )
        ]
        configured_bots = session.get("configured_bots") or []
        bot_button_label = f"🤖 {len(configured_bots)}" if configured_bots else "🤖 Боты"
        row.append(
            InlineKeyboardButton(
                text=bot_button_label,
                callback_data=f"session_bots:{name}"
            )
        )
        if is_protected:
            row.append(
                InlineKeyboardButton(
                    text="📦",
                    callback_data=f"session_archive:{name}"
                )
            )
        else:
            row.append(
                InlineKeyboardButton(
                    text="🗑",
                    callback_data=f"session_delete:{name}"
                )
            )
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton(text="🧹 Очистить слоты/реестр", callback_data="session_clear_all"),
    ])
    keyboard.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data="session_refresh"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def vk_pool_kb(bots: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for bot in bots:
        rows.append([
            InlineKeyboardButton(text=f"🗑 @{bot}", callback_data=f"vkpool_del:{bot}")
        ])
    rows.append([
        InlineKeyboardButton(text="➕ Добавить", callback_data="vkpool_add"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data="vkpool_refresh"),
    ])
    rows.append([
        InlineKeyboardButton(text="⬅️ Назад к сессиям", callback_data="session_refresh"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_maintenance_kb() -> InlineKeyboardMarkup:
    """Меню обслуживания бота"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🗄 Импорт БД", callback_data="load_database"),
                InlineKeyboardButton(text="🧹 Очистка", callback_data="admin_cleanup"),
            ],
            [
                InlineKeyboardButton(text="♻️ Перезапуск", callback_data="admin_restart_confirm"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"),
            ],
        ]
    )


def session_slot_select_kb(slot: str, session_names: list[str], current: Optional[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for name in session_names:
        prefix_parts = []
        if any(cfg.name == name for cfg in TELEGRAM_SESSIONS):
            prefix_parts.append("🛡")
        if name == current:
            prefix_parts.append("✅")
        prefix = (" ".join(prefix_parts) + " ") if prefix_parts else ""
        buttons.append([
            InlineKeyboardButton(
                text=f"{prefix}{name}",
                callback_data=f"session_slot_assign:{slot}:{name}"
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            text="🗑 Очистить",
            callback_data=f"session_slot_assign:{slot}:none"
        )
    ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="session_refresh"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def session_bot_selector_kb(
    session_alias: str,
    available_bots: list[str],
    selected_bots: set[str],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for idx, bot in enumerate(available_bots):
        icon = "✅" if bot in selected_bots else "⚪️"
        button = InlineKeyboardButton(
            text=f"{icon} @{bot}",
            callback_data=f"session_bot_toggle:{session_alias}:{idx}"
        )
        current_row.append(button)
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    rows.append([
        InlineKeyboardButton(
            text="🧹 Все боты",
            callback_data=f"session_bot_reset:{session_alias}"
        )
    ])
    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="session_bots_back"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def session_delete_confirm_kb(session_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить",
                    callback_data=f"session_delete_confirm:{session_name}"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="session_refresh"
                ),
            ]
        ]
    )


def confirm_action_kb(confirm_callback: str, cancel_callback: str = "main_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=confirm_callback),
                InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_callback),
            ]
        ]
    )


def session_auth_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Запустить мастер", callback_data="sessionauth_start")],
            [InlineKeyboardButton(text="📋 Статус авторизации", callback_data="sessionauth_status")],
            [
                InlineKeyboardButton(text="🛑 Отмена", callback_data="sessionauth_cancel"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"),
            ],
        ]
    )


def session_auth_slot_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Slot A", callback_data="sessionauth_slot:slot_a"),
                InlineKeyboardButton(text="Slot B", callback_data="sessionauth_slot:slot_b"),
            ],
            [InlineKeyboardButton(text="🗄 Резерв (без назначения)", callback_data="sessionauth_slot:reserve")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sessionauth_back")],
        ]
    )
