"""Обработчики админских функций"""

import logging
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from bot.config import ADMIN_IDS, VK_BOT_USERNAME
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    db_load_menu_kb
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
    clear_user_session
)
from db_module import VKDatabase
from services.vk_service import VKService

router = Router()
logger = logging.getLogger("admin_handler")


def is_admin(user_id: int) -> bool:
    """Проверка является ли пользователь администратором"""
    return user_id in ADMIN_IDS


@router.message(Command("botstatus"))
async def cmd_bot_status(msg: Message, vk_service: VKService):
    """Проверка статуса VK бота (только для админов)"""
    if not is_admin(msg.from_user.id):
        return

    status_msg = await msg.answer("🔄 Проверяю статус бота...")

    try:
        # Проверяем баланс
        balance = await vk_service.check_balance()

        # Получаем статистику
        status_text = f"""
🤖 <b>Статус VK бота</b>

📱 <b>Бот:</b> @{VK_BOT_USERNAME}
💰 <b>Баланс:</b> {balance or 'Неизвестно'} поисков
📊 <b>Обработано:</b> {vk_service.processed_count} ссылок
❌ <b>Ошибок:</b> {vk_service.error_count}

📡 <b>Состояние:</b> {'✅ Активен' if vk_service.is_initialized else '❌ Не инициализирован'}
🕐 <b>Время работы:</b> {datetime.now().strftime('%H:%M:%S')}
"""

        await status_msg.edit_text(status_text, reply_markup=back_to_menu_kb())

    except Exception as e:
        await status_msg.edit_text(
            f"❌ Ошибка проверки: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.message(Command("debug"))
async def cmd_debug(msg: Message, db: VKDatabase):
    """Команда для отладки (только для админов)"""
    if not is_admin(msg.from_user.id):
        return

    user_id = msg.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await msg.answer("❌ Нет активной сессии", reply_markup=back_to_menu_kb())
        return

    # Получаем статистику БД
    db_stats = await db.get_database_statistics()
    phone_stats = await db.get_phone_statistics()

    debug_info = f"""
🐛 <b>Отладочная информация</b>

<b>VK Бот:</b> @{VK_BOT_USERNAME}

<b>Сессия пользователя:</b>
- Ссылок всего: {len(session.get('links', []))}
- Результатов: {len(session.get('results', {}))}
- Режим: {session.get('file_mode', 'none')}

<b>База данных:</b>
- Всего записей: {db_stats['total_records']}
- С данными: {db_stats['with_data']}
- Без данных: {db_stats['without_data']}

<b>Телефоны:</b>
- Уникальных: {phone_stats['total_unique_phones']}
- С несколькими VK: {phone_stats['phones_with_multiple_links']}
"""

    if session.get('results'):
        results = session['results']
        debug_info += "\n<b>Последние результаты:</b>"

        for i, (link, data) in enumerate(list(results.items())[:5]):
            debug_info += f"\n{i + 1}. {link[:30]}..."
            debug_info += f"\n   📱 Телефоны: {len(data.get('phones', []))}"
            debug_info += f"\n   👤 Имя: {'✓' if data.get('full_name') else '✗'}"
            debug_info += f"\n   🎂 ДР: {'✓' if data.get('birth_date') else '✗'}"

        if len(results) > 5:
            debug_info += f"\n\n... и еще {len(results) - 5} результатов"

    await msg.answer(debug_info, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "load_database")
async def on_load_database(call: CallbackQuery):
    """Режим загрузки базы данных"""
    await call.answer()
    user_id = call.from_user.id

    # Проверка прав администратора
    if not is_admin(user_id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    # Устанавливаем режим загрузки БД
    session = {"db_load_mode": True}
    await save_user_session(user_id, session)

    await call.message.edit_text(MESSAGES["db_load_mode"], reply_markup=db_load_menu_kb())


@router.callback_query(F.data == "cancel_db_load")
async def on_cancel_db_load(call: CallbackQuery):
    """Отмена загрузки БД"""
    await call.answer("❌ Загрузка отменена")
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))


@router.message(Command("broadcast"))
async def cmd_broadcast(msg: Message, db: VKDatabase, bot):
    """Рассылка сообщения всем пользователям (только для главного админа)"""
    if msg.from_user.id != ADMIN_IDS[0]:  # Только первый админ
        return

    # Проверяем формат команды
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer(
            "📢 <b>Рассылка сообщений</b>\n\n"
            "Использование:\n"
            "<code>/broadcast Ваше сообщение</code>",
            reply_markup=back_to_menu_kb()
        )
        return

    broadcast_text = parts[1]

    # Получаем список всех пользователей
    users = await db.get_all_users()

    # Счетчики
    sent = 0
    failed = 0

    status_msg = await msg.answer(f"📤 Начинаю рассылку {len(users)} пользователям...")

    for user in users:
        try:
            await bot.send_message(
                user['user_id'],
                f"📢 <b>Сообщение от администрации:</b>\n\n{broadcast_text}"
            )
            sent += 1
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user['user_id']}: {e}")
            failed += 1

        # Обновляем статус каждые 10 сообщений
        if (sent + failed) % 10 == 0:
            await status_msg.edit_text(
                f"📤 Рассылка...\n"
                f"✅ Отправлено: {sent}\n"
                f"❌ Ошибок: {failed}"
            )

    # Финальный отчет
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 Статистика:\n"
        f"- Всего пользователей: {len(users)}\n"
        f"- Успешно отправлено: {sent}\n"
        f"- Ошибок: {failed}",
        reply_markup=back_to_menu_kb()
    )


@router.message(Command("dbstats"))
async def cmd_db_stats(msg: Message, db: VKDatabase):
    """Подробная статистика БД (только для админов)"""
    if not is_admin(msg.from_user.id):
        return

    # Получаем различную статистику
    general_stats = await db.get_database_statistics()
    phone_stats = await db.get_phone_statistics()
    user_stats = await db.get_users_statistics()

    stats_text = f"""
📊 <b>Статистика базы данных</b>

<b>🔗 VK профили:</b>
- Всего записей: {general_stats['total_records']:,}
- С данными: {general_stats['with_data']:,}
- Без данных: {general_stats['without_data']:,}
- Эффективность: {(general_stats['with_data'] / general_stats['total_records'] * 100):.1f}%

<b>📱 Телефоны:</b>
- Уникальных номеров: {phone_stats['total_unique_phones']:,}
- С несколькими VK: {phone_stats['phones_with_multiple_links']:,}

<b>👥 Пользователи:</b>
- Всего: {user_stats['total_users']:,}
- Активных за 7 дней: {user_stats['active_7d']:,}
- Активных за 30 дней: {user_stats['active_30d']:,}
"""

    # Добавляем топ телефонов
    if phone_stats['top_phones']:
        stats_text += "\n<b>🏆 Топ телефонов (по кол-ву профилей):</b>\n"
        for i, (phone, count) in enumerate(phone_stats['top_phones'][:5], 1):
            stats_text += f"{i}. {phone} - {count} профилей\n"

    await msg.answer(stats_text, reply_markup=back_to_menu_kb())