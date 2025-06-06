"""Обработчики команд статистики"""

import logging
from datetime import datetime, timedelta

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile

from bot.config import ADMIN_IDS
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import back_to_menu_kb, main_menu_kb
from bot.utils.export import export_statistics_report
from db_module import VKDatabase

router = Router()
logger = logging.getLogger("stats_handler")


@router.message(Command("stats"))
async def cmd_user_stats(msg: Message, db: VKDatabase):
    """Показ статистики пользователя"""
    user_id = msg.from_user.id
    stats = await db.get_user_statistics(user_id)

    efficiency = 0
    if stats["total_checked"] > 0:
        efficiency = int((stats["found_data_count"] / stats["total_checked"]) * 100)

    stats_text = MESSAGES["user_stats"].format(
        user_id=user_id,
        total_checked=stats["total_checked"],
        found_data_count=stats["found_data_count"],
        days_active=stats["days_active"],
        efficiency=efficiency
    )

    await msg.answer(stats_text, reply_markup=back_to_menu_kb())


@router.message(Command("status"))
async def cmd_status(msg: Message):
    """Проверка текущего статуса обработки"""
    user_id = msg.from_user.id

    # Импортируем здесь чтобы избежать циклических импортов
    from bot.utils.session_manager import get_user_session
    from bot.utils.helpers import create_progress_bar, format_time
    from bot.keyboards.inline import processing_menu_kb, finish_kb

    session = await get_user_session(user_id)

    if not session or not session.get("links"):
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    total = len(session.get("links", []))
    results = session.get("results", {})
    processed = len(results)

    # Правильный подсчет статистики
    found = 0
    not_found = 0

    for data in results.values():
        if data.get("phones") or data.get("full_name") or data.get("birth_date"):
            found += 1
        else:
            not_found += 1

    pending = total - processed

    progress_bar = create_progress_bar(processed, total)
    percent = int((processed / total) * 100) if total > 0 else 0

    status_text = MESSAGES["processing_status"].format(
        progress_bar=progress_bar,
        processed=processed,
        total=total,
        percent=percent,
        found=found,
        pending=pending,
        not_found=not_found,
        time=format_time()
    )

    await msg.answer(status_text, reply_markup=processing_menu_kb() if pending > 0 else finish_kb())


@router.message(Command("export"))
async def cmd_export(msg: Message, bot):
    """Экспорт результатов текущей сессии"""
    user_id = msg.from_user.id

    from bot.utils.session_manager import get_user_session
    from bot.utils.export import create_excel_from_results

    session = await get_user_session(user_id)

    if not session:
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    all_results = session.get("results", {})
    links_order = session.get("links_order", [])

    if not links_order:
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    # Генерируем файл с результатами
    files = await create_excel_from_results(all_results, links_order)

    for file_path, caption in files:
        try:
            await bot.send_document(
                msg.chat.id,
                FSInputFile(file_path),
                caption=caption
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            await msg.answer(f"⚠️ Не удалось отправить файл: {str(e)}")

    from bot.keyboards.inline import finish_kb
    await msg.answer("Готово! Выберите дальнейшее действие:", reply_markup=finish_kb())


@router.message(Command("cancel"))
async def cmd_cancel(msg: Message):
    """Отмена текущей операции"""
    user_id = msg.from_user.id

    from bot.utils.session_manager import clear_user_session
    await clear_user_session(user_id)

    await msg.answer("🚫 Обработка отменена. Все данные очищены.", reply_markup=main_menu_kb(user_id, ADMIN_IDS))


@router.callback_query(F.data == "user_stats")
async def on_user_stats(call: CallbackQuery, db: VKDatabase):
    """Callback для показа статистики пользователя"""
    await call.answer()
    user_id = call.from_user.id
    stats = await db.get_user_statistics(user_id)

    efficiency = 0
    if stats["total_checked"] > 0:
        efficiency = int((stats["found_data_count"] / stats["total_checked"]) * 100)

    stats_text = MESSAGES["user_stats"].format(
        user_id=user_id,
        total_checked=stats["total_checked"],
        found_data_count=stats["found_data_count"],
        days_active=stats["days_active"],
        efficiency=efficiency
    )

    await call.message.edit_text(stats_text, reply_markup=back_to_menu_kb())


@router.message(Command("mystats"))
async def cmd_my_detailed_stats(msg: Message, db: VKDatabase):
    """Детальная статистика пользователя с экспортом"""
    user_id = msg.from_user.id

    # Получаем расширенную статистику
    basic_stats = await db.get_user_statistics(user_id)

    # Получаем статистику по периодам
    today_stats = await db.get_user_statistics_by_period(user_id, 1)
    week_stats = await db.get_user_statistics_by_period(user_id, 7)
    month_stats = await db.get_user_statistics_by_period(user_id, 30)

    # Формируем текст
    efficiency = 0
    if basic_stats["total_checked"] > 0:
        efficiency = int((basic_stats["found_data_count"] / basic_stats["total_checked"]) * 100)

    detailed_text = f"""
📊 <b>Детальная статистика</b>

👤 ID: <code>{user_id}</code>

<b>📈 За все время:</b>
- Проверено ссылок: {basic_stats['total_checked']:,}
- Найдено данных: {basic_stats['found_data_count']:,}
- Эффективность: {efficiency}%
- Дней активности: {basic_stats['days_active']}

<b>📅 Сегодня:</b>
- Проверено: {today_stats['checked']}
- Найдено: {today_stats['found']}

<b>📅 За 7 дней:</b>
- Проверено: {week_stats['checked']}
- Найдено: {week_stats['found']}

<b>📅 За 30 дней:</b>
- Проверено: {month_stats['checked']}
- Найдено: {month_stats['found']}
"""

    # Добавляем кнопку экспорта
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    export_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать отчет", callback_data="export_my_stats")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ]
    )

    await msg.answer(detailed_text, reply_markup=export_kb)


@router.callback_query(F.data == "export_my_stats")
async def on_export_my_stats(call: CallbackQuery, db: VKDatabase, bot):
    """Экспорт статистики пользователя"""
    await call.answer("📥 Генерирую отчет...")
    user_id = call.from_user.id

    # Получаем все данные для отчета
    stats = await db.get_user_statistics(user_id)

    try:
        # Создаем Excel отчет
        report_path = await export_statistics_report(stats)

        # Отправляем файл
        await bot.send_document(
            call.message.chat.id,
            FSInputFile(report_path),
            caption="📊 Ваш персональный отчет статистики"
        )

        await call.message.answer("✅ Отчет успешно сгенерирован!")

    except Exception as e:
        logger.error(f"Ошибка при экспорте статистики: {e}")
        await call.message.answer("❌ Ошибка при создании отчета")


@router.message(Command("top"))
async def cmd_top_users(msg: Message, db: VKDatabase):
    """Топ пользователей (только для админов)"""
    if msg.from_user.id not in ADMIN_IDS:
        return

    # Получаем топ пользователей
    top_users = await db.get_top_users(limit=10)

    if not top_users:
        await msg.answer("📊 Пока нет статистики по пользователям", reply_markup=back_to_menu_kb())
        return

    top_text = "🏆 <b>Топ 10 пользователей</b>\n\n"

    medals = ["🥇", "🥈", "🥉"]

    for i, user in enumerate(top_users):
        medal = medals[i] if i < 3 else f"{i + 1}."

        efficiency = 0
        if user['total_checked'] > 0:
            efficiency = int((user['found_data'] / user['total_checked']) * 100)

        name = user['first_name'] or user['username'] or "Unknown"

        top_text += f"{medal} <b>{name}</b>\n"
        top_text += f"   Проверено: {user['total_checked']:,} | "
        top_text += f"Найдено: {user['found_data']:,} | "
        top_text += f"Эффективность: {efficiency}%\n\n"

    await msg.answer(top_text, reply_markup=back_to_menu_kb())