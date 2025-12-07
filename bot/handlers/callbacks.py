import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

from bot.config import ADMIN_IDS, EXPORT_DATE_FORMAT
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    finish_kb,
    continue_kb,
    duplicate_actions_kb,
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
    clear_user_session
)
from bot.utils.export import create_excel_from_results, create_json_report, restore_processor_from_session
from bot.handlers.search import start_processing
from services.analysis_service import FileAnalyzer
from services.excel_service import ExcelProcessor
from bot.utils.helpers import create_temp_dir
from bot.utils.admin_notifications import notify_admins
from db_module import VKDatabase

router = Router()
logger = logging.getLogger("callbacks_handler")


@router.callback_query(F.data == "my_results")
async def on_my_results(call: CallbackQuery):
    """Показ результатов текущей сессии"""
    await call.answer()
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("results"):
        await call.message.edit_text(
            "📭 У вас пока нет сохраненных результатов.\n\n"
            "Начните с загрузки файла или отправки ссылок!",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    results = session["results"]
    total = len(results)

    # Подсчет статистики
    found_count = sum(
        1 for data in results.values()
        if data.get("phones") or data.get("full_name") or data.get("birth_date")
    )
    not_found_count = total - found_count

    stats_text = f"""
📊 <b>Текущие результаты</b>

- Всего проверено: {total}
- Найдено данных: {found_count} ✅
- Без результата: {not_found_count} ❌

Выберите действие:
"""

    await call.message.edit_text(stats_text, reply_markup=finish_kb())


@router.callback_query(F.data == "admin_restart_confirm")
async def on_admin_restart_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Перезапустить", callback_data="restart_bot"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu"),
            ]
        ]
    )
    await call.answer()
    await call.message.edit_text(
        "♻️ Перезапустить бота? Все текущие процессы будут остановлены.",
        reply_markup=keyboard
    )


@router.callback_query(F.data == "restart_bot")
async def on_restart_bot(call: CallbackQuery, bot: Bot):
    """Перезапуск бота (доступно только администраторам)."""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        await call.answer("♻️ Перезапуск...", show_alert=False)
    except Exception as exc:
        logger.warning("Не удалось ответить на callback перед перезапуском: %s", exc)
    await call.message.answer(
        "♻️ Запускаю перезапуск. Бот вернётся в сеть через несколько секунд.",
        reply_markup=main_menu_kb(call.from_user.id, ADMIN_IDS)
    )
    actor = call.from_user.full_name or call.from_user.username or str(call.from_user.id)
    await notify_admins(bot, f"♻️ <b>{actor}</b> инициировал перезапуск бота.")

    async def _restart():
        await asyncio.sleep(2)
        os.execl(sys.executable, sys.executable, *sys.argv)

    asyncio.create_task(_restart())


@router.callback_query(F.data == "download_results")
async def on_download_results(call: CallbackQuery, bot):
    """Скачивание результатов"""
    await call.answer("📥 Подготавливаю файл...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("results"):
        await call.message.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    all_results = session.get("results", {})
    links_order = session.get("links_order", [])

    file_expected = bool(session and session.get("temp_file"))
    processor = restore_processor_from_session(session)
    processor_ready = bool(processor and processor.original_df is not None)

    files_to_send = []

    if processor_ready:
        temp_dir = create_temp_dir(prefix="export")
        ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
        output_path = temp_dir / f"vk_data_complete_{ts}.xlsx"

        success = processor.save_results_with_original_data(all_results, output_path)
        if success:
            found_count = sum(1 for data in all_results.values() if data.get("phones"))
            not_found_count = len(links_order) - found_count

            caption = f"""📊 Файл с результатами готов!

✅ Обработано: {len(links_order)} ссылок
📱 Найдены телефоны: {found_count}
❌ Без телефонов: {not_found_count}

💾 Все исходные данные сохранены!"""

            files_to_send.append((output_path, caption))
        else:
            await call.message.answer(
                "⚠️ Не удалось сохранить файл с исходными данными. Попробуйте ещё раз или загрузите файл заново.",
                reply_markup=finish_kb()
            )
            return
    elif file_expected:
        file_name = (session or {}).get("file_name") or "исходный файл"
        await call.message.answer(
            f"⚠️ Исходный файл «{file_name}» недоступен, поэтому я не могу обновить его новыми данными. "
            "Загрузите файл заново и повторите поиск.",
            reply_markup=finish_kb()
        )
        return
    else:
        files_to_send = await create_excel_from_results(all_results, links_order)

    # Отправляем файлы
    for file_path, caption in files_to_send:
        try:
            await bot.send_document(
                call.message.chat.id,
                FSInputFile(file_path),
                caption=caption
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            await call.message.answer(f"⚠️ Не удалось отправить файл: {str(e)}")

    await call.message.answer("✅ Готово!", reply_markup=finish_kb())


@router.callback_query(F.data == "add_more")
async def on_add_more(call: CallbackQuery):
    """Добавление еще ссылок к текущей сессии"""
    await call.answer()
    add_more_text = """
➕ <b>Добавление ссылок</b>

Вы можете добавить еще ссылки к текущей сессии.

Отправьте мне:
- Новый Excel файл
- Или ссылки в сообщении

<i>Все новые результаты будут добавлены к существующим.</i>
"""
    await call.message.edit_text(add_more_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "export_current")
async def on_export_current(call: CallbackQuery, bot):
    """Экспорт текущих результатов"""
    await call.answer("📊 Экспортирую текущие результаты...")
    await on_download_results(call, bot)


@router.callback_query(F.data == "continue")
async def on_continue(call: CallbackQuery, db: VKDatabase, vk_service, bot):
    """Продолжение обработки после лимита"""
    await call.answer("▶️ Возобновляю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("links"):
        await call.message.edit_text(
            "❌ Активная сессия не найдена. Отправьте ссылки или файл заново.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    pending_links = session.get("pending_links") or []
    if not pending_links:
        await call.message.edit_text(
            "✅ Нет отложенных ссылок — можно скачать результаты или начать новый поиск.",
            reply_markup=finish_kb()
        )
        return


@router.callback_query(F.data == "cancel_all_tasks")
async def on_cancel_all_tasks(call: CallbackQuery, task_queue=None):
    await call.answer()
    if not task_queue:
        await call.message.answer("⚠️ Очередь задач работает в старом режиме. Попробуйте остановить обработку вручную.")
        return

    user_id = call.from_user.id
    cancelled = await task_queue.cancel_user_tasks(user_id)
    await task_queue.set_user_cancel_flag(user_id)
    await clear_user_session(user_id)

    await call.message.answer(
        f"🛑 Отмена отправлена. Переведено в cancelled: {cancelled} задач(и).",
        reply_markup=back_to_menu_kb()
    )


# Обработчики дубликатов
@router.callback_query(F.data == "remove_duplicates")
async def on_remove_duplicates(call: CallbackQuery, db, vk_service, bot):
    """Удаление дубликатов из обработки"""
    if not db:
        await call.answer("❌ Сервис временно недоступен", show_alert=True)
        return

    await call.answer("🗑 Удаляю дубликаты...")
    user_id = call.from_user.id

    if user_id not in ADMIN_IDS:
        await call.answer("🚫 Доступ только для администраторов", show_alert=True)
        return

    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    duplicate_check = session.get("duplicate_check", {})

    # Восстанавливаем processor если нужно
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)

    # Оставляем только новые ссылки (исключаем все типы дубликатов)
    links_to_process = duplicate_check.get("new", [])

    if not links_to_process:
        stats = duplicate_check.get("stats", {})
        await call.message.edit_text(
            f"ℹ️ Все ссылки являются дубликатами.\n\n"
            f"📊 Статистика:\n"
            f"- Дубликатов по VK: {stats.get('duplicate_by_vk', 0)}\n"
            f"- Дубликатов по телефонам: {stats.get('duplicate_by_phone', 0)}\n"
            f"- Дубликатов по обоим: {stats.get('duplicate_by_both', 0)}\n\n"
            f"Нет новых ссылок для обработки.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    await call.message.edit_text(
        f"✅ Дубликаты удалены!\n\n"
        f"Будет обработано: {len(links_to_process)} новых ссылок"
    )

    # Запускаем обработку только новых ссылок
    await start_processing(call.message, links_to_process, processor, duplicate_check, user_id, db, vk_service, bot)


@router.callback_query(F.data == "keep_all")
async def on_keep_all(call: CallbackQuery, db, vk_service, bot):
    """Обработка всех ссылок включая дубликаты"""
    if not db:
        await call.answer("❌ Сервис временно недоступен", show_alert=True)
        return

    await call.answer("📋 Обрабатываю все ссылки...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    # Получаем все данные из сессии
    all_links = session.get("all_links", [])
    duplicate_check = session.get("duplicate_check", {})

    # Восстанавливаем processor если нужно
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)

    # Получаем статистику для отображения
    stats = duplicate_check.get("stats", {})
    total_duplicates = stats.get("duplicate_by_vk", 0) + stats.get("duplicate_by_phone", 0) + stats.get(
        "duplicate_by_both", 0)

    await call.message.edit_text(
        f"✅ Начинаю обработку всех {len(all_links)} ссылок\n\n"
        f"<i>Из них дубликатов: {total_duplicates}</i>\n"
        f"<i>Данные из кеша будут использованы автоматически</i>"
    )

    # Запускаем обработку всех ссылок
    await start_processing(
        call.message,
        all_links,
        processor,
        duplicate_check,
        user_id,
        db,
        vk_service,
        bot
    )


@router.callback_query(F.data == "update_duplicates")
async def on_update_duplicates(call: CallbackQuery, db, vk_service, bot):
    """Обновление данных дубликатов"""
    if not db:
        await call.answer("❌ Сервис временно недоступен", show_alert=True)
        return

    await call.answer("🔄 Обновляю данные...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    duplicate_check = session.get("duplicate_check", {})

    # Восстанавливаем processor если нужно
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)

    # Будем перепроверять только дубликаты без данных (исключаем дубликаты по телефонам)
    links_to_update = duplicate_check.get("duplicates_no_data", [])

    # Исключаем ссылки которые являются дубликатами по телефонам
    phone_duplicates = set(duplicate_check.get("duplicate_phones", {}).keys())
    links_to_update = [link for link in links_to_update if link not in phone_duplicates]

    if not links_to_update:
        await call.message.edit_text(
            "ℹ️ Нет дубликатов для обновления.\n"
            "Все существующие дубликаты уже имеют данные или являются дубликатами по телефонам.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    await call.message.edit_text(
        f"🔄 Обновляю данные для {len(links_to_update)} ссылок без результатов"
    )

    # Запускаем обработку
    await start_processing(call.message, links_to_update, processor, duplicate_check, user_id, db, vk_service, bot)


@router.callback_query(F.data == "cancel_processing")
async def on_cancel_processing(call: CallbackQuery):
    """Отмена обработки"""
    await call.answer()
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(
        "🚫 Обработка отменена.",
        reply_markup=main_menu_kb(user_id, ADMIN_IDS)
    )


# Обработчики анализа файлов
@router.callback_query(F.data == "analysis_details")
async def on_analysis_details(call: CallbackQuery, db):
    """Показ деталей анализа"""
    await call.answer()
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.answer("❌ Результаты анализа не найдены")
        return

    analysis = session['analysis_result']
    analyzer = FileAnalyzer(db)
    details_text = await analyzer.format_analysis_details(analysis)

    # Отправляем новое сообщение с деталями
    await call.message.answer(details_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "export_analysis")
async def on_export_analysis(call: CallbackQuery, bot):
    """Экспорт результатов анализа"""
    await call.answer("💾 Экспортирую отчет...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.answer("❌ Результаты анализа не найдены")
        return

    analysis = session['analysis_result']

    try:
        # Создаем JSON отчет
        json_path = await create_json_report(analysis, "analysis_report")

        # Отправляем файл
        await bot.send_document(
            call.message.chat.id,
            FSInputFile(json_path),
            caption="📊 Полный отчет анализа файла"
        )

        await call.message.answer("✅ Отчет успешно экспортирован!")

    except Exception as e:
        logger.error(f"Ошибка при экспорте анализа: {e}")
        await call.message.answer("❌ Ошибка при экспорте отчета")


# Обработчики управления процессом
@router.callback_query(F.data == "pause")
async def on_pause(call: CallbackQuery):
    """Пауза обработки"""
    await call.answer("⏸ Обработка приостановлена")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if session:
        session["paused"] = True
        await save_user_session(user_id, session)

    # Импортируем клавиатуру для паузы
    from bot.keyboards.inline import InlineKeyboardButton, InlineKeyboardMarkup
    
    # Создаем клавиатуру с кнопкой возобновления
    resume_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Продолжить обработку", callback_data="resume"),
                InlineKeyboardButton(text="📊 Статистика", callback_data="stats")
            ],
            [
                InlineKeyboardButton(text="📥 Скачать текущие результаты", callback_data="export_current")
            ]
        ]
    )
    
    pause_text = """
⏸ <b>Обработка приостановлена</b>

Ваш прогресс сохранен. Вы можете:
- Продолжить обработку в любое время
- Скачать текущие результаты
"""
    await call.message.edit_text(pause_text, reply_markup=resume_kb)


@router.callback_query(F.data == "stats")
async def on_stats_update(call: CallbackQuery):
    """Обновление статистики во время обработки"""
    await call.answer("📊 Обновляю статистику...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("links"):
        await call.message.answer(MESSAGES["no_session"])
        return

    # Считаем статистику
    total = len(session.get("links", []))
    results = session.get("results", {})
    processed = len(results)

    found = sum(
        1 for data in results.values()
        if data.get("phones") or data.get("full_name") or data.get("birth_date")
    )
    not_found = processed - found
    pending = total - processed

    # Формируем сообщение
    from bot.utils.helpers import create_progress_bar, format_time

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

    # Обновляем сообщение
    from bot.keyboards.inline import processing_menu_kb
    await call.message.edit_text(
        status_text,
        reply_markup=processing_menu_kb() if pending > 0 else finish_kb()
    )


@router.callback_query(F.data == "resume")
async def on_resume(call: CallbackQuery, db, vk_service, bot):
    """Возобновление обработки после паузы"""
    await call.answer("▶️ Проверяю состояние…")
    user_id = call.from_user.id
    session = await get_user_session(user_id)
    
    if session:
        pending_links = list(session.get("pending_links") or [])
        delayed_links = list((session.get("delayed_links") or {}).keys())

        def _remaining_links() -> List[str]:
            order = session.get("links_order") or session.get("links") or []
            pending_set = set(pending_links)
            delayed_set = set(delayed_links)
            result: List[str] = []
            if order:
                for link in order:
                    if link in pending_set or link in delayed_set:
                        result.append(link)
                        pending_set.discard(link)
                        delayed_set.discard(link)
            for link in pending_links:
                if link not in result:
                    result.append(link)
            for link in delayed_links:
                if link not in result:
                    result.append(link)
            return result

        processing_active = bool(session.get("processing_active"))
        session["paused"] = False
        await save_user_session(user_id, session)

        if processing_active:
            await call.answer("▶️ Возобновляю обработку…", show_alert=False)
            total = len(session.get("links", []))
            processed = len(session.get("results", {}))
            found = sum(
                1 for data in (session.get("results") or {}).values()
                if data.get("phones") or data.get("full_name") or data.get("birth_date")
            )
            not_found = processed - found
            pending = max(total - processed, 0)
            from bot.utils.helpers import create_progress_bar
            from bot.keyboards.inline import processing_menu_kb
            progress_bar = create_progress_bar(processed, total)
            percent = int((processed / total) * 100) if total > 0 else 0
            resume_text = f"""▶️ <b>Обработка возобновлена</b>

{progress_bar}
<b>Прогресс:</b> {processed}/{total} ({percent}%)

📊 <b>Статистика:</b>
✅ Найдено данных: {found}
❌ Без результата: {not_found}
⏳ Осталось: {pending}

<i>Продолжаю с текущих сессий…</i>"""
            await call.message.edit_text(resume_text, reply_markup=processing_menu_kb())
            return

        remaining_links = _remaining_links()
        if not remaining_links:
            await call.message.edit_text(
                "⚠️ Нет ссылок для возобновления. Загрузите новый файл или начните новый поиск.",
                reply_markup=finish_kb()
            )
            return

        duplicate_check = session.get("duplicate_check") or {}
        processor = restore_processor_from_session(session)

        await call.message.answer(
            f"♻️ Перезапускаю обработку оставшихся {len(remaining_links)} ссылок…"
        )
        await start_processing(
            message=call.message,
            links_to_process=remaining_links,
            processor=processor,
            duplicate_check=duplicate_check,
            user_id=user_id,
            db=db,
            vk_service=vk_service,
            bot=bot,
            force_use_cache=True,
            skip_duplicate_filter=True,
            resume_session=session,
        )
    else:
        await call.message.edit_text(
            "❌ Сессия не найдена",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )


@router.callback_query(F.data == "process_with_cache")
async def on_process_with_cache(call: CallbackQuery, db, vk_service, bot):
    """Обработка с использованием кеша"""
    await call.answer("💾 Обрабатываю данные с использованием кеша...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)
    
    if not session or not session.get("force_search_available"):
        await call.message.edit_text(
            "❌ Сессия не найдена",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Получаем ссылки для поиска
    links_to_process = session.get("cached_links", session.get("links", []))
    
    # Восстанавливаем processor из файла
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            from services.excel_service import ExcelProcessor
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)
            # Восстанавливаем маппинг если есть
            if session.get('vk_links_mapping'):
                processor.vk_links_mapping = session['vk_links_mapping']
    
    if not links_to_process:
        await call.message.edit_text(
            "❌ Не найдены ссылки для обработки",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Запускаем обычную обработку с использованием кеша
    await call.message.edit_text(
        f"🔄 <b>Обрабатываю данные с кешем</b>\n\n"
        f"📊 Будет обработано: {len(links_to_process)} ссылок\n"
        f"💾 Данные из кеша будут использованы автоматически\n"
        f"⏳ Это может занять некоторое время..."
    )
    
    # Очищаем предыдущие результаты для свежей обработки
    session["results"] = {}
    session["force_search_available"] = False  # Убираем флаг, чтобы не зациклиться
    await save_user_session(user_id, session)
    
    # Используем обычную функцию обработки с кешем
    from bot.handlers.search import start_processing
    await start_processing(
        call.message, 
        links_to_process, 
        processor,
        {},  # Пустой duplicate_check, так как проверяем все
        user_id, 
        db, 
        vk_service, 
        bot,
        force_use_cache=True
    )


@router.callback_query(F.data == "force_full_recheck")
async def on_force_full_recheck(call: CallbackQuery, db, vk_service, bot):
    """Принудительная проверка всех ссылок без использования кеша"""
    await call.answer("🔄 Запускаю полную проверку без кеша...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)
    
    if not session or not session.get("force_search_available"):
        await call.message.edit_text(
            "❌ Сессия не найдена",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Получаем ссылки для поиска
    links_to_process = session.get("cached_links", session.get("links", []))
    
    # Восстанавливаем processor из файла
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            from services.excel_service import ExcelProcessor
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)
            # Восстанавливаем маппинг если есть
            if session.get('vk_links_mapping'):
                processor.vk_links_mapping = session['vk_links_mapping']
    
    if not links_to_process:
        await call.message.edit_text(
            "❌ Не найдены ссылки для обработки",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Проверяем баланс
    from bot.handlers.balance import check_balance_before_processing
    if not await check_balance_before_processing(call.message, len(links_to_process), len(links_to_process), vk_service):
        return
    
    # Запускаем принудительную проверку без кеша
    await call.message.edit_text(
        f"🔄 <b>Запускаю полную проверку без кеша</b>\n\n"
        f"📊 Будет проверено заново: {len(links_to_process)} ссылок\n"
        f"⚠️ Кеш будет обновлен новыми данными\n"
        f"⏳ Это может занять некоторое время..."
    )
    
    # Очищаем предыдущие результаты
    session["results"] = {}
    session["force_search_available"] = False
    session["force_no_cache"] = True  # Флаг для игнорирования кеша
    await save_user_session(user_id, session)
    
    # Используем функцию принудительного поиска без кеша
    from bot.handlers.search import force_search_without_cache
    await force_search_without_cache(
        call.message, 
        links_to_process, 
        processor, 
        user_id, 
        db, 
        vk_service, 
        bot
    )


@router.callback_query(F.data == "process_only_new")
async def on_process_only_new(call: CallbackQuery, db, vk_service, bot):
    """Обработка только новых ссылок (которых нет в кеше)"""
    await call.answer("🆕 Обрабатываю только новые ссылки...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)
    
    if not session:
        await call.message.edit_text(
            "❌ Сессия не найдена",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Получаем все ссылки и проверяем какие есть в кеше
    all_links = session.get("links", [])
    if not all_links:
        await call.message.edit_text(
            "❌ Не найдены ссылки для обработки",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return
    
    # Получаем ссылки из кеша
    cached_results = await db.get_cached_results(all_links) if db else {}
    new_links = [link for link in all_links if link not in cached_results]
    
    if not new_links:
        await call.message.edit_text(
            "ℹ️ Все ссылки уже проверены и находятся в кеше.\n"
            "Используйте '💾 Обработать с кешем' для получения результатов.",
            reply_markup=all_cached_menu_kb()
        )
        return
    
    # Восстанавливаем processor
    processor = None
    if session.get('temp_file'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            from services.excel_service import ExcelProcessor
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)
            if session.get('vk_links_mapping'):
                processor.vk_links_mapping = session['vk_links_mapping']
    
    # Проверяем баланс
    from bot.handlers.balance import check_balance_before_processing
    total_links = len(all_links)
    if not await check_balance_before_processing(call.message, total_links, len(new_links), vk_service):
        return
    
    await call.message.edit_text(
        f"🆕 <b>Обрабатываю только новые ссылки</b>\n\n"
        f"📊 Новых ссылок: {len(new_links)}\n"
        f"💾 В кеше: {len(cached_results)}\n"
        f"⏳ Начинаю проверку..."
    )
    
    # Запускаем обработку только новых ссылок
    from bot.handlers.search import start_processing
    await start_processing(
        call.message,
        new_links,
        processor,
        {},
        user_id,
        db,
        vk_service,
        bot
    )


@router.callback_query(F.data == "force_continue_processing")
async def on_force_continue_processing(call: CallbackQuery, db, vk_service, bot):
    """Продолжение обработки при недостатке поисков"""
    await call.answer("▶️ Продолжаю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    pending = session.get("pending_processing")
    if not pending:
        await call.message.edit_text(
            "⚠️ Отложенная обработка не найдена. Попробуйте запустить файл заново.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    links_to_process = pending.get("links") or session.get("links", [])
    if not links_to_process:
        session.pop("pending_processing", None)
        await save_user_session(user_id, session)
        await call.message.edit_text(
            "❌ Нет ссылок для обработки. Запустите процесс заново.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    # Восстанавливаем processor из временного файла
    processor = None
    temp_path = session.get("temp_file")
    if temp_path:
        file_path = Path(temp_path)
        if file_path.exists():
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)
            if session.get("vk_links_mapping"):
                processor.vk_links_mapping = session["vk_links_mapping"]

    duplicate_check = pending.get("duplicate_check", {})

    # Обновляем сессию: убираем pending и устанавливаем флаг принудительного продолжения
    session["force_balance_override"] = True
    session.pop("pending_processing", None)
    await save_user_session(user_id, session)

    await call.message.edit_text("▶️ Продолжаю обработку с текущим балансом...")

    await start_processing(
        call.message,
        links_to_process,
        processor,
        duplicate_check,
        user_id,
        db,
        vk_service,
        bot
    )
