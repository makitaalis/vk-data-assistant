import logging
from pathlib import Path
import tempfile
import json
from datetime import datetime

from aiogram import Router, F
from aiogram.types import CallbackQuery, FSInputFile

from bot.config import ADMIN_IDS, EXPORT_DATE_FORMAT
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    finish_kb,
    continue_kb,
    duplicate_actions_kb
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
    clear_user_session
)
from bot.utils.export import create_excel_from_results, create_json_report
from bot.handlers.search import start_processing
from services.analysis_service import FileAnalyzer
from services.excel_service import ExcelProcessor

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

    # Восстанавливаем processor из сессии если возможно
    processor = None
    if session.get('temp_file') and session.get('vk_column_name'):
        file_path = Path(session['temp_file'])
        if file_path.exists():
            processor = ExcelProcessor()
            processor.load_excel_file(file_path)

    # Генерируем файлы
    files_to_send = []

    if processor and processor.original_df is not None:
        # Если есть processor с исходными данными, используем его
        temp_dir = Path(tempfile.mkdtemp())
        ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
        output_path = temp_dir / f"vk_data_complete_{ts}.xlsx"

        # Сохраняем с исходными данными
        success = processor.save_results_with_original_data(all_results, output_path)

        if success:
            # Подсчет статистики
            found_count = sum(1 for data in all_results.values() if data.get("phones"))
            not_found_count = len(links_order) - found_count

            caption = f"""📊 Файл с результатами готов!

✅ Обработано: {len(links_order)} ссылок
📱 Найдены телефоны: {found_count}
❌ Без телефонов: {not_found_count}

💾 Все исходные данные сохранены!"""

            files_to_send.append((output_path, caption))
    else:
        # Если нет processor, используем стандартный метод
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
async def on_continue(call: CallbackQuery):
    """Продолжение обработки после лимита"""
    await call.answer("▶️ Продолжаю обработку...")

    # TODO: Реализовать продолжение обработки
    await call.message.edit_text(
        "⚠️ Функция в разработке\n\n"
        "Пока вы можете скачать текущие результаты и продолжить позже.",
        reply_markup=continue_kb()
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

    pause_text = """
⏸ <b>Обработка приостановлена</b>

Ваш прогресс сохранен. Вы можете:
- Продолжить обработку в любое время
- Скачать текущие результаты
- Отменить и начать заново
"""
    await call.message.edit_text(pause_text, reply_markup=continue_kb())


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


@router.callback_query(F.data == "cancel")
async def on_cancel(call: CallbackQuery):
    """Отмена текущей операции"""
    await call.answer()
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(
        MESSAGES["operation_cancelled"],
        reply_markup=main_menu_kb(user_id, ADMIN_IDS)
    )