"""Обработчики для работы с файлами с поддержкой анализа дубликатов"""

import logging
import tempfile
from pathlib import Path

import pandas as pd
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery

from bot.config import ADMIN_IDS, MAX_LINKS_PER_FILE, ENABLE_DUPLICATE_REMOVAL
from bot.handlers.search import start_processing
from bot.keyboards.inline import (
    duplicate_actions_kb,
    main_menu_kb,
    back_to_menu_kb,
    file_action_menu_kb,
    analysis_results_kb,
    file_duplicates_menu_kb
)
from bot.utils.messages import MESSAGES
from bot.utils.session_manager import (
    get_user_session,
    clear_user_session,
    save_user_session
)
from db_loader import DatabaseLoader
from db_module import VKDatabase
from services.analysis_service import FileAnalyzer
from services.excel_service import ExcelProcessor

router = Router()
logger = logging.getLogger("files_handler")


@router.message(F.document)
async def on_document(msg: Message, db: VKDatabase, bot):
    """Обработка документов"""
    user_id = msg.from_user.id

    # Проверяем сессию пользователя
    session = await get_user_session(user_id)

    # Если включен режим загрузки БД
    if session and session.get("db_load_mode"):
        # Проверка прав администратора
        if user_id not in ADMIN_IDS:
            await msg.answer(MESSAGES["error_no_access"])
            return

        # Обработка файлов для загрузки в БД
        await handle_db_load(msg, db)
        return

    # Обычная обработка Excel файла
    if not msg.document.file_name.endswith(".xlsx"):
        await msg.answer(MESSAGES["error_file_format"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    await handle_excel_file(msg, bot)


async def handle_excel_file(msg: Message, bot):
    """Обработка Excel файла для поиска"""
    user_id = msg.from_user.id

    # Создаем временную папку и скачиваем файл
    temp_dir = Path(tempfile.mkdtemp())
    path_in = temp_dir / msg.document.file_name
    await bot.download(msg.document.file_id, destination=path_in)

    # Используем ExcelProcessor для первичного анализа
    processor = ExcelProcessor()
    links, row_indices, success = processor.load_excel_file(path_in)

    if not success or not links:
        await msg.answer(
            MESSAGES["error_no_vk_links"],
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    # Получаем анализ дубликатов
    duplicate_analysis = processor.get_duplicate_analysis()
    file_info = processor.get_file_info()

    # Сохраняем информацию о файле в сессию
    session = {
        'temp_file': str(path_in),
        'file_name': msg.document.file_name,
        'file_mode': 'pending',
        'processor': processor,
        'duplicate_analysis': duplicate_analysis,
        'file_info': file_info
    }
    await save_user_session(user_id, session)

    # Проверяем, превышает ли количество ссылок лимит
    if duplicate_analysis['total_links'] > MAX_LINKS_PER_FILE:
        await msg.answer(
            MESSAGES["error_file_too_large"].format(
                total=duplicate_analysis['total_links'],
                max_links=MAX_LINKS_PER_FILE
            ),
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    # Показываем информацию о файле с учетом дубликатов
    if ENABLE_DUPLICATE_REMOVAL and duplicate_analysis['duplicate_count'] > 0:
        # Если есть дубликаты внутри файла, показываем расширенную информацию
        prompt_text = MESSAGES["file_with_duplicates"].format(
            filename=msg.document.file_name,
            total_links=duplicate_analysis['total_links'],
            unique_links=duplicate_analysis['unique_links'],
            duplicate_count=duplicate_analysis['duplicate_count'],
            duplicate_percent=duplicate_analysis['duplicate_percent']
        )

        # Добавляем информацию о топ дубликатах
        if duplicate_analysis['top_duplicates']:
            top_text = "\n\n<b>🔝 Топ дубликатов:</b>\n"
            for i, (link, count) in enumerate(duplicate_analysis['top_duplicates'][:3], 1):
                top_text += f"{i}. <code>{link}</code> - {count} раз\n"
            prompt_text += top_text

        await msg.answer(prompt_text, reply_markup=file_duplicates_menu_kb())
    else:
        # Стандартное меню если дубликатов нет
        prompt_text = MESSAGES["file_action_prompt"].format(
            filename=msg.document.file_name,
            size=duplicate_analysis['unique_links']
        )
        await msg.answer(prompt_text, reply_markup=file_action_menu_kb())


async def handle_db_load(msg: Message, db: VKDatabase):
    """Обработка файлов для загрузки в БД"""
    # Собираем все документы из сообщения
    documents = []
    if msg.document:
        documents.append(msg.document)

    # Обрабатываем документы
    loader = DatabaseLoader(db)
    total_stats = {
        "files_count": 0,
        "added": 0,
        "updated": 0,
        "errors": 0,
        "duplicates": 0
    }

    status_msg = await msg.answer("🔄 Начинаю загрузку файлов в базу данных...")

    for doc in documents:
        if not doc.file_name.endswith('.xlsx'):
            continue

        try:
            # Скачиваем файл
            temp_dir = Path(tempfile.mkdtemp())
            file_path = temp_dir / doc.file_name
            await msg.bot.download(doc.file_id, destination=file_path)

            # Загружаем в БД
            stats = await loader.load_from_excel(file_path, msg.from_user.id)

            # Анализируем связи
            records, _ = loader.process_excel_file(file_path)
            network_data = loader.find_all_related_data(records)

            # Обновляем общую статистику
            total_stats["files_count"] += 1
            total_stats["added"] += stats["added"]
            total_stats["updated"] += stats["updated"]
            total_stats["errors"] += stats["errors"]
            total_stats["duplicates"] += stats.get("duplicates", 0)

            # Обновляем статус
            status_text = f"""🔄 Обработано файлов: {total_stats['files_count']}
Добавлено: {total_stats['added']}, Обновлено: {total_stats['updated']}
Дубликатов пропущено: {total_stats['duplicates']}

📊 Найдено связей:
Телефонов с несколькими VK: {network_data['stats']['phones_with_multiple_vk']}
VK с несколькими телефонами: {network_data['stats']['vk_with_multiple_phones']}"""

            await status_msg.edit_text(status_text)

        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {doc.file_name}: {e}")
            total_stats["errors"] += 1

    # Получаем общую статистику БД
    db_stats = await db.get_database_statistics()

    # Показываем итоговый результат
    complete_text = MESSAGES["db_load_complete"].format(
        files_count=total_stats["files_count"],
        added=total_stats["added"],
        updated=total_stats["updated"],
        duplicates=total_stats["duplicates"],
        errors=total_stats["errors"],
        total_records=db_stats["total_records"],
        with_data=db_stats["with_data"],
        without_data=db_stats["without_data"]
    )

    await status_msg.edit_text(complete_text, reply_markup=back_to_menu_kb())

    # Очищаем режим загрузки БД
    await clear_user_session(msg.from_user.id)


@router.callback_query(F.data == "analyze_only")
async def on_analyze_only(call: CallbackQuery, db: VKDatabase):
    """Анализ файла без обработки"""
    await call.answer("🔍 Начинаю анализ...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('temp_file'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    # Обновляем статус анализа
    progress_msg = await call.message.edit_text(
        MESSAGES["analysis_in_progress"].format(
            vk_status="🔄",
            phone_status="⏳",
            network_status="⏳",
            duplicate_status="⏳"
        )
    )

    try:
        # Выполняем анализ
        file_path = Path(session['temp_file'])
        analyzer = FileAnalyzer(db)

        # Поэтапный анализ с обновлением статуса
        await progress_msg.edit_text(
            MESSAGES["analysis_in_progress"].format(
                vk_status="✅",
                phone_status="🔄",
                network_status="⏳",
                duplicate_status="⏳"
            )
        )

        analysis = await analyzer.analyze_file(file_path)

        await progress_msg.edit_text(
            MESSAGES["analysis_in_progress"].format(
                vk_status="✅",
                phone_status="✅",
                network_status="🔄",
                duplicate_status="⏳"
            )
        )

        # Форматируем результаты
        result_text = await analyzer.format_analysis_message(analysis)

        # Сохраняем результаты анализа в сессию
        session['analysis_result'] = analysis
        session['file_mode'] = 'analyzed'
        await save_user_session(user_id, session)

        # Показываем результаты
        await progress_msg.edit_text(result_text, reply_markup=analysis_results_kb())

    except Exception as e:
        logger.error(f"Ошибка при анализе файла: {e}")
        await progress_msg.edit_text(
            f"❌ Ошибка при анализе файла: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.callback_query(F.data == "process_only")
async def on_process_only(call: CallbackQuery, db: VKDatabase, vk_service, bot):
    """Обработка файла без анализа"""
    await call.answer("📤 Начинаю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('temp_file'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    file_path = Path(session['temp_file'])
    processor = session.get('processor')

    if not processor:
        # Если processor не сохранен, создаем новый
        processor = ExcelProcessor()
        links, row_indices, success = processor.load_excel_file(file_path)

        if not success or not links:
            await call.message.edit_text(
                MESSAGES["error_no_vk_links"],
                reply_markup=main_menu_kb(user_id, ADMIN_IDS)
            )
            return
    else:
        # Используем сохраненный processor
        links = processor.get_links_without_duplicates()

    # НОВОЕ: Проверяем баланс перед обработкой
    from bot.handlers.balance import check_balance_before_processing
    if not await check_balance_before_processing(call.message, len(links), vk_service):
        return

    # НОВОЕ: Извлекаем телефоны из файла для проверки дубликатов
    from db_loader import DatabaseLoader
    loader = DatabaseLoader(db)
    records, _ = loader.process_excel_file(file_path)

    # Создаем карту телефонов для каждой VK ссылки
    phones_map = {}
    for record in records:
        if not record["link"].startswith("phone:"):
            phones_map[record["link"]] = record.get("phones", [])

    # Сохраняем информацию в сессию с processor
    session_data = {
        "links": links,
        "links_order": links,
        "results": {},
        "all_links": links,
        "temp_file": session.get('temp_file'),
        "file_name": session.get('file_name'),
        "processor": processor,  # Важно: сохраняем processor
        "file_mode": "processing",
        "phones_map": phones_map  # НОВОЕ: сохраняем карту телефонов
    }
    await save_user_session(user_id, session_data)

    # Проверяем дубликаты с учетом телефонов
    duplicate_check = await db.check_duplicates_extended(links, phones_map)

    # Если есть дубликаты, показываем анализ
    total = len(links)
    stats = duplicate_check["stats"]

    # Считаем общее количество дубликатов
    total_duplicates = stats["duplicate_by_vk"] + stats["duplicate_by_phone"] + stats["duplicate_by_both"]

    if total_duplicates > 0:
        # Подсчитываем дубликаты с данными
        with_data_count = len(duplicate_check["duplicates_with_data"])
        no_data_count = len(duplicate_check["duplicates_no_data"])

        analysis_text = f"""📊 <b>Анализ файла завершен!</b>

📁 Файл: <code>{session.get('file_name', 'файл')}</code>

<b>📊 Статистика VK ссылок:</b>
- Всего: {total}
- Новых: {stats['new']}
- Дубликатов по VK: {stats['duplicate_by_vk'] + stats['duplicate_by_both']}

<b>📱 Статистика телефонов:</b>
- Дубликатов по телефонам: {stats['duplicate_by_phone'] + stats['duplicate_by_both']}

<b>📋 Итого дубликатов: {total_duplicates}</b>
- С данными: {with_data_count}
- Без данных: {no_data_count + len(duplicate_check.get('duplicate_phones', {}))}

<b>Что делать с дубликатами?</b>"""

        await call.message.edit_text(analysis_text, reply_markup=duplicate_actions_kb())

        # Сохраняем duplicate_check
        session_data["duplicate_check"] = duplicate_check
        await save_user_session(user_id, session_data)
    else:
        # Запускаем обработку
        await call.message.edit_text(f"📤 Начинаю обработку {len(links)} ссылок...")
        await start_processing(call.message, links, processor, duplicate_check, user_id, db, vk_service, bot)


@router.callback_query(F.data == "process_with_duplicates")
async def on_process_with_duplicates(call: CallbackQuery, db: VKDatabase, vk_service, bot):
    """Обработка файла со всеми дубликатами"""
    await call.answer("📤 Обрабатываю все ссылки...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('processor'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    processor = session['processor']
    all_links = processor.all_links_found  # Все ссылки включая дубликаты

    # Проверяем баланс
    from bot.handlers.balance import check_balance_before_processing
    if not await check_balance_before_processing(call.message, len(all_links), vk_service):
        return

    # Подготавливаем данные для обработки
    session_data = {
        "links": all_links,
        "links_order": all_links,
        "results": {},
        "all_links": all_links,
        "temp_file": session.get('temp_file'),
        "file_name": session.get('file_name'),
        "processor": processor,
        "file_mode": "processing"
    }
    await save_user_session(user_id, session_data)

    await call.message.edit_text(f"📤 Начинаю обработку {len(all_links)} ссылок (включая дубликаты)...")
    await start_processing(call.message, all_links, processor, {}, user_id, db, vk_service, bot)


@router.callback_query(F.data == "process_unique_only")
async def on_process_unique_only(call: CallbackQuery, db: VKDatabase, vk_service, bot):
    """Обработка только уникальных ссылок"""
    await call.answer("🔍 Удаляю дубликаты...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('processor'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    processor = session['processor']
    unique_links = processor.get_links_without_duplicates()
    duplicate_analysis = processor.get_duplicate_analysis()

    # Показываем что было удалено
    await call.message.edit_text(
        MESSAGES["duplicates_removed"].format(
            removed_count=duplicate_analysis['duplicate_count'],
            unique_count=len(unique_links)
        )
    )

    # Небольшая задержка для показа сообщения
    import asyncio
    await asyncio.sleep(1.5)

    # Проверяем баланс
    from bot.handlers.balance import check_balance_before_processing
    if not await check_balance_before_processing(call.message, len(unique_links), vk_service):
        return

    # Подготавливаем данные для обработки
    session_data = {
        "links": unique_links,
        "links_order": unique_links,
        "results": {},
        "all_links": unique_links,
        "temp_file": session.get('temp_file'),
        "file_name": session.get('file_name'),
        "processor": processor,
        "file_mode": "processing"
    }
    await save_user_session(user_id, session_data)

    await call.message.edit_text(f"📤 Начинаю обработку {len(unique_links)} уникальных ссылок...")
    await start_processing(call.message, unique_links, processor, {}, user_id, db, vk_service, bot)


@router.callback_query(F.data == "show_duplicate_details")
async def on_show_duplicate_details(call: CallbackQuery):
    """Показать детали дубликатов"""
    await call.answer()
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('duplicate_analysis'):
        await call.message.edit_text("❌ Данные не найдены", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    duplicate_analysis = session['duplicate_analysis']

    details_text = MESSAGES["duplicate_details"].format(
        total_duplicates=duplicate_analysis['duplicate_count'],
        unique_duplicates=len(duplicate_analysis['duplicates'])
    )

    # Добавляем информацию о топ дубликатах
    if duplicate_analysis['top_duplicates']:
        details_text += "\n\n<b>🔝 Топ-10 самых частых дубликатов:</b>\n\n"
        for i, (link, count) in enumerate(duplicate_analysis['top_duplicates'], 1):
            rows = duplicate_analysis['duplicate_rows'].get(link, [])[:5]
            rows_text = ", ".join(map(str, rows))
            if len(duplicate_analysis['duplicate_rows'].get(link, [])) > 5:
                rows_text += "..."

            details_text += f"{i}. <code>{link}</code>\n"
            details_text += f"   📊 Встречается: {count} раз\n"
            details_text += f"   📋 Строки Excel: {rows_text}\n\n"

    await call.message.answer(details_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "analyze_and_process")
async def on_analyze_and_process(call: CallbackQuery, db: VKDatabase):
    """Анализ и затем обработка"""
    # Сначала выполняем анализ
    await on_analyze_only(call, db)


@router.callback_query(F.data == "process_after_analysis")
async def on_process_after_analysis(call: CallbackQuery, db: VKDatabase, vk_service, bot):
    """Обработка после анализа"""
    await call.answer("📤 Начинаю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.edit_text("❌ Результаты анализа не найдены", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    analysis = session['analysis_result']
    file_path = Path(session['temp_file'])

    # Извлекаем VK ссылки из анализа
    vk_links = [r['link'] for r in analysis['records'] if not r['link'].startswith('phone:')]

    if not vk_links:
        await call.message.edit_text(
            MESSAGES["error_no_vk_links"],
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    # Используем ExcelProcessor
    processor = ExcelProcessor()
    links, row_indices, success = processor.load_excel_file(file_path)

    if not success:
        await call.message.edit_text(
            "❌ Ошибка при загрузке файла",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )
        return

    # Обновляем сессию с processor
    session["links"] = vk_links
    session["links_order"] = vk_links
    session["results"] = {}
    session["processor"] = processor  # Важно: сохраняем processor
    session["all_links"] = vk_links
    session["file_mode"] = "processing"
    await save_user_session(user_id, session)

    # Используем информацию о дубликатах из анализа
    duplicate_check = analysis['duplicates']['vk']

    await call.message.edit_text(f"📤 Начинаю обработку {len(vk_links)} ссылок...")
    await start_processing(call.message, vk_links, processor, duplicate_check, user_id, db, vk_service, bot)


@router.callback_query(F.data == "cancel_file")
async def on_cancel_file(call: CallbackQuery):
    """Отмена операции с файлом"""
    await call.answer()
    user_id = call.from_user.id

    # Очищаем сессию
    await clear_user_session(user_id)

    await call.message.edit_text(
        MESSAGES["operation_cancelled"],
        reply_markup=main_menu_kb(user_id, ADMIN_IDS)
    )