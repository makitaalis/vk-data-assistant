"""Обработчики поиска по ссылкам и телефонам"""

import asyncio
import logging
import time
import re
from typing import Dict, Any, List
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from bot.config import ADMIN_IDS, VK_LINK_PATTERN
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    processing_menu_kb,
    finish_kb,
    continue_kb,
    duplicate_actions_kb, disclaimer_kb
)
from bot.utils.helpers import (
    create_progress_bar,
    format_time,
    safe_edit_message,
    validate_vk_link,
    extract_vk_links
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
    clear_user_session,
    check_user_accepted_disclaimer
)
from bot.utils.export import create_excel_from_results
from db_module import VKDatabase
from services.vk_service import VKService
from services.excel_service import ExcelProcessor

router = Router()
logger = logging.getLogger("search_handler")


@router.message(Command("findphone"))
async def cmd_find_phone(msg: Message, db: VKDatabase):
    """Поиск по номеру телефона"""
    # Извлекаем номер из команды
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer(MESSAGES["search_phone"], reply_markup=back_to_menu_kb())
        # Устанавливаем режим ожидания телефона
        await save_user_session(msg.from_user.id, {"waiting_phone": True})
        return

    # Очищаем номер от всех символов кроме цифр
    phone = re.sub(r'[^\d]', '', parts[1])

    # Валидация номера
    if len(phone) != 11 or not phone.startswith('7'):
        await msg.answer(MESSAGES["error_invalid_phone"], reply_markup=back_to_menu_kb())
        return

    # Поиск в базе
    results = await db.find_links_by_phone(phone)

    if not results:
        await msg.answer(
            f"❌ Номер <code>{phone}</code> не найден в базе данных",
            reply_markup=back_to_menu_kb()
        )
        return

    # Формируем ответ
    response = f"📱 <b>Результаты поиска для номера {phone}:</b>\n\n"
    response += f"Найдено профилей: {len(results)}\n\n"

    for i, result in enumerate(results[:10], 1):  # Показываем максимум 10
        response += f"{i}. <a href='{result['link']}'>{result['link']}</a>\n"
        if result['full_name']:
            response += f"   👤 {result['full_name']}\n"
        if result['birth_date']:
            response += f"   🎂 {result['birth_date']}\n"

        # Показываем все телефоны профиля
        other_phones = [p for p in result['phones'] if p != phone]
        if other_phones:
            response += f"   📞 Другие телефоны: {', '.join(other_phones)}\n"

        response += "\n"

    if len(results) > 10:
        response += f"... и еще {len(results) - 10} профилей"

    await msg.answer(response, reply_markup=back_to_menu_kb(), disable_web_page_preview=True)


@router.callback_query(F.data == "search_phone")
async def on_search_phone(call: CallbackQuery):
    """Обработчик кнопки поиска по телефону"""
    await call.answer()
    await call.message.edit_text(MESSAGES["search_phone"], reply_markup=back_to_menu_kb())
    # Устанавливаем режим ожидания телефона
    await save_user_session(call.from_user.id, {"waiting_phone": True})


@router.message(F.text)
async def on_text_message(msg: Message, db: VKDatabase):
    """Обработка текстовых сообщений с ссылками или телефонами"""
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    # Проверяем сессию
    session = await get_user_session(user_id)

    # Если ждем номер телефона
    if session.get("waiting_phone"):
        # Очищаем номер от всех символов кроме цифр
        phone = re.sub(r'[^\d]', '', msg.text)

        # Валидация номера
        if len(phone) == 11 and phone.startswith('7'):
            # Поиск в базе
            results = await db.find_links_by_phone(phone)

            if not results:
                await msg.answer(
                    f"❌ Номер <code>{phone}</code> не найден в базе данных",
                    reply_markup=main_menu_kb(user_id, ADMIN_IDS)
                )
            else:
                # Формируем ответ
                response = f"📱 <b>Результаты поиска для номера {phone}:</b>\n\n"
                response += f"Найдено профилей: {len(results)}\n\n"

                for i, result in enumerate(results[:10], 1):  # Показываем максимум 10
                    response += f"{i}. <a href='{result['link']}'>{result['link']}</a>\n"
                    if result['full_name']:
                        response += f"   👤 {result['full_name']}\n"
                    if result['birth_date']:
                        response += f"   🎂 {result['birth_date']}\n"

                    # Показываем все телефоны профиля
                    other_phones = [p for p in result['phones'] if p != phone]
                    if other_phones:
                        response += f"   📞 Другие телефоны: {', '.join(other_phones)}\n"

                    response += "\n"

                if len(results) > 10:
                    response += f"... и еще {len(results) - 10} профилей"

                await msg.answer(response, reply_markup=main_menu_kb(user_id, ADMIN_IDS), disable_web_page_preview=True)
        else:
            await msg.answer(MESSAGES["error_invalid_phone"], reply_markup=back_to_menu_kb())

        # Очищаем режим ожидания
        await clear_user_session(user_id)
        return

    # Извлекаем ссылки
    links = extract_vk_links(msg.text)

    if not links:
        # Проверяем, не команда ли это из inline меню
        if msg.text in ["📤 Загрузить файл", "🔗 Отправить ссылки", "📊 Мои результаты", "📚 Помощь"]:
            await msg.answer("Пожалуйста, используйте кнопки меню ☝️", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        else:
            await msg.answer(
                "🔍 Не нашел ссылок VK в вашем сообщении.\n\n"
                "Отправьте ссылки в формате:\n"
                "<code>https://vk.com/id123456</code>",
                reply_markup=main_menu_kb(user_id, ADMIN_IDS)
            )
        return

    # Сохраняем ссылки в сессию и начинаем обработку
    session_data = {
        "links": links,
        "links_order": links,
        "results": {},
        "all_links": links
    }
    await save_user_session(user_id, session_data)

    # Проверяем дубликаты
    duplicate_check = await db.check_duplicates_extended(links)

    # Показываем анализ дубликатов
    total = len(links)
    new_count = len(duplicate_check["new"])
    duplicate_count = len(duplicate_check["duplicates_with_data"]) + len(duplicate_check["duplicates_no_data"])
    with_data_count = len(duplicate_check["duplicates_with_data"])
    no_data_count = len(duplicate_check["duplicates_no_data"])

    if duplicate_count > 0:
        analysis_text = MESSAGES["duplicate_analysis"].format(
            filename="Прямые ссылки",
            total=total,
            new_count=new_count,
            duplicate_count=duplicate_count,
            with_data_count=with_data_count,
            no_data_count=no_data_count
        )
        await msg.answer(analysis_text, reply_markup=duplicate_actions_kb())

        # Сохраняем duplicate_check в сессию
        session_data["duplicate_check"] = duplicate_check
        await save_user_session(user_id, session_data)
    else:
        # Если дубликатов нет, сразу начинаем обработку
        await msg.answer(f"📤 Начинаю обработку {len(links)} ссылок...")
        await start_processing(msg, links, None, duplicate_check, user_id, db)


async def start_processing(
        message: Message,
        links_to_process: List[str],
        processor: ExcelProcessor,
        duplicate_check: Dict,
        user_id: int,
        db: VKDatabase,
        vk_service: VKService = None,
        bot=None
):
    """Запускает обработку ссылок с учетом кеша"""

    # Получаем закешированные результаты
    cached_results = await db.get_cached_results(links_to_process)

    # Определяем, какие ссылки нужно проверить через VK
    links_to_check = [link for link in links_to_process if link not in cached_results]

    # Статус-сообщение
    total = len(links_to_process)
    from_cache = len(cached_results)

    progress_bar = create_progress_bar(from_cache, total)
    status_text = MESSAGES["processing_with_cache"].format(
        progress_bar=progress_bar,
        processed=from_cache,
        total=total,
        percent=int((from_cache / total) * 100) if total > 0 else 0,
        found=sum(1 for r in cached_results.values() if r.get("phones") or r.get("full_name") or r.get("birth_date")),
        from_cache=from_cache,
        new_checks=0,
        not_found=sum(
            1 for r in cached_results.values() if not (r.get("phones") or r.get("full_name") or r.get("birth_date"))),
        time=format_time()
    )

    status = await message.answer(status_text, reply_markup=processing_menu_kb())

    # Начинаем с результатов из кеша
    all_results = dict(cached_results)

    # Сохраняем результаты и порядок ссылок в сессию
    session = {
        "results": all_results,
        "links": links_to_process,
        "links_order": links_to_process,
    }
    await save_user_session(user_id, session)

    # Если все результаты из кеша
    if not links_to_check:
        await finish_processing(message, all_results, processor, links_to_process, user_id, db, bot)
        return

    # Создаем очередь для новых проверок
    queue = asyncio.Queue()
    for link in links_to_check:
        await queue.put(link)

    new_checks_count = 0
    last_status_text = ""
    start_time = time.time()

    async def result_cb(link: str, result_data: Dict[str, Any]):
        nonlocal new_checks_count, last_status_text

        # Сохраняем результат
        all_results[link] = result_data

        # Сохраняем в базу данных
        await db.save_result(link, result_data, user_id)

        # Обновляем сессию с новыми результатами
        session["results"] = all_results
        await save_user_session(user_id, session)

        new_checks_count += 1
        processed = len(all_results)

        # Правильный подсчет статистики
        found_count = 0
        not_found_count = 0

        for data in all_results.values():
            if data.get("phones") or data.get("full_name") or data.get("birth_date"):
                found_count += 1
            else:
                not_found_count += 1

        # Обновляем статус каждые 5 обработанных ссылок
        if new_checks_count % 5 == 0:
            progress_bar = create_progress_bar(processed, total)
            percent = int((processed / total) * 100)

            # Добавляем информацию о скорости
            elapsed = time.time() - start_time
            speed = new_checks_count / elapsed if elapsed > 0 else 0
            eta = (total - processed) / speed if speed > 0 else 0

            new_status_text = f"""⚡ <b>Обработка данных</b>

{progress_bar}
<b>Прогресс:</b> {processed}/{total} ({percent}%)

📊 <b>Статистика:</b>
✅ Найдено данных: {found_count}
💾 Из кеша: {from_cache}
🔍 Новых проверок: {new_checks_count}
❌ Без результата: {not_found_count}

⚡ <b>Скорость:</b> {speed:.1f} ссылок/сек
⏱ <b>Осталось:</b> ~{int(eta)} сек

<i>Обновлено: {format_time()}</i>"""

            if new_status_text != last_status_text:
                await safe_edit_message(status, new_status_text, reply_markup=processing_menu_kb())
                last_status_text = new_status_text

    async def limit_cb():
        # Сохраняем прогресс при достижении лимита
        session["partial_results"] = all_results
        session["links_order"] = links_to_process
        await save_user_session(user_id, session)

        limit_message = MESSAGES["limit_reached"].format(
            processed=len(all_results),
            total=total
        )

        await status.edit_text(limit_message, reply_markup=continue_kb())

    # Если VK сервис не передан, значит это прямые ссылки
    if vk_service:
        await vk_service.process_queue(queue, result_cb, limit_cb)
    else:
        # Для прямых ссылок нужно будет инициализировать VK сервис
        # Это будет сделано в главном файле
        pass

    # Обработка завершена успешно
    await finish_processing(message, all_results, processor, links_to_process, user_id, db, bot)


async def finish_processing(
        message: Message,
        results: Dict[str, Dict],
        processor: ExcelProcessor,
        links_order: List[str],
        user_id: int,
        db: VKDatabase,
        bot=None
):
    """Завершает обработку и отправляет результаты"""

    # Генерируем файл с результатами
    files = await create_excel_from_results(results, links_order)

    if files:
        # Правильный подсчет статистики
        found_count = 0
        not_found_count = 0

        for data in results.values():
            # Проверяем есть ли хоть какие-то данные
            has_phones = bool(data.get("phones", []))
            has_name = bool(data.get("full_name", ""))
            has_birth = bool(data.get("birth_date", ""))

            if has_phones or has_name or has_birth:
                found_count += 1
            else:
                not_found_count += 1

        # Отправляем сообщение о завершении
        complete_text = MESSAGES["session_complete"].format(
            total=len(results),
            found=found_count,
            not_found=not_found_count
        )

        await message.answer(complete_text, reply_markup=finish_kb())

        # Отправляем файлы
        if bot:
            from aiogram.types import FSInputFile
            for file_path, caption in files:
                try:
                    await bot.send_document(
                        message.chat.id,
                        FSInputFile(file_path),
                        caption=caption
                    )
                except Exception as e:
                    logger.error(f"Ошибка при отправке файла: {e}")
                    await message.answer(f"⚠️ Не удалось отправить файл: {str(e)}")
    else:
        await message.answer(
            "❌ Произошла ошибка при создании файла с результатами.\n"
            "Попробуйте еще раз или обратитесь к администратору.",
            reply_markup=main_menu_kb(user_id, ADMIN_IDS)
        )

    # Очищаем сессию
    await clear_user_session(user_id)