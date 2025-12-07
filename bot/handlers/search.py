"""Обработчики поиска по ссылкам и телефонам - оптимизированная версия"""

import asyncio
import logging
import re
import time
from typing import Dict, Any, List, Optional, Tuple
import tempfile
from pathlib import Path
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.dispatcher.event.bases import SkipHandler

from bot.config import (
    ADMIN_IDS,
    EXPORT_DATE_FORMAT,
    USE_CACHE,
    ADMIN_USE_CACHE,
    MAX_WORKERS_PER_JOB,
    PROGRESS_HEARTBEAT_SECONDS,
    STUCK_RESEND_INTERVAL_SECONDS,
    STUCK_RESEND_MAX_ATTEMPTS,
)
from bot.keyboards.inline import finish_kb, processing_menu_kb
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    duplicate_actions_kb, disclaimer_kb
)
from bot.utils.export import create_excel_from_results, restore_processor_from_session
from bot.utils.helpers import (
    create_progress_bar,
    format_time,
    extract_vk_links,
    safe_edit_message,
)
from bot.utils.messages import MESSAGES
from bot.utils.session_manager import (
    get_user_session,
    clear_user_session,
    check_user_accepted_disclaimer,
    save_user_session
)
from services.excel_service import ExcelProcessor
from services.config_service import get_config_service
from bot.utils.admin_notifications import notify_admins

router = Router()
logger = logging.getLogger("search_handler")

# Интервал автоповтора зависит от конфигурации ботов
AUTO_RESUME_MIN_INTERVAL = 20.0

try:
    from bot.config import BOT_HOLD_DURATION_SECONDS as _HOLD_DEFAULT
except Exception:  # pragma: no cover
    _HOLD_DEFAULT = 60
try:
    from bot.handlers.balance import processing_paused as _BALANCE_FLAG
except Exception:  # pragma: no cover
    _BALANCE_FLAG = False
TECHNICAL_ERROR_CODES = {
    "no_bots_available",
    "watchdog_timeout",
    "search_stuck",
    "timeout",
    "flood_wait",
}
STUCK_RETRY_ERRORS = {"watchdog_timeout", "search_stuck", "timeout"}
LIMIT_REASON_TEXT = {
    "vk_text_limit": "VK бот сообщил о лимите — ждём освобождения или подключите другую сессию.",
    "vk_limit": "Достигнут лимит запросов. Попробуем автоматически повторить немного позже.",
    "flood_wait": "Telegram ограничил частоту запросов. Ждём снятия FloodWait и продолжим автоматически.",
    "stuck_search": "Сторонний бот завис на «идёт поиск». Повторим запрос через несколько минут автоматически.",
}

def format_session_assignments_summary(session_data: Dict[str, Any]) -> str:
    assignments = (session_data or {}).get("session_assignments") or {}
    if not assignments:
        return ""

    progress_map = session_data.get("session_progress") or {}
    parts = []
    for key, total in assignments.items():
        completed = progress_map.get(key, total)
        label = key if key != "default" else "default"
        parts.append(f"{label}: {completed}/{total}")

    return "📡 <b>Сессии:</b> " + " | ".join(parts)


@router.message(Command("findphone"))
async def cmd_find_phone(msg: Message, db):
    """Поиск по номеру телефона"""
    if not db:
        await msg.answer("❌ Сервис временно недоступен")
        return

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


@router.message(F.text & ~F.text.startswith("/"))
async def on_text_message(msg: Message, db, vk_service, bot):
    """Обработка текстовых сообщений с ссылками или телефонами"""
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    # Проверяем сессию
    session = await get_user_session(user_id)

    session_auth_state = session.get("session_auth_state", {})
    if session_auth_state and session_auth_state.get("step") not in (None, "idle"):
        raise SkipHandler()

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

    # Проверяем дубликаты (для прямых ссылок не проверяем телефоны)
    duplicate_check = await db.check_duplicates_extended(links, None)

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
        await start_processing(msg, links, None, duplicate_check, user_id, db, vk_service, bot)


async def _should_use_cache(user_id: int) -> bool:
    try:
        config = get_config_service()
        if user_id in ADMIN_IDS:
            return await config.get_admin_use_cache()
        return await config.get_use_cache()
    except Exception:
        return ADMIN_USE_CACHE if user_id in ADMIN_IDS else USE_CACHE


async def start_processing(
        message: Message,
        links_to_process: List[str],
        processor: ExcelProcessor,
        duplicate_check: Dict,
        user_id: int,
        db,
        vk_service=None,
        bot=None,
        force_use_cache: bool = False,
        force_no_cache: bool = False,
        skip_duplicate_filter: bool = False,
        resume_session: Optional[Dict[str, Any]] = None,
        task_queue=None,
):
    """Запускает обработку ссылок с учетом кеша и проверкой баланса"""

    if not db:
        await message.answer("❌ Сервис временно недоступен")
        return

    # НОВОЕ: Проверяем баланс при необходимости и следим за паузами
    from bot.handlers.balance import check_balance_before_processing, is_processing_paused

    previous_session = resume_session or await get_user_session(user_id)
    if not duplicate_check:
        duplicate_check = (previous_session or {}).get("duplicate_check") or {}

    # НОВОЕ: Фильтруем ссылки с учетом дубликатов по телефонам (может быть отключено)
    if skip_duplicate_filter:
        actual_links_to_process = list(links_to_process)
        skipped_phone_duplicates = []
    else:
        actual_links_to_process = []
        skipped_phone_duplicates = []

        if duplicate_check and "duplicate_phones" in duplicate_check:
            for link in links_to_process:
                if link in duplicate_check["duplicate_phones"]:
                    skipped_phone_duplicates.append(link)
                else:
                    actual_links_to_process.append(link)
        else:
            actual_links_to_process = links_to_process

    # Обновляем количество для проверки
    links_to_process = actual_links_to_process

    if force_no_cache:
        use_cache = False
    else:
        use_cache = force_use_cache or await _should_use_cache(user_id)

    # Получаем закешированные результаты (если разрешено)
    if use_cache:
        cached_results = await db.get_cached_results(links_to_process)
    else:
        cached_results = {}

    # Определяем, какие ссылки нужно проверить через VK
    if use_cache:
        links_to_check = [link for link in links_to_process if link not in cached_results]
    else:
        links_to_check = links_to_process

    # Финальный пересчет требуемых проверок с учётом кеша
    required_checks = len(links_to_process) if not use_cache else len(links_to_check)
    if required_checks > 0:
        if not vk_service:
            await message.answer("❌ VK сервис недоступен")
            return
        links_count = len(links_to_process)
        if not await check_balance_before_processing(
            message,
            total_links=links_count,
            required_checks=required_checks,
            vk_service=vk_service
        ):
            return

    # Персистентная очередь: кладём задачи и выходим (старый поток остаётся как fallback)
    if task_queue:
        task_ids = await task_queue.enqueue_links(user_id, links_to_check)
        session_state = previous_session or {}
        session_state["links"] = links_to_process
        session_state["pending_links"] = []
        session_state["queued_task_ids"] = task_ids
        await save_user_session(user_id, session_state)
        await message.answer(
            f"✅ Принято в обработку: {len(task_ids)} ссылок. Прогресс сохраняется и будет обработан в фоне.",
            reply_markup=processing_menu_kb()
        )
        return

    existing_results = {}
    if resume_session and previous_session:
        existing_results = dict(previous_session.get("results") or {})
        if existing_results:
            cached_results = {**existing_results, **cached_results}
            links_to_check = [link for link in links_to_process if link not in cached_results]

    base_links = previous_session.get("links") if previous_session and previous_session.get("links") else links_to_process
    links_order_full = (
        previous_session.get("links_order")
        if previous_session and previous_session.get("links_order")
        else base_links
    )
    total = len(base_links) if base_links else len(links_to_process)
    from_cache = len(cached_results)

    preserved_keys = (
        "temp_file",
        "file_name",
        "file_mode",
        "vk_links_mapping",
        "duplicate_analysis",
        "file_info",
        "vk_column_name",
        "vk_column_index",
        "all_links",
        "unique_links",
        "stuck_attempts",
        "session_assignments",
        "session_progress",
        "technical_errors",
        "duplicate_check",
    )
    preserved_session = {}
    if previous_session:
        for key in preserved_keys:
            if key in previous_session:
                preserved_session[key] = previous_session[key]
    if duplicate_check:
        preserved_session["duplicate_check"] = duplicate_check

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
    status_lock = asyncio.Lock()
    heartbeat_stop = asyncio.Event()
    last_result_ts = time.time()

    # Начинаем с результатов из кеша/предыдущих запусков
    all_results = dict(cached_results)

    link_sessions_prev = {}
    if previous_session and previous_session.get("link_sessions"):
        link_sessions_prev = previous_session.get("link_sessions")

    session = {
        **preserved_session,
        "results": all_results,
        "links": base_links,
        "links_order": links_order_full,
        "processor": processor,
        "link_sessions": link_sessions_prev,
        "session_assignments": preserved_session.get("session_assignments") or {},
        "session_progress": preserved_session.get("session_progress") or {},
        "pending_links": list(links_to_check),
        "delayed_links": dict(previous_session.get("delayed_links") or {}) if previous_session else {},
        "paused": False,
        "processing_active": True,
        "duplicate_check": preserved_session.get("duplicate_check") or duplicate_check,
    }
    session.pop("cancelled", None)
    session.setdefault("technical_errors", preserved_session.get("technical_errors") or {})
    session.setdefault("stuck_attempts", preserved_session.get("stuck_attempts") or {})

    if processor:
        vk_mapping = getattr(processor, "vk_links_mapping", None)
        if vk_mapping:
            session["vk_links_mapping"] = vk_mapping
        all_links_found = getattr(processor, "all_links_found", None)
        if all_links_found:
            session["all_links"] = all_links_found
            session["unique_links"] = processor.get_links_without_duplicates()
        if getattr(processor, "vk_column_name", None):
            session["vk_column_name"] = processor.vk_column_name
        if getattr(processor, "vk_column_index", None) is not None:
            session["vk_column_index"] = processor.vk_column_index

    await save_user_session(user_id, session)

    async def _set_processing_flag(active: bool):
        current = await get_user_session(user_id)
        if not current:
            return
        current["processing_active"] = active
        await save_user_session(user_id, current)

    await _set_processing_flag(True)

    stuck_attempts: Dict[str, int] = dict(session.get("stuck_attempts") or {})
    session["stuck_attempts"] = stuck_attempts
    delayed_links: Dict[str, float] = dict(session.get("delayed_links") or {})
    hold_notice_sent = False

    def _log_queue_state(reason: str, **extra: Any):
        """Фиксирует в логах текущее состояние очереди и причину ожидания."""
        pending_count = len(session.get("pending_links") or [])
        delayed_count = len(delayed_links)
        limit_reason = session.get("limit_reason")
        paused_flag = bool(session.get("paused"))
        balance_flag = bool(session.get("balance_pause"))
        tech_errors = dict(session.get("technical_errors") or {})
        payload = {
            "pending": pending_count,
            "delayed": delayed_count,
            "limit_reason": limit_reason,
            "paused": paused_flag,
            "balance_pause": balance_flag,
            "stuck_links": len(stuck_attempts),
            "technical_errors": tech_errors,
        }
        if extra:
            payload.update(extra)
        logger.info("📋 Очередь (%s): %s", reason, payload)

    async def _release_delayed_links():
        """Возвращает отложенные ссылки в очередь, когда истек холд."""
        nonlocal session, hold_notice_sent
        if not delayed_links:
            return
        now = time.time()
        pending_links = session.get("pending_links") or []
        changed = False
        for link, ready_at in list(delayed_links.items()):
            if ready_at <= now:
                if link not in pending_links and link not in all_results:
                    pending_links.append(link)
                delayed_links.pop(link, None)
                changed = True
                logger.info(
                    "▶️ Ссылка %s возвращена в обработку (холд завершён)",
                    link,
                )
        if changed:
            session["pending_links"] = pending_links
            session["delayed_links"] = delayed_links
            if not delayed_links and session.get("limit_reason"):
                session.pop("limit_reason", None)
                hold_notice_sent = False
            await save_user_session(user_id, session)
            _log_queue_state("hold_release")

    async def _schedule_hold_retry(links_to_delay: List[str], reason: str, delay_seconds: float):
        """Откладывает указанные ссылки до истечения HOLD."""
        nonlocal session
        if not links_to_delay:
            return
        ready_at = time.time() + max(0.0, delay_seconds)
        pending_links = session.get("pending_links") or []
        scheduled = 0
        for link in links_to_delay:
            if link in all_results:
                continue
            if link in pending_links:
                pending_links.remove(link)
            delayed_links[link] = ready_at
            scheduled += 1
        session["pending_links"] = pending_links
        session["delayed_links"] = delayed_links
        session["limit_reason"] = reason
        await save_user_session(user_id, session)
        human_time = datetime.fromtimestamp(ready_at).strftime("%H:%M:%S")
        logger.warning(
            "⏳ Отложено %s ссылок до %s (reason=%s)",
            scheduled,
            human_time,
            reason,
        )
        _log_queue_state("hold_schedule", reason=reason, ready_at=human_time, scheduled=scheduled)

    async def _handle_stuck_retry(link: str, error_code: str, result_data: Dict[str, Any]) -> bool:
        """Планирует повторные попытки для зависших запросов."""
        nonlocal session, stuck_attempts
        attempts = int(stuck_attempts.get(link, 0)) + 1
        stuck_attempts[link] = attempts
        session["stuck_attempts"] = stuck_attempts
        pending_links = session.get("pending_links") or []
        if link in pending_links:
            pending_links.remove(link)
            session["pending_links"] = pending_links
        await save_user_session(user_id, session)

        if attempts > STUCK_RESEND_MAX_ATTEMPTS:
            stuck_attempts.pop(link, None)
            session["stuck_attempts"] = stuck_attempts
            result_data["error"] = "stuck_failed"
            result_data["note"] = (
                f"Нет ответа от внешнего бота после {STUCK_RESEND_MAX_ATTEMPTS} повторов."
            )
            result_data["attempts"] = attempts - 1
            await save_user_session(user_id, session)
            logger.error(
                "🧊 Ссылка %s остановлена после %s попыток (%s)",
                link,
                attempts - 1,
                error_code,
            )
            return False

        delay = max(float(STUCK_RESEND_INTERVAL_SECONDS), AUTO_RESUME_MIN_INTERVAL)
        await _schedule_hold_retry([link], "stuck_search", delay)
        tech_map = session.get("technical_errors") or {}
        tech_map["stuck_search"] = tech_map.get("stuck_search", 0) + 1
        session["technical_errors"] = tech_map
        await save_user_session(user_id, session)
        logger.warning(
            "🔁 Ссылка %s зависла (%s). Повтор #%s через %.0f с (до %s попыток)",
            link,
            error_code,
            attempts,
            delay,
            STUCK_RESEND_MAX_ATTEMPTS,
        )
        links_order = session.get("links_order") or []
        link_idx = None
        if links_order and link in links_order:
            try:
                link_idx = links_order.index(link) + 1
            except ValueError:
                link_idx = None
        compact_link = link
        if len(compact_link) > 60:
            compact_link = f"{link[:25]}…{link[-15:]}"
        label = f"#{link_idx} " if link_idx else ""
        wait_hint_text = (
            f"🔁 Повторим ссылку {label}{compact_link} через ~{int(delay)}с "
            f"(попытка {attempts}/{STUCK_RESEND_MAX_ATTEMPTS})"
        )
        await _render_status(
            wait_hint=wait_hint_text,
            force=True,
        )
        return True

    # Если все результаты из кеша
    if not links_to_check:
        await finish_processing(message, all_results, processor, links_to_process, user_id, db, bot)
        return

    # Проверяем наличие vk_service
    if not vk_service:
        await status.edit_text(
            "❌ VK сервис недоступен. Могу показать только результаты из кеша.",
            reply_markup=finish_kb()
        )
        return

    new_checks_count = 0
    last_status_text = ""
    last_status_update_ts = 0.0
    start_time = time.time()

    def _progress_key(name: Optional[str]) -> str:
        return name or "default"

    def _get_service_wait_hint() -> Optional[str]:
        if not vk_service:
            return None
        wait_cb = getattr(vk_service, "get_wait_hint", None)
        if callable(wait_cb):
            try:
                return wait_cb()
            except Exception as exc:
                logger.debug("Не удалось получить wait_hint: %s", exc)
        return None

    async def _render_status(*, wait_hint: Optional[str] = None, force: bool = False):
        nonlocal last_status_text, last_status_update_ts
        processed = len(all_results)
        found_count = 0
        not_found_count = 0

        for data in all_results.values():
            if data.get("phones") or data.get("full_name") or data.get("birth_date"):
                found_count += 1
            else:
                not_found_count += 1

        progress_bar = create_progress_bar(processed, total)
        percent = int((processed / total) * 100) if total > 0 else 0

        elapsed = time.time() - start_time
        speed = new_checks_count / elapsed if elapsed > 0 else 0.0
        eta = (total - processed) / speed if speed > 0 else 0.0
        session_line = format_session_assignments_summary(session)

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

"""
        tech_errors = (session or {}).get("technical_errors") or {}
        if tech_errors:
            issues = " | ".join(f"{code}: {count}" for code, count in sorted(tech_errors.items()))
            new_status_text += f"⚠️ <b>Тех. ошибки:</b> {issues}\n"
        limit_reason = (session or {}).get("limit_reason")
        if limit_reason:
            hint_text = LIMIT_REASON_TEXT.get(limit_reason, "Достигнут лимит, ждём автоматического продолжения.")
            new_status_text += f"⚠️ {hint_text}\n"
        if (session or {}).get("balance_pause"):
            new_status_text += "⏸ <b>Проверяем баланс… обработка временно остановлена.</b>\n"
        if session_line:
            new_status_text += f"{session_line}\n"

        if wait_hint is None:
            pending_hint = _get_service_wait_hint()
            if pending_hint:
                new_status_text += f"{pending_hint} (можно нажать «⏸ Пауза», чтобы подождать освобождения)\n"
        elif wait_hint:
            new_status_text += f"{wait_hint}\n"

        new_status_text += f"""

<i>Обновлено: {format_time()}</i>"""

        if force or new_status_text != last_status_text:
            async with status_lock:
                await safe_edit_message(status, new_status_text, reply_markup=processing_menu_kb())
                last_status_text = new_status_text
                last_status_update_ts = time.time()

    async def result_cb(link: str, result_data: Dict[str, Any]):
        nonlocal new_checks_count, last_status_text, last_status_update_ts, last_result_ts

        # НОВОЕ: Ждем если обработка приостановлена для проверки баланса
        while is_processing_paused():
            await asyncio.sleep(0.5)

        error_code = (result_data or {}).get("error")
        if error_code in TECHNICAL_ERROR_CODES:
            if error_code in STUCK_RETRY_ERRORS:
                handled = await _handle_stuck_retry(link, error_code, result_data)
                if handled:
                    return
                error_code = result_data.get("error")
                if error_code not in TECHNICAL_ERROR_CODES:
                    # Ошибка конвертирована в финальный результат (stuck_failed)
                    stuck_attempts.pop(link, None)
                else:
                    blockers = result_data.get("blockers")
                    logger.warning(
                        "⚠️ Пропускаем сохранение %s из-за технической ошибки %s — ссылка останется в очереди%s",
                        link,
                        error_code,
                        f" (blockers={blockers})" if blockers else "",
                    )
                    tech_map = session.get("technical_errors") or {}
                    tech_map[error_code] = tech_map.get(error_code, 0) + 1
                    session["technical_errors"] = tech_map
                    pending_links = session.get("pending_links") or []
                    if link not in pending_links:
                        pending_links.append(link)
                        session["pending_links"] = pending_links
                    await save_user_session(user_id, session)
                    return
            else:
                blockers = result_data.get("blockers")
                logger.warning(
                    "⚠️ Пропускаем сохранение %s из-за технической ошибки %s — ссылка останется в очереди%s",
                    link,
                    error_code,
                    f" (blockers={blockers})" if blockers else "",
                )
                tech_map = session.get("technical_errors") or {}
                tech_map[error_code] = tech_map.get(error_code, 0) + 1
                session["technical_errors"] = tech_map
                pending_links = session.get("pending_links") or []
                if link not in pending_links:
                    pending_links.append(link)
                    session["pending_links"] = pending_links
                await save_user_session(user_id, session)
                return

        # Сохраняем результат
        all_results[link] = result_data

        # Сохраняем в базу данных
        await db.save_result(link, result_data, user_id)

        # Обновляем сессию с новыми результатами
        session["results"] = all_results
        pending_links = session.get("pending_links") or []
        if link in pending_links:
            pending_links.remove(link)
            session["pending_links"] = pending_links
        delayed_map = session.get("delayed_links") or {}
        if link in delayed_map:
            delayed_map.pop(link, None)
            session["delayed_links"] = delayed_map
        if link in stuck_attempts:
            stuck_attempts.pop(link, None)
            session["stuck_attempts"] = stuck_attempts

        assigned_session = session.get("link_sessions", {}).get(link)
        progress_map = session.get("session_progress") or {}
        progress_key = _progress_key(assigned_session)
        if progress_key in progress_map:
            progress_map[progress_key] = progress_map.get(progress_key, 0) + 1
            session["session_progress"] = progress_map
        await save_user_session(user_id, session)

        new_checks_count += 1
        last_result_ts = time.time()
        processed = len(all_results)

        # Обновляем статус каждые 3 обработанных ссылки
        now_ts = time.time()
        should_heartbeat = (now_ts - last_status_update_ts) >= PROGRESS_HEARTBEAT_SECONDS

        if new_checks_count % 3 == 0 or processed == total or should_heartbeat:
            await _render_status()

    def _resolve_worker_sessions(service) -> List[Optional[str]]:
        if not service:
            return [None]

        slot_names: List[str] = []
        ready: set[str] = set()
        if hasattr(service, "get_active_session_names"):
            try:
                ready = set(service.get_active_session_names() or [])
            except Exception as exc:
                logger.warning("Не удалось получить список готовых сессий: %s", exc)
                ready = set()

        if hasattr(service, "get_slot_assignments"):
            try:
                slots = service.get_slot_assignments() or {}
                for slot in ("slot_a", "slot_b"):
                    name = slots.get(slot)
                    if name and name not in slot_names:
                        if ready and name not in ready:
                            continue
                        slot_names.append(name)
            except Exception as exc:
                logger.warning("Не удалось получить слоты VK сервиса: %s", exc)

        active_names: List[str] = []
        if hasattr(service, "get_active_session_names"):
            try:
                active_names = service.get_active_session_names() or []
            except Exception as exc:
                logger.error(f"Не удалось получить список активных сессий: {exc}")

        active_set = {name for name in active_names if name}
        if active_set:
            slot_names = [name for name in slot_names if name in active_set]

        fallback = list(getattr(service, "session_order", []) or [])
        if getattr(service, "primary_session_name", None):
            fallback = [service.primary_session_name] + [name for name in fallback if name != service.primary_session_name]

        # Фильтруем fallback по готовым сессиям, если знаем список ready/active
        if ready:
            fallback = [name for name in fallback if name in ready]
        resolved = slot_names or active_names or fallback
        resolved = [name for name in resolved if name]
        return resolved or [None]

    worker_sessions = _resolve_worker_sessions(vk_service)
    if not worker_sessions:
        worker_sessions = [None]

    async def _build_assignments(batch_links: List[str]) -> Tuple[Dict[Optional[str], List[str]], Dict[str, Optional[str]], int]:
        nonlocal session
        if not batch_links:
            return {}, {}, 0

        active_from_service: List[str] = []
        if hasattr(vk_service, "get_active_session_names"):
            try:
                active_from_service = vk_service.get_active_session_names()
            except Exception as exc:
                logger.warning("Не удалось получить активные сессии VK сервиса: %s", exc)

        def _preferred_sessions():
            slots = {}
            if hasattr(vk_service, "get_slot_assignments"):
                try:
                    slots = vk_service.get_slot_assignments()
                except Exception:
                    slots = {}
            ordered: List[str] = []
            for slot in ("slot_a", "slot_b"):
                name = slots.get(slot)
                if name and name not in ordered:
                    ordered.append(name)
            active_ready = {name for name in active_from_service if name}
            if active_ready:
                ordered = [name for name in ordered if name in active_ready]
            return ordered

        preferred = _preferred_sessions()
        active_pool: List[str] = []
        candidate_sets = [
            [name for name in preferred if name in worker_sessions],
            [name for name in active_from_service if name in worker_sessions],
            worker_sessions,
        ]
        for candidate in candidate_sets:
            if candidate:
                active_pool = candidate
                break

        active_pool = [name for name in active_pool if name] or [None]

        max_workers = max(1, min(len(active_pool), MAX_WORKERS_PER_JOB))
        processing_sessions = active_pool[:max_workers]

        assignments: Dict[Optional[str], List[str]] = {}
        if not batch_links:
            target_session = processing_sessions[0] if processing_sessions else active_pool[0]
            assignments[target_session] = []
        else:
            for session_name in processing_sessions:
                assignments[session_name] = []
            for idx, link in enumerate(batch_links):
                target_idx = idx % len(processing_sessions)
                target_session = processing_sessions[target_idx]
                assignments[target_session].append(link)

        assignments = {name: links for name, links in assignments.items() if links or not batch_links}
        link_map: Dict[str, Optional[str]] = {}
        for session_name, payload in assignments.items():
            for link in payload:
                link_map[link] = session_name

        tracked_assignments = dict(session.get("session_assignments") or {})
        for name, payload in assignments.items():
            key = _progress_key(name)
            tracked_assignments[key] = tracked_assignments.get(key, 0) + len(payload)
        session["session_assignments"] = tracked_assignments
        progress_map = session.get("session_progress") or {}
        for key in tracked_assignments.keys():
            progress_map.setdefault(key, 0)
        session["session_progress"] = progress_map
        session["link_sessions"] = {**session.get("link_sessions", {}), **link_map}
        await save_user_session(user_id, session)

        active_worker_targets = sum(1 for payload in assignments.values() if payload) or len(assignments)
        if hasattr(vk_service, "update_load_metrics"):
            try:
                await vk_service.update_load_metrics(len(batch_links), active_worker_targets)
            except Exception as exc:
                logger.warning("Не удалось обновить метрики нагрузки VK сервиса: %s", exc)

        active_sessions = [name for name in assignments.keys() if name]
        logger.info(
            "🧵 Запускаем %s воркеров (сессии: %s) для %s ссылок",
            len(assignments),
            ", ".join(active_sessions) if active_sessions else "default",
            len(batch_links),
        )
        return assignments, link_map, active_worker_targets

    links_remaining = list(links_to_check)
    session["pending_links"] = list(links_remaining)
    await save_user_session(user_id, session)
    link_session_map: Dict[str, Optional[str]] = {}

    async def _get_control_flags():
        current = await get_user_session(user_id)
        if not current:
            return False, True
        return bool(current.get("paused")), bool(current.get("cancelled"))

    async def _session_worker(session_name: Optional[str], links_for_worker: List[str]):
        nonlocal hold_notice_sent
        worker_label = session_name or "default"

        pause_notice_logged = False

        for idx, link in enumerate(links_for_worker):
            paused, cancelled = await _get_control_flags()
            if cancelled:
                logger.info("⛔️ Пользователь отменил обработку, воркер %s завершает работу", worker_label)
                return
            while paused:
                if not pause_notice_logged:
                    pending_cnt = len(session.get("pending_links") or [])
                    logger.info(
                        "⏸ Воркер %s приостановлен пользователем (pending=%s)",
                        worker_label,
                        pending_cnt,
                    )
                    pause_notice_logged = True
                await asyncio.sleep(0.5)
                paused, cancelled = await _get_control_flags()
                if cancelled:
                    logger.info("⛔️ Пользователь отменил обработку, воркер %s завершает работу", worker_label)
                    return
            if pause_notice_logged and not paused:
                logger.info("▶️ Воркер %s продолжает работу после паузы", worker_label)
                pause_notice_logged = False

            wait_hint_cb = getattr(vk_service, "get_wait_hint", None)
            if callable(wait_hint_cb) and wait_hint_cb():
                await asyncio.sleep(PROGRESS_HEARTBEAT_SECONDS)

            try:
                result_data = await vk_service.search_vk_data(link, preferred_session=session_name)
            except Exception as exc:
                logger.exception("Ошибка при обработке %s через %s: %s", link, worker_label, exc)
                result_data = {
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": str(exc),
                }

            if result_data is None:
                result_data = {}
            if not isinstance(result_data, dict):
                result_data = {}

            if result_data.get("error") == "no_available_bots":
                await message.answer(
                    "⚠️ Нет доступных Telegram-сессий для обработки. Задача приостановлена, попробуйте позже или обратитесь к администратору.",
                    reply_markup=main_menu_kb(message.from_user.id, ADMIN_IDS),
                )
                logger.warning("⛔️ %s: нет доступных ботов, воркер завершает работу", worker_label)
                return

            if result_data.get("error") == "limit_reached":
                limit_reason = result_data.get("limit_reason") or "vk_limit"
                hold_interval = max(float(_HOLD_DEFAULT) + 5.0, AUTO_RESUME_MIN_INTERVAL)
                remaining_slice = links_for_worker[idx:]
                await _schedule_hold_retry(remaining_slice, limit_reason, hold_interval)
                await _render_status(
                    wait_hint=f"⚠️ {worker_label} достиг лимита. Автоповтор через ~{int(hold_interval)}с.",
                    force=True,
                )
                if bot and not hold_notice_sent:
                    hold_notice_sent = True
                    user = message.from_user
                    mention = (
                        f"@{user.username}" if user.username else f"{user.full_name or 'ID ' + str(user.id)}"
                    )
                    await notify_admins(
                        bot,
                        f"⚠️ {mention} упёрся в лимит поисков. Отложено {len(remaining_slice)} ссылок.",
                    )
                logger.warning(
                    "⚠️ %s: достигнут лимит (reason=%s), ссылки отложены на %.1fs",
                    worker_label,
                    limit_reason,
                    hold_interval,
                )
                return

            if result_data.get("error") == "flood_wait":
                wait_seconds = float(result_data.get("wait_seconds") or 0.0)
                hold_interval = max(wait_seconds, float(_HOLD_DEFAULT), AUTO_RESUME_MIN_INTERVAL)
                remaining_slice = links_for_worker[idx:]
                await _schedule_hold_retry(remaining_slice, "flood_wait", hold_interval)
                await _render_status(
                    wait_hint=f"⚠️ {worker_label} получил FloodWait. Автоповтор через ~{int(hold_interval)}с.",
                    force=True,
                )
                if bot and not hold_notice_sent:
                    hold_notice_sent = True
                    user = message.from_user
                    mention = (
                        f"@{user.username}" if user.username else f"{user.full_name or 'ID ' + str(user.id)}"
                    )
                    await notify_admins(
                        bot,
                        f"⚠️ {mention} упёрся в FloodWait. Отложено {len(remaining_slice)} ссылок.",
                    )
                logger.warning(
                    "⚠️ %s: FloodWait (wait=%.1fs), ссылки отложены на %.1fs",
                    worker_label,
                    wait_seconds,
                    hold_interval,
                )
                return

            assigned_session = link_session_map.get(link)
            if assigned_session and "session_name" not in result_data:
                result_data["session_name"] = assigned_session

            await result_cb(link, result_data)

    async def _heartbeat_loop():
        while not heartbeat_stop.is_set():
            await asyncio.sleep(PROGRESS_HEARTBEAT_SECONDS)
            if heartbeat_stop.is_set():
                break
            hint = _get_service_wait_hint()
            idle_for = time.time() - last_result_ts
            if not hint and idle_for >= PROGRESS_HEARTBEAT_SECONDS * 1.5:
                hint = f"⏳ Ждем ответ бота… {int(idle_for)}с"
            if hint:
                try:
                    await _render_status(wait_hint=hint, force=True)
                except Exception as exc:
                    logger.debug("Heartbeat update failed: %s", exc)

    while links_remaining:
        session["pending_links"] = list(links_remaining)
        await save_user_session(user_id, session)
        assignments, link_session_map, _ = await _build_assignments(links_remaining)
        if not assignments:
            break

        tasks = [
            asyncio.create_task(_session_worker(name, payload))
            for name, payload in assignments.items()
            if payload
        ]
        heartbeat_task = asyncio.create_task(_heartbeat_loop())
        if tasks:
            try:
                await asyncio.gather(*tasks)
            finally:
                heartbeat_stop.set()
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
        else:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

            session = await get_user_session(user_id) or session
        if session.get("cancelled"):
            break

        await _release_delayed_links()
        links_remaining = list(session.get("pending_links") or [])

        if not links_remaining and delayed_links:
            next_ready = min(delayed_links.values())
            wait_seconds = max(next_ready - time.time(), 1.0)
            await _render_status(
                wait_hint=f"⚠️ Все ссылки ждут освобождения бота. Автопродолжение через ~{int(wait_seconds)}с",
                force=True,
            )
            logger.info(
                "⏳ Очередь заблокирована HOLD'ом: delayed_links=%s, ожидание %.1f с",
                len(delayed_links),
                wait_seconds,
            )
            _log_queue_state("hold_wait", wait_seconds=wait_seconds)
            await asyncio.sleep(wait_seconds)
            await _release_delayed_links()
            links_remaining = list(session.get("pending_links") or [])

        if session.get("limit_reason") and not delayed_links:
            session.pop("limit_reason", None)
            await save_user_session(user_id, session)
            hold_notice_sent = False
            _log_queue_state("limit_cleared")

        if not links_remaining:
            break

    current_session = await get_user_session(user_id) or session
    if current_session:
        current_session["processing_active"] = False
        await save_user_session(user_id, current_session)

    remaining_pending = list(session.get("pending_links") or [])
    remaining_delayed = list((session.get("delayed_links") or {}).keys())
    resume_candidates = [
        link for link in (session.get("links_order") or [])
        if link in remaining_pending or link in remaining_delayed
    ]
    for link in remaining_pending:
        if link not in resume_candidates:
            resume_candidates.append(link)
    for link in remaining_delayed:
        if link not in resume_candidates:
            resume_candidates.append(link)

    if resume_candidates:
        logger.info(
            "♻️ Перезапуск обработки оставшихся %s ссылок (pending=%s, delayed=%s)",
            len(resume_candidates),
            len(remaining_pending),
            len(remaining_delayed),
        )
        await start_processing(
            message=message,
            links_to_process=resume_candidates,
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
        return

    # Обработка завершена успешно
    await finish_processing(message, all_results, processor, links_to_process, user_id, db, bot)


async def force_search_without_cache(
        message: Message,
        links_to_process: List[str],
        processor: Optional[ExcelProcessor],
        user_id: int,
        db,
        vk_service=None,
        bot=None,
):
    """Полный перезапуск проверки ссылок без использования кеша и фильтрации по телефонам."""
    if not db or not vk_service:
        await message.answer("❌ Сервис временно недоступен")
        return

    normalized_links = [link for link in links_to_process if isinstance(link, str) and link.strip()]
    if not normalized_links:
        await message.answer("❌ Нет ссылок для повторной проверки", reply_markup=main_menu_kb(user_id, ADMIN_IDS))
        return

    await start_processing(
        message=message,
        links_to_process=normalized_links,
        processor=processor,
        duplicate_check={},
        user_id=user_id,
        db=db,
        vk_service=vk_service,
        bot=bot,
        force_use_cache=False,
        force_no_cache=True,
        skip_duplicate_filter=True,
    )


async def finish_processing(
        message: Message,
        results: Dict[str, Dict],
        processor: ExcelProcessor,
        links_order: List[str],
        user_id: int,
        db,
        bot=None
):
    """Завершает обработку и отправляет результаты"""

    session = await get_user_session(user_id)
    file_expected = bool(session and session.get("temp_file"))

    processor_ready = bool(processor and processor.original_df is not None)
    if not processor_ready:
        restored = restore_processor_from_session(session)
        if restored and restored.original_df is not None:
            processor = restored
            processor_ready = True

    files_to_send: List[Tuple[Path, str]] = []
    file_error_message: Optional[str] = None

    if processor_ready:
        temp_dir = Path(tempfile.mkdtemp())
        ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
        output_path = temp_dir / f"vk_data_complete_{ts}.xlsx"

        success = processor.save_results_with_original_data(results, output_path)
        if success:
            found_count = sum(1 for data in results.values() if data.get("phones"))
            not_found_count = len(links_order) - found_count
            caption = f"""📊 Файл с результатами готов!

✅ Обработано: {len(links_order)} ссылок
📱 Найдены телефоны: {found_count}
❌ Без телефонов: {not_found_count}

💾 Все исходные данные сохранены!"""
            files_to_send.append((output_path, caption))
        else:
            file_error_message = "⚠️ Не удалось сохранить файл с исходными данными. Повторите попытку или загрузите файл заново."
    elif file_expected:
        file_name = (session or {}).get("file_name") or "исходный файл"
        file_error_message = (
            f"⚠️ Не удалось найти исходный файл «{file_name}». "
            "Я не могу обновить его новыми телефонами. Загрузите файл заново и повторите поиск."
        )
    else:
        files_to_send = await create_excel_from_results(results, links_order)

    # Формируем и отправляем итоговое сообщение по сессии
    found_count = 0
    not_found_count = 0
    for data in results.values():
        has_phones = bool(data.get("phones", []))
        has_name = bool(data.get("full_name", ""))
        has_birth = bool(data.get("birth_date", ""))
        if has_phones or has_name or has_birth:
            found_count += 1
        else:
            not_found_count += 1

    complete_text = MESSAGES["session_complete"].format(
        total=len(results),
        found=found_count,
        not_found=not_found_count
    )
    session_summary = format_session_assignments_summary(session or {})
    if session_summary:
        complete_text += f"\n\n{session_summary}"

    await message.answer(complete_text, reply_markup=finish_kb())

    if files_to_send and bot:
        for file_path, caption in files_to_send:
            try:
                await bot.send_document(
                    message.chat.id,
                    FSInputFile(file_path),
                    caption=caption
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке файла: {e}")
                await message.answer(f"⚠️ Не удалось отправить файл: {str(e)}")
    elif file_error_message:
        await message.answer(file_error_message, reply_markup=finish_kb())
    elif not files_to_send:
        await message.answer(
            "❌ Произошла ошибка при создании файла с результатами.\n"
            "Попробуйте еще раз или обратитесь к администратору.",
            reply_markup=finish_kb()
        )

    # Очищаем сессию
    await clear_user_session(user_id)
