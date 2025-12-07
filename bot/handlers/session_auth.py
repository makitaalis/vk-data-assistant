"""Мастер авторизации Telegram-сессий через бота."""

import re
import logging
from typing import Optional, Dict, Any

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.dispatcher.event.bases import SkipHandler

from bot.config import ADMIN_IDS
from bot.keyboards.inline import (
    session_auth_menu_kb,
    session_auth_slot_kb,
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
)
from services.config_service import get_config_service
from services.session_auth_service import SessionAuthManager
from services.vk_multibot_service import VKMultiBotService

router = Router()
logger = logging.getLogger("session_auth_handler")
SESSION_AUTH_KEY = "session_auth_state"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def _get_auth_state(user_id: int) -> dict:
    session = await get_user_session(user_id) or {}
    return session.get(SESSION_AUTH_KEY, {})


async def _set_auth_state(user_id: int, state: dict):
    session = await get_user_session(user_id) or {}
    session[SESSION_AUTH_KEY] = state
    await save_user_session(user_id, session)


async def _clear_auth_state(user_id: int):
    session = await get_user_session(user_id) or {}
    if SESSION_AUTH_KEY in session:
        del session[SESSION_AUTH_KEY]
    await save_user_session(user_id, session)


@router.message(Command("session_auth"))
async def cmd_session_auth(
    msg: Message,
    session_auth_manager: SessionAuthManager,
):
    if not is_admin(msg.from_user.id):
        return

    await _set_auth_state(msg.from_user.id, {"step": "idle"})
    text = (
        "🔐 <b>Мастер авторизации Telegram-сессий</b>\n\n"
        "Через это меню можно привязать телефон к сессии и сразу активировать её "
        "в Slot A/B.\n\n"
        "1. Выберите слот или резерв.\n"
        "2. Укажите имя сессии (как в конфиге).\n"
        "3. Введите номер телефона и код из Telegram.\n\n"
        "После успешной авторизации сессия автоматически появится в списке."
    )
    await msg.answer(text, reply_markup=session_auth_menu_kb())


@router.callback_query(F.data == "admin_session_auth")
async def on_admin_session_auth(call: CallbackQuery):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    await _set_auth_state(call.from_user.id, {"step": "idle"})
    text = (
        "🔐 <b>Мастер авторизации Telegram-сессий</b>\n\n"
        "Через это меню можно привязать телефон к сессии и сразу активировать её "
        "в Slot A/B.\n\n"
        "1. Выберите слот или резерв.\n"
        "2. Укажите имя сессии (как в конфиге).\n"
        "3. Введите номер телефона и код из Telegram.\n\n"
        "После успешной авторизации сессия автоматически появится в списке."
    )
    await call.message.answer(text, reply_markup=session_auth_menu_kb())


@router.callback_query(F.data == "sessionauth_start")
async def on_sessionauth_start(call: CallbackQuery):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    await _set_auth_state(call.from_user.id, {"step": "choose_slot"})
    await call.message.answer(
        "Выберите слот, в который нужно установить сессию.\n"
        "Можно выбрать резерв, если хотите просто обновить данные без назначения.",
        reply_markup=session_auth_slot_kb()
    )


@router.callback_query(F.data == "sessionauth_back")
async def on_sessionauth_back(call: CallbackQuery):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await _set_auth_state(call.from_user.id, {"step": "idle"})
    await call.message.answer("Возврат в мастер авторизации.", reply_markup=session_auth_menu_kb())


@router.callback_query(F.data == "sessionauth_status")
async def on_sessionauth_status(
    call: CallbackQuery,
    session_auth_manager: SessionAuthManager,
):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    job = await session_auth_manager.get_job_status(call.from_user.id)
    if not job:
        await call.message.answer("ℹ️ Нет активной авторизации.", reply_markup=session_auth_menu_kb())
        return

    text = (
        "📡 <b>Статус авторизации</b>\n"
        f"• Сессия: <code>{job['session_name']}</code>\n"
        f"• Телефон: <code>{job['phone']}</code>\n"
        f"• Слот: {job.get('slot') or 'резерв'}\n"
        f"• Статус: {job['status']}\n"
    )
    if job.get("password_required"):
        text += "\n🔐 Требуется пароль 2FA"
    await call.message.answer(text, reply_markup=session_auth_menu_kb())


@router.callback_query(F.data == "sessionauth_cancel")
async def on_sessionauth_cancel(
    call: CallbackQuery,
    session_auth_manager: SessionAuthManager,
):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    await session_auth_manager.cancel_job(call.from_user.id)
    await _clear_auth_state(call.from_user.id)
    await call.message.answer("⏹ Процесс авторизации остановлен.", reply_markup=session_auth_menu_kb())


@router.callback_query(F.data.startswith("sessionauth_slot:"))
async def on_sessionauth_slot(call: CallbackQuery):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    slot_key = call.data.split(":", 1)[1]
    slot_map = {
        "slot_a": "slot_a",
        "slot_b": "slot_b",
        "reserve": None,
    }
    slot_value = slot_map.get(slot_key)
    if slot_key not in slot_map:
        await call.answer("Некорректный слот", show_alert=True)
        return

    state = {"step": "enter_session_name", "slot": slot_value}
    await _set_auth_state(call.from_user.id, state)
    await call.message.answer(
        "Введите имя сессии из конфигурации (например, <code>user_session_15167864134</code>)."
    )


@router.message(F.text)
async def on_sessionauth_text(
    msg: Message,
    session_auth_manager: SessionAuthManager,
    vk_service: VKMultiBotService,
):
    if not is_admin(msg.from_user.id):
        return

    # Игнорируем команды, но подсказываем пользователю, что ожидаем ввод по шагу
    if msg.text.startswith("/"):
        await msg.answer("⚠️ В процессе авторизации используйте текстовые ответы (номер, код, пароль).")
        return

    state = await _get_auth_state(msg.from_user.id)
    step = state.get("step")

    if not step or step == "idle":
        return

    if step == "enter_session_name":
        session_name = msg.text.strip()
        if session_name in vk_service.sessions:
            await msg.answer(f"✅ Найдена существующая сессия <code>{session_name}</code>.")
        else:
            await msg.answer(
                f"ℹ️ Сессия <code>{session_name}</code> не найдена в конфиге. "
                "Она будет создана автоматически после авторизации."
            )
        state["session_name"] = session_name
        state["is_new_session"] = session_name not in vk_service.sessions
        state["step"] = "enter_phone"
        await _set_auth_state(msg.from_user.id, state)
        await msg.answer("Введите номер телефона в формате +79991234567.")
        return

    if step == "enter_phone":
        phone_raw = re.sub(r"[^\d+]", "", msg.text.strip())
        if phone_raw.startswith("8"):
            phone_raw = "+7" + phone_raw[1:]
        elif not phone_raw.startswith("+"):
            phone_raw = "+" + phone_raw

        digits = re.sub(r"\D", "", phone_raw)
        if len(digits) < 10:
            await msg.answer("❌ Номер слишком короткий. Попробуйте еще раз.")
            return

        session_name = state.get("session_name")
        slot = state.get("slot")
        state["phone"] = phone_raw
        try:
            await session_auth_manager.start_job(
                admin_id=msg.from_user.id,
                session_name=session_name,
                phone=phone_raw,
                slot=slot,
            )
        except ValueError as exc:
            await msg.answer(f"❌ {exc}")
            return
        except RuntimeError as exc:
            await msg.answer(f"⚠️ {exc}")
            return

        state["step"] = "waiting_code"
        await _set_auth_state(msg.from_user.id, state)
        await msg.answer(
            f"📨 Код отправлен на {phone_raw}. Введите 5-значный код из Telegram.\n"
            "Если код не приходит, нажмите «Отмена» и начните заново."
        )
        return

    if step == "waiting_code":
        code = re.sub(r"\D", "", msg.text)
        if len(code) < 3:
            await msg.answer("❌ Код выглядит неверным. Попробуйте еще раз.")
            return

        try:
            result = await session_auth_manager.submit_code(msg.from_user.id, code)
        except RuntimeError as exc:
            await msg.answer(f"⚠️ {exc}")
            return

        if result.get("status") == "password_required":
            state["step"] = "waiting_password"
            await _set_auth_state(msg.from_user.id, state)
            await msg.answer("🔐 Укажите пароль 2FA.")
            return

        await _finalize_authorization(msg, state, result, vk_service)
        return

    if step == "waiting_password":
        password = msg.text.strip()
        if not password:
            await msg.answer("❌ Пароль не может быть пустым.")
            return

        try:
            result = await session_auth_manager.submit_password(msg.from_user.id, password)
        except RuntimeError as exc:
            await msg.answer(f"⚠️ {exc}")
            return

        await _finalize_authorization(msg, state, result, vk_service)
        return


async def _finalize_authorization(
    msg: Message,
    state: dict,
    result: Dict[str, Any],
    vk_service: VKMultiBotService,
):
    profile = result.get("profile", {})
    session_name = result.get("session_name")
    slot = state.get("slot")
    config = get_config_service()
    phone_number = profile.get("phone") or state.get("phone")

    if session_name not in vk_service.sessions:
        await vk_service.register_session(session_name, phone_number, enabled=True)
        await config.upsert_session_definition(session_name, phone_number, True)
    elif state.get("is_new_session"):
        await config.upsert_session_definition(session_name, phone_number, True)

    await config.set_session_enabled(session_name, True)

    if slot in {"slot_a", "slot_b"}:
        slots = await config.get_session_slots()
        other_slot = "slot_b" if slot == "slot_a" else "slot_a"
        if slots.get(other_slot) == session_name:
            await config.set_session_slot(other_slot, None)
            slots[other_slot] = None
        await config.set_session_slot(slot, session_name)
        slots[slot] = session_name
        await vk_service.apply_slot_assignments(slots)
    else:
        # Резервная авторизация — просто обновим сессию
        await vk_service.enable_session(session_name, reason="session_auth")

    await _clear_auth_state(msg.from_user.id)

    text = (
        "✅ <b>Сессия авторизована!</b>\n\n"
        f"• Сессия: <code>{session_name}</code>\n"
        f"• Телефон: <code>{phone_number or profile.get('phone')}</code>\n"
        f"• Пользователь: {profile.get('first_name','')} {profile.get('last_name','')}\n"
        f"• Slot: {slot or 'резерв'}\n\n"
        "Можете проверить статус через /session_status."
    )
    await msg.answer(text, reply_markup=session_auth_menu_kb())
