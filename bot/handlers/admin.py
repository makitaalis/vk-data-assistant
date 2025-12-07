"""Обработчики админских функций"""

import asyncio
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup

from bot.config import ADMIN_IDS, VK_BOT_USERNAME, VK_BOT_USERNAMES, SESSION_DIR, TELEGRAM_SESSIONS
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    main_menu_kb,
    back_to_menu_kb,
    db_load_menu_kb,
    settings_kb,
    session_control_kb,
    session_slot_select_kb,
    session_delete_confirm_kb,
    confirm_action_kb,
    session_bot_selector_kb,
    vk_pool_kb,
    admin_maintenance_kb,
)
from bot.utils.session_manager import (
    get_user_session,
    save_user_session,
    clear_user_session
)
from bot.utils.helpers import safe_edit_message
from bot.utils.admin_notifications import notify_admins
from db_module import VKDatabase
from services.vk_multibot_service import VKMultiBotService
from services.config_service import get_config_service

router = Router()
logger = logging.getLogger("admin_handler")
ROOT_DIR = Path(__file__).resolve().parents[2]


def is_admin(user_id: int) -> bool:
    """Проверка является ли пользователь администратором"""
    return user_id in ADMIN_IDS


async def build_session_status_view(vk_service: VKMultiBotService) -> tuple[str, InlineKeyboardMarkup]:
    """Формирование текста и клавиатуры для управления сессиями"""
    config = get_config_service()
    stats = await vk_service.get_stats()
    session_mode = await config.get_session_mode()
    sessions = stats.get("sessions", [])
    slots = stats.get("slots", {})

    mode_labels = {
        "primary": "только Primary",
        "secondary": "только Secondary",
        "both": "две сессии одновременно",
    }
    mode_desc = mode_labels.get(session_mode, session_mode)

    text = [
        "📡 <b>Telegram-сессии</b>",
        f"• Режим: <code>{session_mode}</code> ({mode_desc})",
        f"• Всего VK ботов: {stats['total_bots']}",
        f"• Доступно сейчас: {stats['available_bots']}",
        "",
        "🔧 <b>Состояние сессий:</b>",
    ]

    if not sessions:
        text.append("❌ Сессии не настроены")
    else:
        text.append("<b>Имя | Боты | Запросы | Ошибки | Статусы</b>")
        for session in sessions:
            runtime_icon = "🟢" if session.get("enabled") else "⚪️"
            configured_icon = "✅" if session.get("configured") else "⚠️"
            name = session.get("name")
            bots_available = session.get("bots_available", 0)
            bots_total = session.get("bots_total", 0)
            info_lines = [
                f"{runtime_icon} <b>{name}</b> — {bots_available}/{bots_total} ботов (конфиг: {configured_icon})",
                f"   • Запросов: {session.get('requests', 0)} | Ошибок: {session.get('errors', 0)}",
            ]
            hold = session.get("bots_on_hold", 0)
            limited = session.get("bots_limited", 0)
            removed = session.get("bots_removed", 0)
            status_bits = []
            if hold:
                status_bits.append(f"hold: {hold}")
            if limited:
                status_bits.append(f"limit: {limited}")
            if removed:
                status_bits.append(f"removed: {removed}")
            if status_bits:
                info_lines.append("   • Статусы: " + ", ".join(status_bits))
            configured_bots = session.get("configured_bots") or []
            if configured_bots:
                preview = ", ".join(f"@{bot}" for bot in configured_bots[:3])
                if len(configured_bots) > 3:
                    preview += f" +{len(configured_bots) - 3}"
                info_lines.append(f"   • Боты ({len(configured_bots)}): {preview}")
            text.extend(info_lines)

    text.append(f"\n🕐 Обновлено: {datetime.now().strftime('%H:%M:%S')}")

    text.append("")
    text.append("🎯 <b>Активные слоты:</b>")
    slot_names = [
        ("slot_a", "Slot A"),
        ("slot_b", "Slot B"),
    ]
    for slot_key, label in slot_names:
        assigned = slots.get(slot_key)
        assigned_entry = next((s for s in sessions if s.get("name") == assigned), None)
        if assigned_entry:
            runtime_icon = "🟢" if assigned_entry.get("enabled") else "⚪️"
            assigned_text = f"{runtime_icon} <code>{assigned}</code>"
        else:
            assigned_text = "⚪️ не назначен"
        text.append(f"• {label}: {assigned_text}")

    protected = {cfg.name for cfg in TELEGRAM_SESSIONS}
    keyboard = session_control_kb(session_mode, sessions, slots, protected_sessions=protected)
    return "\n".join(text), keyboard


def list_session_names(vk_service: VKMultiBotService) -> list[str]:
    return list(vk_service.sessions.keys())


def _format_session_list(names: list[str]) -> str:
    if not names:
        return "Нет зарегистрированных сессий"
    return "\n".join(f"• {name}" for name in names)


def _extract_session_name(text: str) -> Optional[str]:
    if not text:
        return None
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip()


async def _update_session_message(message: Message, vk_service: VKMultiBotService):
    text, keyboard = await build_session_status_view(vk_service)
    await message.edit_text(text, reply_markup=keyboard)


async def _render_session_bot_menu(message: Message, vk_service: VKMultiBotService, session_name: str):
    if session_name not in vk_service.sessions:
        await message.edit_text("❌ Сессия недоступна", reply_markup=back_to_menu_kb())
        return

    available_bots = vk_service.get_available_bot_usernames()
    if not available_bots:
        await message.edit_text("❌ VK боты не настроены", reply_markup=back_to_menu_kb())
        return

    selected = vk_service.get_session_assigned_bots(session_name)
    selected_set = set(selected)
    if selected:
        preview = ", ".join(f"@{bot}" for bot in selected[:6])
        if len(selected) > 6:
            preview += f" +{len(selected) - 6}"
        current_line = f"Текущий список: {preview}"
    else:
        current_line = "Текущий список: все доступные боты"

    text_lines = [
        f"🤖 <b>Боты для сессии {session_name}</b>",
        "Отметьте тех, кто будет использоваться при поиске.",
        "Если снять все галочки, задействуем весь список по умолчанию.",
        "",
        f"Всего VK ботов: {len(available_bots)}",
        current_line,
    ]

    session_alias = vk_service.get_session_alias(session_name)
    keyboard = session_bot_selector_kb(session_alias, available_bots, selected_set)
    await safe_edit_message(message, "\n".join(text_lines), reply_markup=keyboard)


async def _render_vk_pool(message: Message, vk_service: VKMultiBotService):
    bots = vk_service.get_available_bot_usernames()
    text_lines = [
        "🤖 <b>VK бот-пул</b>",
        "Добавляйте/удаляйте имена ботов без перезапуска.",
        "Можно задать лимит запросов (квоту) для каждого бота.",
        "",
        f"Всего: {len(bots)}",
    ]
    if bots:
        text_lines.append("Текущий список:\n" + "\n".join(f"• @{b}" for b in bots))
    else:
        text_lines.append("❌ Список пуст — добавьте хотя бы одного бота.")
    await safe_edit_message(message, "\n".join(text_lines), reply_markup=vk_pool_kb(bots))


@router.message(Command("botstatus"))
async def cmd_bot_status(msg: Message, vk_service: VKMultiBotService):
    """Проверка статуса VK ботов (только для админов)"""
    if not is_admin(msg.from_user.id):
        return

    status_msg = await msg.answer("🔄 Проверяю статус ботов...")

    try:
        # Получаем статистику всех ботов
        stats = await vk_service.get_stats()
        
        # Проверяем баланс всех ботов
        balance_info = await vk_service.check_balance()

        # Формируем текст статуса
        search_stats = stats.get("search_stats") or {}

        status_text = f"""
🤖 <b>Статус VK ботов</b>

📊 <b>Общая статистика:</b>
• Всего ботов: {stats['total_bots']}
• Доступно ботов: {stats['available_bots']}
• Всего запросов: {stats['total_requests']}
• Обработано: {stats['total_processed']}
• Ошибок: {stats['total_errors']}

🔄 <b>Статус каждого бота:</b>"""

        if search_stats:
            totals_links = search_stats.get("total_links_sent", 0)
            totals_req = search_stats.get("total_bot_requests", 0)
            today_links = search_stats.get("today_links_sent", 0)
            today_req = search_stats.get("today_bot_requests", 0)
            status_text += (
                "\n📈 <b>Поисковые метрики:</b>\n"
                f"• Ссылок отправлено всего: {totals_links:,}\n"
                f"• Запросов к ботам всего: {totals_req:,}\n"
                f"• Сегодня отправлено ссылок: {today_links:,}\n"
                f"• Сегодня запросов к ботам: {today_req:,}\n"
            )

            today_sessions = search_stats.get("sessions_today") or {}
            if today_sessions:
                status_text += "   • Сегодня по сессиям:\n"
                for name, payload in today_sessions.items():
                    links_val = payload.get("links_sent", 0)
                    req_val = payload.get("bot_requests", 0)
                    status_text += f"     - {name}: links={links_val}, req={req_val}\n"
        
        for bot_stat in stats['bots']:
            status_icon = "✅" if bot_stat['available'] else "❌"
            limit_icon = "⚠️" if bot_stat['limit_reached'] else ""
            status_text += f"\n{status_icon} Бот {bot_stat['index']}: @{bot_stat['username']}"
            status_text += f"\n  • Запросов: {bot_stat['requests']}"
            if bot_stat['errors'] > 0:
                status_text += f" | Ошибок: {bot_stat['errors']}"
            if bot_stat['limit_reached']:
                status_text += f" {limit_icon} ЛИМИТ"
            status_text += "\n"
        
        if balance_info:
            status_text += f"\n💰 <b>Баланс:</b>\n{balance_info}"

        sessions = stats.get("sessions", [])
        if sessions:
            status_text += "\n📡 <b>Telegram-сессии</b>\n"
            status_text += f"• Режим: <code>{stats.get('session_mode')}</code>\n"
            for session in sessions:
                runtime_icon = "🟢" if session.get("enabled") else "⚪️"
                configured_icon = "✅" if session.get("configured") else "⚠️"
                bots_info = f"{session.get('bots_available', 0)}/{session.get('bots_total', 0)}"
                status_text += (
                    f"{runtime_icon} {session.get('name')} — {bots_info} ботов (конфиг: {configured_icon})\n"
                    f"   • Запросов: {session.get('requests', 0)} | Ошибок: {session.get('errors', 0)}\n"
                )
                extras = []
                if session.get("bots_on_hold"):
                    extras.append(f"hold: {session['bots_on_hold']}")
                if session.get("bots_limited"):
                    extras.append(f"limit: {session['bots_limited']}")
                if session.get("bots_removed"):
                    extras.append(f"removed: {session['bots_removed']}")
                if extras:
                    status_text += f"   • Статусы: {', '.join(extras)}\n"
        
        status_text += f"""

🕐 <b>Время проверки:</b> {datetime.now().strftime('%H:%M:%S')}
🔄 <b>Текущий индекс ротации:</b> {stats['current_rotation_index'] + 1}
"""

        await status_msg.edit_text(status_text, reply_markup=back_to_menu_kb())

    except Exception as e:
        await status_msg.edit_text(
            f"❌ Ошибка проверки: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.message(Command("session_status"))
async def cmd_session_status(msg: Message, vk_service: VKMultiBotService):
    """Отображение статуса Telegram-сессий (админ)"""
    if not is_admin(msg.from_user.id):
        return

    text, keyboard = await build_session_status_view(vk_service)
    await msg.answer(text, reply_markup=keyboard)


@router.callback_query(F.data == "admin_session_status")
async def on_admin_session_status(call: CallbackQuery, vk_service: VKMultiBotService):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    text, keyboard = await build_session_status_view(vk_service)
    await call.message.edit_text(text, reply_markup=keyboard)


async def _handle_session_toggle_command(msg: Message, vk_service: VKMultiBotService, enable: bool):
    if not is_admin(msg.from_user.id):
        return

    session_name = _extract_session_name(msg.text or "")
    available = list_session_names(vk_service)

    if not session_name:
        await msg.answer(
            "⚙️ Укажите имя сессии.\nДоступно:\n" + _format_session_list(available),
            reply_markup=back_to_menu_kb()
        )
        return

    if session_name not in available:
        await msg.answer(
            f"❌ Сессия <b>{session_name}</b> не найдена.\nДоступно:\n" + _format_session_list(available),
            reply_markup=back_to_menu_kb()
        )
        return

    config = get_config_service()

    if enable:
        await config.set_session_enabled(session_name, True)
        await vk_service.enable_session(session_name, reason="manual")
        await msg.answer(f"✅ Сессия <b>{session_name}</b> активирована", reply_markup=back_to_menu_kb())
    else:
        await config.set_session_enabled(session_name, False)
        await vk_service.disable_session(session_name, disconnect_clients=True, reason="manual")
        await msg.answer(f"⏸ Сессия <b>{session_name}</b> отключена", reply_markup=back_to_menu_kb())


@router.message(Command("session_enable"))
async def cmd_session_enable(msg: Message, vk_service: VKMultiBotService):
    """Включение указанной Telegram-сессии"""
    await _handle_session_toggle_command(msg, vk_service, enable=True)


@router.message(Command("session_disable"))
async def cmd_session_disable(msg: Message, vk_service: VKMultiBotService):
    """Отключение указанной Telegram-сессии"""
    await _handle_session_toggle_command(msg, vk_service, enable=False)


@router.callback_query(F.data.startswith("session_mode:"))
async def on_session_mode_change(call: CallbackQuery, vk_service: VKMultiBotService):
    """Переключение режима работы сессий"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    mode = call.data.split(":", 1)[1]
    config = get_config_service()
    try:
        await config.set_session_mode(mode)
        vk_service.set_session_mode(mode)
        await call.answer(f"✅ Режим {mode} активирован", show_alert=False)
    except ValueError as exc:
        await call.answer(f"❌ {exc}", show_alert=True)
        return

    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data.startswith("session_slot:"))
async def on_session_slot_request(call: CallbackQuery, vk_service: VKMultiBotService):
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    slot = call.data.split(":", 1)[1]
    slot_label = "Slot A" if slot == "slot_a" else "Slot B"

    session_names = list(vk_service.sessions.keys())
    slots = vk_service.get_slot_assignments()
    current = slots.get(slot)

    if not session_names:
        await call.answer("Нет доступных сессий", show_alert=True)
        return

    keyboard = session_slot_select_kb(slot, session_names, current)
    await call.message.edit_text(
        f"Выберите сессию для {slot_label}.\nТекущая: <code>{current or 'не назначен'}</code>",
        reply_markup=keyboard
    )


@router.callback_query(F.data.startswith("session_slot_assign:"))
async def on_session_slot_assign(call: CallbackQuery, vk_service: VKMultiBotService):
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Некорректный запрос", show_alert=True)
        return

    slot = parts[1]
    target = parts[2]

    config = get_config_service()
    slots = await config.get_session_slots()

    if target == "none":
        await config.set_session_slot(slot, None)
        slots[slot] = None
        await vk_service.apply_slot_assignments(slots)
        await call.answer("✅ Слот очищен")
        await _update_session_message(call.message, vk_service)
        return

    session_names = set(vk_service.sessions.keys())
    if target not in session_names:
        await call.answer("❌ Сессия недоступна", show_alert=True)
        return

    other_slot = "slot_b" if slot == "slot_a" else "slot_a"
    if slots.get(other_slot) == target:
        await config.set_session_slot(other_slot, None)
        slots[other_slot] = None

    await config.set_session_slot(slot, target)
    slots[slot] = target

    await vk_service.apply_slot_assignments(slots)
    await call.answer(f"✅ {target} назначена на {slot}")
    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data.startswith("session_bots:"))
async def on_session_bots_menu(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_name = call.data.split(":", 1)[1]
    await call.answer()
    await _render_session_bot_menu(call.message, vk_service, session_name)


@router.callback_query(F.data.startswith("session_bot_toggle:"))
async def on_session_bot_toggle(call: CallbackQuery, vk_service: VKMultiBotService):
    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Некорректный запрос", show_alert=True)
        return

    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_alias, bot_index_raw = parts[1], parts[2]
    session_name = vk_service.resolve_session_alias(session_alias)
    if not session_name or session_name not in vk_service.sessions:
        await call.answer("❌ Сессия недоступна", show_alert=True)
        return

    available_bots = vk_service.get_available_bot_usernames()
    try:
        bot_index = int(bot_index_raw)
    except ValueError:
        await call.answer("Некорректный бот", show_alert=True)
        return
    if bot_index < 0 or bot_index >= len(available_bots):
        await call.answer("❌ Бот не найден", show_alert=True)
        return
    bot_name = available_bots[bot_index]
    available_set = set(available_bots)

    config = get_config_service()
    current = await config.get_session_bots(session_name)
    selected = {bot for bot in current if bot in available_set}
    if bot_name in selected:
        selected.remove(bot_name)
    else:
        selected.add(bot_name)
    new_list = [bot for bot in available_bots if bot in selected]

    await call.answer("⏳ Обновляю...", show_alert=False)
    try:
        await config.set_session_bots(session_name, new_list)
        await vk_service.update_session_bots(session_name, new_list)
    except Exception as exc:
        logger.exception("Не удалось обновить ботов для %s: %s", session_name, exc)
        await call.message.answer(f"❌ Ошибка обновления списка ботов: {exc}")
        return

    await _render_session_bot_menu(call.message, vk_service, session_name)


@router.callback_query(F.data.startswith("session_bot_reset:"))
async def on_session_bot_reset(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_alias = call.data.split(":", 1)[1]
    session_name = vk_service.resolve_session_alias(session_alias)
    if not session_name or session_name not in vk_service.sessions:
        await call.answer("❌ Сессия недоступна", show_alert=True)
        return

    config = get_config_service()
    await call.answer("⏳ Возвращаю всех ботов...", show_alert=False)
    try:
        await config.set_session_bots(session_name, [])
        await vk_service.update_session_bots(session_name, [])
    except Exception as exc:
        logger.exception("Не удалось сбросить ботов для %s: %s", session_name, exc)
        await call.message.answer(f"❌ Ошибка сброса списка ботов: {exc}")
        return

    await _render_session_bot_menu(call.message, vk_service, session_name)


@router.callback_query(F.data == "session_clear_all")
async def on_session_clear_all(call: CallbackQuery, vk_service: VKMultiBotService):
    """Очищает слоты A/B и реестр сессий в ConfigService."""
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await call.answer("🧹 Очищаю...", show_alert=False)
    config = get_config_service()

    try:
        # Снимаем слоты
        for slot in ("slot_a", "slot_b"):
            await config.set_session_slot(slot, None)

        # Отключаем и удаляем все зарегистрированные сессии
        registry = await config.list_registered_sessions()
        for entry in registry:
            name = entry.get("name")
            if not name:
                continue
            await config.set_session_enabled(name, False)
            await config.set_session_bots(name, [])
            await config.remove_session_definition(name)

        # Полная очистка реестра/ботов
        await config.clear_session_registry()
        await config.clear_all_session_bots()

        # Отключаем все сессии на уровне сервиса, очищаем слоты
        for name in list(vk_service.sessions.keys()):
            try:
                await vk_service.disable_session(name, disconnect_clients=True, reason="admin_clear")
            except Exception:
                pass
        await vk_service.apply_slot_assignments({"slot_a": None, "slot_b": None})

        await call.message.answer(
            "✅ Реестр сессий и слоты очищены. Авторизуйте новые сессии через /session_auth.",
            reply_markup=back_to_menu_kb()
        )
    except Exception as exc:
        logger.exception("Не удалось очистить реестр сессий: %s", exc)
        await call.message.answer(f"❌ Ошибка при очистке: {exc}", reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "session_bots_back")
async def on_session_bots_back(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await call.answer()
    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data == "vk_pool")
async def on_vk_pool_menu(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await call.answer()
    await _render_vk_pool(call.message, vk_service)


@router.callback_query(F.data == "vkpool_refresh")
async def on_vk_pool_refresh(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await call.answer("🔄", show_alert=False)
    await _render_vk_pool(call.message, vk_service)


@router.callback_query(F.data.startswith("vkpool_del:"))
async def on_vk_pool_delete(call: CallbackQuery, vk_service: VKMultiBotService):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    username = call.data.split(":", 1)[1]
    ok = await vk_service.remove_vk_bot(username)
    if not ok:
        await call.answer("❌ Нельзя удалить: бот не найден или это последний", show_alert=True)
    else:
        await call.answer(f"🗑 @{username} удалён", show_alert=False)
        if vk_service.config_service:
            try:
                await vk_service.config_service.remove_vk_bot_quota(username)
            except Exception:
                pass
    await _render_vk_pool(call.message, vk_service)


@router.callback_query(F.data == "vkpool_add")
async def on_vk_pool_add_prompt(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    session = await get_user_session(call.from_user.id) or {}
    session["vk_pool_add"] = True
    await save_user_session(call.from_user.id, session)
    await call.message.answer("Введите username VK бота (без @), чтобы добавить его в пул:", reply_markup=back_to_menu_kb())


@router.message()
async def on_vk_pool_add_text(msg: Message, vk_service: VKMultiBotService):
    if not is_admin(msg.from_user.id):
        return
    session = await get_user_session(msg.from_user.id) or {}
    if not session.get("vk_pool_add"):
        return

    text = (msg.text or "").strip()
    parts = text.split()
    username = parts[0].lstrip("@") if parts else ""
    quota = None
    if len(parts) > 1:
        try:
            quota = int(parts[1])
        except Exception:
            quota = None
    session.pop("vk_pool_add", None)
    await save_user_session(msg.from_user.id, session)

    if not username:
        await msg.answer("❌ Имя бота не может быть пустым. Повторите ввод.", reply_markup=back_to_menu_kb())
        return

    ok = await vk_service.add_vk_bot(username)
    if not ok:
        await msg.answer("❌ Не удалось добавить бота. Проверьте имя.", reply_markup=back_to_menu_kb())
        return

    if quota and vk_service.config_service:
        try:
            await vk_service.config_service.set_vk_bot_quota(username, quota)
            await msg.answer(f"✅ Бот @{username} добавлен с квотой {quota}.", reply_markup=back_to_menu_kb())
        except Exception as exc:
            await msg.answer(f"⚠️ Бот добавлен, но квоту задать не удалось: {exc}", reply_markup=back_to_menu_kb())
            return

    await msg.answer(f"✅ Бот @{username} добавлен в пул. Обновите панель сессий.", reply_markup=back_to_menu_kb())


@router.callback_query(F.data.startswith("session_toggle:"))
async def on_session_toggle(call: CallbackQuery, vk_service: VKMultiBotService):
    """Включение/отключение сессии из меню"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_name = call.data.split(":", 1)[1]
    session_state = vk_service.sessions.get(session_name)

    if not session_state:
        await call.answer("❌ Сессия не найдена", show_alert=True)
        return

    enable = not session_state.is_enabled
    config = get_config_service()

    if enable:
        await config.set_session_enabled(session_name, True)
        await vk_service.enable_session(session_name, reason="manual")
        await call.answer(f"✅ {session_name} включена")
    else:
        await config.set_session_enabled(session_name, False)
        await vk_service.disable_session(session_name, disconnect_clients=True, reason="manual")
        await call.answer(f"⏸ {session_name} отключена")

    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data.startswith("session_delete:"))
async def on_session_delete_request(call: CallbackQuery, vk_service: VKMultiBotService):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_name = call.data.split(":", 1)[1]
    session_state = vk_service.sessions.get(session_name)
    config_session = next((cfg for cfg in TELEGRAM_SESSIONS if cfg.name == session_name), None)
    if not session_state:
        await call.answer("❌ Сессия не найдена", show_alert=True)
        return
    if config_session:
        await call.answer("❌ Системные сессии нельзя удалить", show_alert=True)
        return

    warning_text = (
        f"⚠️ <b>Удаление сессии {session_name}</b>\n\n"
        "Будут удалены все файлы Telegram-сессии (.session, .session_string) и настройки в Redis.\n"
        "Продолжить?"
    )
    await call.message.edit_text(warning_text, reply_markup=session_delete_confirm_kb(session_name))


@router.callback_query(F.data.startswith("session_delete_confirm:"))
async def on_session_delete_confirm(call: CallbackQuery, vk_service: VKMultiBotService, bot: Bot):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_name = call.data.split(":", 1)[1]
    config = get_config_service()

    # Снимаем сессии со слотов
    try:
        slots = await config.get_session_slots()
        for slot, assigned in slots.items():
            if assigned == session_name:
                await config.set_session_slot(slot, None)
                vk_service.session_slots[slot] = None
    except Exception as exc:
        logger.warning("Не удалось обновить слоты при удалении %s: %s", session_name, exc)

    # Отключаем в конфиге и сервисе
    await config.set_session_enabled(session_name, False)
    await vk_service.disable_session(session_name, disconnect_clients=True, reason="deleted")

    # Удаляем описание динамической сессии
    try:
        await config.remove_session_definition(session_name)
    except Exception:
        pass

    # Очищаем структуру VKMultiBotService
    vk_service.sessions.pop(session_name, None)
    vk_service.session_order = [name for name in vk_service.session_order if name != session_name]
    vk_service.bots = [bot for bot in vk_service.bots if bot.session_name != session_name]
    vk_service.clear_session_alias(session_name)
    if vk_service.primary_session_name == session_name:
        vk_service.primary_session_name = vk_service.session_order[0] if vk_service.session_order else None

    # Удаляем файлы
    session_dir = Path(SESSION_DIR) / session_name
    if session_dir.exists():
        shutil.rmtree(session_dir, ignore_errors=True)

    await call.answer(f"✅ Сессия {session_name} удалена", show_alert=True)
    user = call.from_user
    actor = user.full_name or user.username or str(user.id)
    await notify_admins(
        bot,
        f"🗑 <b>{actor}</b> удалил Telegram-сессию <code>{session_name}</code>.",
    )
    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data.startswith("session_archive:"))
async def on_session_archive(call: CallbackQuery, vk_service: VKMultiBotService, bot: Bot):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    session_name = call.data.split(":", 1)[1]
    config_session = next((cfg for cfg in TELEGRAM_SESSIONS if cfg.name == session_name), None)
    if not config_session:
        await call.answer("ℹ️ Архив доступен только для системных сессий", show_alert=True)
        return

    session_state = vk_service.sessions.get(session_name)
    if not session_state:
        await call.answer("❌ Сессия не найдена", show_alert=True)
        await _update_session_message(call.message, vk_service)
        return

    config = get_config_service()
    slots = await config.get_session_slots()
    for slot, assigned in slots.items():
        if assigned == session_name:
            await config.set_session_slot(slot, None)
            vk_service.session_slots[slot] = None

    await config.set_session_enabled(session_name, False)
    await vk_service.disable_session(session_name, disconnect_clients=True, reason="archived")
    await call.answer(f"📦 {session_name} переведена в резерв", show_alert=True)
    user = call.from_user
    actor = user.full_name or user.username or str(user.id)
    await notify_admins(
        bot,
        f"📦 <b>{actor}</b> перевёл системную сессию <code>{session_name}</code> в резерв.",
    )
    await _update_session_message(call.message, vk_service)


@router.callback_query(F.data == "admin_cleanup")
async def on_admin_cleanup(call: CallbackQuery):
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    text = (
        "🧹 <b>Очистка временных файлов</b>\n\n"
        "Будут обнулены основные логи, удалены временные выгрузки в data/temp и старые exports/debug.\n"
        "Процедура может занять до минуты."
    )
    keyboard = confirm_action_kb("admin_cleanup_run")
    await call.message.edit_text(text, reply_markup=keyboard)


@router.callback_query(F.data == "admin_cleanup_run")
async def on_admin_cleanup_run(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    script_path = ROOT_DIR / "scripts" / "weekly_cleanup.sh"
    if not script_path.exists():
        await call.message.edit_text(
            "❌ Скрипт очистки не найден. Проверьте наличие scripts/weekly_cleanup.sh",
            reply_markup=main_menu_kb(call.from_user.id, ADMIN_IDS)
        )
        return

    await call.answer("🧹 Очищаю...", show_alert=False)
    await call.message.edit_text("🧹 Выполняю очистку, подождите...")

    try:
        process = await asyncio.create_subprocess_exec(
            "/bin/bash",
            str(script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
    except Exception as exc:
        logger.error("Ошибка запуска cleanup-скрипта: %s", exc)
        await call.message.edit_text(
            f"❌ Не удалось запустить очистку: {exc}",
            reply_markup=main_menu_kb(call.from_user.id, ADMIN_IDS)
        )
        return

    if process.returncode == 0:
        output = stdout.decode().strip()
        snippet = "\n".join(output.splitlines()[-5:]) if output else "Операции выполнены."
        result_text = f"✅ Очистка завершена успешно.\n\n<code>{snippet}</code>"
        status_short = "завершена успешно"
    else:
        error_text = stderr.decode().strip() or "см. логи"
        result_text = (
            f"❌ Очистка завершилась с ошибкой (код {process.returncode}).\n\n<code>{error_text}</code>"
        )
        status_short = f"завершилась с ошибкой (код {process.returncode})"

    await call.message.edit_text(
        result_text,
        reply_markup=main_menu_kb(call.from_user.id, ADMIN_IDS)
    )
    actor = call.from_user.full_name or call.from_user.username or str(call.from_user.id)
    await notify_admins(
        bot,
        f"🧹 <b>{actor}</b> запустил очистку: {status_short}.",
    )


@router.callback_query(F.data == "session_refresh")
async def on_session_refresh(call: CallbackQuery, vk_service: VKMultiBotService):
    """Обновление панели сессий"""
    if not is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    await call.answer("🔄", show_alert=False)
    await _update_session_message(call.message, vk_service)


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


@router.message(Command("settings"))
async def cmd_settings(msg: Message):
    """Управление настройками бота (только для админов)"""
    if not is_admin(msg.from_user.id):
        await msg.answer("⛔ У вас нет прав для этой операции")
        return

    try:
        config = get_config_service()
        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await msg.answer(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )
    except Exception as e:
        logger.error(f"Ошибка получения настроек: {e}")
        await msg.answer(f"❌ Ошибка: {str(e)}", reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "toggle_use_cache")
async def on_toggle_use_cache(call: CallbackQuery):
    """Переключение USE_CACHE"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    try:
        config = get_config_service()
        new_value = await config.toggle_use_cache()

        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await call.message.edit_text(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )

        status = "включен" if new_value else "выключен"
        await call.answer(f"✅ Кеш для всех {status}", show_alert=True)
        logger.info(f"👑 Администратор {call.from_user.id} изменил USE_CACHE на {new_value}")

    except Exception as e:
        logger.error(f"Ошибка переключения USE_CACHE: {e}")
        await call.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@router.callback_query(F.data == "toggle_admin_use_cache")
async def on_toggle_admin_use_cache(call: CallbackQuery):
    """Переключение ADMIN_USE_CACHE"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    try:
        config = get_config_service()
        new_value = await config.toggle_admin_use_cache()

        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await call.message.edit_text(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )

        status = "включен" if new_value else "выключен"
        await call.answer(f"✅ Кеш для админов {status}", show_alert=True)
        logger.info(f"👑 Администратор {call.from_user.id} изменил ADMIN_USE_CACHE на {new_value}")

    except Exception as e:
        logger.error(f"Ошибка переключения ADMIN_USE_CACHE: {e}")
        await call.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@router.callback_query(F.data == "toggle_duplicate_removal")
async def on_toggle_duplicate_removal(call: CallbackQuery):
    """Переключение ENABLE_DUPLICATE_REMOVAL"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    try:
        config = get_config_service()
        new_value = await config.toggle_enable_duplicate_removal()

        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await call.message.edit_text(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )

        status = "включена" if new_value else "выключена"
        await call.answer(f"✅ Проверка дубликатов {status}", show_alert=True)
        logger.info(f"👑 Администратор {call.from_user.id} изменил ENABLE_DUPLICATE_REMOVAL на {new_value}")

    except Exception as e:
        logger.error(f"Ошибка переключения ENABLE_DUPLICATE_REMOVAL: {e}")
        await call.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@router.callback_query(F.data == "refresh_settings")
async def on_refresh_settings(call: CallbackQuery):
    """Обновление отображения настроек"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    try:
        config = get_config_service()
        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await call.message.edit_text(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )
        await call.answer("✅ Настройки обновлены")

    except Exception as e:
        logger.error(f"Ошибка обновления настроек: {e}")
        await call.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@router.callback_query(F.data == "admin_settings")
async def on_admin_settings(call: CallbackQuery):
    """Открытие меню настроек из главного меню"""
    await call.answer()

    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    try:
        config = get_config_service()
        settings = await config.get_all_settings()
        settings_text = await config.get_settings_display()

        await call.message.edit_text(
            settings_text,
            reply_markup=settings_kb(
                settings['use_cache'],
                settings['admin_use_cache'],
                settings['enable_duplicate_removal']
            )
        )

    except Exception as e:
        logger.error(f"Ошибка получения настроек: {e}")
        await call.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@router.callback_query(F.data == "admin_maintenance")
async def on_admin_maintenance(call: CallbackQuery):
    """Меню обслуживания: очистка, перезапуск, импорт"""
    await call.answer()
    if not is_admin(call.from_user.id):
        await call.answer("⛔ У вас нет прав", show_alert=True)
        return
    text = (
        "🛠 <b>Обслуживание</b>\n\n"
        "Здесь доступны операции очистки, импорта и перезапуска бота. "
        "Изменения применяются сразу, без перезапуска вручную."
    )
    await call.message.edit_text(text, reply_markup=admin_maintenance_kb())
