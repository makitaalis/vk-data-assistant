"""Утилиты для отправки уведомлений и файлов администраторам."""

import logging
from pathlib import Path
from typing import Iterable, Optional, Tuple
from datetime import datetime, timezone

from aiogram.types import FSInputFile

from bot.config import ADMIN_IDS

logger = logging.getLogger("admin_notifications")


async def notify_admins(bot, message: str, *, prefix: Optional[str] = "🚨 <b>Системное уведомление</b>"):
    """Отправляет текстовое уведомление всем администраторам."""
    admin_ids = ADMIN_IDS or []

    if not admin_ids:
        logger.warning("Администраторы не настроены, сообщение пропущено: %s", message)
        return

    final_text = f"{prefix}\n\n{message}" if prefix else message

    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, final_text)
        except Exception as exc:
            logger.error("Не удалось отправить уведомление админу %s: %s", admin_id, exc)


async def send_files_to_admins(bot, message: Optional[str], files: Iterable[Tuple[Path, str]]):
    """
    Отправляет сообщение и список файлов всем администраторам.

    Args:
        bot: Экземпляр бота aiogram.
        message: Текстовое сообщение для админов (может быть None).
        files: Итерируемый объект с кортежами (Path, caption).
    """
    admin_ids = ADMIN_IDS or []

    if not admin_ids:
        logger.warning("Администраторы не настроены, пропускаем отправку файлов.")
        return

    file_list = list(files)

    for admin_id in admin_ids:
        try:
            if message:
                await bot.send_message(admin_id, message)

            for file_path, caption in file_list:
                if not Path(file_path).exists():
                    logger.warning("Файл %s не найден, пропускаем его отправку админу %s", file_path, admin_id)
                    continue

                await bot.send_document(
                    admin_id,
                    FSInputFile(file_path),
                    caption=caption or None
                )
        except Exception as exc:
            logger.error("Не удалось отправить файлы админу %s: %s", admin_id, exc)


async def send_daily_summary(bot, stats_manager, vk_service=None):
    """Формирует и отправляет краткую сводку за текущий день."""
    if not stats_manager:
        return

    try:
        snapshot = await stats_manager.get_snapshot()
    except Exception as exc:
        logger.error("Не удалось получить статистику: %s", exc)
        return

    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    per_day = snapshot.get("per_day") or {}
    today_bucket = per_day.get(today_key, {})

    total_links = today_bucket.get("links_sent", 0)
    total_requests = today_bucket.get("bot_requests", 0)
    sessions_today = today_bucket.get("sessions", {})

    top_sessions = sorted(
        sessions_today.items(),
        key=lambda kv: kv[1].get("links_sent", 0),
        reverse=True,
    )[:3]

    lines = [
        f"📈 <b>Сводка за {today_key}</b>",
        f"🔗 Ссылок отправлено: {total_links}",
        f"🤖 Запросов к ботам: {total_requests}",
    ]

    if top_sessions:
        lines.append("🏅 Активные сессии:")
        for name, data in top_sessions:
            lines.append(f"• {name}: {data.get('links_sent', 0)} ссылок")

    if vk_service:
        try:
            stats = await vk_service.get_stats()
            active_sessions = sum(1 for session in stats.get("sessions", []) if session.get("enabled"))
            lines.append(f"✅ Активных сессий сейчас: {active_sessions}")
        except Exception as exc:
            logger.warning("Не удалось получить статистику VK сервиса: %s", exc)

    await notify_admins(bot, "\n".join(lines), prefix="📈 <b>Ежедневная сводка</b>")
