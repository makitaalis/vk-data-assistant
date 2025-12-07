"""Обработчики для функционала проверки баланса"""

import asyncio
import logging
from typing import Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.config import ADMIN_IDS, VK_BOT_USERNAME, BALANCE_LIMIT_CHECK_ENABLED
from bot.keyboards.inline import main_menu_kb, processing_menu_kb, insufficient_balance_kb
from bot.utils.session_manager import get_user_session
from bot.utils.helpers import create_progress_bar, format_time
from bot.utils.messages import MESSAGES

router = Router()
logger = logging.getLogger("balance_handler")

# Глобальный флаг для приостановки обработки
processing_paused = False
balance_check_lock = asyncio.Lock()


@router.message(Command("balance"))
async def cmd_balance(msg: Message, vk_service):
    """Обработчик команды /balance - получает актуальный баланс через меню бота"""
    
    # Отправляем сообщение о проверке
    primary_bot = (VK_BOT_USERNAME or "").lstrip("@") or "sherlock_bot_ne_bot"
    status_msg = await msg.answer(
        f"🔄 Подключаюсь к @{primary_bot} для проверки баланса..."
    )
    
    try:
        # Получаем баланс через новый метод (через меню бота)
        balance_overview = await vk_service.get_balance_overview()

        from bot.keyboards.inline import back_to_menu_kb
        await status_msg.edit_text(
            balance_overview,
            reply_markup=back_to_menu_kb()
        )
            
    except Exception as e:
        logger.error(f"Ошибка при проверке баланса: {e}")
        from bot.keyboards.inline import back_to_menu_kb
        await status_msg.edit_text(
            "❌ Ошибка при проверке баланса.\n"
            f"Детали: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.callback_query(F.data == "check_balance")
async def on_check_balance(call: CallbackQuery, vk_service):
    """Обработчик кнопки Баланс в главном меню - получает актуальный баланс через меню бота"""
    await call.answer("🔄 Получаю актуальный баланс...")

    try:
        primary_bot = (VK_BOT_USERNAME or "").lstrip("@") or "sherlock_bot_ne_bot"
        await call.message.edit_text(
            f"🔄 Подключаюсь к @{primary_bot} для проверки баланса..."
        )

        balance_overview = await vk_service.get_balance_overview()

        from bot.keyboards.inline import back_to_menu_kb
        await call.message.edit_text(
            balance_overview,
            reply_markup=back_to_menu_kb()
        )

    except Exception as e:
        logger.error(f"Ошибка при проверке баланса: {e}")
        from bot.keyboards.inline import back_to_menu_kb
        await call.message.edit_text(
            "❌ Ошибка при проверке баланса.\n"
            f"Детали: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.callback_query(F.data == "check_balance_processing")
async def on_check_balance_during_processing(call: CallbackQuery, vk_service):
    """Обработчик кнопки Баланс во время обработки"""
    global processing_paused

    await call.answer("🔄 Приостанавливаю обработку для проверки баланса...")

    async with balance_check_lock:
        try:
            # Приостанавливаем все обработки
            processing_paused = True
            logger.info("⏸ Все обработки приостановлены для проверки баланса")
            user_id = call.from_user.id
            session = await get_user_session(user_id) or {}
            session["balance_pause"] = True
            await save_user_session(user_id, session)

            # Ждем завершения текущих операций
            await asyncio.sleep(1.5)

            # Проверяем баланс (возвращает число)
            balance = await vk_service.check_balance()

            if balance is not None:
                # Формируем сообщение
                if balance < 100:
                    balance_text = f"💰 Доступно поисков: {balance} ⚠️ (осталось мало)"
                else:
                    balance_text = f"💰 Доступно поисков: {balance}"

                # Отправляем новое сообщение с балансом
                await call.message.answer(balance_text)

                # Обновляем сообщение с прогрессом
                user_id = call.from_user.id
                session = await get_user_session(user_id)

                if session and session.get("links"):
                    # Восстанавливаем отображение прогресса
                    total = len(session.get("links", []))
                    results = session.get("results", {})
                    processed = len(results)

                    # Считаем статистику
                    found = sum(
                        1 for data in results.values()
                        if data.get("phones") or data.get("full_name") or data.get("birth_date")
                    )
                    not_found = processed - found
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

                    try:
                        await call.message.edit_text(status_text, reply_markup=processing_menu_kb())
                    except:
                        pass
            else:
                await call.message.answer("❌ Не удалось проверить баланс, продолжаю обработку...")

        finally:
            # Возобновляем обработку
            processing_paused = False
            logger.info("▶️ Обработки возобновлены")
            user_id = call.from_user.id
            session = await get_user_session(user_id) or {}
            if session.pop("balance_pause", None):
                await save_user_session(user_id, session)


async def check_balance_before_processing(
    message: Message,
    total_links: int,
    required_checks: int,
    vk_service,
    allow_force: bool = False
) -> bool:
    """
    Проверяет достаточно ли поисков перед началом обработки

    Returns:
        True если можно продолжать, False если недостаточно поисков
    """
    if not BALANCE_LIMIT_CHECK_ENABLED:
        return True

    try:
        status_msg = await message.answer("🔄 Проверяю доступные поиски...")

        balance = await vk_service.check_balance()

        if balance is None:
            # Не удалось проверить, разрешаем продолжить
            await status_msg.delete()
            return True

        if balance < required_checks:
            # Недостаточно поисков
            already_in_cache = max(total_links - required_checks, 0)
            message_text = (
                f"❌ Недостаточно поисков!\n\n"
                f"Всего ссылок: {total_links}\n"
                f"Уже в базе: {already_in_cache}\n"
                f"Новых проверок требуется: {required_checks}\n"
                f"Доступно: {balance} поисков"
            )

            if allow_force:
                message_text += "\n\nВыберите действие:"
                markup = insufficient_balance_kb()
            else:
                message_text += "\n\nПополните баланс для обработки этого файла."
                markup = main_menu_kb(message.from_user.id, ADMIN_IDS)

            await status_msg.edit_text(
                message_text,
                reply_markup=markup
            )
            return False

        # Достаточно поисков
        await status_msg.delete()

        # Показываем предупреждение если мало поисков
        if balance < 100:
            await message.answer(f"⚠️ Внимание: осталось только {balance} поисков!")

        return True

    except Exception as e:
        logger.error(f"Ошибка при проверке баланса перед обработкой: {e}")
        # При ошибке разрешаем продолжить
        return True


def is_processing_paused() -> bool:
    """Проверяет, приостановлена ли обработка для проверки баланса"""
    return processing_paused
