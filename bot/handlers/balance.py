"""Обработчики для функционала проверки баланса"""

import asyncio
import logging
from typing import Optional

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message

from bot.config import ADMIN_IDS
from bot.keyboards.inline import main_menu_kb, processing_menu_kb
from bot.utils.session_manager import get_user_session
from bot.utils.helpers import create_progress_bar, format_time
from bot.utils.messages import MESSAGES

router = Router()
logger = logging.getLogger("balance_handler")

# Глобальный флаг для приостановки обработки
processing_paused = False
balance_check_lock = asyncio.Lock()


@router.callback_query(F.data == "check_balance")
async def on_check_balance(call: CallbackQuery, vk_service):
    """Обработчик кнопки Баланс в главном меню"""
    await call.answer("🔄 Проверяю баланс...")

    try:
        balance = await vk_service.check_balance()

        if balance is not None:
            # Формируем сообщение
            if balance < 100:
                balance_text = f"💰 Доступно поисков: {balance} ⚠️ (осталось мало)"
            else:
                balance_text = f"💰 Доступно поисков: {balance}"

            await call.message.answer(balance_text)
        else:
            await call.message.answer("❌ Не удалось проверить баланс, попробуйте позже")

    except Exception as e:
        logger.error(f"Ошибка при проверке баланса: {e}")
        await call.message.answer("❌ Ошибка при проверке баланса")


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

            # Ждем завершения текущих операций
            await asyncio.sleep(1.5)

            # Проверяем баланс
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


async def check_balance_before_processing(
    message: Message,
    links_count: int,
    vk_service
) -> bool:
    """
    Проверяет достаточно ли поисков перед началом обработки

    Returns:
        True если можно продолжать, False если недостаточно поисков
    """
    try:
        status_msg = await message.answer("🔄 Проверяю доступные поиски...")

        balance = await vk_service.check_balance()

        if balance is None:
            # Не удалось проверить, разрешаем продолжить
            await status_msg.delete()
            return True

        if balance < links_count:
            # Недостаточно поисков
            await status_msg.edit_text(
                f"❌ Недостаточно поисков!\n\n"
                f"В файле: {links_count} ссылок\n"
                f"Доступно: {balance} поисков\n\n"
                f"Пополните баланс для обработки этого файла.",
                reply_markup=main_menu_kb(message.from_user.id, ADMIN_IDS)
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