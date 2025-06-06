"""Middleware для проверки авторизации и условий использования"""

from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery, TelegramObject

from bot.utils.session_manager import check_user_accepted_disclaimer
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import disclaimer_kb


class AuthMiddleware(BaseMiddleware):
    """Middleware для проверки, принял ли пользователь условия использования"""

    # Команды и callback, которые разрешены без принятия условий
    ALLOWED_COMMANDS = ["/start"]
    ALLOWED_CALLBACKS = ["accept_disclaimer", "reject_disclaimer"]

    def __init__(self, db, vk_service):
        """Инициализация с зависимостями"""
        self.db = db
        self.vk_service = vk_service

    async def __call__(
            self,
            handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
            event: TelegramObject,
            data: Dict[str, Any]
    ) -> Any:
        """Обработка события"""

        # Добавляем зависимости в data
        data["db"] = self.db
        data["vk_service"] = self.vk_service

        # Получаем user_id
        user = None
        if isinstance(event, (Message, CallbackQuery)):
            user = event.from_user

        if not user:
            return await handler(event, data)

        user_id = user.id

        # Проверяем тип события
        if isinstance(event, Message):
            # Для команд проверяем, разрешена ли она
            if event.text and event.text.startswith("/"):
                command = event.text.split()[0]
                if command in self.ALLOWED_COMMANDS:
                    return await handler(event, data)

        elif isinstance(event, CallbackQuery):
            # Для callback проверяем, разрешен ли он
            if event.data in self.ALLOWED_CALLBACKS:
                return await handler(event, data)

        # Проверяем, принял ли пользователь условия
        accepted = await check_user_accepted_disclaimer(user_id)

        # Если не принял, проверяем в БД
        if not accepted and self.db:
            accepted = await self.db.check_user_accepted_disclaimer(user_id)

        if not accepted:
            # Отправляем disclaimer
            if isinstance(event, Message):
                await event.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
            elif isinstance(event, CallbackQuery):
                await event.message.edit_text(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
                await event.answer()
            return

        # Если все хорошо, передаем управление дальше
        return await handler(event, data)