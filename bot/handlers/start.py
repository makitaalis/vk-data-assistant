"""Обработчики команд /start, /help и связанных callback"""

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery

from bot.config import ADMIN_IDS
from bot.utils.messages import MESSAGES
from bot.keyboards.inline import (
    disclaimer_kb,
    main_menu_kb,
    back_to_menu_kb
)
from bot.utils.session_manager import (
    check_user_accepted_disclaimer,
    set_user_accepted_disclaimer
)
from db_module import VKDatabase

router = Router()


@router.message(CommandStart())
async def cmd_start(msg: Message, db: VKDatabase):
    """Обработчик команды /start"""
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    await msg.answer(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))


@router.message(Command("help"))
async def cmd_help(msg: Message):
    """Обработчик команды /help"""
    await msg.answer(MESSAGES["help"], reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "accept_disclaimer")
async def on_accept_disclaimer(call: CallbackQuery, db: VKDatabase):
    """Обработчик принятия условий использования"""
    await call.answer("✅ Условия приняты")
    user_id = call.from_user.id

    # Сохраняем информацию о пользователе
    user_data = {
        "username": call.from_user.username,
        "first_name": call.from_user.first_name,
        "last_name": call.from_user.last_name
    }

    await set_user_accepted_disclaimer(user_id, user_data, db)
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))


@router.callback_query(F.data == "reject_disclaimer")
async def on_reject_disclaimer(call: CallbackQuery):
    """Обработчик отказа от условий использования"""
    await call.answer()
    await call.message.edit_text(
        "❌ Вы отказались от условий использования.\n\n"
        "К сожалению, без согласия с условиями вы не можете использовать бота.\n\n"
        "Если передумаете - используйте команду /start"
    )


@router.callback_query(F.data == "main_menu")
async def on_main_menu(call: CallbackQuery):
    """Обработчик возврата в главное меню"""
    await call.answer()
    user_id = call.from_user.id
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id, ADMIN_IDS))


@router.callback_query(F.data == "help")
async def on_help(call: CallbackQuery):
    """Обработчик кнопки помощи"""
    await call.answer()
    await call.message.edit_text(MESSAGES["help"], reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "upload_file")
async def on_upload_file(call: CallbackQuery):
    """Обработчик кнопки загрузки файла"""
    await call.answer()
    await call.message.edit_text(MESSAGES["upload_file"], reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "send_links")
async def on_send_links(call: CallbackQuery):
    """Обработчик кнопки отправки ссылок"""
    await call.answer()
    await call.message.edit_text(MESSAGES["send_links"], reply_markup=back_to_menu_kb())