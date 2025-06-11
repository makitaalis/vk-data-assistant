"""Главный файл для запуска бота"""

import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeChat

from bot.config import (
    BOT_TOKEN,
    VK_BOT_USERNAME,
    API_ID,
    API_HASH,
    SESSION_NAME,
    ACCOUNT_PHONE,
    ADMIN_IDS,
    DATA_DIR,
    DEBUG_DIR
)
from bot.utils.session_manager import init_redis, close_redis
from bot.middleware.auth import AuthMiddleware
from db_module import VKDatabase
from services.vk_service import VKService

# Импорт роутеров
from bot.handlers import start, search, files, admin, callbacks, stats

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bot_main")


async def setup_bot_commands(bot: Bot):
    """Настройка команд бота"""
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="📚 Руководство пользователя"),
        BotCommand(command="status", description="📊 Текущий прогресс"),
        BotCommand(command="export", description="📥 Получить результаты"),
        BotCommand(command="stats", description="📈 Моя статистика"),
        BotCommand(command="findphone", description="🔍 Поиск по телефону"),
        BotCommand(command="cancel", description="🚫 Отменить обработку"),
    ]

    # Добавляем команды для админов
    admin_commands = commands + [
        BotCommand(command="botstatus", description="🤖 Статус VK бота"),
        BotCommand(command="debug", description="🐛 Отладочная информация"),
        BotCommand(command="dbstats", description="📊 Статистика БД"),
        BotCommand(command="broadcast", description="📢 Рассылка"),
        BotCommand(command="top", description="🏆 Топ пользователей")
    ]

    # Устанавливаем команды для обычных пользователей
    await bot.set_my_commands(commands)

    # Устанавливаем расширенные команды для админов
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(
                admin_commands,
                scope=BotCommandScopeChat(chat_id=admin_id)
            )
        except:
            pass

    logger.info("✅ Команды бота настроены")


def init_project_structure():
    """Создает необходимую структуру папок"""
    DATA_DIR.mkdir(exist_ok=True)
    DEBUG_DIR.mkdir(exist_ok=True)
    (DATA_DIR / '.gitkeep').touch(exist_ok=True)
    (DEBUG_DIR / '.gitkeep').touch(exist_ok=True)
    logger.info("✅ Структура проекта инициализирована")


async def notify_admins(bot: Bot, message: str):
    """Отправка уведомлений администраторам"""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 <b>Системное уведомление</b>\n\n{message}"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")


async def main():
    """Основная функция запуска"""
    # Инициализация структуры проекта
    init_project_structure()

    # Инициализация базы данных
    logger.info("🔄 Инициализация PostgreSQL...")
    db = VKDatabase()
    await db.init()

    # Инициализация Redis
    await init_redis()

    # Инициализация VK сервиса
    logger.info(f"🔄 Инициализация VK сервиса с ботом @{VK_BOT_USERNAME}...")
    vk_service = VKService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    await vk_service.initialize()

    # Проверка баланса
    balance = await vk_service.check_balance()
    if balance:
        logger.info(f"💰 Баланс VK бота: {balance} поисков")
    else:
        logger.warning("⚠️ Не удалось получить баланс бота")

    # Создаем бота
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # Создаем диспетчер
    dp = Dispatcher()

    # Подключаем middleware с зависимостями
    auth_middleware = AuthMiddleware(db, vk_service)
    dp.message.middleware(auth_middleware)
    dp.callback_query.middleware(auth_middleware)

    # Регистрируем роутеры
    dp.include_router(start.router)
    dp.include_router(search.router)
    dp.include_router(files.router)
    dp.include_router(admin.router)
    dp.include_router(callbacks.router)
    dp.include_router(stats.router)

    # Настройка команд бота
    await setup_bot_commands(bot)

    # Уведомление админов о запуске
    startup_message = f"✅ Бот запущен и готов к работе!\n\n"
    startup_message += f"🤖 Используется VK бот: @{VK_BOT_USERNAME}"
    if balance:
        startup_message += f"\n💰 Доступно поисков: {balance}"

    await notify_admins(bot, startup_message)

    logger.info("✅ Бот успешно запущен")

    try:
        # Запускаем поллинг
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        # Закрываем соединения
        await vk_service.close()
        await close_redis()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")