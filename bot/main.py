"""Главный файл для запуска бота"""

import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeChat

from bot.config import (
    BOT_TOKEN,
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

# Глобальные объекты
db: VKDatabase = None
vk_service: VKService = None
bot: Bot = None


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
        BotCommand(command="botstatus", description="🤖 Статус VK ботов"),
        BotCommand(command="debug", description="🐛 Отладочная информация")
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


async def on_startup(dispatcher: Dispatcher):
    """Действия при запуске бота"""
    global db, vk_service

    # Получаем bot из диспетчера
    bot = dispatcher["bot"]

    # Инициализация структуры проекта
    init_project_structure()

    # Инициализация базы данных
    logger.info("🔄 Инициализация PostgreSQL...")
    db = VKDatabase()
    await db.init()

    # Инициализация Redis
    await init_redis()

    # Инициализация VK сервиса
    logger.info("🔄 Инициализация VK сервиса...")
    vk_service = VKService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    await vk_service.initialize()

    # Проверка баланса
    balance = await vk_service.check_balance()
    if balance:
        logger.info(f"💰 Баланс VK бота: {balance} поисков")
        if balance < 50:
            await notify_admins(bot, f"⚠️ Низкий баланс VK бота: {balance} поисков!")

    # Настройка команд бота
    await setup_bot_commands(bot)

    # Уведомление админов о запуске
    await notify_admins(bot, "✅ Бот запущен и готов к работе!")

    logger.info("✅ Бот успешно запущен")


async def on_shutdown(dispatcher: Dispatcher):
    """Действия при остановке бота"""
    global vk_service, db

    # Получаем bot из диспетчера
    bot = dispatcher["bot"]

    # Закрываем VK сервис
    if vk_service:
        await vk_service.close()

    # Закрываем соединение с Redis
    await close_redis()

    # Закрываем соединение с БД
    if db:
        await db.close()

    # Уведомляем админов
    await notify_admins(bot, "👋 Бот остановлен")

    logger.info("👋 Бот остановлен")


async def main():
    """Основная функция запуска"""
    global bot

    # Создаем бота
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # Создаем диспетчер
    dp = Dispatcher()

    # Подключаем middleware
    dp.message.middleware(AuthMiddleware())
    dp.callback_query.middleware(AuthMiddleware())

    # Регистрируем роутеры
    dp.include_router(start.router)
    dp.include_router(search.router)
    dp.include_router(files.router)
    dp.include_router(admin.router)
    dp.include_router(callbacks.router)
    dp.include_router(stats.router)

    # Делаем объекты доступными для хендлеров через контекст
    dp["db"] = db
    dp["vk_service"] = vk_service
    dp["bot"] = bot

    # Регистрируем хендлеры жизненного цикла
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запускаем поллинг
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")