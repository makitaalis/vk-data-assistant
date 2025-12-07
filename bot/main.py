"""Главный файл для запуска бота"""

import asyncio
import logging
import time
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeChat

from bot.config import (
    BOT_TOKEN,
    VK_BOT_USERNAME,
    VK_BOT_USERNAMES,
    API_ID,
    API_HASH,
    SESSION_NAME,
    ACCOUNT_PHONE,
    ADMIN_IDS,
    DATA_DIR,
    DEBUG_DIR,
    USE_CACHE,
    ADMIN_USE_CACHE,
    ENABLE_DUPLICATE_REMOVAL,
    SESSION_STORAGE_MODE,
    SESSION_DIR,
    TELEGRAM_SESSIONS,
    SESSION_MODE,
    TelegramSessionConfig,
    DB_TASK_QUEUE_ENABLED,
    DB_TASK_QUEUE_BATCH,
    DB_TASK_QUEUE_STALE_MINUTES,
)
from bot.utils.session_manager import init_redis, close_redis, get_redis
from bot.utils.admin_notifications import notify_admins, send_daily_summary
from bot.middleware.auth import AuthMiddleware
from db_module import VKDatabase
from services.vk_multibot_service import VKMultiBotService
from services.session_auth_service import SessionAuthManager
from services.config_service import initialize_config_service
from services.search_stats_service import SearchStatsManager
from services.task_queue_service import TaskQueueService

# Импорт роутеров
from bot.handlers import start, search, files, admin, callbacks, stats, balance, session_auth

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
        BotCommand(command="balance", description="💰 Проверить баланс"),
        BotCommand(command="status", description="📊 Текущий прогресс"),
        BotCommand(command="export", description="📥 Получить результаты"),
        BotCommand(command="stats", description="📈 Моя статистика"),
        BotCommand(command="findphone", description="🔍 Поиск по телефону"),
    ]

    # Добавляем команды для админов
    admin_commands = commands + [
        BotCommand(command="botstatus", description="🤖 Статус VK бота"),
        BotCommand(command="settings", description="⚙️ Настройки бота"),
        BotCommand(command="debug", description="🐛 Отладочная информация"),
        BotCommand(command="dbstats", description="📊 Статистика БД"),
        BotCommand(command="broadcast", description="📢 Рассылка"),
        BotCommand(command="top", description="🏆 Топ пользователей"),
        BotCommand(command="session_status", description="📡 Панель сессий"),
        BotCommand(command="session_auth", description="🔐 Авторизация сессии"),
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


async def main():
    """Основная функция запуска"""
    # Инициализация структуры проекта
    init_project_structure()
    stats_dir = DATA_DIR / "stats"
    stats_dir.mkdir(parents=True, exist_ok=True)
    search_stats_manager = SearchStatsManager(stats_dir / "search_stats.json")

    # Инициализация базы данных
    logger.info("🔄 Инициализация PostgreSQL...")
    db = VKDatabase()
    await db.init()
    task_queue: Optional[TaskQueueService] = None
    if DB_TASK_QUEUE_ENABLED:
        task_queue = TaskQueueService(db, batch_size=DB_TASK_QUEUE_BATCH, stale_minutes=DB_TASK_QUEUE_STALE_MINUTES)

    # Инициализация Redis
    await init_redis()

    # Инициализация ConfigService для динамических настроек
    logger.info("🔄 Инициализация ConfigService...")
    redis_client = get_redis()
    config_service = await initialize_config_service(redis_client, USE_CACHE, ADMIN_USE_CACHE, ENABLE_DUPLICATE_REMOVAL)
    await config_service.set_use_cache(USE_CACHE)
    await config_service.set_admin_use_cache(ADMIN_USE_CACHE)
    logger.info("✅ ConfigService инициализирован")

    session_mode = await config_service.get_session_mode()
    slot_assignments = await config_service.get_session_slots()
    session_bot_overrides = await config_service.get_all_session_bots()
    configured_names = [session.name for session in TELEGRAM_SESSIONS]
    if not slot_assignments.get("slot_a") and configured_names:
        await config_service.set_session_slot("slot_a", configured_names[0])
        slot_assignments["slot_a"] = configured_names[0]
    if not slot_assignments.get("slot_b"):
        for candidate in configured_names[1:]:
            if candidate != slot_assignments.get("slot_a"):
                await config_service.set_session_slot("slot_b", candidate)
                slot_assignments["slot_b"] = candidate
                break

    sessions_for_service: list[TelegramSessionConfig] = []

    def _make_session_config(name: str, phone: str, enabled: bool) -> TelegramSessionConfig:
        storage_dir = SESSION_DIR / name
        storage_dir.mkdir(parents=True, exist_ok=True)
        return TelegramSessionConfig(
            name=name,
            phone=phone,
            enabled=enabled,
            storage_dir=storage_dir,
        )

    for session in TELEGRAM_SESSIONS:
        is_enabled = await config_service.get_session_enabled(session.name, default=session.enabled)
        sessions_for_service.append(_make_session_config(session.name, session.phone, is_enabled))

    dynamic_sessions = await config_service.list_registered_sessions()
    known_names = {session.name for session in sessions_for_service}
    for entry in dynamic_sessions:
        name = entry.get("name")
        if not name or name in known_names:
            continue
        phone = entry.get("phone") or "+10000000000"
        enabled = bool(entry.get("enabled", True))
        sessions_for_service.append(_make_session_config(name, phone, enabled))
        known_names.add(name)

    # Инициализация VK мульти-бот сервиса
    logger.info(f"🔄 Инициализация VK сервиса с {len(VK_BOT_USERNAMES)} ботами...")
    vk_service = VKMultiBotService(
        API_ID,
        API_HASH,
        sessions=sessions_for_service,
        session_mode=session_mode,
        session_storage_mode=SESSION_STORAGE_MODE,
        session_dir=SESSION_DIR,
        stats_manager=search_stats_manager,
        session_bot_assignments=session_bot_overrides,
        config_service=config_service,
    )
    vk_service.config_service = config_service
    await vk_service.sync_vk_pool_from_config()
    initialized_bots = await vk_service.initialize_with_session_auth()
    await vk_service.apply_slot_assignments(slot_assignments)

    session_auth_manager = SessionAuthManager(API_ID, API_HASH, SESSION_DIR)
    
    logger.info(f"✅ Инициализировано {initialized_bots} из {len(VK_BOT_USERNAMES)} ботов")
    
    # Проверка баланса всех ботов
    balance_info = await vk_service.check_balance()
    if balance_info:
        logger.info(f"💰 Баланс ботов:\n{balance_info}")
    else:
        logger.warning("⚠️ Не удалось получить баланс ботов")

    # Создаем бота
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    async def vk_alert_handler(text: str):
        await notify_admins(bot, text)

    vk_service.set_alert_handler(vk_alert_handler)

    # Создаем диспетчер
    dp = Dispatcher()

    # Подключаем middleware с зависимостями
    auth_middleware = AuthMiddleware(db, vk_service, session_auth_manager=session_auth_manager, task_queue=task_queue)
    dp.message.middleware(auth_middleware)
    dp.callback_query.middleware(auth_middleware)

    # Регистрируем роутеры
    dp.include_router(start.router)
    dp.include_router(session_auth.router)
    dp.include_router(search.router)
    dp.include_router(files.router)
    dp.include_router(admin.router)
    dp.include_router(callbacks.router)
    dp.include_router(stats.router)
    dp.include_router(balance.router)

    # Настройка команд бота
    await setup_bot_commands(bot)

    # Уведомление админов о запуске
    startup_message = "✅ Бот запущен и готов к работе!"

    await notify_admins(bot, startup_message)
    await send_daily_summary(bot, search_stats_manager, vk_service)

    logger.info("✅ Бот успешно запущен")

    inactivity_stop = asyncio.Event()
    async def inactivity_watchdog():
        idle_alert_sent = False
        while not inactivity_stop.is_set():
            await asyncio.sleep(60)
            if not vk_service.has_pending_requests():
                idle_alert_sent = False
                continue
            idle = time.time() - vk_service.get_last_activity_ts()
            if idle >= 300 and not idle_alert_sent:
                idle_alert_sent = True
                await notify_admins(
                    bot,
                    f"⚠️ Нет обработанных ссылок уже {int(idle)}с. Проверьте состояние VK ботов."
                )
            elif idle < 300:
                idle_alert_sent = False

    inactivity_task = asyncio.create_task(inactivity_watchdog())
    queue_stop = asyncio.Event()

    async def queue_worker_loop():
        if not task_queue:
            return
        logger.info("🧵 Запуск фонового воркера очереди задач")
        last_stats_alert = 0.0
        while not queue_stop.is_set():
            try:
                tasks = await task_queue.fetch_batch()
                if not tasks:
                    await asyncio.sleep(1.0)
                    # Периодический опрос статуса очереди
                    now = asyncio.get_running_loop().time()
                    if now - last_stats_alert >= 60:
                        last_stats_alert = now
                        stats = await task_queue.stats()
                        pending = stats.get("pending", 0)
                        failed = stats.get("failed", 0)
                        cancelled = stats.get("cancelled", 0)
                        if pending > 100 or failed > 0:
                            details = f"pending={pending}, failed={failed}, cancelled={cancelled}"
                            failed_top = []
                            if failed > 0:
                                try:
                                    summary = await task_queue.failed_summary(limit=3, window_hours=6)
                                    if summary:
                                        failed_top = [
                                            f"{row.get('error')}: {row.get('cnt')}"
                                            for row in summary
                                        ]
                                except Exception as exc:
                                    logger.debug("Не удалось собрать сводку ошибок очереди: %s", exc)
                            if failed_top:
                                details = details + " | " + "; ".join(failed_top)
                            await notify_admins(
                                bot,
                                f"⚠️ Очередь задач: {details}."
                            )
                    continue
                for task in tasks:
                    task_id = task["id"]
                    link = task["link"]
                    user_id = task["user_id"]
                    try:
                        # Проверяем, не отменены ли задачи пользователя
                        if task_queue.is_user_cancelled(user_id):
                            await task_queue.fail(task_id, "cancelled")
                            continue
                        user_stats = await task_queue.user_stats(user_id)
                        if user_stats.get("cancelled", 0) > 0 and user_stats.get("pending", 0) == 0:
                            await task_queue.fail(task_id, "cancelled")
                            continue
                        result = await vk_service.search_vk_data(link, preferred_session=task.get("session_name"))
                        if result.get("error") == "no_available_bots":
                            await task_queue.fail(task_id, "no_available_bots")
                            continue
                        await task_queue.complete(task_id, result)
                    except Exception as exc:
                        await task_queue.fail(task_id, str(exc))
            except Exception as exc:
                logger.exception("Ошибка воркера очереди: %s", exc)
                await asyncio.sleep(2.0)

    queue_task = asyncio.create_task(queue_worker_loop())

    try:
        # Запускаем поллинг
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        inactivity_stop.set()
        inactivity_task.cancel()
        queue_stop.set()
        if 'queue_task' in locals():
            queue_task.cancel()
        try:
            await inactivity_task
        except Exception:
            pass
        if 'queue_task' in locals():
            try:
                await queue_task
            except Exception:
                pass
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
