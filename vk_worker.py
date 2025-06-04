import asyncio
import datetime
import logging
import os
import pathlib
import re
import time
from typing import Callable, Coroutine, List, Dict, Optional, Tuple, Any, Set

from telethon import TelegramClient, events, utils
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputPeerUser, Message, PeerUser
from telethon.errors import FloodWaitError, AuthKeyError, SessionPasswordNeededError
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vk_worker")

# Список всех ботов (10 копий)
VK_BOT_POOL = [
    "sherlock_XIS_bot",  # Основной бот
    "Anon_clear_weaponvk_bot",  # Копия 2
    "vk_memosimo_2_bot",  # Копия 3
    "vk_memosimo_3_bot",  # Копия 4
    "vk_memorizeumuskringenivikusbot_5",  # Копия 5
    "vk_memorizeumuskringenivikusbot_6",  # Копия 6
    "vk_memorizeumuskringenivikusbot_7",  # Копия 7
    "vk_memorizeumuskringenivikusbot_8",  # Копия 8
    "vk_memorizeumuskringenivikusbot_9",  # Копия 9
    "vk_memorizeumuskringenivikusbot_10",  # Копия 10
]

# Константы
MAX_RETRIES = 3
RETRY_DELAY = 2.0
INITIAL_DELAY = 1.5
MESSAGE_TIMEOUT = 10.0
BOT_ERROR_COOLDOWN = 10.0  # 10 секунд отдыха при ошибке
MAX_BOT_ERRORS = 10  # Максимум ошибок перед отключением бота

# Пути к папкам
DATA_DIR = pathlib.Path("data")
DEBUG_DIR = pathlib.Path("debug")

# Паттерны для валидации
VK_LINK_PATTERN = re.compile(r'^https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.]+)$')
PHONE_PATTERN = re.compile(r'(?<!\d)7\d{10}(?!\d)')


# Инициализация структуры проекта
def init_project_structure():
    """Создает необходимую структуру папок и файлов проекта"""
    # Создаем папку data, если её нет
    DATA_DIR.mkdir(exist_ok=True)

    # Создаем папку debug, если её нет
    DEBUG_DIR.mkdir(exist_ok=True)

    # Создаем файл .gitkeep в каждой папке, чтобы git отслеживал пустые папки
    (DATA_DIR / '.gitkeep').touch(exist_ok=True)
    (DEBUG_DIR / '.gitkeep').touch(exist_ok=True)

    logger.info("✅ Структура проекта инициализирована")


# Загрузка переменных окружения
load_dotenv()
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
PHONE = os.environ.get("ACCOUNT_PHONE")
PROXY = os.environ.get("PROXY", None)
USE_BOT_POOL = os.environ.get("USE_BOT_POOL", "true").lower() == "true"


class BotWorker:
    """Класс для работы с одним ботом"""

    def __init__(self, bot_username: str, bot_id: int, client: TelegramClient):
        self.bot_username = bot_username
        self.bot_id = bot_id
        self.client = client
        self.bot_entity = None
        self.is_active = True
        self.error_count = 0
        self.consecutive_errors = 0
        self.last_error_time = None
        self.processed_count = 0

        # Для отслеживания текущего поиска
        self.current_link = None
        self.result_found = asyncio.Event()
        self.search_message_id = None
        self.current_result = None

    async def initialize(self):
        """Инициализация бота"""
        try:
            self.bot_entity = await self.client.get_entity(self.bot_username)
            await self.client.send_message(self.bot_entity, "/start")
            await asyncio.sleep(1)
            logger.info(f"✅ Бот #{self.bot_id} ({self.bot_username}) инициализирован")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации бота #{self.bot_id}: {e}")
            self.is_active = False
            return False

    async def process_link(self, link: str) -> Optional[Dict[str, Any]]:
        """Обработка одной ссылки"""
        if not self.is_active:
            return None

        self.current_link = link
        self.result_found.clear()
        self.search_message_id = None
        self.current_result = None

        try:
            # Отправляем ссылку
            message = await self.client.send_message(self.bot_entity, link)
            logger.info(f"📤 Бот #{self.bot_id} обрабатывает: {link}")

            # Ждем результат
            await asyncio.sleep(INITIAL_DELAY)

            try:
                await asyncio.wait_for(self.result_found.wait(), timeout=MESSAGE_TIMEOUT)
                result = self.current_result
            except asyncio.TimeoutError:
                # Проверяем историю сообщений
                result = await self._check_history()

            if result:
                self.processed_count += 1
                self.consecutive_errors = 0
                logger.info(f"✅ Бот #{self.bot_id} нашел данные для {link}")
            else:
                logger.info(f"❓ Бот #{self.bot_id} не нашел данные для {link}")
                result = {"phones": [], "full_name": "", "birth_date": ""}

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка бота #{self.bot_id} при обработке {link}: {e}")
            self.error_count += 1
            self.consecutive_errors += 1
            self.last_error_time = time.time()

            # Если слишком много ошибок подряд - отключаем бота
            if self.consecutive_errors >= MAX_BOT_ERRORS:
                self.is_active = False
                logger.error(f"🚫 Бот #{self.bot_id} отключен из-за множественных ошибок")

            return None

    async def _check_history(self) -> Optional[Dict[str, Any]]:
        """Проверка истории сообщений"""
        try:
            messages = await self.client(GetHistoryRequest(
                peer=self.bot_entity,
                limit=20,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            for msg in messages.messages:
                if msg.text and self._is_result_message(msg.text):
                    return self._extract_all_data(msg.text)

        except Exception as e:
            logger.error(f"Ошибка при проверке истории бота #{self.bot_id}: {e}")

        return None

    def process_message(self, message_text: str, message_id: int):
        """Обработка входящего сообщения от бота"""
        if not message_text:
            return

        # Проверка на поисковое сообщение
        if any(phrase in message_text for phrase in ["Идёт поиск", "Searching"]):
            self.search_message_id = message_id
            return

        # Проверка на результат
        if self._is_result_message(message_text):
            self.current_result = self._extract_all_data(message_text)
            self.result_found.set()

    def _is_result_message(self, text: str) -> bool:
        """Проверка, является ли сообщение результатом"""
        if not text or len(text) < 50:
            return False

        indicators = [
            ("ID:" in text and "Вконтакте" in text),
            "Телефоны:" in text,
            "Полное имя:" in text,
            re.search(r'-\s*\d+', text) and len(text) > 50
        ]

        return any(indicators)

    def _extract_all_data(self, text: str) -> Dict[str, Any]:
        """Извлечение данных из текста"""
        result = {
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }

        if not text:
            return result

        # Извлекаем телефоны
        result["phones"] = self._extract_phones(text)

        # Извлекаем полное имя
        full_name_patterns = [
            r'Полное имя:[\s\*`]*(.*?)(?:\*|`|\n|$)',
            r'Full name:[\s\*`]*(.*?)(?:\*|`|\n|$)',
        ]

        for pattern in full_name_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                full_name = match.group(1).strip()
                full_name = re.sub(r'[\*`]', '', full_name)
                if full_name and full_name != "Не указано":
                    result["full_name"] = full_name
                    break

        # Извлекаем дату рождения
        birth_date_patterns = [
            r'Дата рождения:[\s\*`]*(.*?)(?:\*|`|\n|$)',
            r'День рождения:[\s\*`]*(.*?)(?:\*|`|\n|$)',
        ]

        for pattern in birth_date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                birth_date = match.group(1).strip()
                birth_date = re.sub(r'[\*`]', '', birth_date)
                if birth_date and birth_date != "Не указано":
                    result["birth_date"] = birth_date
                    break

        return result

    def _extract_phones(self, text: str) -> List[str]:
        """Извлечение телефонов из текста"""
        phones = []

        if not text:
            return phones

        # Прямой поиск 11-значных номеров
        direct_matches = PHONE_PATTERN.findall(text)

        if direct_matches:
            seen = set()
            for phone in direct_matches:
                clean_phone = re.sub(r'[\*`\s\-\(\)]', '', phone)
                if clean_phone not in seen and len(clean_phone) == 11:
                    phones.append(clean_phone)
                    seen.add(clean_phone)

            return phones[:4]

        # Поиск в секции "Телефоны:"
        phone_section_match = re.search(r'Телефоны:(.*?)(?:👁|ID:|Полное имя:|$)', text, re.DOTALL)
        if phone_section_match:
            phone_section = phone_section_match.group(1)
            section_phones = PHONE_PATTERN.findall(phone_section)

            # Также ищем номера после тире
            dash_phones = re.findall(r'-\s*(\d{11})', phone_section)
            section_phones.extend(dash_phones)

            seen = set()
            for phone in section_phones:
                clean_phone = re.sub(r'[^\d]', '', phone)
                if len(clean_phone) == 11 and clean_phone.startswith('7') and clean_phone not in seen:
                    phones.append(clean_phone)
                    seen.add(clean_phone)
                    if len(phones) >= 4:
                        break

        return phones[:4]


class MultiVKWorker:
    """Основной класс для работы с пулом из 10 ботов"""

    def __init__(
            self,
            queue: asyncio.Queue,
            result_callback: Callable[[str, Dict[str, Any]], Coroutine],
            limit_callback: Callable[[], Coroutine],
            admin_notification_callback: Optional[Callable[[str], Coroutine]] = None
    ):
        self.queue = queue
        self.result_callback = result_callback
        self.limit_callback = limit_callback
        self.admin_notification_callback = admin_notification_callback
        self.client = None
        self.bots: List[BotWorker] = []
        self.limit_reached = asyncio.Event()

        # Счетчики
        self.processed_count = 0
        self.total_count = 0
        self.error_count = 0
        self.start_time = None

        # Для мониторинга баланса
        self.initial_balance = None
        self.current_balance = None
        self.last_balance_check = 0

    async def start(self):
        """Запуск обработки с пулом ботов"""
        self.total_count = self.queue.qsize()
        self.start_time = time.time()

        # Настройка прокси
        proxy_config = None
        if PROXY:
            try:
                proxy_parts = PROXY.replace('://', ':').split(':')
                if len(proxy_parts) >= 3:
                    proxy_config = {
                        'proxy_type': proxy_parts[0],
                        'addr': proxy_parts[-2],
                        'port': int(proxy_parts[-1]),
                    }
                    if '@' in PROXY:
                        auth_part = PROXY.split('@')[0].split('://')[-1]
                        if ':' in auth_part:
                            proxy_config['username'] = auth_part.split(':')[0]
                            proxy_config['password'] = auth_part.split(':')[1]
            except Exception as e:
                logger.error(f"❌ Ошибка парсинга прокси: {e}")

        # Инициализация клиента
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy_config)

        try:
            await self.client.start(phone=PHONE)
            logger.info("✅ Авторизация в Telegram завершена")

            # Инициализация ботов
            await self._initialize_bots()

            # Проверка начального баланса
            await self._check_balance()

            if self.initial_balance and self.initial_balance < 10:
                logger.warning(f"⚠️ Низкий баланс: осталось {self.initial_balance} поисков")
                if self.admin_notification_callback:
                    await self.admin_notification_callback(
                        f"⚠️ Низкий баланс VK ботов: осталось {self.initial_balance} поисков!"
                    )

            # Запуск обработки
            await self._process_queue()

        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if self.client and not self.limit_reached.is_set():
                try:
                    await self.client.disconnect()
                    logger.info("👋 Отключен от Telegram")
                except:
                    pass

    async def _initialize_bots(self):
        """Инициализация всех ботов в пуле"""
        if USE_BOT_POOL:
            logger.info(f"🚀 Инициализация пула из {len(VK_BOT_POOL)} ботов...")

            # Настройка обработчиков событий для всех ботов
            @self.client.on(events.NewMessage())
            async def handle_new_message(event):
                if not event.message or not event.message.text:
                    return

                # Проверка на лимит
                if any(phrase in event.message.text for phrase in ["Лимит запросов исчерпан", "Too many requests"]):
                    logger.error("⚠️ Достигнут лимит запросов!")
                    self.limit_reached.set()
                    await self.limit_callback()
                    if self.admin_notification_callback:
                        await self.admin_notification_callback(
                            "🚨 Достигнут лимит VK ботов! Требуется пополнение баланса."
                        )
                    return

                # Находим бота-отправителя
                sender_id = event.message.peer_id.user_id if hasattr(event.message.peer_id, 'user_id') else None
                for bot in self.bots:
                    if bot.bot_entity and hasattr(bot.bot_entity, 'id') and bot.bot_entity.id == sender_id:
                        bot.process_message(event.message.text, event.message.id)
                        break

            @self.client.on(events.MessageEdited())
            async def handle_edited_message(event):
                if not event.message or not event.message.text:
                    return

                sender_id = event.message.peer_id.user_id if hasattr(event.message.peer_id, 'user_id') else None
                for bot in self.bots:
                    if bot.bot_entity and hasattr(bot.bot_entity, 'id') and bot.bot_entity.id == sender_id:
                        bot.process_message(event.message.text, event.message.id)
                        break

            # Инициализация каждого бота
            for i, bot_username in enumerate(VK_BOT_POOL):
                bot = BotWorker(bot_username, i + 1, self.client)
                if await bot.initialize():
                    self.bots.append(bot)
                await asyncio.sleep(0.5)  # Небольшая задержка между инициализациями

            if not self.bots:
                raise Exception("Не удалось инициализировать ни одного бота!")

            logger.info(f"✅ Инициализировано {len(self.bots)} ботов")
        else:
            # Режим с одним ботом (для обратной совместимости)
            bot = BotWorker(VK_BOT_POOL[0], 1, self.client)
            if await bot.initialize():
                self.bots.append(bot)
            else:
                raise Exception("Не удалось инициализировать бота!")

    async def _check_balance(self):
        """Проверка текущего баланса"""
        try:
            # Используем первого активного бота для проверки
            bot = next((b for b in self.bots if b.is_active), None)
            if not bot:
                return

            # Отправляем /profile
            await self.client.send_message(bot.bot_entity, "/profile")
            await asyncio.sleep(1)

            # Получаем последние сообщения
            messages = await self.client(GetHistoryRequest(
                peer=bot.bot_entity,
                limit=5,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            # Ищем сообщение с кнопкой "Мой профиль"
            for msg in messages.messages:
                if msg.reply_markup and msg.reply_markup.rows:
                    for row in msg.reply_markup.rows:
                        for button in row.buttons:
                            if hasattr(button, 'text') and button.text == "Мой профиль":
                                # Нажимаем кнопку
                                await msg.click(0, 0)  # Первая кнопка в первом ряду
                                await asyncio.sleep(1)

                                # Получаем обновленное сообщение
                                updated_messages = await self.client(GetHistoryRequest(
                                    peer=bot.bot_entity,
                                    limit=5,
                                    offset_date=None,
                                    offset_id=0,
                                    max_id=0,
                                    min_id=0,
                                    add_offset=0,
                                    hash=0
                                ))

                                # Парсим баланс
                                for updated_msg in updated_messages.messages:
                                    if updated_msg.text and "Доступно поисков:" in updated_msg.text:
                                        match = re.search(r'Доступно поисков:\s*(\d+)', updated_msg.text)
                                        if match:
                                            self.current_balance = int(match.group(1))
                                            if self.initial_balance is None:
                                                self.initial_balance = self.current_balance
                                            logger.info(f"💰 Текущий баланс: {self.current_balance} поисков")
                                            return

        except Exception as e:
            logger.error(f"Ошибка при проверке баланса: {e}")

    async def _process_queue(self):
        """Обработка очереди ссылок"""
        # Запускаем воркеров (по количеству активных ботов)
        workers = []
        worker_count = min(len([b for b in self.bots if b.is_active]), 10)

        logger.info(f"🚀 Запуск {worker_count} воркеров для обработки {self.total_count} ссылок")

        for i in range(worker_count):
            worker = asyncio.create_task(self._worker(i))
            workers.append(worker)

        # Запускаем мониторинг баланса
        if self.initial_balance:
            monitor_task = asyncio.create_task(self._monitor_balance())
            workers.append(monitor_task)

        # Ждем завершения всех воркеров
        await asyncio.gather(*workers, return_exceptions=True)

        # Финальная статистика
        elapsed = time.time() - self.start_time
        speed = self.processed_count / elapsed if elapsed > 0 else 0

        logger.info(f"""
✅ Обработка завершена!
📊 Статистика:
- Обработано: {self.processed_count}/{self.total_count}
- Время: {int(elapsed)} сек
- Скорость: {speed:.1f} ссылок/сек
- Ошибок: {self.error_count}
        """)

    async def _worker(self, worker_id: int):
        """Рабочая корутина для обработки ссылок"""
        logger.info(f"🔧 Воркер #{worker_id} запущен")

        while not self.limit_reached.is_set():
            try:
                # Получаем ссылку из очереди
                link = await asyncio.wait_for(self.queue.get(), timeout=1.0)

                # Выбираем доступного бота
                bot = self._get_next_bot()
                if not bot:
                    # Все боты недоступны, ждем
                    await self.queue.put(link)  # Возвращаем ссылку в очередь
                    await asyncio.sleep(BOT_ERROR_COOLDOWN)
                    continue

                # Проверяем, не пора ли боту отдохнуть
                if bot.last_error_time and (time.time() - bot.last_error_time) < BOT_ERROR_COOLDOWN:
                    await self.queue.put(link)  # Возвращаем ссылку в очередь
                    await asyncio.sleep(1)
                    continue

                # Обрабатываем ссылку
                result = await bot.process_link(link)

                if result is not None:
                    # Успешная обработка
                    await self.result_callback(link, result)
                    self.processed_count += 1

                    # Логирование прогресса
                    if self.processed_count % 50 == 0:
                        elapsed = time.time() - self.start_time
                        speed = self.processed_count / elapsed
                        logger.info(f"📊 Прогресс: {self.processed_count}/{self.total_count} ({speed:.1f} ссылок/сек)")
                else:
                    # Ошибка обработки
                    self.error_count += 1
                    await self.queue.put(link)  # Возвращаем ссылку в очередь для повторной попытки

                self.queue.task_done()

            except asyncio.TimeoutError:
                # Очередь пуста
                if self.queue.empty():
                    break
            except Exception as e:
                logger.error(f"❌ Ошибка в воркере #{worker_id}: {e}")

        logger.info(f"🏁 Воркер #{worker_id} завершен")

    def _get_next_bot(self) -> Optional[BotWorker]:
        """Получить следующего доступного бота (round-robin)"""
        # Фильтруем только активных ботов
        active_bots = [b for b in self.bots if b.is_active]

        if not active_bots:
            # Проверяем, может некоторые боты уже отдохнули
            for bot in self.bots:
                if (not bot.is_active and
                        bot.consecutive_errors < MAX_BOT_ERRORS and
                        bot.last_error_time and
                        (time.time() - bot.last_error_time) > BOT_ERROR_COOLDOWN):
                    bot.is_active = True
                    bot.consecutive_errors = 0
                    logger.info(f"✅ Бот #{bot.bot_id} восстановлен после отдыха")

            active_bots = [b for b in self.bots if b.is_active]

        if not active_bots:
            logger.error("❌ Нет доступных ботов!")
            if self.admin_notification_callback:
                asyncio.create_task(self.admin_notification_callback(
                    "🚨 Все VK боты недоступны! Проверьте систему."
                ))
            return None

        # Выбираем бота с наименьшим количеством обработанных запросов
        return min(active_bots, key=lambda b: b.processed_count)

    async def _monitor_balance(self):
        """Мониторинг баланса во время работы"""
        while not self.limit_reached.is_set() and not self.queue.empty():
            try:
                # Проверяем баланс каждые 100 обработанных ссылок
                if self.processed_count > 0 and self.processed_count % 100 == 0:
                    await self._check_balance()

                    if self.current_balance is not None:
                        # Проверяем критически низкий баланс
                        if self.current_balance < 50 and self.current_balance != self.last_balance_check:
                            logger.warning(f"⚠️ Осталось {self.current_balance} поисков!")
                            if self.admin_notification_callback:
                                await self.admin_notification_callback(
                                    f"⚠️ Критически низкий баланс VK: осталось {self.current_balance} поисков!"
                                )
                            self.last_balance_check = self.current_balance

                await asyncio.sleep(30)  # Проверка каждые 30 секунд

            except Exception as e:
                logger.error(f"Ошибка мониторинга баланса: {e}")
                await asyncio.sleep(60)


# Для обратной совместимости оставляем старый класс VKWorker
VKWorker = MultiVKWorker