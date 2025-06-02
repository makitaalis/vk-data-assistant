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

# Имя целевого бота
VK_BOT = "vk_memorizeumuskringenivikusbot"

# Константы для повторных попыток и задержек
MAX_RETRIES = 3  # Количество попыток
RETRY_DELAY = 2.0  # Задержка между попытками в секундах
INITIAL_DELAY = 1.5  # Начальная задержка после отправки ссылки
MESSAGE_TIMEOUT = 10.0  # Таймаут ожидания результата

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

# Валидация конфигурации
if not API_ID or not API_HASH:
    logger.error("❌ API_ID и API_HASH должны быть указаны в .env файле")
    raise ValueError("Отсутствуют необходимые параметры конфигурации")


class VKWorker:
    def __init__(
            self,
            queue: asyncio.Queue,
            result_callback: Callable[[str, Dict[str, Any]], Coroutine],
            limit_callback: Callable[[], Coroutine],
    ):
        # Инициализация структуры проекта при создании экземпляра класса
        init_project_structure()

        self.queue = queue
        self.result_callback = result_callback
        self.limit_callback = limit_callback
        self.client = None
        self.limit_reached = asyncio.Event()
        self.vk_bot_entity = None

        # Счетчики для отслеживания прогресса
        self.processed_count = 0
        self.total_count = 0
        self.error_count = 0

        # Текущая обрабатываемая ссылка
        self.current_link = None

        # Флаг для отслеживания найденного результата
        self.result_found = asyncio.Event()

        # ID сообщения с поиском, которое будет отредактировано
        self.search_message_id = None

        # Множество для отслеживания уже обработанных ссылок
        self.processed_links: Set[str] = set()

        # Счетчик последовательных ошибок
        self.consecutive_errors = 0

    async def start(self):
        """Запуск клиента и обработка очереди ссылок"""
        # Устанавливаем общее количество ссылок
        self.total_count = self.queue.qsize()

        # Конфигурация прокси
        proxy_config = None
        if PROXY:
            try:
                # Парсим прокси формата: protocol://user:pass@host:port
                proxy_parts = PROXY.replace('://', ':').split(':')
                if len(proxy_parts) >= 3:
                    proxy_config = {
                        'proxy_type': proxy_parts[0],  # socks5, http
                        'addr': proxy_parts[-2],
                        'port': int(proxy_parts[-1]),
                    }
                    # Добавляем авторизацию если есть
                    if '@' in PROXY:
                        auth_part = PROXY.split('@')[0].split('://')[-1]
                        if ':' in auth_part:
                            proxy_config['username'] = auth_part.split(':')[0]
                            proxy_config['password'] = auth_part.split(':')[1]
                    logger.info(f"🌐 Используется прокси: {proxy_config['addr']}:{proxy_config['port']}")
            except Exception as e:
                logger.error(f"❌ Ошибка парсинга прокси: {e}")
                proxy_config = None

        # Инициализация Telethon клиента
        self.client = TelegramClient(
            SESSION_NAME,
            API_ID,
            API_HASH,
            proxy=proxy_config
        )

        try:
            await self.client.start(phone=PHONE)
            logger.info("✅ Авторизация в Telegram завершена")

            # Получаем сущность бота один раз
            try:
                self.vk_bot_entity = await self.client.get_entity(VK_BOT)
                logger.info(f"✅ Подключен к боту: {utils.get_display_name(self.vk_bot_entity)}")
            except Exception as e:
                logger.error(f"❌ Не удалось найти бота {VK_BOT}: {e}")
                raise

            # Настройка обработчика новых сообщений
            @self.client.on(events.NewMessage(from_users=self.vk_bot_entity))
            async def handle_new_message(event):
                if not event.message or not event.message.text:
                    return

                message = event.message
                logger.info(f"📨 Новое сообщение: {message.id} - {message.text[:100]}...")

                # Проверяем сообщение о лимите
                if any(phrase in message.text for phrase in ["Лимит запросов исчерпан", "Too many requests"]):
                    logger.info("⚠️ Достигнут лимит запросов")
                    self.limit_reached.set()
                    await self.limit_callback()
                    return

                # Проверяем, это сообщение "Идёт поиск"?
                if any(phrase in message.text for phrase in ["Идёт поиск", "Searching"]):
                    logger.info(f"🔍 Получено сообщение поиска с ID: {message.id}")
                    self.search_message_id = message.id
                    self.save_message_to_debug_file(self.current_link, message.text, message.id, "NEW")
                    return

                # Остальные сообщения могут содержать результаты
                await self.process_possible_result(message.text, message.id)

            # Обработчик отредактированных сообщений
            @self.client.on(events.MessageEdited(from_users=self.vk_bot_entity))
            async def handle_edited_message(event):
                if not event.message or not event.message.text:
                    return

                message = event.message
                logger.info(f"🔄 Отредактировано сообщение: {message.id} - {message.text[:100]}...")

                # Если это редактирование нашего сообщения поиска, проверяем результаты
                if self.search_message_id and message.id == self.search_message_id:
                    logger.info("✅ Сообщение с поиском отредактировано - это результат!")
                    self.save_message_to_debug_file(self.current_link, message.text, message.id, "EDITED")
                    await self.process_possible_result(message.text, message.id)
                else:
                    # Для других отредактированных сообщений тоже проверяем
                    await self.process_possible_result(message.text, message.id)

            # Отправляем /start команду
            await self.send_start_command()

            # Обрабатываем очередь ссылок
            while not self.queue.empty() and not self.limit_reached.is_set():
                try:
                    await self.process_next_link()

                    # Сохраняем прогресс через каждые 50 ссылок
                    self.processed_count += 1
                    if self.processed_count % 50 == 0:
                        logger.info(f"📊 Прогресс: обработано {self.processed_count}/{self.total_count} ссылок")

                    # Сбрасываем счетчик последовательных ошибок при успехе
                    self.consecutive_errors = 0

                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке ссылки: {e}")
                    self.error_count += 1
                    self.consecutive_errors += 1

                    # Если слишком много последовательных ошибок, делаем паузу
                    if self.consecutive_errors >= 5:
                        logger.warning("⚠️ Слишком много последовательных ошибок, делаем паузу...")
                        await asyncio.sleep(30)
                        self.consecutive_errors = 0

        except FloodWaitError as e:
            logger.error(f"⏰ Необходимо подождать {e.seconds} секунд из-за флуд-контроля")
            self.limit_reached.set()
            await self.limit_callback()
        except AuthKeyError as e:
            logger.error(f"🔑 Ошибка авторизации: {e}")
            logger.error("Попробуйте удалить файл сессии и авторизоваться заново")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при работе клиента: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Отключаемся от Telethon
            if self.client and not self.limit_reached.is_set():
                try:
                    await self.client.disconnect()
                    logger.info("👋 Отключен от Telegram")
                except:
                    pass

    async def process_possible_result(self, text: str, message_id: int):
        """Обрабатывает возможное сообщение с результатом"""
        if not text or not self.current_link:
            return

        # Проверяем, содержит ли сообщение признаки результата
        is_result = False

        # Признаки результата: "ID:", "Вконтакте", "Телефоны:"
        result_indicators = [
            ("ID:" in text and "Вконтакте" in text),
            "Телефоны:" in text,
            (re.search(r'-\s*\d+', text) and len(text) > 50),
            "Полное имя:" in text,
            "Дата рождения:" in text
        ]

        if any(result_indicators):
            logger.info("✅ Найдено сообщение с результатом")
            is_result = True

        if is_result and not self.result_found.is_set():
            try:
                # Извлекаем все данные
                result_data = self.extract_all_data(text)

                # Логируем найденные данные
                logger.info(f"📊 Результаты: {result_data}")

                # Вызываем callback с результатом
                await self.result_callback(self.current_link, result_data)

                # Устанавливаем флаг, что результат найден
                self.result_found.set()
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке результата: {e}")

    async def send_start_command(self):
        """Отправка команды /start боту"""
        try:
            message = await self.client.send_message(self.vk_bot_entity, "/start")
            logger.info(f"📤 Отправлена команда /start (ID: {message.id})")

            # Ждем немного, чтобы бот успел ответить
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке /start: {e}")

    def save_message_to_debug_file(self, link: str, message_text: str, message_id: int, message_type: str = ""):
        """Сохранить сообщение в отладочный файл для анализа"""
        try:
            # Проверка и создание директории debug
            DEBUG_DIR.mkdir(exist_ok=True)

            # Создаем отладочный файл
            debug_file = DEBUG_DIR / "messages_debug.txt"
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Ограничиваем размер файла (максимум 10 МБ)
            if debug_file.exists() and debug_file.stat().st_size > 10 * 1024 * 1024:
                # Переименовываем старый файл
                old_file = DEBUG_DIR / f"messages_debug_{timestamp.replace(':', '-')}.txt"
                debug_file.rename(old_file)

            # Добавляем сообщение в файл
            with debug_file.open("a", encoding="utf-8") as f:
                f.write(f"\n{'=' * 80}\n")
                f.write(f"ВРЕМЯ: {timestamp}\n")
                f.write(f"ТИП: {message_type}\n")
                f.write(f"ССЫЛКА: {link}\n")
                f.write(f"ID СООБЩЕНИЯ: {message_id}\n")
                f.write(f"СОДЕРЖИМОЕ:\n{message_text}\n")
                f.write(f"{'=' * 80}\n")

        except Exception as e:
            logger.error(f"Ошибка при сохранении в отладочный файл: {e}")

    def validate_link(self, link: str) -> bool:
        """Валидация VK ссылки"""
        if not link or not isinstance(link, str):
            return False

        link = link.strip()
        if not VK_LINK_PATTERN.match(link):
            return False

        # Дополнительные проверки
        if len(link) > 200:
            return False

        return True

    async def process_next_link(self):
        """Обработка следующей ссылки в очереди"""
        try:
            self.current_link = await asyncio.wait_for(self.queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return

        # Валидация ссылки
        if not self.validate_link(self.current_link):
            logger.warning(f"⚠️ Невалидная ссылка: {self.current_link}")
            self.queue.task_done()
            return

        # Проверяем, не обрабатывали ли мы уже эту ссылку
        if self.current_link in self.processed_links:
            logger.info(f"⏭️ Пропуск уже обработанной ссылки: {self.current_link}")
            self.queue.task_done()
            return

        # Добавляем ссылку в список обработанных
        self.processed_links.add(self.current_link)

        logger.info(f"🔗 Обработка ссылки: {self.current_link}")

        # Сбрасываем флаги и ID для новой ссылки
        self.result_found.clear()
        self.search_message_id = None

        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # 1. Отправляем ссылку боту
                message = await self.client.send_message(self.vk_bot_entity, self.current_link)
                logger.info(f"📤 Отправлено сообщение с ID: {message.id}")

                # 2. Ждем начальное время
                await asyncio.sleep(INITIAL_DELAY)

                # 3. Ждем, пока не найдем результат или не достигнем таймаута
                try:
                    # Ждем, пока result_found не станет True или не истечет таймаут
                    await asyncio.wait_for(self.result_found.wait(), timeout=MESSAGE_TIMEOUT)
                    logger.info("✅ Результат успешно найден и обработан")
                    break  # Выходим из цикла retry
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Время ожидания истекло (попытка {retry_count + 1}/{MAX_RETRIES})")

                    # Если результат не найден, проверяем историю сообщений
                    if not self.result_found.is_set():
                        logger.info("🔎 Проверяем историю сообщений...")

                        # Получаем последние сообщения
                        messages = await self.get_recent_messages(20)

                        # Ищем подходящее сообщение с результатом
                        result_found_in_history = False
                        for msg in messages:
                            if msg.text:
                                self.save_message_to_debug_file(self.current_link, msg.text, msg.id, "HISTORY")

                                # Проверяем, может ли это быть результат
                                if any([
                                    ("ID:" in msg.text and "Вконтакте" in msg.text),
                                    "Телефоны:" in msg.text,
                                    "Полное имя:" in msg.text,
                                    (len(msg.text) > 100 and re.search(r'-\s*7\d{10}', msg.text))
                                ]):
                                    # Извлекаем все данные
                                    result_data = self.extract_all_data(msg.text)
                                    await self.result_callback(self.current_link, result_data)
                                    result_found_in_history = True
                                    break

                        if result_found_in_history:
                            break  # Выходим из цикла retry

                retry_count += 1
                if retry_count < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

            except FloodWaitError as e:
                logger.error(f"⏰ Флуд-контроль: необходимо подождать {e.seconds} секунд")
                self.limit_reached.set()
                await self.limit_callback()
                break
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке ссылки {self.current_link}: {e}")
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

        # Если после всех попыток результат не найден
        if not self.result_found.is_set():
            logger.warning(f"❌ Результат не найден для {self.current_link}")
            empty_result = {"phones": [], "full_name": "", "birth_date": ""}
            await self.result_callback(self.current_link, empty_result)

        # Помечаем задачу как выполненную
        self.queue.task_done()

        # Делаем небольшую паузу между запросами
        await asyncio.sleep(1.0)

    async def get_recent_messages(self, limit: int = 10) -> List[Message]:
        """Получение последних сообщений из диалога с ботом"""
        try:
            history = await self.client(GetHistoryRequest(
                peer=self.vk_bot_entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            return history.messages
        except Exception as e:
            logger.error(f"Ошибка при получении истории сообщений: {e}")
            return []

    def extract_all_data(self, text: str) -> Dict[str, Any]:
        """Извлечение всех данных из текста сообщения: телефоны, полное имя, дата рождения"""
        result = {
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }

        # Проверяем наличие текста
        if not text:
            logger.warning("Пустой текст для извлечения данных")
            return result

        logger.info(f"📝 Анализ сообщения для извлечения всех данных...")

        try:
            # Извлекаем телефоны
            result["phones"] = self.extract_phones(text)

            # Извлекаем полное имя
            full_name_patterns = [
                r'Полное имя:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'Full name:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'ФИО:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'Имя:[\s\*`]*(.*?)(?:\*|`|\n|$)'
            ]

            for pattern in full_name_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    # Очищаем от специальных символов разметки
                    full_name = match.group(1).strip()
                    full_name = re.sub(r'[\*`]', '', full_name)
                    if full_name and full_name != "Не указано":
                        result["full_name"] = full_name
                        logger.info(f"✅ Найдено полное имя: {result['full_name']}")
                        break

            # Извлекаем дату рождения
            birth_date_patterns = [
                r'Дата рождения:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'День рождения:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'ДР:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'Birthday:[\s\*`]*(.*?)(?:\*|`|\n|$)',
                r'Birth date:[\s\*`]*(.*?)(?:\*|`|\n|$)'
            ]

            for pattern in birth_date_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    # Очищаем от специальных символов разметки
                    birth_date = match.group(1).strip()
                    birth_date = re.sub(r'[\*`]', '', birth_date)
                    if birth_date and birth_date != "Не указано":
                        result["birth_date"] = birth_date
                        logger.info(f"✅ Найдена дата рождения: {result['birth_date']}")
                        break

        except Exception as e:
            logger.error(f"⚠️ Ошибка при извлечении данных: {e}")

        return result

    def extract_phones(self, text: str) -> List[str]:
        """Извлечение телефонных номеров из текста"""
        phones = []

        # Проверяем наличие текста
        if not text:
            logger.warning("Пустой текст для извлечения телефонов")
            return []

        try:
            # Отдельно логируем текст для отладки (только первые 300 символов)
            logger.info(f"Текст для извлечения телефонов: {text[:300]}...")

            # Метод 1: Прямой поиск всех 11-значных номеров, начинающихся с 7
            direct_matches = PHONE_PATTERN.findall(text)

            if direct_matches:
                # Очищаем от возможных символов разметки и удаляем дубликаты
                seen = set()
                for phone in direct_matches:
                    clean_phone = re.sub(r'[\*`\s\-\(\)]', '', phone)
                    if clean_phone not in seen and len(clean_phone) == 11:
                        phones.append(clean_phone)
                        seen.add(clean_phone)

                logger.info(f"✅ Найдены телефоны напрямую: {phones}")
                return phones[:4]  # Максимум 4 номера

            # Метод 2: Поиск в секции "Телефоны:"
            phone_section_match = re.search(r'Телефоны:(.*?)(?:👁|ID:|Полное имя:|$)', text, re.DOTALL)
            if phone_section_match:
                phone_section = phone_section_match.group(1)
                logger.info(f"📱 Найдена секция 'Телефоны:': {phone_section[:100]}...")

                # Ищем номера в секции
                section_phones = PHONE_PATTERN.findall(phone_section)

                # Также ищем номера после тире
                dash_phones = re.findall(r'-\s*(\d{11})', phone_section)
                section_phones.extend(dash_phones)

                # Очищаем и проверяем
                seen = set()
                for phone in section_phones:
                    clean_phone = re.sub(r'[^\d]', '', phone)
                    if len(clean_phone) == 11 and clean_phone.startswith('7') and clean_phone not in seen:
                        phones.append(clean_phone)
                        seen.add(clean_phone)
                        if len(phones) >= 4:
                            break

            # Метод 3: Поиск номеров в списках (после тире или точек)
            if not phones:
                list_pattern = r'(?:^|\n)\s*[\-•·]\s*(\d[\d\s\-\(\)]{10,})'
                list_matches = re.findall(list_pattern, text, re.MULTILINE)

                for match in list_matches:
                    clean_number = re.sub(r'[^\d]', '', match)
                    if len(clean_number) == 11 and clean_number.startswith('7'):
                        if clean_number not in phones:
                            phones.append(clean_number)
                            if len(phones) >= 4:
                                break

        except Exception as e:
            logger.error(f"⚠️ Ошибка при извлечении телефонов: {e}")

        # Логируем итоговый результат
        if phones:
            logger.info(f"📞 Всего найдено номеров: {len(phones)} - {phones}")
        else:
            logger.warning("❓ Номера не найдены ни одним из методов")

        return phones[:4]  # Возвращаем максимум 4 номера