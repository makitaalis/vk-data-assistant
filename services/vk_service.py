import asyncio
import logging
import re
import time
from typing import Dict, Any, Optional, Callable, Coroutine
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest

logger = logging.getLogger("vk_service")

# Константы
VK_BOT_USERNAME = "sherlock_XIS_bot"  # Основной бот
MESSAGE_TIMEOUT = 15.0  # Увеличиваем таймаут
INITIAL_DELAY = 2.0
RETRY_DELAY = 1.0
MAX_RETRIES = 3

# Паттерны для парсинга
PHONE_PATTERN = re.compile(r'(?<!\d)7\d{10}(?!\d)')


class VKService:
    """Упрощенный сервис для работы с одним VK ботом"""

    def __init__(self, api_id: int, api_hash: str, session_name: str, phone: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.phone = phone

        self.client: Optional[TelegramClient] = None
        self.bot_entity = None
        self.is_initialized = False

        # Для отслеживания текущего поиска
        self.current_link = None
        self.result_event = asyncio.Event()
        self.current_result = None
        self.search_message_id = None

        # Счетчики
        self.processed_count = 0
        self.error_count = 0

    async def initialize(self):
        """Инициализация клиента и бота"""
        try:
            self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
            await self.client.start(phone=self.phone)
            logger.info("✅ Авторизация в Telegram завершена")

            # Получаем entity бота
            self.bot_entity = await self.client.get_entity(VK_BOT_USERNAME)

            # Отправляем /start для инициализации
            await self.client.send_message(self.bot_entity, "/start")
            await asyncio.sleep(1)

            # Настраиваем обработчики событий
            self._setup_handlers()

            self.is_initialized = True
            logger.info(f"✅ Бот {VK_BOT_USERNAME} инициализирован")

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise

    def _setup_handlers(self):
        """Настройка обработчиков сообщений"""

        @self.client.on(events.NewMessage(from_users=self.bot_entity))
        async def handle_new_message(event):
            if not event.message or not event.message.text:
                return

            await self._process_message(event.message.text, event.message.id)

        @self.client.on(events.MessageEdited(from_users=self.bot_entity))
        async def handle_edited_message(event):
            if not event.message or not event.message.text:
                return

            await self._process_message(event.message.text, event.message.id)

    async def _process_message(self, text: str, message_id: int):
        """Обработка сообщения от бота"""
        if not text:
            return

        # Проверка на лимит
        if any(phrase in text for phrase in ["Лимит запросов исчерпан", "Too many requests", "limit"]):
            logger.error("⚠️ Достигнут лимит запросов!")
            self.current_result = {"error": "limit_reached"}
            self.result_event.set()
            return

        # Проверка на сообщение о поиске
        if any(phrase in text for phrase in ["Идёт поиск", "Searching", "Ищу"]):
            self.search_message_id = message_id
            logger.debug(f"🔍 Начат поиск для {self.current_link}")
            return

        # Проверка на сообщение об ошибке
        if any(phrase in text for phrase in ["не найден", "ошибка", "error", "Попробуйте позже"]):
            logger.warning(f"⚠️ Бот вернул ошибку для {self.current_link}")
            self.current_result = {"phones": [], "full_name": "", "birth_date": ""}
            self.result_event.set()
            return

        # Проверка на результат
        if self._is_result_message(text):
            self.current_result = self._parse_result(text)
            self.result_event.set()

    def _is_result_message(self, text: str) -> bool:
        """Проверка, является ли сообщение результатом поиска"""
        if not text or len(text) < 30:
            return False

        # Индикаторы результата
        indicators = [
            "ID:" in text,
            "Телефон" in text,
            "Полное имя:" in text,
            "Вконтакте" in text,
            bool(re.search(r'https?://vk\.com/', text)),
            # Проверка на наличие номеров телефонов
            bool(PHONE_PATTERN.search(text))
        ]

        # Если есть хотя бы 2 индикатора - это результат
        return sum(indicators) >= 2

    def _parse_result(self, text: str) -> Dict[str, Any]:
        """Парсинг результата поиска"""
        result = {
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }

        if not text:
            return result

        # Извлечение телефонов
        result["phones"] = self._extract_phones(text)

        # Извлечение полного имени
        name_patterns = [
            r'Полное имя:\s*[`*]?(.*?)(?:[`*\n]|$)',
            r'Full name:\s*[`*]?(.*?)(?:[`*\n]|$)',
            r'ФИО:\s*[`*]?(.*?)(?:[`*\n]|$)',
        ]

        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                name = match.group(1).strip()
                name = re.sub(r'[`*]', '', name)
                if name and name not in ["Не указано", "не указано", "—", "-"]:
                    result["full_name"] = name
                    break

        # Извлечение даты рождения
        birth_patterns = [
            r'Дата рождения:\s*[`*]?(.*?)(?:[`*\n]|$)',
            r'День рождения:\s*[`*]?(.*?)(?:[`*\n]|$)',
            r'ДР:\s*[`*]?(.*?)(?:[`*\n]|$)',
            r'Birthday:\s*[`*]?(.*?)(?:[`*\n]|$)',
        ]

        for pattern in birth_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                birth = match.group(1).strip()
                birth = re.sub(r'[`*]', '', birth)
                if birth and birth not in ["Не указано", "не указано", "—", "-"]:
                    result["birth_date"] = birth
                    break

        logger.debug(f"📋 Распарсен результат: {len(result['phones'])} телефонов, "
                     f"имя: {'есть' if result['full_name'] else 'нет'}, "
                     f"ДР: {'есть' if result['birth_date'] else 'нет'}")

        return result

    def _extract_phones(self, text: str) -> list[str]:
        """Извлечение телефонов из текста"""
        phones = []
        seen = set()

        # Метод 1: Прямой поиск 11-значных номеров
        direct_matches = PHONE_PATTERN.findall(text)

        for phone in direct_matches:
            clean_phone = re.sub(r'[^\d]', '', phone)
            if len(clean_phone) == 11 and clean_phone.startswith('7') and clean_phone not in seen:
                phones.append(clean_phone)
                seen.add(clean_phone)
                if len(phones) >= 4:  # Максимум 4 телефона
                    return phones

        # Метод 2: Поиск в секции "Телефоны:" или после "—"
        phone_sections = [
            r'Телефон[ыи]?:(.*?)(?:👁|ID:|Полное имя:|$)',
            r'Phone[s]?:(.*?)(?:👁|ID:|Full name:|$)',
            r'—\s*(\d{11})',
            r'-\s*(\d{11})',
        ]

        for pattern in phone_sections:
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                if isinstance(match, str):
                    # Ищем все числа в секции
                    numbers = re.findall(r'\d{10,11}', match)
                    for number in numbers:
                        if len(number) == 11 and number.startswith('7'):
                            if number not in seen:
                                phones.append(number)
                                seen.add(number)
                        elif len(number) == 10 and number.startswith('9'):
                            # Добавляем код страны
                            full_number = '7' + number
                            if full_number not in seen:
                                phones.append(full_number)
                                seen.add(full_number)

                        if len(phones) >= 4:
                            return phones

        return phones[:4]

    async def search_vk_link(self, link: str) -> Dict[str, Any]:
        """Поиск данных по VK ссылке"""
        if not self.is_initialized:
            raise RuntimeError("VKService не инициализирован")

        self.current_link = link
        self.result_event.clear()
        self.current_result = None

        for attempt in range(MAX_RETRIES):
            try:
                # Отправляем ссылку боту
                await self.client.send_message(self.bot_entity, link)
                logger.info(f"📤 Отправлена ссылка: {link} (попытка {attempt + 1})")

                # Ждем начальную задержку
                await asyncio.sleep(INITIAL_DELAY)

                # Ждем результат
                try:
                    await asyncio.wait_for(self.result_event.wait(), timeout=MESSAGE_TIMEOUT)
                except asyncio.TimeoutError:
                    # Проверяем историю сообщений
                    logger.warning(f"⏱ Таймаут ожидания, проверяю историю...")
                    result = await self._check_history()
                    if result:
                        self.current_result = result

                if self.current_result:
                    # Проверка на лимит
                    if isinstance(self.current_result, dict) and self.current_result.get("error") == "limit_reached":
                        raise Exception("Достигнут лимит запросов")

                    self.processed_count += 1
                    return self.current_result

                # Если результат не получен, пробуем еще раз
                logger.warning(f"❓ Результат не получен, попытка {attempt + 1}/{MAX_RETRIES}")
                await asyncio.sleep(RETRY_DELAY)

            except Exception as e:
                logger.error(f"❌ Ошибка при поиске {link}: {e}")
                if "лимит" in str(e).lower():
                    raise

                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * 2)
                else:
                    self.error_count += 1

        # Если ничего не нашли после всех попыток
        return {"phones": [], "full_name": "", "birth_date": ""}

    async def _check_history(self) -> Optional[Dict[str, Any]]:
        """Проверка истории сообщений на наличие результата"""
        try:
            messages = await self.client(GetHistoryRequest(
                peer=self.bot_entity,
                limit=10,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            # Ищем последний результат
            for msg in messages.messages:
                if msg.text and self._is_result_message(msg.text):
                    # Проверяем, что это результат для нашей ссылки
                    if self.current_link in msg.text or "vk.com" in msg.text:
                        return self._parse_result(msg.text)

        except Exception as e:
            logger.error(f"Ошибка при проверке истории: {e}")

        return None

    async def check_balance(self) -> Optional[int]:
        """Проверка баланса поисков"""
        try:
            # Отправляем команду /profile
            await self.client.send_message(self.bot_entity, "/profile")
            await asyncio.sleep(1)

            # Получаем сообщения
            messages = await self.client(GetHistoryRequest(
                peer=self.bot_entity,
                limit=5,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            # Ищем сообщение с балансом
            for msg in messages.messages:
                if msg.text and "Доступно поисков:" in msg.text:
                    match = re.search(r'Доступно поисков:\s*(\d+)', msg.text)
                    if match:
                        return int(match.group(1))

        except Exception as e:
            logger.error(f"Ошибка при проверке баланса: {e}")

        return None

    async def process_queue(
            self,
            queue: asyncio.Queue,
            result_callback: Callable[[str, Dict[str, Any]], Coroutine],
            limit_callback: Callable[[], Coroutine]
    ):
        """Обработка очереди ссылок"""
        total = queue.qsize()
        processed = 0
        start_time = time.time()

        logger.info(f"🚀 Начинаю обработку {total} ссылок")

        while not queue.empty():
            try:
                link = await queue.get()

                try:
                    result = await self.search_vk_link(link)
                    await result_callback(link, result)
                    processed += 1

                    # Логирование прогресса
                    if processed % 10 == 0:
                        elapsed = time.time() - start_time
                        speed = processed / elapsed if elapsed > 0 else 0
                        logger.info(f"📊 Прогресс: {processed}/{total} ({speed:.1f} ссылок/сек)")

                except Exception as e:
                    if "лимит" in str(e).lower():
                        logger.error("⚠️ Достигнут лимит запросов!")
                        await limit_callback()
                        # Возвращаем ссылку в очередь
                        await queue.put(link)
                        break
                    else:
                        logger.error(f"Ошибка обработки {link}: {e}")
                        # Возвращаем пустой результат
                        await result_callback(link, {"phones": [], "full_name": "", "birth_date": ""})

                finally:
                    queue.task_done()

            except asyncio.CancelledError:
                break

        elapsed = time.time() - start_time
        logger.info(f"✅ Обработка завершена: {processed} ссылок за {int(elapsed)} сек")

    async def close(self):
        """Закрытие соединения"""
        if self.client:
            await self.client.disconnect()
            logger.info("👋 Отключен от Telegram")