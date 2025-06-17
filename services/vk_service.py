import asyncio
import logging
import re
import time
from typing import Dict, Any, Optional, Callable, Coroutine, List

from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest

from bot.config import VK_BOT_USERNAME

logger = logging.getLogger("vk_service")

# Константы
MESSAGE_TIMEOUT = 15.0  # Увеличиваем таймаут
INITIAL_DELAY = 2.0
RETRY_DELAY = 1.0
MAX_RETRIES = 3

# Паттерны для парсинга
PHONE_PATTERN = re.compile(r'(?<!\d)7\d{10}(?!\d)')


# Глобальный флаг для проверки паузы
def is_processing_paused() -> bool:
    """Проверяет, приостановлена ли обработка"""
    # Импортируем локально для избежания циклической зависимости
    try:
        from bot.handlers.balance import processing_paused
        return processing_paused
    except:
        return False


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

        # Для отслеживания текущего поиска (старый метод)
        self.current_link = None
        self.result_event = asyncio.Event()
        self.current_result = None
        self.search_message_id = None

        # НОВОЕ: Для пакетной обработки
        self.sent_messages = {}  # {msg_id: {"link": link, "time": time}}
        self.results_queue = asyncio.Queue()  # Очередь готовых результатов
        self.batch_mode = False  # Флаг режима работы

        # Счетчики
        self.processed_count = 0
        self.error_count = 0

    async def initialize(self):
        """Инициализация клиента и бота"""
        try:
            self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
            await self.client.start(phone=self.phone)
            logger.info("✅ Авторизация в Telegram завершена")

            # Получаем entity бота из конфига
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

            # НОВОЕ: Обработка в зависимости от режима
            if self.batch_mode:
                await self._process_message_batch_mode(event.message)
            else:
                await self._process_message(event.message.text, event.message.id)

        @self.client.on(events.MessageEdited(from_users=self.bot_entity))
        async def handle_edited_message(event):
            if not event.message or not event.message.text:
                return

            # НОВОЕ: Обработка в зависимости от режима
            if self.batch_mode:
                await self._process_message_batch_mode(event.message)
            else:
                await self._process_message(event.message.text, event.message.id)

    # НОВЫЙ МЕТОД: Обработка сообщений в пакетном режиме
    async def _process_message_batch_mode(self, message):
        """Обработка сообщений в пакетном режиме"""
        text = message.text

        # Проверяем, это ответ на наше сообщение?
        if message.reply_to_msg_id and message.reply_to_msg_id in self.sent_messages:
            # Это ответ на нашу ссылку!
            request_info = self.sent_messages[message.reply_to_msg_id]
            link = request_info["link"]

            # Проверка на лимит
            if any(phrase in text for phrase in ["Лимит запросов исчерпан", "Too many requests", "limit"]):
                logger.error(f"⚠️ Достигнут лимит запросов для {link}!")
                result = {"link": link, "error": "limit_reached", "phones": [], "full_name": "", "birth_date": ""}
                await self.results_queue.put(result)
                del self.sent_messages[message.reply_to_msg_id]
                return

            # Проверка на результат
            if self._is_result_message(text):
                result = self._parse_result(text)
                result["link"] = link
                result["response_time"] = time.time() - request_info["time"]

                # Добавляем в очередь результатов
                await self.results_queue.put(result)

                # Удаляем из ожидающих
                del self.sent_messages[message.reply_to_msg_id]

                logger.info(f"✅ Получен результат для {link} (время ответа: {result['response_time']:.1f}с)")
            elif any(phrase in text for phrase in ["не найден", "ошибка", "error", "Попробуйте позже"]):
                # Ошибка поиска
                result = {"link": link, "phones": [], "full_name": "", "birth_date": "", "error": "not_found"}
                await self.results_queue.put(result)
                del self.sent_messages[message.reply_to_msg_id]
                logger.warning(f"⚠️ Бот вернул ошибку для {link}")

    # НОВЫЙ МЕТОД: Отправка пакета ссылок
    async def send_link_batch(self, links: List[str], batch_delay: float = 0.3) -> List[int]:
        """Отправляет пакет ссылок и запоминает их ID"""
        sent_ids = []

        # Убираем дубликаты из пакета
        unique_links = list(dict.fromkeys(links))
        if len(unique_links) != len(links):
            logger.warning(f"⚠️ В пакете были дубликаты: {len(links)} -> {len(unique_links)}")
            links = unique_links

        for i, link in enumerate(links):
            try:
                msg = await self.client.send_message(self.bot_entity, link)

                # Сохраняем mapping
                self.sent_messages[msg.id] = {
                    "link": link,
                    "time": time.time()
                }
                sent_ids.append(msg.id)

                logger.info(f"📤 [{i + 1}/{len(links)}] Отправлена ссылка: {link} (msg_id={msg.id})")

                # Небольшая задержка между отправками (кроме последней)
                if i < len(links) - 1:
                    await asyncio.sleep(batch_delay)

            except Exception as e:
                logger.error(f"❌ Ошибка при отправке {link}: {e}")

        return sent_ids

    # НОВЫЙ МЕТОД: Ожидание результатов пакета
    async def wait_for_batch_results(self, batch_links: List[str], timeout: float = 20.0) -> Dict[str, Dict]:
        """Ждет результаты для пакета запросов"""
        start_time = time.time()
        results = {}
        expected_count = len(batch_links)

        logger.info(f"⏳ Ожидание {expected_count} результатов (таймаут: {timeout}с)...")

        while len(results) < expected_count and (time.time() - start_time) < timeout:
            try:
                # Пытаемся получить результат из очереди
                result = await asyncio.wait_for(self.results_queue.get(), timeout=0.5)

                link = result.get("link")
                if link and link in batch_links:
                    results[link] = result
                    logger.info(f"📊 Получен результат {len(results)}/{expected_count} для {link}")

            except asyncio.TimeoutError:
                # Проверяем, не слишком ли долго ждем
                elapsed = time.time() - start_time
                if elapsed > timeout * 0.8:  # 80% времени прошло
                    logger.warning(f"⏰ Приближается таймаут, получено {len(results)}/{expected_count}")
                continue

        # Обрабатываем таймауты для недополученных результатов
        for link in batch_links:
            if link not in results:
                logger.warning(f"⏱ Таймаут для {link}")
                results[link] = {
                    "link": link,
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": "timeout"
                }

        elapsed = time.time() - start_time
        logger.info(f"✅ Пакет обработан за {elapsed:.1f}с, получено {len(results)} результатов")

        return results

    # НОВЫЙ МЕТОД: Пакетная обработка очереди
    async def process_queue_batch(
            self,
            queue: asyncio.Queue,
            result_callback: Callable[[str, Dict[str, Any]], Coroutine],
            limit_callback: Callable[[], Coroutine],
            batch_size: int = 3,
            batch_delay: float = 0.3,
            inter_batch_delay: float = 1.0,
            batch_timeout: float = 20.0
    ):
        """Обработка очереди пакетами"""
        # Включаем пакетный режим
        self.batch_mode = True

        total = queue.qsize()
        processed = 0
        start_time = time.time()

        logger.info(f"🚀 Начинаю пакетную обработку {total} ссылок (пакеты по {batch_size})")

        try:
            while not queue.empty():
                # НОВОЕ: Проверяем паузу для проверки баланса
                while is_processing_paused():
                    logger.debug("⏸ Обработка приостановлена для проверки баланса")
                    await asyncio.sleep(0.5)

                # Собираем пакет
                batch = []
                for _ in range(min(batch_size, queue.qsize())):
                    if not queue.empty():
                        batch.append(await queue.get())

                if not batch:
                    break

                logger.info(
                    f"📦 Обработка пакета {(processed // batch_size) + 1} из {(total + batch_size - 1) // batch_size} ({len(batch)} ссылок)")

                try:
                    # Отправляем пакет запросов
                    sent_ids = await self.send_link_batch(batch, batch_delay)

                    # Ждем результаты
                    results = await self.wait_for_batch_results(batch, batch_timeout)

                    # Обрабатываем результаты
                    for link in batch:
                        if link in results:
                            result = results[link]
                            # Убираем служебное поле link из результата
                            clean_result = {k: v for k, v in result.items() if k != "link"}
                            await result_callback(link, clean_result)

                            if not result.get("error"):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            # Не должно произойти, но на всякий случай
                            await result_callback(link,
                                                  {"phones": [], "full_name": "", "birth_date": "", "error": "unknown"})
                            self.error_count += 1

                        queue.task_done()
                        processed += 1

                    # Логирование прогресса
                    elapsed = time.time() - start_time
                    speed = processed / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / speed if speed > 0 else 0

                    logger.info(
                        f"📊 Прогресс: {processed}/{total} ({processed / total * 100:.1f}%) | "
                        f"Скорость: {speed:.1f} ссылок/сек | "
                        f"Осталось: ~{int(eta)} сек"
                    )

                    # Проверка на лимит
                    if any(r.get("error") == "limit_reached" for r in results.values()):
                        logger.error("⚠️ Достигнут лимит запросов!")
                        await limit_callback()
                        # Возвращаем необработанные ссылки в очередь
                        while not queue.empty():
                            item = await queue.get()
                            await queue.put(item)
                            queue.task_done()
                        break

                    # Пауза между пакетами
                    if not queue.empty():
                        logger.info(f"⏸ Пауза {inter_batch_delay}с между пакетами...")
                        await asyncio.sleep(inter_batch_delay)

                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке пакета: {e}")
                    # Помечаем все ссылки пакета как ошибочные
                    for link in batch:
                        await result_callback(link, {"phones": [], "full_name": "", "birth_date": "", "error": str(e)})
                        queue.task_done()
                        processed += 1
                        self.error_count += 1

            elapsed = time.time() - start_time
            logger.info(
                f"✅ Пакетная обработка завершена: {processed} ссылок за {int(elapsed)} сек "
                f"({processed / elapsed:.1f} ссылок/сек)"
            )

        finally:
            # Выключаем пакетный режим
            self.batch_mode = False
            # Очищаем неполученные результаты
            self.sent_messages.clear()
            # Очищаем очередь результатов
            while not self.results_queue.empty():
                try:
                    await self.results_queue.get_nowait()
                except:
                    break

    # ВСЕ ОСТАЛЬНЫЕ МЕТОДЫ ОСТАЮТСЯ БЕЗ ИЗМЕНЕНИЙ
    # (Включая старые методы для обратной совместимости)

    async def _process_message(self, text: str, message_id: int):
        """Обработка сообщения от бота (старый метод)"""
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
                # Очищаем от Markdown форматирования
                name = re.sub(r'\*\*([^*]+)\*\*', r'\1', name)  # **жирный**
                name = re.sub(r'\*([^*]+)\*', r'\1', name)  # *курсив*
                name = re.sub(r'__([^_]+)__', r'\1', name)  # __жирный__
                name = re.sub(r'_([^_]+)_', r'\1', name)  # _курсив_
                name = re.sub(r'[`*_~]', '', name)  # остальные символы

                if name and name not in ["Не указано", "не указано", "—", "-"]:
                    result["full_name"] = name.strip()
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

    async def check_balance(self) -> Optional[int]:
        """Проверка баланса поисков через кнопку 'Мой профиль'"""
        if not self.is_initialized:
            raise RuntimeError("VKService не инициализирован")

        try:
            # Очищаем предыдущие результаты
            self.result_event.clear()
            self.current_result = None

            # Отправляем /start для получения меню
            await self.client.send_message(self.bot_entity, "/start")
            await asyncio.sleep(1)

            # Получаем последние сообщения
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

            # Ищем сообщение с inline кнопками
            for msg in messages.messages:
                if msg.reply_markup and hasattr(msg.reply_markup, 'rows'):
                    for row in msg.reply_markup.rows:
                        for button in row.buttons:
                            # Проверяем тип кнопки
                            if hasattr(button, 'text') and 'профиль' in button.text.lower():
                                # Проверяем, что это не URL кнопка
                                if hasattr(button, 'data') and button.data:
                                    await msg.click(data=button.data)
                                    logger.info("✅ Нажата кнопка 'Мой профиль'")
                                    await asyncio.sleep(2)
                                    break
                                elif hasattr(button, 'url'):
                                    logger.warning("⚠️ Кнопка 'Мой профиль' - это URL кнопка, не могу нажать")
                                    continue

            # Получаем обновленные сообщения после нажатия кнопки
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

            # Ищем сообщение с балансом
            for msg in messages.messages:
                if msg.text and "Доступно поисков:" in msg.text:
                    # Парсим количество поисков
                    match = re.search(r'Доступно поисков:\s*(\d+)', msg.text)
                    if match:
                        balance = int(match.group(1))
                        logger.info(f"✅ Получен баланс: {balance} поисков")
                        return balance

            logger.warning("⚠️ Не удалось найти информацию о балансе в ответе")
            return None

        except asyncio.TimeoutError:
            logger.error("⏱ Таймаут при проверке баланса")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке баланса: {e}")
            return None

    async def search_vk_link(self, link: str) -> Dict[str, Any]:
        """Поиск данных по VK ссылке (старый метод для обратной совместимости)"""
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

    async def process_queue(
            self,
            queue: asyncio.Queue,
            result_callback: Callable[[str, Dict[str, Any]], Coroutine],
            limit_callback: Callable[[], Coroutine]
    ):
        """Обработка очереди ссылок (старый метод для обратной совместимости)"""
        total = queue.qsize()
        processed = 0
        start_time = time.time()

        logger.info(f"🚀 Начинаю обработку {total} ссылок")

        while not queue.empty():
            try:
                # НОВОЕ: Проверяем, не приостановлена ли обработка
                while is_processing_paused():
                    logger.debug("⏸ Обработка приостановлена для проверки баланса")
                    await asyncio.sleep(0.5)

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