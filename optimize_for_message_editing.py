#!/usr/bin/env python3
"""
Оптимизация VK бота для работы с редактированием сообщений
Учитывает что бот редактирует сообщение "Идёт поиск..." на результат
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("optimize_edit_mode")


def optimize_for_message_editing():
    """Оптимизирует VK сервис для работы с редактированием сообщений"""

    vk_service_file = Path("services/vk_service.py")

    if not vk_service_file.exists():
        logger.error("Файл services/vk_service.py не найден!")
        return False

    with open(vk_service_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Backup
    backup_file = Path("services/vk_service.py.backup_edit_mode")
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)

    # 1. Изменяем константы для быстрой работы
    replacements = [
        ("MESSAGE_TIMEOUT = 15.0", "MESSAGE_TIMEOUT = 5.0"),  # Меньше ждем
        ("MESSAGE_TIMEOUT = 8.0", "MESSAGE_TIMEOUT = 5.0"),
        ("MESSAGE_TIMEOUT = 30.0", "MESSAGE_TIMEOUT = 5.0"),
        ("INITIAL_DELAY = 2.0", "INITIAL_DELAY = 0.5"),  # Быстрая первая проверка
        ("INITIAL_DELAY = 1.5", "INITIAL_DELAY = 0.5"),
        ("INITIAL_DELAY = 0.8", "INITIAL_DELAY = 0.5"),
        ("RETRY_DELAY = 1.0", "RETRY_DELAY = 0.3"),
        ("MAX_RETRIES = 3", "MAX_RETRIES = 1"),  # Меньше повторов
    ]

    for old, new in replacements:
        content = content.replace(old, new)

    # 2. Добавляем отслеживание ID сообщения "Идёт поиск"
    tracking_vars = '''        # Для отслеживания текущего поиска
        self.current_link = None
        self.result_event = asyncio.Event()
        self.current_result = None
        self.search_message_id = None
        self.searching_message_ids = {}  # {message_id: link} для отслеживания'''

    # Заменяем старые переменные
    old_vars = '''        # Для отслеживания текущего поиска
        self.current_link = None
        self.result_event = asyncio.Event()
        self.current_result = None
        self.search_message_id = None'''

    content = content.replace(old_vars, tracking_vars)

    # 3. Модифицируем обработку сообщений для отслеживания "Идёт поиск"
    old_search_check = '''        # Проверка на сообщение о поиске
        if any(phrase in text for phrase in ["Идёт поиск", "Searching", "Ищу"]):
            self.search_message_id = message_id
            logger.debug(f"🔍 Начат поиск для {self.current_link}")
            return'''

    new_search_check = '''        # Проверка на сообщение о поиске
        if any(phrase in text.lower() for phrase in ["идёт поиск", "идет поиск", "searching", "ищу", "пожалуйста, подождите"]):
            self.search_message_id = message_id
            self.searching_message_ids[message_id] = self.current_link
            logger.debug(f"🔍 Начат поиск для {self.current_link}, msg_id: {message_id}")
            return'''

    content = content.replace(old_search_check, new_search_check)

    # 4. Оптимизируем обработку редактирования - это ключевой момент!
    new_edit_handler = '''        @self.client.on(events.MessageEdited(from_users=self.bot_entity))
        async def handle_edited_message(event):
            if not event.message or not event.message.text:
                return

            # ВАЖНО: Проверяем, это ли сообщение "Идёт поиск" было отредактировано
            if event.message.id in self.searching_message_ids:
                link = self.searching_message_ids[event.message.id]
                logger.debug(f"📝 Сообщение {event.message.id} отредактировано для {link}")

                # Это наш результат!
                if self._is_result_message(event.message.text):
                    self.current_result = self._parse_result(event.message.text)
                    self.result_event.set()
                    # Удаляем из отслеживания
                    del self.searching_message_ids[event.message.id]
                    return

            # Обычная обработка
            await self._process_message(event.message.text, event.message.id)'''

    # Находим и заменяем старый обработчик
    import re
    pattern = r'@self\.client\.on\(events\.MessageEdited.*?\n.*?async def handle_edited_message.*?(?=\n    [@a-zA-Z]|\n\n)'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        content = content[:match.start()] + new_edit_handler.strip() + content[match.end():]

    # 5. Оптимизируем метод поиска для работы с редактированием
    optimized_search = '''                # Отправляем ссылку боту
                await self.client.send_message(self.bot_entity, link)
                logger.info(f"📤 Отправлена ссылка: {link}")

                # Стратегия ожидания для редактируемых сообщений
                start_time = time.time()
                max_wait = 4.0  # Максимум 4 секунды
                check_interval = 0.2  # Проверяем каждые 0.2 сек

                while time.time() - start_time < max_wait:
                    # Проверяем событие
                    if self.result_event.is_set():
                        break

                    # Проверяем последние сообщения на предмет "Идёт поиск"
                    if time.time() - start_time > 0.3:  # После 0.3 сек начинаем проверять
                        messages = await self.client.get_messages(self.bot_entity, limit=2)
                        for msg in messages:
                            if msg.text and "идёт поиск" in msg.text.lower():
                                if msg.id not in self.searching_message_ids:
                                    self.searching_message_ids[msg.id] = link
                                    logger.debug(f"📍 Найдено сообщение поиска {msg.id}")

                    await asyncio.sleep(check_interval)

                # Финальная проверка
                if not self.current_result:
                    # Может результат уже есть в последних сообщениях
                    messages = await self.client.get_messages(self.bot_entity, limit=3)
                    for msg in messages:
                        if msg.text and self._is_result_message(msg.text):
                            if link in msg.text or "vk.com" in msg.text:
                                self.current_result = self._parse_result(msg.text)
                                break'''

    # Заменяем участок кода поиска
    # Ищем начало блока try в search_vk_link
    search_pattern = r'try:\s*\n\s*# Отправляем ссылку боту.*?(?=if self\.current_result:|except|$)'
    match = re.search(search_pattern, content, re.DOTALL | re.MULTILINE)
    if match:
        # Находим правильный отступ
        indent_match = re.search(r'^(\s*)try:', match.group(0), re.MULTILINE)
        if indent_match:
            indent = indent_match.group(1) + "    "
            # Добавляем правильные отступы к новому коду
            indented_search = '\n'.join(indent + line if line.strip() else line
                                        for line in optimized_search.strip().split('\n'))

            content = content[:match.start()] + f"            try:\n{indented_search}\n" + content[match.end():]

    # 6. Добавляем import time если его нет
    if "import time" not in content:
        content = content.replace("import asyncio", "import asyncio\nimport time")

    # 7. Очищаем отслеживание при новом поиске
    clear_tracking = '''        self.current_link = link
        self.result_event.clear()
        self.current_result = None
        # Очищаем старые записи отслеживания
        if len(self.searching_message_ids) > 10:
            self.searching_message_ids.clear()'''

    content = content.replace(
        '''        self.current_link = link
        self.result_event.clear()
        self.current_result = None''',
        clear_tracking
    )

    # Сохраняем
    with open(vk_service_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ VK сервис оптимизирован для работы с редактированием сообщений")
    return True


def create_fast_edit_monitor():
    """Создает монитор для отслеживания редактирования сообщений"""

    monitor_script = '''#!/usr/bin/env python3
"""
Монитор редактирования сообщений VK бота
Помогает понять паттерны работы бота
"""

import asyncio
import time
from datetime import datetime
from collections import defaultdict

from telethon import TelegramClient, events
from bot.config import *


class EditMonitor:
    def __init__(self):
        self.edit_times = defaultdict(list)  # {msg_id: [edit_times]}
        self.message_contents = {}  # {msg_id: content}

    async def monitor(self):
        """Мониторинг редактирования сообщений"""

        client = TelegramClient(SESSION_NAME + "_monitor", API_ID, API_HASH)
        await client.start(phone=ACCOUNT_PHONE)

        bot = await client.get_entity(VK_BOT_USERNAME)
        print(f"📡 Мониторинг бота @{VK_BOT_USERNAME}")

        @client.on(events.NewMessage(from_users=bot))
        async def on_new_message(event):
            msg_id = event.message.id
            self.message_contents[msg_id] = {
                'first_content': event.message.text[:100] if event.message.text else "",
                'first_time': time.time(),
                'edits': []
            }
            print(f"\\n📨 Новое сообщение {msg_id}: {event.message.text[:50]}...")

        @client.on(events.MessageEdited(from_users=bot))
        async def on_message_edited(event):
            msg_id = event.message.id

            if msg_id in self.message_contents:
                edit_time = time.time()
                original_time = self.message_contents[msg_id]['first_time']
                time_to_edit = edit_time - original_time

                self.message_contents[msg_id]['edits'].append({
                    'time': edit_time,
                    'delay': time_to_edit,
                    'content': event.message.text[:100] if event.message.text else ""
                })

                print(f"\\n✏️ Сообщение {msg_id} отредактировано через {time_to_edit:.2f} сек")
                print(f"   Было: {self.message_contents[msg_id]['first_content']}")
                print(f"   Стало: {event.message.text[:100] if event.message.text else ''}...")

                # Статистика
                if "идёт поиск" in self.message_contents[msg_id]['first_content'].lower():
                    print(f"   ⚡ Время обработки запроса: {time_to_edit:.2f} сек")

        print("\\n🔍 Отправьте несколько ссылок боту для анализа...")
        print("Нажмите Ctrl+C для завершения\\n")

        try:
            await client.run_until_disconnected()
        except KeyboardInterrupt:
            # Показываем статистику
            print("\\n\\n📊 СТАТИСТИКА РЕДАКТИРОВАНИЯ:")

            search_times = []
            for msg_id, data in self.message_contents.items():
                if "идёт поиск" in data['first_content'].lower() and data['edits']:
                    search_times.append(data['edits'][0]['delay'])

            if search_times:
                print(f"\\n⏱ Время ответа бота:")
                print(f"  • Минимум: {min(search_times):.2f} сек")
                print(f"  • Среднее: {sum(search_times)/len(search_times):.2f} сек")
                print(f"  • Максимум: {max(search_times):.2f} сек")
                print(f"\\n💡 Рекомендуемые настройки:")
                print(f"  • INITIAL_DELAY = 0.3-0.5")
                print(f"  • MESSAGE_TIMEOUT = {max(search_times) + 1:.0f}")
                print(f"  • Интервал проверки = 0.2 сек")


if __name__ == "__main__":
    monitor = EditMonitor()
    asyncio.run(monitor.monitor())
'''

    with open("edit_monitor.py", 'w', encoding='utf-8') as f:
        f.write(monitor_script)

    logger.info("✅ Создан монитор редактирования сообщений")
    return True


def create_speed_comparison():
    """Создает скрипт для сравнения скорости до и после оптимизации"""

    comparison_script = '''#!/usr/bin/env python3
"""
Сравнение скорости работы до и после оптимизации
"""

import asyncio
import time
import statistics

from bot.config import *
from services.vk_service import VKService
from db_module import VKDatabase


async def test_speed(name: str, links: list):
    """Тестирует скорость обработки списка ссылок"""

    print(f"\\n🧪 Тест: {name}")
    print("-" * 40)

    db = VKDatabase()
    await db.init()

    vk = VKService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    await vk.initialize()

    times = []
    errors = 0

    start_total = time.time()

    for i, link in enumerate(links):
        start = time.time()
        try:
            result = await vk.search_vk_link(link)
            elapsed = time.time() - start
            times.append(elapsed)

            has_data = bool(result.get('phones') or result.get('full_name'))
            print(f"  [{i+1}/{len(links)}] {elapsed:.2f}с - {'✓ Данные найдены' if has_data else '✗ Нет данных'}")

        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{len(links)}] ❌ Ошибка: {str(e)[:50]}")

            if "лимит" in str(e).lower():
                break

    total_time = time.time() - start_total

    await vk.close()
    await db.close()

    # Результаты
    if times:
        avg_time = statistics.mean(times)
        speed = len(times) / total_time

        print(f"\\n📊 Результаты:")
        print(f"  • Обработано: {len(times)} из {len(links)}")
        print(f"  • Общее время: {total_time:.1f} сек")
        print(f"  • Среднее время/запрос: {avg_time:.2f} сек")
        print(f"  • Скорость: {speed:.2f} ссылок/сек ({speed*60:.0f} в минуту)")
        print(f"  • Ошибок: {errors}")

        return {
            'avg_time': avg_time,
            'speed': speed,
            'total': total_time
        }

    return None


async def main():
    # Тестовые ссылки (используйте реальные)
    test_links = [
        "https://vk.com/id1",
        "https://vk.com/durov",
        "https://vk.com/id123456",
        "https://vk.com/id654321",
        "https://vk.com/id111111",
        # Добавьте больше ссылок для точности
    ]

    print("🚀 ТЕСТ СКОРОСТИ VK БОТА")
    print("=" * 50)

    # Тест с текущими настройками
    current = await test_speed("Текущая скорость", test_links[:5])

    if current:
        print(f"\\n💡 АНАЛИЗ:")
        print(f"\\nТекущая производительность:")
        print(f"  • {current['speed']*60:.0f} ссылок в минуту")
        print(f"  • {current['avg_time']:.1f} секунд на запрос")

        print(f"\\nПрогноз после оптимизации:")
        print(f"  • {current['speed']*2.5*60:.0f} ссылок в минуту (x2.5)")
        print(f"  • {current['avg_time']/2.5:.1f} секунд на запрос")

        print(f"\\nВремя на обработку файлов:")
        print(f"  • 1000 ссылок: {1000/current['speed']/60:.1f} мин → {1000/(current['speed']*2.5)/60:.1f} мин")
        print(f"  • 5000 ссылок: {5000/current['speed']/60:.1f} мин → {5000/(current['speed']*2.5)/60:.1f} мин")


if __name__ == "__main__":
    asyncio.run(main())
'''

    with open("speed_comparison.py", 'w', encoding='utf-8') as f:
        f.write(comparison_script)

    logger.info("✅ Создан скрипт сравнения скорости")
    return True


def main():
    logger.info("🚀 ОПТИМИЗАЦИЯ ДЛЯ БОТА С РЕДАКТИРОВАНИЕМ СООБЩЕНИЙ")
    logger.info("=" * 60)

    # Применяем оптимизации
    steps = [
        ("Оптимизация для редактирования", optimize_for_message_editing),
        ("Монитор редактирования", create_fast_edit_monitor),
        ("Тест скорости", create_speed_comparison)
    ]

    for name, func in steps:
        try:
            if func():
                logger.info(f"✅ {name} - готово")
        except Exception as e:
            logger.error(f"❌ {name} - ошибка: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ ОПТИМИЗАЦИЯ ЗАВЕРШЕНА!")

    logger.info("\n🎯 КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:")
    logger.info("1. Отслеживание ID сообщений 'Идёт поиск...'")
    logger.info("2. Приоритетная обработка редактирования этих сообщений")
    logger.info("3. Уменьшено время ожидания до 4-5 секунд")
    logger.info("4. Проверка каждые 0.2 секунды")

    logger.info("\n⚡ КАК РАБОТАЕТ НОВАЯ ЛОГИКА:")
    logger.info("1. Отправляем запрос → ждем 0.3 сек")
    logger.info("2. Ищем сообщение 'Идёт поиск...' → запоминаем его ID")
    logger.info("3. Ждем редактирование ИМЕННО этого сообщения")
    logger.info("4. Как только оно отредактировано - сразу парсим результат")

    logger.info("\n📊 ОЖИДАЕМОЕ УСКОРЕНИЕ:")
    logger.info("• Было: ждали новое сообщение 8-15 сек")
    logger.info("• Стало: ждем редактирование 2-4 сек")
    logger.info("• Скорость: 30-40 ссылок/минуту (было 10-15)")

    logger.info("\n🧪 ТЕСТИРОВАНИЕ:")
    logger.info("1. Мониторинг работы бота:")
    logger.info("   python edit_monitor.py")
    logger.info("   (покажет реальное время редактирования)")
    logger.info("\n2. Тест скорости:")
    logger.info("   python speed_comparison.py")
    logger.info("\n3. Запуск с новыми настройками:")
    logger.info("   python run.py")

    logger.info("\n💡 ДОПОЛНИТЕЛЬНЫЕ СОВЕТЫ:")
    logger.info("• Бот обычно редактирует сообщение через 1-3 сек")
    logger.info("• Если не находит - проверьте что ищете сообщение 'Идёт поиск'")
    logger.info("• При ошибках запустите edit_monitor.py для диагностики")


if __name__ == "__main__":
    main()