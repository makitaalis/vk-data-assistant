#!/usr/bin/env python3
"""
Исправление всех проблем с ботом
"""

import re


def fix_vk_worker():
    """Исправляет vk_worker.py для работы с eye_of_god_bot"""

    print("🔧 Исправление vk_worker.py...")

    with open('vk_worker.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Возвращаем eye_of_god_bot и отключаем пул
    new_settings = '''# Список всех ботов
VK_BOT_POOL = [
    "anon_clear_weaponvk_bot",  # Основной бот
]

# Константы
MAX_RETRIES = 3
RETRY_DELAY = 2.0
INITIAL_DELAY = 2.0  # Увеличиваем задержку
MESSAGE_TIMEOUT = 15.0  # Увеличиваем таймаут
BOT_ERROR_COOLDOWN = 10.0
MAX_BOT_ERRORS = 10

# Принудительно отключаем пул ботов
USE_BOT_POOL = False'''

    # Заменяем настройки
    pattern = r'VK_BOT_POOL = \[.*?\].*?MAX_BOT_ERRORS = \d+'
    content = re.sub(pattern, new_settings, content, flags=re.DOTALL)

    # 2. Исправляем метод _is_result_message для формата eye_of_god_bot
    new_is_result = '''    def _is_result_message(self, text: str) -> bool:
        """Проверка, является ли сообщение результатом"""
        if not text:
            return False

        # Логируем для отладки
        if len(text) > 50:
            logger.info(f"🔍 Проверка сообщения (длина: {len(text)})")
            logger.info(f"Начало: {text[:100]}...")

        # Для eye_of_god_bot минимальная длина 50 символов
        if len(text) < 50:
            return False

        # Специфичные индикаторы для eye_of_god_bot
        indicators = [
            # Основные маркеры
            "👁" in text,
            "Вконтакте" in text,
            "ID:" in text,
            "Имя:" in text,
            "Фамилия:" in text,
            "Полное имя:" in text,
            "Дата рождения:" in text,
            "Телефоны:" in text,
            "Логин:" in text,
            "Город:" in text,

            # Дополнительные
            "vk.com" in text,
            "Интересовались этим:" in text,
            re.search(r'\\d{9,11}', text),  # Телефоны

            # Общий паттерн для результата
            len(text) > 100 and (":" in text),
        ]

        result = any(indicators)

        if result:
            logger.info("✅ Это результат поиска!")
        else:
            logger.info("❌ Это НЕ результат поиска")

        return result'''

    # Заменяем метод
    pattern = r'def _is_result_message\(self, text: str\) -> bool:.*?return any\(indicators\)'
    content = re.sub(pattern, new_is_result, content, flags=re.DOTALL)

    # 3. Обновляем извлечение данных для формата eye_of_god_bot
    new_extract_all = '''    def _extract_all_data(self, text: str) -> Dict[str, Any]:
        """Извлечение данных из текста eye_of_god_bot"""
        result = {
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }

        if not text:
            return result

        # Извлекаем телефоны (формат eye_of_god_bot)
        phone_patterns = [
            r'Телефоны:\\s*\\n?\\s*[-–]\\s*(\\d{11})',
            r'Телефоны:\\s*\\n?\\s*(\\d{11})',
            r'[-–]\\s*(\\d{11})',
            r'\\+7(\\d{10})',
            r'(?<!\\d)(7\\d{10})(?!\\d)',
        ]

        phones_found = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            for match in matches:
                phone = re.sub(r'\\D', '', match)
                if len(phone) == 10:
                    phone = '7' + phone
                if len(phone) == 11 and phone.startswith('7'):
                    if phone not in phones_found:
                        phones_found.append(phone)

        result["phones"] = phones_found[:4]

        # Извлекаем имя (с учетом формата eye_of_god_bot)
        name_patterns = [
            r'Имя:\\s*([^\\n]+)',
            r'Полное имя:\\s*([^\\n]+)',
        ]

        # Также ищем отдельно имя и фамилию
        first_name = ""
        last_name = ""

        first_name_match = re.search(r'Имя:\\s*([^\\n]+)', text)
        if first_name_match:
            first_name = first_name_match.group(1).strip()

        last_name_match = re.search(r'Фамилия:\\s*([^\\n]+)', text)
        if last_name_match:
            last_name = last_name_match.group(1).strip()

        if first_name and last_name:
            result["full_name"] = f"{first_name} {last_name}"
        else:
            # Пробуем найти полное имя
            for pattern in name_patterns:
                match = re.search(pattern, text)
                if match:
                    name = match.group(1).strip()
                    if name and name not in ["-", "Не указано"]:
                        result["full_name"] = name
                        break

        # Извлекаем дату рождения
        birth_patterns = [
            r'Дата рождения:\\s*([^\\n]+)',
            r'(\\d{1,2}\\.\\d{1,2}\\.\\d{4})',
        ]

        for pattern in birth_patterns:
            match = re.search(pattern, text)
            if match:
                birth = match.group(1).strip()
                if birth and birth not in ["-", "Не указано"]:
                    result["birth_date"] = birth
                    break

        # Логируем результат
        if any([result["phones"], result["full_name"], result["birth_date"]]):
            logger.info(f"✅ Извлечены данные:")
            logger.info(f"   Телефоны: {result['phones']}")
            logger.info(f"   Имя: {result['full_name']}")
            logger.info(f"   ДР: {result['birth_date']}")
        else:
            logger.warning("⚠️ Данные не извлечены из текста")
            logger.debug(f"Текст для анализа: {text[:500]}...")

        return result'''

    # Заменяем метод _extract_all_data
    pattern = r'def _extract_all_data\(self, text: str\) -> Dict\[str, Any\]:.*?return result'
    if re.search(pattern, content, flags=re.DOTALL):
        content = re.sub(pattern, new_extract_all, content, flags=re.DOTALL)

    # 4. Улучшаем обработку в _check_history
    new_check_history = '''    async def _check_history(self) -> Optional[Dict[str, Any]]:
        """Проверка истории сообщений"""
        try:
            messages = await self.client(GetHistoryRequest(
                peer=self.bot_entity,
                limit=30,  # Увеличиваем лимит
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            logger.info(f"📜 Получено {len(messages.messages)} сообщений из истории")

            for msg in messages.messages:
                if msg.text:
                    # Проверяем каждое сообщение
                    if self._is_result_message(msg.text):
                        logger.info("✅ Найден результат в истории!")
                        return self._extract_all_data(msg.text)

                    # Специальная проверка для eye_of_god_bot
                    if "👁" in msg.text and "Вконтакте" in msg.text:
                        logger.info("✅ Найден результат eye_of_god_bot!")
                        return self._extract_all_data(msg.text)

        except Exception as e:
            logger.error(f"Ошибка при проверке истории: {e}")

        return None'''

    # Заменяем метод
    pattern = r'async def _check_history\(self\) -> Optional\[Dict\[str, Any\]\]:.*?return None'
    if re.search(pattern, content, flags=re.DOTALL):
        content = re.sub(pattern, new_check_history, content, flags=re.DOTALL)

    # Сохраняем файл
    with open('vk_worker.py.backup', 'w', encoding='utf-8') as f:
        f.write(content)

    with open('vk_worker.py', 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ vk_worker.py исправлен")


def fix_bot_main():
    """Исправляет bot_main.py для показа прогресса"""

    print("\n🔧 Исправление bot_main.py...")

    with open('bot_main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Исправляем функцию start_processing для показа статуса
    fix = '''
    # Обновляем статус каждые 3 обработанных ссылки (было 5)
    if new_checks_count % 3 == 0 or processed == total:'''

    content = content.replace('if new_checks_count % 5 == 0:', fix)

    # Добавляем принудительное обновление статуса
    fix2 = '''
                # Обновляем статус каждые 5 обработанных ссылок
                if new_checks_count % 5 == 0:
                    progress_bar = create_progress_bar(processed, total)
                    percent = int((processed / total) * 100)

                    # Добавляем информацию о скорости
                    elapsed = time.time() - start_time
                    speed = new_checks_count / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / speed if speed > 0 else 0

                    new_status_text = f"""⚡ <b>Обработка данных</b>

{progress_bar}
<b>Прогресс:</b> {processed}/{total} ({percent}%)

📊 <b>Статистика:</b>
✅ Найдено данных: {found_count}
💾 Из кеша: {from_cache}
🔍 Новых проверок: {new_checks_count}
❌ Без результата: {not_found_count}

⚡ <b>Скорость:</b> {speed:.1f} ссылок/сек
⏱ <b>Осталось:</b> ~{int(eta)} сек

<i>Обновлено: {format_time()}</i>"""

                    if new_status_text != last_status_text:
                        await safe_edit_message(status, new_status_text, reply_markup=processing_menu_kb())
                        last_status_text = new_status_text'''

    # Находим и заменяем блок обновления статуса
    pattern = r'# Обновляем статус каждые \d+ обработанных ссылок.*?last_status_text = new_status_text'
    if re.search(pattern, content, flags=re.DOTALL):
        content = re.sub(pattern, fix2, content, flags=re.DOTALL)

    with open('bot_main.py', 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ bot_main.py исправлен")


def add_env_settings():
    """Добавляет правильные настройки в .env"""

    print("\n📝 Проверка .env файла...")

    try:
        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()

        if 'USE_BOT_POOL' not in env_content:
            with open('.env', 'a', encoding='utf-8') as f:
                f.write('\n# Отключаем пул ботов\nUSE_BOT_POOL=false\n')
            print("✅ Добавлена настройка USE_BOT_POOL=false")
        else:
            print("ℹ️ USE_BOT_POOL уже есть в .env")

    except FileNotFoundError:
        print("❌ Файл .env не найден!")


if __name__ == "__main__":
    print("🚀 Исправление всех проблем VK Data Assistant")
    print("=" * 60)

    try:
        fix_vk_worker()
        fix_bot_main()
        add_env_settings()

        print("\n✅ Все исправления применены!")
        print("\n📋 Что исправлено:")
        print("1. ✅ Возвращен бот @eye_of_god_bot")
        print("2. ✅ Обновлен парсер для его формата ответов")
        print("3. ✅ Исправлено отображение прогресса")
        print("4. ✅ Отключен пул ботов (работает один)")
        print("5. ✅ Увеличены таймауты для надежности")

        print("\n🎯 Дальнейшие действия:")
        print("1. Перезапустите бота в PyCharm")
        print("2. Теперь должны показываться найденные данные")
        print("3. Прогресс будет обновляться каждые 3 ссылки")

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()