#!/usr/bin/env python3
"""
Исправление парсинга имен с Markdown форматированием
"""

import re
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_markdown")


def update_vk_service():
    """Обновляет vk_service.py для правильной очистки Markdown"""

    vk_service_file = Path("services/vk_service.py")

    if not vk_service_file.exists():
        logger.error("Файл services/vk_service.py не найден!")
        return False

    with open(vk_service_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Создаем резервную копию
    backup_file = Path("services/vk_service.py.backup_markdown")
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    logger.info(f"✅ Создана резервная копия: {backup_file}")

    # Находим место где очищается имя
    old_clean_pattern = '''        # Очистка имени
        if name_found:
            # Удаляем лишние символы
            name_found = re.sub(r'[*`_~]', '', name_found)
            # Удаляем emoji
            name_found = re.sub(r'[^\\w\\s\\-]', '', name_found, flags=re.UNICODE)
            name_found = name_found.strip()'''

    new_clean_pattern = '''        # Очистка имени
        if name_found:
            # Удаляем Markdown форматирование (**, *, __, _, ~~, `, ```)
            name_found = re.sub(r'\*\*([^*]+)\*\*', r'\\1', name_found)  # **жирный**
            name_found = re.sub(r'\*([^*]+)\*', r'\\1', name_found)      # *курсив*
            name_found = re.sub(r'__([^_]+)__', r'\\1', name_found)      # __жирный__
            name_found = re.sub(r'_([^_]+)_', r'\\1', name_found)        # _курсив_
            name_found = re.sub(r'~~([^~]+)~~', r'\\1', name_found)      # ~~зачеркнутый~~
            name_found = re.sub(r'```([^`]+)```', r'\\1', name_found)    # ```код```
            name_found = re.sub(r'`([^`]+)`', r'\\1', name_found)        # `код`

            # Удаляем оставшиеся символы форматирования
            name_found = re.sub(r'[*`_~]', '', name_found)

            # Удаляем лишние пробелы
            name_found = re.sub(r'\\s+', ' ', name_found)
            name_found = name_found.strip()'''

    # Заменяем паттерн
    if old_clean_pattern in content:
        content = content.replace(old_clean_pattern, new_clean_pattern)
        logger.info("✅ Обновлена очистка имени")
    else:
        # Если не нашли точный паттерн, ищем альтернативный
        logger.warning("⚠️ Не найден точный паттерн, ищу альтернативный...")

        # Ищем место после "if name_found:"
        pattern = r'(if name_found:\s*\n)(.*?)(name_found = name_found\.strip\(\))'
        match = re.search(pattern, content, re.DOTALL)

        if match:
            new_section = match.group(1) + '''            # Удаляем Markdown форматирование (**, *, __, _, ~~, `, ```)
            name_found = re.sub(r'\\*\\*([^*]+)\\*\\*', r'\\1', name_found)  # **жирный**
            name_found = re.sub(r'\\*([^*]+)\\*', r'\\1', name_found)      # *курсив*
            name_found = re.sub(r'__([^_]+)__', r'\\1', name_found)      # __жирный__
            name_found = re.sub(r'_([^_]+)_', r'\\1', name_found)        # _курсив_
            name_found = re.sub(r'~~([^~]+)~~', r'\\1', name_found)      # ~~зачеркнутый~~
            name_found = re.sub(r'```([^`]+)```', r'\\1', name_found)    # ```код```
            name_found = re.sub(r'`([^`]+)`', r'\\1', name_found)        # `код`

            # Удаляем оставшиеся символы форматирования
            name_found = re.sub(r'[*`_~]', '', name_found)

            # Удаляем лишние пробелы
            name_found = re.sub(r'\\s+', ' ', name_found)
            ''' + match.group(3)

            content = content[:match.start()] + new_section + content[match.end():]
            logger.info("✅ Добавлена очистка Markdown через альтернативный метод")
        else:
            logger.error("❌ Не удалось найти место для вставки очистки Markdown")
            logger.info("Попробуем добавить универсальную функцию...")

            # Добавляем функцию очистки в начало класса
            class_pattern = r'(class VKService:.*?\n\n)'
            clean_function = '''def _clean_markdown(self, text: str) -> str:
        """Очищает текст от Markdown форматирования"""
        if not text:
            return text

        # Удаляем Markdown форматирование
        text = re.sub(r'\\*\\*([^*]+)\\*\\*', r'\\1', text)  # **жирный**
        text = re.sub(r'\\*([^*]+)\\*', r'\\1', text)      # *курсив*
        text = re.sub(r'__([^_]+)__', r'\\1', text)      # __жирный__
        text = re.sub(r'_([^_]+)_', r'\\1', text)        # _курсив_
        text = re.sub(r'~~([^~]+)~~', r'\\1', text)      # ~~зачеркнутый~~
        text = re.sub(r'```([^`]+)```', r'\\1', text)    # ```код```
        text = re.sub(r'`([^`]+)`', r'\\1', text)        # `код`

        # Удаляем оставшиеся символы
        text = re.sub(r'[*`_~]', '', text)

        # Удаляем лишние пробелы
        text = re.sub(r'\\s+', ' ', text)
        return text.strip()

    '''

            # Вставляем функцию после определения класса
            match = re.search(class_pattern, content, re.DOTALL)
            if match:
                content = content[:match.end()] + clean_function + content[match.end():]

                # Теперь обновляем все места где присваивается full_name
                content = re.sub(
                    r'result\["full_name"\] = name_found',
                    'result["full_name"] = self._clean_markdown(name_found)',
                    content
                )

                logger.info("✅ Добавлена функция _clean_markdown")

    # Также обновляем парсинг даты рождения (на случай если там тоже есть форматирование)
    # Находим место где присваивается birth_date
    content = re.sub(
        r'(result\["birth_date"\] = birth_found)',
        r'result["birth_date"] = re.sub(r\'[*`_~]\', \'\', birth_found) if birth_found else birth_found',
        content
    )

    # Сохраняем обновленный файл
    with open(vk_service_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ Файл services/vk_service.py обновлен")
    return True


def quick_test_markdown_cleaning():
    """Быстрый тест очистки Markdown"""
    import re

    test_cases = [
        ("**Михаил** **Баязитов**", "Михаил Баязитов"),
        ("**Вика** **Пискунова**", "Вика Пискунова"),
        ("**Alina** **Mishina**", "Alina Mishina"),
        ("*Иван* *Петров*", "Иван Петров"),
        ("__Тест__ __Имя__", "Тест Имя"),
        ("**Софья** Кондратьева", "Софья Кондратьева"),
        ("Обычное имя", "Обычное имя"),
    ]

    logger.info("\n🧪 Тестирование очистки Markdown:")

    for original, expected in test_cases:
        # Применяем очистку
        cleaned = re.sub(r'\*\*([^*]+)\*\*', r'\1', original)
        cleaned = re.sub(r'\*([^*]+)\*', r'\1', cleaned)
        cleaned = re.sub(r'__([^_]+)__', r'\1', cleaned)
        cleaned = re.sub(r'_([^_]+)_', r'\1', cleaned)
        cleaned = re.sub(r'[*`_~]', '', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        status = "✅" if cleaned == expected else "❌"
        logger.info(f"{status} '{original}' -> '{cleaned}' (ожидалось: '{expected}')")


def main():
    """Основная функция"""
    logger.info("🚀 Исправление парсинга имен с Markdown форматированием")
    logger.info("=" * 60)

    # Сначала тестируем
    quick_test_markdown_cleaning()

    logger.info("\n" + "=" * 60)

    # Обновляем файл
    if update_vk_service():
        logger.info("\n✅ Исправление применено успешно!")
        logger.info("\n📋 Что сделано:")
        logger.info("1. Добавлена очистка от Markdown символов (**, *, __, _, ~~, ``)")
        logger.info("2. Создана резервная копия: services/vk_service.py.backup_markdown")
        logger.info("\n🔄 Теперь:")
        logger.info("1. Перезапустите бота")
        logger.info("2. Обработайте несколько ссылок")
        logger.info("3. Проверьте, что имена теперь отображаются без ** символов")

        logger.info("\n💡 Если нужно откатить изменения:")
        logger.info("cp services/vk_service.py.backup_markdown services/vk_service.py")
    else:
        logger.error("\n❌ Не удалось применить исправление")
        logger.info("Проверьте структуру файла services/vk_service.py")


if __name__ == "__main__":
    main()