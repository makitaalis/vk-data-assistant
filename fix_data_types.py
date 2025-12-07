#!/usr/bin/env python3
"""
Быстрое исправление проблемы с типами данных при работе с PostgreSQL JSONB
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_data_types")


def fix_database_py():
    """Исправляет database.py для правильной работы с JSONB"""

    database_file = Path("database.py")

    if not database_file.exists():
        logger.error("Файл database.py не найден!")
        return False

    with open(database_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Backup
    with open("database.py.original", 'w', encoding='utf-8') as f:
        f.write(content)

    # Исправление 1: get_cached_results
    old_code = '''            for row in rows:
                results[row["link"]] = {
                    "phones": row["phones"] or [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                }'''

    new_code = '''            for row in rows:
                # Обработка phones - PostgreSQL JSONB возвращает уже распарсенный список
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    # На всякий случай, если вернулась строка
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                results[row["link"]] = {
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                }'''

    content = content.replace(old_code, new_code)

    # Исправление 2: check_duplicates_extended - аналогично
    old_pattern = '''link_data = {
                    "link": row["link"],
                    "phones": row["phones"] or [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "found_data": row["found_data"]
                }'''

    new_pattern = '''# Обработка phones из JSONB
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                link_data = {
                    "link": row["link"],
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "found_data": row["found_data"]
                }'''

    content = content.replace(old_pattern, new_pattern)

    # Исправление 3: find_links_by_phone
    old_find = '''results.append({
                    "link": row["link"],
                    "phones": row["phones"] or [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "checked_at": row["checked_at"]
                })'''

    new_find = '''# Обработка phones из JSONB
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                results.append({
                    "link": row["link"],
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "checked_at": row["checked_at"]
                })'''

    content = content.replace(old_find, new_find)

    # Сохраняем исправленный файл
    with open(database_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ database.py исправлен")
    return True


def fix_export_py():
    """Исправляет export.py для корректной обработки данных"""

    export_file = Path("bot/utils/export.py")

    if not export_file.exists():
        logger.error("Файл bot/utils/export.py не найден!")
        return False

    with open(export_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Backup
    with open("bot/utils/export.py.original", 'w', encoding='utf-8') as f:
        f.write(content)

    # Добавляем импорт json если его нет
    if "import json" not in content:
        content = "import json\n" + content

    # Исправляем обработку данных в create_excel_from_results
    old_extract = '''            # Извлекаем данные
            phones = result_data.get("phones", [])
            full_name = result_data.get("full_name", "")
            birth_date = result_data.get("birth_date", "")'''

    new_extract = '''            # Извлекаем данные с проверкой типов
            phones = result_data.get("phones", [])

            # Обработка phones - убедимся что это список
            if phones is None:
                phones = []
            elif isinstance(phones, str):
                # Если строка, пробуем распарсить JSON
                if phones.startswith('['):
                    try:
                        phones = json.loads(phones)
                    except:
                        phones = []
                else:
                    # Если просто строка с номером
                    phones = [phones] if phones else []
            elif not isinstance(phones, list):
                phones = []

            # Убедимся что элементы списка - строки
            phones = [str(p) for p in phones if p]

            full_name = result_data.get("full_name", "")
            birth_date = result_data.get("birth_date", "")

            # Преобразуем в строки, обрабатывая None
            full_name = str(full_name) if full_name is not None else ""
            birth_date = str(birth_date) if birth_date is not None else ""'''

    content = content.replace(old_extract, new_extract)

    # Аналогичное исправление для файла с найденными данными
    old_found = '''if data.get("phones") or data.get("full_name") or data.get("birth_date"):
                    phones = data.get("phones", [])
                    row_data = {'''

    new_found = '''if data.get("phones") or data.get("full_name") or data.get("birth_date"):
                    phones = data.get("phones", [])

                    # Обработка phones
                    if phones is None:
                        phones = []
                    elif isinstance(phones, str):
                        if phones.startswith('['):
                            try:
                                phones = json.loads(phones)
                            except:
                                phones = []
                        else:
                            phones = [phones] if phones else []
                    elif not isinstance(phones, list):
                        phones = []

                    phones = [str(p) for p in phones if p]

                    row_data = {'''

    content = content.replace(old_found, new_found)

    # Сохраняем исправленный файл
    with open(export_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ bot/utils/export.py исправлен")
    return True


def main():
    """Основная функция"""
    logger.info("🚀 Применение исправлений для корректной работы с данными")

    # Исправляем файлы
    success = True

    if not fix_database_py():
        success = False

    if not fix_export_py():
        success = False

    if success:
        logger.info("\n✅ Все исправления применены успешно!")
        logger.info("\nТеперь:")
        logger.info("1. Перезапустите бота")
        logger.info("2. Попробуйте обработать несколько ссылок")
        logger.info("3. Проверьте выходной Excel файл")
        logger.info("\nЕсли проблема сохраняется, используйте диагностические версии файлов")
    else:
        logger.error("\n❌ Некоторые файлы не удалось исправить")
        logger.info("Проверьте структуру проекта и пути к файлам")


if __name__ == "__main__":
    main()