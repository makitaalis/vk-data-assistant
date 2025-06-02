#!/usr/bin/env python3
"""
Скрипт для исправления импортов в bot_main.py
"""

import re


def fix_bot_main():
    """Исправляет импорты и другие проблемы в bot_main.py"""

    with open('bot_main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Удаляем дублирующиеся импорты
    content = content.replace('from aiogram.methods.set_my_commands import SetMyCommands\n', '')

    # Удаляем неиспользуемые импорты
    content = content.replace('from collections import defaultdict, OrderedDict', 'from collections import OrderedDict')

    # Исправляем все Path на pathlib.Path
    content = re.sub(r'\bPath\(', 'pathlib.Path(', content)

    # Удаляем ненужные импорты внутри функций
    content = content.replace('from db_loader import DatabaseLoader\n    ', '')
    content = content.replace('from excel_processor import ExcelProcessor\n    ', '')

    # Сохраняем исправленный файл
    with open('bot_main_fixed.py', 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ Файл исправлен и сохранен как bot_main_fixed.py")
    print("Переименуйте его в bot_main.py после проверки")


if __name__ == "__main__":
    fix_bot_main()