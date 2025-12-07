#!/usr/bin/env python3
"""
Скрипт для настройки и очистки проекта VK Data Assistant Bot
"""

import os
import shutil
import sys
from pathlib import Path


def cleanup_project():
    """Очистка проекта от лишних файлов"""

    project_root = Path(__file__).parent

    # Файлы для удаления
    files_to_remove = [
        'update_db_imports.py',
        'excel_processor.py',  # Дубликат services/excel_service.py
    ]

    # Папки для очистки
    folders_to_clean = [
        '__pycache__',
        '.pytest_cache',
        '.mypy_cache',
    ]

    print("🧹 Очистка проекта...")

    # Удаляем файлы
    for file_name in files_to_remove:
        file_path = project_root / file_name
        if file_path.exists():
            file_path.unlink()
            print(f"  ✅ Удален: {file_name}")

    # Очищаем папки
    for folder_name in folders_to_clean:
        for folder in project_root.rglob(folder_name):
            if folder.is_dir():
                shutil.rmtree(folder)
                print(f"  ✅ Очищена папка: {folder}")

    # Создаем необходимые директории если их нет
    required_dirs = [
        'data',
        'data/temp',
        'debug',
        'logs',
    ]

    for dir_name in required_dirs:
        dir_path = project_root / dir_name
        dir_path.mkdir(exist_ok=True, parents=True)
        print(f"  ✅ Создана/проверена папка: {dir_name}")

    # Проверяем наличие .env файла
    env_file = project_root / '.env'
    env_example = project_root / '.env.example'

    if not env_file.exists() and env_example.exists():
        shutil.copy(env_example, env_file)
        print("  ⚠️  Создан .env файл из .env.example - проверьте настройки!")

    print("\n✅ Очистка завершена!")


def check_dependencies():
    """Проверка зависимостей"""
    print("\n📦 Проверка зависимостей...")

    try:
        import aiogram
        import telethon
        import pandas
        import asyncpg
        import redis
        print("  ✅ Все основные зависимости установлены")
    except ImportError as e:
        print(f"  ❌ Отсутствует зависимость: {e}")
        print("  Выполните: pip install -r requirements.txt")
        return False

    return True


def check_config():
    """Проверка конфигурации"""
    print("\n⚙️ Проверка конфигурации...")

    env_file = Path('.env')
    if not env_file.exists():
        print("  ❌ Файл .env не найден!")
        return False

    # Проверяем ключевые переменные
    required_vars = [
        'BOT_TOKEN',
        'API_ID',
        'API_HASH',
        'POSTGRES_HOST',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD'
    ]

    from dotenv import dotenv_values
    config = dotenv_values('.env')

    missing = []
    for var in required_vars:
        if not config.get(var):
            missing.append(var)

    if missing:
        print(f"  ❌ Отсутствуют переменные: {', '.join(missing)}")
        return False

    print("  ✅ Конфигурация корректна")
    return True


def main():
    """Основная функция"""
    print("🚀 Настройка проекта VK Data Assistant Bot\n")

    # Очистка проекта
    cleanup_project()

    # Проверка зависимостей
    if not check_dependencies():
        print("\n❌ Установите недостающие зависимости!")
        sys.exit(1)

    # Проверка конфигурации
    if not check_config():
        print("\n❌ Настройте файл .env!")
        sys.exit(1)

    print("\n✅ Проект готов к запуску!")
    print("\nДля запуска бота выполните:")
    print("  python run.py")
    print("\nИли используйте Docker:")
    print("  docker-compose up -d")


if __name__ == "__main__":
    main()