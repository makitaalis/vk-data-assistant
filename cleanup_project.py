#!/usr/bin/env python3
"""
Скрипт для очистки и настройки проекта VK Data Assistant Bot
Удаляет старые файлы, связанные с пулом ботов
"""

import os
import shutil
from pathlib import Path


def cleanup_old_files():
    """Удаление старых файлов"""

    files_to_remove = [
        'vk_worker.py',  # Старый файл с пулом ботов
        'fix_all_issues.py',  # Временный фикс
        'excel_processor.py',  # Дубликат
        'update_db_imports.py',  # Временный скрипт
        '*.backup',  # Бэкап файлы
        'vk_worker.py.backup',
    ]

    folders_to_clean = [
        '__pycache__',
        '.pytest_cache',
        '.mypy_cache',
    ]

    print("🧹 Очистка проекта от старых файлов...")

    project_root = Path.cwd()

    # Удаляем файлы
    for pattern in files_to_remove:
        if '*' in pattern:
            # Паттерн с wildcard
            for file in project_root.glob(pattern):
                if file.is_file():
                    file.unlink()
                    print(f"  ✅ Удален: {file.name}")
        else:
            # Конкретный файл
            file_path = project_root / pattern
            if file_path.exists():
                file_path.unlink()
                print(f"  ✅ Удален: {pattern}")

    # Очищаем папки кеша
    for folder_name in folders_to_clean:
        for folder in project_root.rglob(folder_name):
            if folder.is_dir():
                shutil.rmtree(folder)
                print(f"  ✅ Очищена папка: {folder}")

    print("\n✅ Очистка завершена!")


def create_structure():
    """Создание правильной структуры проекта"""

    required_dirs = [
        'data',
        'data/temp',
        'debug',
        'logs',
        'bot',
        'bot/handlers',
        'bot/keyboards',
        'bot/utils',
        'bot/middleware',
        'services',
        'db_module',
    ]

    print("\n📁 Создание структуры проекта...")

    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        dir_path.mkdir(exist_ok=True, parents=True)

        # Создаем __init__.py для пакетов Python
        if dir_name in ['bot', 'bot/handlers', 'bot/keyboards', 'bot/utils', 'bot/middleware', 'services', 'db_module']:
            init_file = dir_path / '__init__.py'
            if not init_file.exists():
                init_file.touch()
                print(f"  ✅ Создан: {init_file}")


def check_env():
    """Проверка .env файла"""

    print("\n🔍 Проверка конфигурации...")

    env_file = Path('.env')

    if not env_file.exists():
        print("  ❌ Файл .env не найден!")
        print("  ℹ️  Создайте .env файл на основе примера выше")
        return False

    # Проверяем наличие VK_BOT_USERNAME
    with open(env_file, 'r') as f:
        content = f.read()

    if 'VK_BOT_USERNAME=' not in content:
        print("  ⚠️  В .env отсутствует VK_BOT_USERNAME")
        print("  ℹ️  Добавьте строку: VK_BOT_USERNAME=eye_of_god_bot")
        return False

    print("  ✅ Конфигурация корректна")
    return True


def main():
    """Основная функция"""

    print("🚀 Настройка проекта VK Data Assistant Bot")
    print("=" * 50)

    # Очистка старых файлов
    cleanup_old_files()

    # Создание структуры
    create_structure()

    # Проверка конфигурации
    env_ok = check_env()

    print("\n" + "=" * 50)

    if env_ok:
        print("✅ Проект готов к работе!")
        print("\nДля запуска выполните:")
        print("  python run.py")
    else:
        print("⚠️  Требуется настройка .env файла!")
        print("\nПосле настройки запустите:")
        print("  python run.py")


if __name__ == "__main__":
    main()