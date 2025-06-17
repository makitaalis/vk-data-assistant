#!/usr/bin/env python3
"""
Полная диагностика проблемы с most_common
"""

import os
import sys
import ast
import importlib
import traceback
from pathlib import Path


def find_problem_in_file(filepath):
    """Анализирует файл на наличие проблем"""
    problems = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Проверка 1: Проблемная строка лога
        if "VK ссылки в" in content and "{len(" in content:
            line_no = content[:content.find("VK ссылки в")].count('\n') + 1
            problems.append(f"Строка {line_no}: Найдена проблемная строка с 'VK ссылки в'")

        # Проверка 2: Использование most_common
        if ".most_common" in content:
            # Находим все вхождения
            import re
            for match in re.finditer(r'(\w+)\.most_common', content):
                var_name = match.group(1)
                line_no = content[:match.start()].count('\n') + 1
                problems.append(f"Строка {line_no}: Используется {var_name}.most_common()")

        # Проверка 3: Отсутствие импорта Counter при использовании most_common
        if ".most_common" in content and "from collections import Counter" not in content:
            problems.append("Используется most_common без импорта Counter")

    except Exception as e:
        problems.append(f"Ошибка при анализе файла: {e}")

    return problems


def analyze_imports():
    """Анализирует систему импортов"""
    print("\n📦 Анализ импортов...")

    try:
        # Очищаем кеш импортов
        if 'services.excel_service' in sys.modules:
            del sys.modules['services.excel_service']
        if 'services' in sys.modules:
            del sys.modules['services']

        # Импортируем заново
        import services.excel_service

        print(f"✅ Импортирован файл: {services.excel_service.__file__}")

        # Проверяем наличие Counter
        if hasattr(services.excel_service, 'Counter'):
            print("✅ Counter доступен в модуле")
        else:
            print("❌ Counter НЕ импортирован в модуле")

    except Exception as e:
        print(f"❌ Ошибка при импорте: {e}")
        traceback.print_exc()


def find_all_excel_related_files():
    """Находит все файлы связанные с обработкой Excel"""
    print("\n🔍 Поиск всех связанных файлов...")

    patterns = ['*excel*', '*Excel*', '*loader*', '*process*']
    related_files = set()

    for pattern in patterns:
        for file in Path('.').rglob(f"{pattern}.py"):
            if '__pycache__' not in str(file):
                related_files.add(file)

    return sorted(related_files)


def trace_execution_path():
    """Трассировка пути выполнения"""
    print("\n🔄 Трассировка пути выполнения...")

    # Симулируем загрузку Excel
    try:
        from services.excel_service import ExcelProcessor
        processor = ExcelProcessor()

        print("✅ ExcelProcessor создан успешно")

        # Проверяем методы
        methods = [m for m in dir(processor) if not m.startswith('_')]
        print(f"📋 Доступные методы: {', '.join(methods)}")

    except Exception as e:
        print(f"❌ Ошибка при создании ExcelProcessor: {e}")
        traceback.print_exc()


def check_cache_files():
    """Проверка всех кеш файлов"""
    print("\n💾 Проверка кеша...")

    cache_files = []
    for ext in ['*.pyc', '*.pyo']:
        cache_files.extend(Path('.').rglob(ext))

    if cache_files:
        print(f"⚠️ Найдено {len(cache_files)} кеш-файлов:")
        for cf in cache_files[:5]:  # Показываем первые 5
            print(f"  - {cf}")
        if len(cache_files) > 5:
            print(f"  ... и еще {len(cache_files) - 5} файлов")
    else:
        print("✅ Кеш-файлы не найдены")

    # Проверяем __pycache__ директории
    pycache_dirs = list(Path('.').rglob('__pycache__'))
    if pycache_dirs:
        print(f"\n⚠️ Найдено {len(pycache_dirs)} __pycache__ директорий")


def deep_analysis():
    """Глубокий анализ проблемы"""
    print("\n🔬 ГЛУБОКИЙ АНАЛИЗ ПРОБЛЕМЫ")
    print("=" * 60)

    # 1. Ищем все связанные файлы
    related_files = find_all_excel_related_files()
    print(f"\n📁 Найдено {len(related_files)} связанных файлов:")

    all_problems = {}

    for file in related_files:
        print(f"\n📄 Анализ {file}:")
        problems = find_problem_in_file(file)

        if problems:
            all_problems[str(file)] = problems
            print(f"  ❌ Найдено проблем: {len(problems)}")
            for p in problems:
                print(f"    • {p}")
        else:
            print("  ✅ Проблем не найдено")

    # 2. Анализ импортов
    analyze_imports()

    # 3. Проверка кеша
    check_cache_files()

    # 4. Трассировка
    trace_execution_path()

    # 5. Итоговые рекомендации
    print("\n" + "=" * 60)
    print("📊 ИТОГОВЫЙ АНАЛИЗ:")

    if all_problems:
        print(f"\n❌ Найдены проблемы в {len(all_problems)} файлах:")
        for file, problems in all_problems.items():
            print(f"\n{file}:")
            for p in problems:
                print(f"  • {p}")
    else:
        print("\n⚠️ Явных проблем в коде не найдено.")
        print("Вероятно, проблема в кешировании или в runtime.")

    print("\n💡 РЕКОМЕНДАЦИИ:")
    print("1. Полностью остановите все Python процессы:")
    print("   pkill -f python")
    print("2. Удалите ВСЕ кеш файлы:")
    print("   find . -type f \\( -name '*.pyc' -o -name '*.pyo' \\) -delete")
    print("   find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null")
    print("3. Перезапустите PyCharm")
    print("4. Запустите бота заново")


def create_test_excel():
    """Создает тестовый Excel файл для проверки"""
    print("\n📝 Создание тестового файла...")

    try:
        import pandas as pd

        # Создаем простой тестовый файл
        data = {
            'VK Links': [
                'https://vk.com/id123456',
                'https://vk.com/id234567',
                'https://vk.com/id345678'
            ],
            'Name': ['Test1', 'Test2', 'Test3']
        }

        df = pd.DataFrame(data)
        test_file = Path('test_vk_links.xlsx')
        df.to_excel(test_file, index=False)

        print(f"✅ Создан тестовый файл: {test_file}")
        print("   Попробуйте загрузить его в бота для проверки")

    except Exception as e:
        print(f"❌ Не удалось создать тестовый файл: {e}")


if __name__ == "__main__":
    print("🚀 ПОЛНАЯ ДИАГНОСТИКА ПРОБЛЕМЫ")
    print("=" * 60)

    # Запускаем полный анализ
    deep_analysis()

    # Создаем тестовый файл
    create_test_excel()

    print("\n" + "=" * 60)
    print("✅ Диагностика завершена!")
    print("\nЕсли проблемы найдены, следуйте рекомендациям выше.")
    print("Если нет - отправьте результаты диагностики для дальнейшего анализа.")