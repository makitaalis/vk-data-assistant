#!/usr/bin/env python3
"""
Утилита для анализа структуры Excel файлов перед загрузкой в БД
"""

import pandas as pd
import sys
import json
from pathlib import Path
from db_loader import DatabaseLoader


def analyze_file(file_path: str):
    """Анализирует Excel файл и показывает его структуру"""
    path = Path(file_path)

    if not path.exists():
        print(f"❌ Файл не найден: {file_path}")
        return

    print(f"\n📊 Анализ файла: {path.name}")
    print("=" * 60)

    # Создаем загрузчик
    loader = DatabaseLoader(None)

    # Анализируем структуру
    analysis = loader.analyze_excel_structure(path)

    if "error" in analysis:
        print(f"❌ Ошибка: {analysis['error']}")
        return

    print(f"📁 Файл: {analysis['file_name']}")
    print(f"📊 Строк: {analysis['total_rows']}")
    print(f"📊 Колонок: {analysis['total_columns']}")
    print(f"🔗 Уникальных VK ссылок: {analysis['total_unique_vk_links']}")
    print(f"📱 Уникальных телефонов: {analysis['total_unique_phones']}")

    print("\n📝 Анализ первых строк:")
    for preview in analysis['data_preview']:
        print(f"\n  Строка {preview['row']}:")
        if preview['vk_links']:
            print(f"    🔗 VK ссылки: {', '.join(preview['vk_links'])}")
        if preview['phones']:
            print(f"    📱 Телефоны: {', '.join(preview['phones'])}")
        if preview['full_name']:
            print(f"    👤 Имя: {preview['full_name']}")
        if preview['birth_date']:
            print(f"    🎂 Дата рождения: {preview['birth_date']}")

    # Пробуем обработать файл
    print("\n🔄 Полная обработка файла...")
    records, stats = loader.process_excel_file(path)

    print(f"\n📊 Результаты обработки:")
    print(f"  Строк с VK ссылками: {stats['rows_with_vk_links']}")
    print(f"  Строк с телефонами: {stats['rows_with_phones']}")
    print(f"  Всего VK ссылок: {stats['total_vk_links']}")
    print(f"  Всего телефонов: {stats['total_phones']}")

    # Анализ связей
    print("\n🔍 Анализ связей...")
    network_data = loader.find_all_related_data(records)

    print(f"\n📊 Найденные связи:")
    print(f"  Телефонов с несколькими VK: {network_data['stats']['phones_with_multiple_vk']}")
    print(f"  VK с несколькими телефонами: {network_data['stats']['vk_with_multiple_phones']}")

    # Показываем примеры связей
    if network_data['stats']['phones_with_multiple_vk'] > 0:
        print("\n📱 Примеры телефонов с несколькими VK профилями:")
        count = 0
        for phone, data in network_data['phone_network'].items():
            if len(data['vk_links']) > 1:
                print(f"  {phone}: {len(data['vk_links'])} профилей")
                for vk in data['vk_links'][:3]:
                    print(f"    - {vk}")
                count += 1
                if count >= 3:
                    break

    # Сохраняем детальный отчет
    report_path = path.parent / f"{path.stem}_analysis.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump({
            "analysis": analysis,
            "stats": stats,
            "network": network_data
        }, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Детальный отчет сохранен в: {report_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python analyze_excel.py <путь_к_файлу.xlsx>")
        sys.exit(1)

    analyze_file(sys.argv[1])