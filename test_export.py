#!/usr/bin/env python3
"""
Тестирование экспорта в Excel с новыми полями
"""

import asyncio
import pandas as pd
from pathlib import Path
from bot.utils.export import create_excel_from_results

async def test_export():
    """Тест экспорта с полными данными"""
    
    # Тестовые данные с полными полями
    test_results = {
        "https://vk.com/id1": {
            "phones": ["79161925982", "79219550020", "79539048549"],
            "full_name": "Павел Дуров",
            "birth_date": "10.10.1984"
        },
        "https://vk.com/id2": {
            "phones": ["79046304919", "79211841331"],
            "full_name": "Александра Владимирова",
            "birth_date": "14.2"
        },
        "https://vk.com/id100": {
            "phones": [],
            "full_name": "ВКонтакте",
            "birth_date": ""
        }
    }
    
    links_order = list(test_results.keys())
    
    # Создаем Excel файл
    files = await create_excel_from_results(test_results, links_order)
    
    if files:
        file_path, caption = files[0]
        print(f"✅ Файл создан: {file_path}")
        print(f"📝 Описание: {caption}")
        
        # Читаем и проверяем содержимое
        df = pd.read_excel(file_path)
        print("\n📊 Содержимое файла:")
        print(df.to_string())
        
        # Проверяем наличие всех колонок
        expected_columns = ["Ссылка VK", "ФИО", "Дата рождения"]
        for col in expected_columns:
            if col in df.columns:
                print(f"✅ Колонка '{col}' присутствует")
            else:
                print(f"❌ Колонка '{col}' отсутствует!")
        
        # Проверяем данные
        for idx, row in df.iterrows():
            link = row["Ссылка VK"]
            expected = test_results[link]
            
            if "ФИО" in df.columns:
                actual_name = row.get("ФИО", "")
                expected_name = expected.get("full_name", "")
                if actual_name == expected_name:
                    print(f"✅ {link}: ФИО совпадает ({actual_name})")
                else:
                    print(f"❌ {link}: ФИО не совпадает! Ожидалось: {expected_name}, получено: {actual_name}")
            
            if "Дата рождения" in df.columns:
                actual_birth = str(row.get("Дата рождения", ""))
                if actual_birth == "nan":
                    actual_birth = ""
                expected_birth = expected.get("birth_date", "")
                if actual_birth == expected_birth:
                    print(f"✅ {link}: Дата рождения совпадает ({actual_birth})")
                else:
                    print(f"❌ {link}: Дата не совпадает! Ожидалось: {expected_birth}, получено: {actual_birth}")
        
        return file_path
    else:
        print("❌ Файл не создан")
        return None

if __name__ == "__main__":
    file_path = asyncio.run(test_export())
    if file_path:
        print(f"\n📁 Файл сохранен: {file_path}")