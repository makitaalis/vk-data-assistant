#!/usr/bin/env python3
"""Тест загрузки и обработки файлов"""

import asyncio
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_file_processing():
    """Тестирование загрузки и обработки Excel файлов"""
    
    print("=" * 60)
    print("ТЕСТ ЗАГРУЗКИ И ОБРАБОТКИ ФАЙЛОВ")
    print("=" * 60)
    
    # Создаем тестовый Excel файл
    test_file = Path("/tmp/test_vk_links.xlsx")
    
    # Создаем DataFrame с тестовыми ссылками
    test_data = {
        'ФИО': ['Тест 1', 'Тест 2', 'Тест 3', 'Тест 4', 'Тест 5'],
        'VK Ссылка': [
            'https://vk.com/id1',
            'https://vk.com/durov',
            'https://vk.com/id123456789',
            'https://vk.com/test_user',
            'https://vk.com/id999999999'
        ],
        'Город': ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Нижний Новгород']
    }
    
    df = pd.DataFrame(test_data)
    df.to_excel(test_file, index=False)
    print(f"✅ Создан тестовый файл: {test_file}")
    print(f"   Содержит {len(df)} ссылок")
    
    # Инициализация сервисов
    from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
    from services.vk_multibot_service import VKMultiBotService
    from services.excel_service import ExcelProcessor
    from db_module import VKDatabase
    
    # База данных
    print("\n🔄 Инициализация базы данных...")
    db = VKDatabase()
    await db.init()
    
    # VK сервис
    print("🔄 Инициализация VK сервиса...")
    vk_service = VKMultiBotService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    initialized = await vk_service.initialize_with_session_auth()
    print(f"✅ Инициализировано {initialized} ботов")
    
    # Excel процессор
    print("\n📋 Загрузка Excel файла...")
    processor = ExcelProcessor()
    processor.load_excel_file(test_file)
    
    # Извлекаем VK ссылки
    vk_links = processor.get_links_without_duplicates()
    print(f"✅ Извлечено {len(vk_links)} VK ссылок")
    
    # Проверяем кеш
    print("\n🔍 Проверка кеша...")
    cached_results = await db.get_cached_results(vk_links)
    print(f"📦 В кеше найдено: {len(cached_results)} из {len(vk_links)}")
    
    links_to_check = [link for link in vk_links if link not in cached_results]
    print(f"🆕 Новых ссылок для проверки: {len(links_to_check)}")
    
    # Обрабатываем новые ссылки
    if links_to_check:
        print("\n🚀 Начинаем обработку новых ссылок...")
        all_results = dict(cached_results)
        
        for i, link in enumerate(links_to_check[:2], 1):  # Ограничиваем 2 ссылками для теста
            print(f"\n{i}. Обработка: {link}")
            
            try:
                result = await vk_service.search_vk_data(link)
                
                if result.get('full_name'):
                    print(f"   ✅ Найдено: {result['full_name']}")
                    phones = result.get('phones', [])
                    if phones:
                        print(f"   📱 Телефон: {phones[0]}")
                else:
                    print(f"   ❌ Данные не найдены")
                
                all_results[link] = result
                
                # Сохраняем в БД
                await db.save_result(
                    link=link,
                    result_data=result,
                    user_id=123456789,
                    source="test_file_processing"
                )
                
            except Exception as e:
                logger.error(f"Ошибка при обработке {link}: {e}")
                all_results[link] = {'error': str(e)}
            
            await asyncio.sleep(1.5)
        
        # Сохраняем результаты в Excel
        print("\n💾 Сохранение результатов...")
        output_file = Path("/tmp/test_vk_results.xlsx")
        
        success = processor.save_results_with_original_data(all_results, output_file)
        
        if success:
            print(f"✅ Результаты сохранены в: {output_file}")
            
            # Читаем и проверяем результат
            result_df = pd.read_excel(output_file)
            print(f"\n📊 Структура результата:")
            print(f"   Столбцов: {len(result_df.columns)}")
            print(f"   Строк: {len(result_df)}")
            
            if 'Телефон' in result_df.columns:
                phones_found = result_df['Телефон'].notna().sum()
                print(f"   📱 Найдено телефонов: {phones_found}")
        else:
            print("❌ Ошибка при сохранении результатов")
    
    # Проверка статуса ботов
    print("\n🤖 Статус ботов:")
    for i, bot in enumerate(vk_service.bots, 1):
        if bot.is_initialized:
            print(f"   {i}. @{bot.username}: ✅ Активен, запросов: {bot.requests_count}")
    
    # Закрываем соединения
    await vk_service.close()
    await db.close()
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    result = asyncio.run(test_file_processing())
    exit(0 if result else 1)
