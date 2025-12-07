#!/usr/bin/env python3
"""Тест работы обычной проверки с кешем и принудительного поиска"""

import asyncio
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_cache_and_search():
    """Тестирование работы с кешем и поиска"""
    
    print("=" * 60)
    print("ТЕСТ РАБОТЫ С КЕШЕМ И ПОИСКА")
    print("=" * 60)
    
    # Инициализация сервисов
    from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
    from services.vk_multibot_service import VKMultiBotService
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
    
    # Тестовые ссылки
    test_links = [
        "https://vk.com/id1",        # Должен быть в кеше
        "https://vk.com/durov",      # Должен быть в кеше
        "https://vk.com/id999999999" # Новая ссылка для теста
    ]
    
    print(f"\n📋 Тестовые ссылки: {len(test_links)}")
    print("-" * 60)
    
    # 1. ТЕСТ ОБЫЧНОЙ ПРОВЕРКИ С КЕШЕМ
    print("\n1️⃣ ОБЫЧНАЯ ПРОВЕРКА (с кешем):")
    print("-" * 40)
    
    # Проверяем кеш
    cached_results = await db.get_cached_results(test_links)
    print(f"📦 В кеше найдено: {len(cached_results)} из {len(test_links)} ссылок")
    
    for link, data in cached_results.items():
        has_data = bool(data.get('phones') or data.get('full_name'))
        print(f"   • {link}: {'✅ есть данные' if has_data else '⚠️ пустой результат'}")
    
    # Определяем что нужно искать
    links_to_search = [link for link in test_links if link not in cached_results]
    print(f"\n🔍 Нужно искать: {len(links_to_search)} ссылок")
    
    # Поиск новых
    for link in links_to_search:
        print(f"   Поиск: {link}")
        result = await vk_service.search_vk_data(link)
        
        # Сохраняем в БД
        await db.save_result(
            link=link,
            result_data=result,
            user_id=123456789,
            source="test_normal"
        )
        
        if result.get('full_name'):
            print(f"   ✅ Найдено: {result['full_name']}")
        else:
            print(f"   ❌ Данные не найдены")
    
    # 2. ТЕСТ ПРИНУДИТЕЛЬНОГО ПОИСКА (без кеша)
    print("\n2️⃣ ПРИНУДИТЕЛЬНЫЙ ПОИСК (без кеша):")
    print("-" * 40)
    
    force_search_results = {}
    for i, link in enumerate(test_links, 1):
        print(f"{i}. Принудительный поиск: {link}")
        
        # Всегда ищем, игнорируя кеш
        result = await vk_service.search_vk_data(link)
        force_search_results[link] = result
        
        # Обновляем в БД
        await db.save_result(
            link=link,
            result_data=result,
            user_id=123456789,
            source="test_force"
        )
        
        if result.get('full_name'):
            phones = result.get('phones', [])
            print(f"   ✅ Найдено: {result['full_name']}")
            if phones:
                print(f"   📱 Телефоны: {', '.join(phones[:3])}")
        else:
            print(f"   ❌ Данные не найдены")
        
        # Небольшая задержка
        await asyncio.sleep(1.5)
    
    # 3. СТАТИСТИКА
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА ТЕСТИРОВАНИЯ:")
    print("-" * 60)
    
    print(f"\n🔍 Обычная проверка:")
    print(f"   • Из кеша: {len(cached_results)}")
    print(f"   • Новых поисков: {len(links_to_search)}")
    
    print(f"\n⚡ Принудительный поиск:")
    print(f"   • Всего проверено: {len(force_search_results)}")
    print(f"   • С данными: {sum(1 for r in force_search_results.values() if r.get('full_name'))}")
    
    # Проверка стабильности ботов
    print(f"\n🤖 Статус ботов:")
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
    result = asyncio.run(test_cache_and_search())
    exit(0 if result else 1)
