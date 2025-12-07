#!/usr/bin/env python3
"""Тест обработки таймаутов и зависших поисков"""

import asyncio
import logging
from bot.config import VK_BOT_USERNAMES, API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
from services.vk_multibot_service import VKMultiBotService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_timeout_handling():
    """Тестирование обработки таймаутов"""
    
    print("=" * 60)
    print("ТЕСТ ОБРАБОТКИ ЗАВИСШИХ ПОИСКОВ")
    print("=" * 60)
    
    # Инициализация сервиса
    service = VKMultiBotService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    initialized = await service.initialize_with_session_auth()
    
    print(f"\n✅ Инициализировано {initialized} ботов")
    
    # Тестовые ссылки (включая несуществующие для проверки таймаутов)
    test_links = [
        "https://vk.com/id1",  # Обычный профиль
        "https://vk.com/id999999999999",  # Несуществующий профиль
        "https://vk.com/durov",  # Известный профиль
        "https://vk.com/id_that_does_not_exist_12345",  # Гарантированно несуществующий
    ]
    
    print(f"\n📋 Тестирование {len(test_links)} ссылок:")
    print("-" * 60)
    
    results = []
    for i, link in enumerate(test_links, 1):
        print(f"\n{i}. Тестирование: {link}")
        
        # Засекаем время
        start_time = asyncio.get_event_loop().time()
        
        # Выполняем поиск
        result = await service.search_vk_data(link)
        
        # Считаем время
        elapsed = asyncio.get_event_loop().time() - start_time
        
        # Анализируем результат
        if result.get("error"):
            status = f"❌ Ошибка: {result['error']}"
        elif result.get("full_name"):
            status = f"✅ Найдено: {result['full_name']}"
        else:
            status = "⚠️ Данные не найдены"
        
        print(f"   Статус: {status}")
        print(f"   Время: {elapsed:.2f} сек")
        
        # Сохраняем результат
        results.append({
            "link": link,
            "status": status,
            "time": elapsed,
            "error": result.get("error")
        })
    
    # Статистика по ботам
    print("\n" + "=" * 60)
    print("СТАТИСТИКА БОТОВ:")
    print("-" * 60)
    
    for i, bot in enumerate(service.bots, 1):
        if bot.is_initialized:
            print(f"\n{i}. @{bot.username}:")
            print(f"   Доступен: {'✅' if bot.is_available else '❌'}")
            print(f"   Запросов: {bot.requests_count}")
            print(f"   Ошибок: {bot.errors_count}")
            print(f"   Лимит: {'❌ Достигнут' if bot.limit_reached else '✅ Не достигнут'}")
    
    # Общая статистика
    print("\n" + "=" * 60)
    print("ОБЩАЯ СТАТИСТИКА:")
    print("-" * 60)
    
    total_requests = len(results)
    successful = sum(1 for r in results if "✅" in r["status"])
    failed = sum(1 for r in results if r.get("error"))
    avg_time = sum(r["time"] for r in results) / len(results)
    
    print(f"\nВсего запросов: {total_requests}")
    print(f"Успешных: {successful} ({successful/total_requests*100:.1f}%)")
    print(f"С ошибками: {failed} ({failed/total_requests*100:.1f}%)")
    print(f"Среднее время: {avg_time:.2f} сек")
    
    # Проверка таймаутов
    timeouts = [r for r in results if r.get("error") == "timeout"]
    if timeouts:
        print(f"\n⏱ Таймауты: {len(timeouts)} запросов")
        for t in timeouts:
            print(f"   - {t['link']} ({t['time']:.2f} сек)")
    
    # Закрываем соединения
    await service.close()
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_timeout_handling())
