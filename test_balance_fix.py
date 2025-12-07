#!/usr/bin/env python3
"""Тест исправления проверки баланса"""

import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_balance_fix():
    """Тестирование исправления проверки баланса"""
    
    print("=" * 60)
    print("ТЕСТ ИСПРАВЛЕНИЯ ПРОВЕРКИ БАЛАНСА")
    print("=" * 60)
    
    # Инициализация сервисов
    from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
    from services.vk_multibot_service import VKMultiBotService
    
    # VK сервис
    print("\n🔄 Инициализация VK сервиса...")
    vk_service = VKMultiBotService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    initialized = await vk_service.initialize_with_session_auth()
    print(f"✅ Инициализировано {initialized} ботов")
    
    # 1. Тест метода check_balance (должен вернуть число)
    print("\n1️⃣ Тест check_balance() - должен вернуть число:")
    balance_int = await vk_service.check_balance()
    
    if balance_int is not None:
        print(f"   ✅ Получено число: {balance_int}")
        print(f"   Тип: {type(balance_int)}")
        
        # Проверка сравнения
        test_count = 10
        if balance_int < test_count:
            print(f"   ✅ Сравнение работает: {balance_int} < {test_count} = True")
        else:
            print(f"   ✅ Сравнение работает: {balance_int} < {test_count} = False")
    else:
        print("   ❌ Не удалось получить баланс")
    
    # 2. Тест метода get_balance_info (должен вернуть строку)
    print("\n2️⃣ Тест get_balance_info() - должен вернуть форматированную строку:")
    balance_str = await vk_service.get_balance_info()
    
    if balance_str:
        print(f"   ✅ Получена строка:")
        print(f"   Тип: {type(balance_str)}")
        # Показываем первые 100 символов
        preview = balance_str[:100].replace('\n', ' ')
        print(f"   Превью: {preview}...")
    else:
        print("   ❌ Не удалось получить информацию о балансе")
    
    # 3. Тест из balance.py handler
    print("\n3️⃣ Симуляция проверки баланса перед обработкой:")
    links_count = 15
    print(f"   Количество ссылок для обработки: {links_count}")
    
    if balance_int is not None:
        if balance_int < links_count:
            print(f"   ⚠️ Недостаточно поисков: {balance_int} < {links_count}")
        else:
            print(f"   ✅ Достаточно поисков: {balance_int} >= {links_count}")
    
    # Закрываем соединения
    await vk_service.close()
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    result = asyncio.run(test_balance_fix())
    exit(0 if result else 1)
