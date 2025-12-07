#!/usr/bin/env python3
"""Тест парсинга телефонов из ответов ботов"""

import asyncio
import logging
from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
from services.vk_multibot_service import VKMultiBotService
from telethon import TelegramClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_phone_parsing():
    """Тест извлечения телефонов"""
    
    print("=" * 60)
    print("ТЕСТ ПАРСИНГА ТЕЛЕФОНОВ")
    print("=" * 60)
    
    # Инициализация VK сервиса
    print("\n🔄 Инициализация VK сервиса...")
    vk_service = VKMultiBotService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    initialized = await vk_service.initialize_with_session_auth()
    print(f"✅ Инициализировано {initialized} ботов")
    
    # Тестовая ссылка - используем профиль где могут быть телефоны
    test_link = "https://vk.com/id1"  # Павел Дуров
    
    print(f"\n🔍 Тестируем: {test_link}")
    print("-" * 60)
    
    # Отправляем запрос первому боту напрямую
    bot = vk_service.bots[0]
    if bot.is_initialized:
        print(f"📤 Отправляем запрос боту @{bot.username}")
        
        # Отправляем ссылку
        msg = await bot.client.send_message(bot.entity, test_link)
        print(f"   Сообщение отправлено (ID: {msg.id})")
        
        # Ждем ответ
        await asyncio.sleep(3)
        
        # Получаем последние сообщения
        messages = await bot.client.get_messages(bot.entity, limit=5)
        
        print("\n📥 Полученные сообщения от бота:")
        print("=" * 60)
        
        for i, msg in enumerate(messages, 1):
            if msg.text and msg.sender_id != (await bot.client.get_me()).id:
                print(f"\nСообщение {i}:")
                print("-" * 40)
                print(msg.text)
                print("-" * 40)
                
                # Проверяем текст на наличие телефонов
                import re
                
                # Разные паттерны для телефонов
                patterns = [
                    r'(?<!\d)7\d{10}(?!\d)',  # Текущий паттерн
                    r'\+7\d{10}',  # С плюсом
                    r'8\d{10}',  # Начинается с 8
                    r'\d{3}-\d{3}-\d{4}',  # С дефисами
                    r'\(\d{3}\)\s*\d{3}-\d{4}',  # С скобками
                    r'телефон[:\s]*([^\n]+)',  # После слова телефон
                    r'phone[:\s]*([^\n]+)',  # После слова phone
                    r'📱[:\s]*([^\n]+)',  # После эмодзи телефона
                ]
                
                print("\n🔍 Поиск телефонов:")
                found_phones = []
                for pattern in patterns:
                    phones = re.findall(pattern, msg.text, re.IGNORECASE)
                    if phones:
                        print(f"   Паттерн '{pattern[:20]}...': {phones}")
                        found_phones.extend(phones)
                
                if not found_phones:
                    print("   ❌ Телефоны не найдены")
                else:
                    print(f"   ✅ Найдено телефонов: {len(set(found_phones))}")
    
    # Теперь тестируем через search_vk_data
    print("\n" + "=" * 60)
    print("Тест через search_vk_data:")
    print("-" * 60)
    
    result = await vk_service.search_vk_data(test_link)
    
    print(f"\n📊 Результат парсинга:")
    print(f"   Имя: {result.get('full_name', 'Не найдено')}")
    print(f"   Телефоны: {result.get('phones', [])}")
    print(f"   Дата рождения: {result.get('birth_date', 'Не найдено')}")
    
    if not result.get('phones'):
        print("\n⚠️ ПРОБЛЕМА: Телефоны не извлечены!")
        print("   Возможные причины:")
        print("   1. Бот не возвращает телефоны для этого профиля")
        print("   2. Паттерн PHONE_PATTERN не соответствует формату")
        print("   3. Парсер _parse_result не находит строку с телефоном")
    
    # Закрываем соединения
    await vk_service.close()
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_phone_parsing())
