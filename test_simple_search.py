#!/usr/bin/env python3
"""Простой тест поиска"""

import asyncio
from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
from services.vk_multibot_service import VKMultiBotService

async def test():
    print("Инициализация...")
    service = VKMultiBotService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
    await service.initialize_with_session_auth()
    
    print("\nТестирование поиска...")
    result = await service.search_vk_data("https://vk.com/id328476661")
    
    print(f"\nРезультат:")
    print(f"Имя: {result.get('full_name', 'Не найдено')}")
    print(f"Телефоны: {result.get('phones', [])}")
    
    await service.close()
    print("\n✅ Тест завершен")

asyncio.run(test())
