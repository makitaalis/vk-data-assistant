#!/usr/bin/env python3
"""
Тестирование поиска через новых ботов
"""
import asyncio
from services.vk_multibot_service import VKMultiBotService
from dotenv import load_dotenv
import os

load_dotenv()

async def test_search():
    print("=" * 60)
    print("🔍 ТЕСТИРОВАНИЕ ПОИСКА ЧЕРЕЗ НОВЫХ БОТОВ")
    print("=" * 60)

    # Инициализация сервиса
    api_id = int(os.environ.get("API_ID", 0))
    api_hash = os.environ.get("API_HASH", "")
    phone = os.environ.get("ACCOUNT_PHONE", "")

    service = VKMultiBotService(
        api_id=api_id,
        api_hash=api_hash,
        session_base_name="user_session",
        phone=phone
    )

    try:
        # Инициализация
        print("\n📡 Инициализация сервиса...")
        await service.initialize()
        print(f"✅ Инициализировано {len(service.bots)} ботов")

        # Тестовый поиск
        test_query = "79999999999"
        print(f"\n🔎 Тестовый поиск: {test_query}")

        result = await service.search_vk_data(test_query)

        if result:
            print("\n✅ Результат получен:")
            print(f"📊 Статус: {'Найдено' if result.get('found_data') else 'Не найдено'}")
            if result.get('data'):
                print(f"📝 Данные: {result['data'][:200]}...")
            print(f"🤖 Использован бот: {result.get('bot_used', 'неизвестно')}")
        else:
            print("❌ Результат не получен")

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
    finally:
        # Закрываем соединения
        if hasattr(service, 'bots'):
            for bot in service.bots:
                if bot.client:
                    await bot.client.disconnect()
        print("\n✅ Сервис завершен")

if __name__ == "__main__":
    asyncio.run(test_search())