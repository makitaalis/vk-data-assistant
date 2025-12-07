#!/usr/bin/env python3
"""
Финальное тестирование настройки системы
"""

import asyncio
import os
from telethon import TelegramClient
from bot.config import *

async def test_system():
    """Полное тестирование системы"""
    print("=" * 60)
    print("🧪 ФИНАЛЬНОЕ ТЕСТИРОВАНИЕ СИСТЕМЫ")
    print("=" * 60)

    print("📋 Конфигурация:")
    print(f"  📱 Телефон: {ACCOUNT_PHONE}")
    print(f"  💾 Сессия: {SESSION_NAME}")
    print(f"  🤖 Количество ботов: {len(VK_BOT_USERNAMES)}")
    print("  🤖 Боты:")
    for i, bot in enumerate(VK_BOT_USERNAMES, 1):
        print(f"    {i}. @{bot}")
    print("-" * 40)

    # Проверяем сессию
    session_file = f"{SESSION_NAME}.session"
    print(f"🔍 Проверка сессии: {session_file}")

    if not os.path.exists(session_file):
        print(f"  ❌ Файл сессии не найден: {session_file}")
        return False

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            print("  ❌ Сессия не авторизована")
            return False

        me = await client.get_me()
        print(f"  ✅ Авторизован: {me.first_name} {me.last_name or ''}")
        print(f"  📱 Телефон: {me.phone}")

        # Проверяем каждого бота
        print("\n🤖 Тестирование ботов:")
        working_bots = []

        for i, bot_username in enumerate(VK_BOT_USERNAMES, 1):
            try:
                print(f"  {i}. @{bot_username}...")
                bot = await client.get_entity(f"@{bot_username}")

                # Отправляем тестовое сообщение
                msg = await client.send_message(bot, "/start")

                print(f"     ✅ Доступен: {bot.first_name}")
                print(f"     📨 Тест сообщение ID: {msg.id}")
                working_bots.append(bot_username)

                await asyncio.sleep(1)  # Пауза между запросами

            except Exception as e:
                print(f"     ❌ Ошибка: {str(e)[:50]}...")

        print(f"\n📊 Результаты:")
        print(f"  ✅ Рабочих ботов: {len(working_bots)}/{len(VK_BOT_USERNAMES)}")

        if len(working_bots) == len(VK_BOT_USERNAMES):
            print("  🎉 ВСЕ БОТЫ РАБОТАЮТ!")
        elif working_bots:
            print("  ⚠️ Частично рабочие боты")
        else:
            print("  ❌ НИ ОДИН БОТ НЕ РАБОТАЕТ")

        return len(working_bots) > 0

    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return False

    finally:
        await client.disconnect()

async def main():
    """Основная функция"""
    success = await test_system()

    print("\n" + "=" * 60)
    if success:
        print("🎉 СИСТЕМА ГОТОВА К РАБОТЕ!")
        print("=" * 60)
        print("✅ Следующие шаги:")
        print("1. Запустите основного бота:")
        print("   /home/vkbot/vk-data-assistant/venv/bin/python run.py")
        print("")
        print("2. Или с логированием:")
        print("   nohup /home/vkbot/vk-data-assistant/venv/bin/python run.py > logs/bot.log 2>&1 &")
        print("")
        print("3. Проверить статус:")
        print("   ps aux | grep 'python.*run.py'")
    else:
        print("❌ СИСТЕМА НЕ ГОТОВА")
        print("=" * 60)
        print("🔧 Требуется настройка:")
        print("1. Проверьте авторизацию сессии")
        print("2. Убедитесь что боты доступны")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())