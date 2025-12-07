#!/usr/bin/env python3
"""
Тестирование новой сессии и ботов
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "user_session"
VK_BOT_USERNAMES = os.environ.get("VK_BOT_USERNAMES", "").split(",")

async def test_session():
    print("=" * 60)
    print("🔧 ТЕСТИРОВАНИЕ НОВОЙ СЕССИИ И БОТОВ")
    print("=" * 60)

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()

        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Сессия активна: {me.first_name} {me.last_name or ''} ({me.phone})")
            print(f"📱 ID: {me.id}")
            print()

            # Проверяем доступность ботов
            print(f"🤖 Проверка {len(VK_BOT_USERNAMES)} ботов:")
            print("-" * 40)

            for bot_username in VK_BOT_USERNAMES:
                bot_username = bot_username.strip()
                if not bot_username:
                    continue

                try:
                    # Добавляем @ если нет
                    if not bot_username.startswith("@"):
                        bot_username = "@" + bot_username

                    print(f"\n📍 Проверяю бота: {bot_username}")
                    bot = await client.get_entity(bot_username)

                    if bot:
                        print(f"  ✅ Бот найден: {bot.first_name}")
                        print(f"  🆔 ID: {bot.id}")
                        print(f"  👤 Username: @{bot.username}")

                        # Отправляем тестовое сообщение
                        print(f"  📤 Отправляю тестовое сообщение...")
                        msg = await client.send_message(bot, "/start")
                        print(f"  ✅ Сообщение отправлено (ID: {msg.id})")

                        # Ждем ответ
                        await asyncio.sleep(2)

                        # Получаем последнее сообщение от бота
                        messages = await client.get_messages(bot, limit=1)
                        if messages:
                            print(f"  💬 Ответ бота: {messages[0].text[:100]}...")

                except Exception as e:
                    print(f"  ❌ Ошибка с ботом {bot_username}: {e}")

            print("\n" + "=" * 60)
            print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
            print("=" * 60)

        else:
            print("❌ Сессия не авторизована!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_session())