#!/usr/bin/env python3
"""
Тест новой сессии 77086221203
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "user_session_77086221203"
VK_BOT_USERNAME = "@sherlokaminiusisubot"

async def test_session():
    print("=" * 60)
    print("🧪 ТЕСТ НОВОЙ СЕССИИ 77086221203")
    print("=" * 60)

    # Проверяем наличие файла сессии
    session_file = f"{SESSION_NAME}.session"
    if not os.path.exists(session_file):
        print(f"❌ Файл сессии {session_file} не найден!")
        return False

    print(f"✅ Файл сессии найден: {session_file}")

    # Настройки прокси
    proxy = {
        'proxy_type': 'socks5',
        'addr': '194.31.73.124',
        'port': 60741,
        'username': 'QzYtokLcGL',
        'password': '4MR8FmpoKN',
        'rdns': True
    }

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)

    try:
        print("🔌 Подключение к Telegram...")
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ Сессия не авторизована!")
            return False

        # Получаем информацию о пользователе
        me = await client.get_me()
        print(f"✅ Авторизован как: {me.first_name} {me.last_name or ''}")
        print(f"📱 Телефон: {me.phone}")
        print(f"🆔 ID: {me.id}")

        # Проверяем доступ к VK боту
        print(f"\n🤖 Тестируем доступ к боту {VK_BOT_USERNAME}...")
        try:
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Бот найден: {bot.first_name}")

            # Отправляем тестовое сообщение
            print("📤 Отправляем тестовое сообщение...")
            test_message = await client.send_message(bot, "/start")
            print(f"✅ Сообщение отправлено (ID: {test_message.id})")

            # Ждем ответ
            print("⏳ Ожидаем ответ бота...")
            await asyncio.sleep(3)

            # Получаем последние сообщения
            messages = await client.get_messages(bot, limit=5)
            print(f"📬 Получено {len(messages)} сообщений:")

            for i, msg in enumerate(messages):
                if msg.message:
                    print(f"  {i+1}. {msg.message[:100]}...")

            print("\n🎉 ТЕСТ УСПЕШЕН! Сессия работает корректно.")
            return True

        except Exception as bot_error:
            print(f"❌ Ошибка при работе с ботом: {bot_error}")
            return False

    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False
    finally:
        await client.disconnect()
        print("👋 Отключение от Telegram")

if __name__ == "__main__":
    success = asyncio.run(test_session())
    print(f"\n{'✅ ТЕСТ ПРОЙДЕН' if success else '❌ ТЕСТ НЕ ПРОЙДЕН'}")