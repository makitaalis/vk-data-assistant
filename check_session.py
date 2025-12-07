#!/usr/bin/env python3
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def check():
    # Настройки прокси SOCKS5
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
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Сессия активна!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            # Проверяем бота
            print("\n🤖 Проверка бота...")
            try:
                bot = await client.get_entity(VK_BOT_USERNAME)
                print(f"✅ Бот доступен: {bot.first_name}")
            except Exception as e:
                print(f"❌ Ошибка доступа к боту: {e}")
        else:
            print("❌ Сессия НЕ авторизована!")
            print("Необходима повторная авторизация")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(check())
