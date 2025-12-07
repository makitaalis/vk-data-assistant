#!/usr/bin/env python3
"""
Тест подключения через SOCKS5 прокси
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "test_proxy_session"

async def test_proxy():
    print("=" * 60)
    print("ТЕСТ ПОДКЛЮЧЕНИЯ ЧЕРЕЗ SOCKS5 ПРОКСИ")
    print("=" * 60)
    
    # Настройки прокси SOCKS5
    proxy = {
        'proxy_type': 'socks5',
        'addr': '194.31.73.124',
        'port': 60741,
        'username': 'QzYtokLcGL',
        'password': '4MR8FmpoKN',
        'rdns': True
    }
    
    print(f"🔌 Прокси: {proxy['addr']}:{proxy['port']}")
    print(f"👤 Логин: {proxy['username']}")
    print("=" * 60)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)
    
    try:
        print("\n📱 Подключаюсь к Telegram через прокси...")
        await client.connect()
        
        if client.is_connected():
            print("✅ Успешно подключился к Telegram через прокси!")
            
            # Проверяем авторизацию
            if await client.is_user_authorized():
                me = await client.get_me()
                print(f"\n✅ Сессия активна!")
                print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
                print(f"📱 Телефон: {me.phone}")
            else:
                print("\n⚠️ Сессия не авторизована")
                print("Запустите auth_session.py для авторизации")
        else:
            print("❌ Не удалось подключиться через прокси")
            
    except Exception as e:
        print(f"\n❌ Ошибка при подключении: {e}")
        print("\nВозможные причины:")
        print("1. Прокси недоступен или данные неверны")
        print("2. Проблемы с интернет-соединением")
        print("3. Telegram блокирует прокси")
        
    finally:
        await client.disconnect()
        print("\n📴 Отключился от Telegram")

if __name__ == "__main__":
    asyncio.run(test_proxy())