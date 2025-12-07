#!/usr/bin/env python3
"""
Отправка кода авторизации на телефон
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

async def send_auth_code():
    """Отправляем код авторизации"""
    print("=" * 60)
    print("📱 ОТПРАВКА КОДА АВТОРИЗАЦИИ")
    print("=" * 60)
    print(f"📞 Телефон: {ACCOUNT_PHONE}")
    print(f"🔌 Прокси: 194.31.73.124:60741")
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
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)
    
    try:
        print("\n🔌 Подключаюсь через SOCKS5 прокси...")
        await client.connect()
        
        if not client.is_connected():
            print("❌ Не удалось подключиться через прокси")
            return False
            
        print("✅ Подключение через прокси установлено")
        
        # Проверяем, не авторизованы ли уже
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"\n✅ Сессия уже существует!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            return True
        
        print(f"\n📞 Отправляю код авторизации на {ACCOUNT_PHONE}...")
        await client.send_code_request(ACCOUNT_PHONE)
        
        print("\n🔥" * 25)
        print("✅ КОД ОТПРАВЛЕН В TELEGRAM!")
        print("🔥" * 25)
        print("\n📱 Проверьте Telegram на телефоне")
        print("💬 Найдите сообщение с 5-значным кодом")
        print("\n🔢 Скажите мне код, и я завершу авторизацию!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return False
    finally:
        await client.disconnect()
        print("\n📴 Отключение от Telegram")

if __name__ == "__main__":
    asyncio.run(send_auth_code())