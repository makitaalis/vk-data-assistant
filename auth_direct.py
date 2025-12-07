#!/usr/bin/env python3
"""
Прямая авторизация с кодом, переданным как аргумент
"""
import asyncio
import os
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

async def auth_with_code(code, password=None):
    """Авторизация с конкретным кодом"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM ЧЕРЕЗ SOCKS5")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print(f"🔢 Код: {code}")
    if password:
        print(f"🔒 Пароль 2FA: {'*' * len(password)}")
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
        print("\n📱 Подключаюсь через прокси...")
        await client.connect()
        
        if not client.is_connected():
            print("❌ Не удалось подключиться")
            return False
            
        print("✅ Подключение установлено")
        
        if not await client.is_user_authorized():
            print("\n📞 Отправляю запрос кода...")
            await client.send_code_request(ACCOUNT_PHONE)
            
            print(f"✏️ Использую код: {code}")
            
            try:
                # Пробуем авторизоваться с кодом
                await client.sign_in(ACCOUNT_PHONE, code)
                print("✅ Авторизация с кодом успешна!")
                
            except Exception as e:
                if "PASSWORD_HASH_INVALID" in str(e) or "SessionPasswordNeededError" in str(e):
                    print("🔒 Требуется пароль 2FA")
                    if password:
                        print("🔐 Использую предоставленный пароль...")
                        await client.sign_in(password=password)
                        print("✅ Двухфакторная авторизация успешна!")
                    else:
                        print("❌ Пароль 2FA не предоставлен")
                        return False
                else:
                    print(f"❌ Ошибка авторизации: {e}")
                    return False
        
        # Проверяем результат
        if await client.is_user_authorized():
            me = await client.get_me()
            print("\n🎉" * 30)
            print("✅ АВТОРИЗАЦИЯ ЗАВЕРШЕНА!")
            print("🎉" * 30)
            print(f"👤 Имя: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            print(f"👤 Username: @{me.username or 'не установлен'}")
            return True
        else:
            print("❌ Авторизация не удалась")
            return False
            
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        return False
    finally:
        await client.disconnect()
        print("\n📴 Отключение от Telegram")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python auth_direct.py <код> [пароль_2fa]")
        sys.exit(1)
    
    code = sys.argv[1]
    password = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = asyncio.run(auth_with_code(code, password))
    sys.exit(0 if result else 1)