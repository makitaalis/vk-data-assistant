#!/usr/bin/env python3
import asyncio
import os
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
PHONE = os.environ.get("ACCOUNT_PHONE", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def main():
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM")
    print("=" * 40)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        print(f"📱 Номер телефона: {PHONE}")
        
        # Отправляем код
        print("\n📤 Отправляю запрос кода...")
        result = await client.send_code_request(PHONE)
        print(f"✅ Запрос отправлен!")
        print(f"📊 Код отправлен на: {result.type}")
        
        # Запрашиваем код
        print("\n" + "=" * 40)
        print("КОД ОТПРАВЛЕН НА:")
        if hasattr(result.type, 'email_pattern'):
            print(f"📧 EMAIL: {result.type.email_pattern}")
        else:
            print(f"📱 Telegram или SMS")
        print("=" * 40)
        
        code = input("\n✏️ Введите код: ")
        
        try:
            # Пробуем войти с кодом
            await client.sign_in(PHONE, code)
            print("✅ Авторизация успешна!")
            
        except SessionPasswordNeededError:
            # Нужен пароль 2FA
            print("\n🔒 Требуется пароль 2FA")
            password = input("🔑 Введите пароль: ")
            
            await client.sign_in(password=password)
            print("✅ Авторизация с 2FA успешна!")
            
        except PhoneCodeInvalidError:
            print("❌ Неверный код!")
            await client.disconnect()
            return False
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            await client.disconnect()
            return False
    
    # Проверяем авторизацию
    if await client.is_user_authorized():
        me = await client.get_me()
        print("\n" + "=" * 40)
        print("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
        print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
        print(f"📱 Телефон: {me.phone}")
        print(f"🆔 ID: {me.id}")
        print("=" * 40)
        
        # Проверяем бота
        print("\n🤖 Проверка доступа к боту...")
        try:
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Бот доступен: {bot.first_name}")
        except Exception as e:
            print(f"❌ Ошибка доступа к боту: {e}")
    else:
        print("❌ Сессия не авторизована")
        return False
    
    await client.disconnect()
    return True

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
