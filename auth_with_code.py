#!/usr/bin/env python3
import asyncio
import os
import sys
import time
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def main():
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM С КОДОМ ИЗ ФАЙЛА")
    print("=" * 40)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        print(f"📱 Номер телефона: {PHONE}")
        
        # Отправляем код
        print("\n📤 Отправляю запрос кода...")
        result = await client.send_code_request(PHONE)
        print(f"✅ Запрос отправлен!")
        print(f"📊 Тип: {result.type}")
        
        print("\n" + "=" * 40)
        print("⚠️ ВАЖНО!")
        print("1. Если получили SentCodeTypeSetUpEmailRequired:")
        print("   - Откройте Telegram на телефоне")
        print("   - Зайдите в настройки → Privacy and Security")
        print("   - Добавьте email: aliensobering@gmail.com")
        print("   - Подтвердите email по ссылке в письме")
        print("   - Код придет на email")
        print("")
        print("2. Создайте файл с кодом:")
        print("   /home/vkbot/vk-data-assistant/enter_code.txt")
        print("   (содержимое: только код, например: 12345)")
        print("=" * 40)
        
        # Ждем код в файле
        code_file = "/home/vkbot/vk-data-assistant/enter_code.txt"
        print(f"\n⏳ Жду код в файле {code_file}...")
        
        code = None
        for i in range(60):  # Ждем до 10 минут
            if os.path.exists(code_file):
                with open(code_file, "r") as f:
                    code = f.read().strip()
                if code:
                    print(f"✅ Получен код: {code}")
                    break
            
            time.sleep(10)
            if i % 3 == 0:
                print(f"⏳ Жду код... (прошло {(i+1)*10} сек)")
        
        if not code:
            print("❌ Код не получен за 10 минут")
            await client.disconnect()
            return False
        
        try:
            # Пробуем войти с кодом
            print(f"\n🔐 Вхожу с кодом: {code}")
            await client.sign_in(PHONE, code)
            print("✅ Авторизация успешна!")
            
        except SessionPasswordNeededError:
            # Нужен пароль 2FA
            print("\n🔒 Требуется пароль 2FA")
            
            # Ждем пароль в файле
            pass_file = "/home/vkbot/vk-data-assistant/enter_password.txt"
            print(f"📝 Создайте файл: {pass_file}")
            print("   с вашим паролем 2FA")
            
            password = None
            for i in range(30):  # Ждем до 5 минут
                if os.path.exists(pass_file):
                    with open(pass_file, "r") as f:
                        password = f.read().strip()
                    if password:
                        print(f"✅ Получен пароль")
                        # Удаляем файл с паролем сразу после чтения
                        os.remove(pass_file)
                        break
                
                time.sleep(10)
                if i % 3 == 0:
                    print(f"⏳ Жду пароль... (прошло {(i+1)*10} сек)")
            
            if not password:
                print("❌ Пароль не получен")
                await client.disconnect()
                return False
            
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
            
            # Отправляем тестовое сообщение
            await client.send_message(VK_BOT_USERNAME, "/start")
            print("✅ Тестовое сообщение отправлено")
            
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
