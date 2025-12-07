#!/usr/bin/env python3
"""
Авторизация через файл с кодом
Создайте файл auth_code.txt с кодом из Telegram
"""
import asyncio
import os
import sys
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")
PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def auth_with_file():
    """Авторизация используя код из файла"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ ЧЕРЕЗ ФАЙЛ")
    print("=" * 60)
    print(f"📱 Номер: {PHONE}")
    print(f"💾 Сессия: {SESSION_NAME}.session")
    print("=" * 60)
    
    # Проверяем файл с кодом
    code_file = Path("auth_code.txt")
    password_file = Path("auth_password.txt")
    
    if not code_file.exists():
        print("❌ Файл auth_code.txt не найден!")
        print("\n📝 ИНСТРУКЦИЯ:")
        print("1. Получите код авторизации из Telegram")
        print("2. Создайте файл auth_code.txt")
        print("3. Запишите в него только код (например: 12345)")
        print("4. Запустите этот скрипт снова")
        print("\nПример создания файла:")
        print("echo '12345' > auth_code.txt")
        return False
    
    try:
        code = code_file.read_text().strip()
        if not code:
            print("❌ Файл auth_code.txt пустой!")
            return False
        
        print(f"✅ Код найден в файле: {code}")
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        return False
    
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
            print("✅ Уже авторизован!")
            me = await client.get_me()
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            await client.disconnect()
            # Удаляем файл с кодом
            code_file.unlink()
            return True
        
        print("📞 Отправляем код авторизации...")
        await client.send_code_request(PHONE)
        
        try:
            print(f"🔄 Авторизация с кодом: {code}")
            await client.sign_in(PHONE, code)
            print("✅ Код принят!")
            
        except PhoneCodeInvalidError:
            print("❌ Неверный код!")
            print("Обновите файл auth_code.txt с правильным кодом")
            await client.disconnect()
            return False
            
        except SessionPasswordNeededError:
            print("\n🔒 Требуется пароль двухфакторной аутентификации")
            
            if password_file.exists():
                try:
                    password = password_file.read_text().strip()
                    print("🔑 Используем пароль из файла auth_password.txt")
                    await client.sign_in(password=password)
                    print("✅ Авторизация с 2FA успешна!")
                    # Удаляем файл с паролем
                    password_file.unlink()
                except Exception as e:
                    print(f"❌ Неверный пароль 2FA: {e}")
                    print("Проверьте файл auth_password.txt")
                    await client.disconnect()
                    return False
            else:
                print("❌ Создайте файл auth_password.txt с паролем 2FA")
                print("echo 'your_password' > auth_password.txt")
                await client.disconnect()
                return False
        
        # Проверяем финальную авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"\n✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            # Проверяем доступ к боту
            print(f"\n🤖 Проверяем доступ к боту {VK_BOT_USERNAME}...")
            try:
                bot = await client.get_entity(VK_BOT_USERNAME)
                print(f"✅ Бот доступен: {bot.first_name}")
                
                # Отправляем тестовое сообщение
                msg = await client.send_message(bot, "/start")
                print(f"✅ Тестовое сообщение отправлено (ID: {msg.id})")
                
                print(f"\n🎉 ВСЁ ГОТОВО! Сессия {PHONE} работает!")
                
            except Exception as e:
                print(f"⚠️ Проблема с ботом: {e}")
                print("Но авторизация прошла успешно!")
            
            await client.disconnect()
            
            # Удаляем файлы с секретами
            if code_file.exists():
                code_file.unlink()
            if password_file.exists():
                password_file.unlink()
                
            return True
        else:
            print("❌ Авторизация не завершена")
            await client.disconnect()
            return False
            
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        await client.disconnect()
        return False

async def request_code():
    """Запрос кода авторизации"""
    print("📞 Запрашиваем код авторизации...")
    
    proxy = {
        'proxy_type': 'socks5',
        'addr': '194.31.73.124',
        'port': 60741,
        'username': 'QzYtokLcGL',
        'password': '4MR8FmpoKN',
        'rdns': True
    }
    
    client = TelegramClient(f"{SESSION_NAME}_temp", API_ID, API_HASH, proxy=proxy)
    
    try:
        await client.connect()
        await client.send_code_request(PHONE)
        print(f"✅ Код отправлен на {PHONE}")
        print("📱 Проверьте SMS, звонки или Telegram")
        await client.disconnect()
        
        # Удаляем временную сессию
        temp_file = Path(f"{SESSION_NAME}_temp.session")
        if temp_file.exists():
            temp_file.unlink()
            
    except Exception as e:
        print(f"❌ Ошибка запроса кода: {e}")
        await client.disconnect()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "request":
        # Только запросить код
        asyncio.run(request_code())
        print("\nТеперь создайте файл с кодом:")
        print("echo 'ваш_код' > auth_code.txt")
        print("python auth_with_code_file.py")
    else:
        # Основная авторизация
        success = asyncio.run(auth_with_file())
        
        if success:
            print("\n" + "=" * 60)
            print("🎉 СЕССИЯ НАСТРОЕНА УСПЕШНО!")
            print("Теперь можете запустить бота:")
            print("   python run.py")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("❌ Авторизация не удалась")
            print("Попробуйте еще раз или запросите новый код:")
            print("   python auth_with_code_file.py request")
            print("=" * 60)
