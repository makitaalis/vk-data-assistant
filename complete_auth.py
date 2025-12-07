#!/usr/bin/env python3
"""
Завершение авторизации для текущей сессии (по умолчанию +15167864134)
"""
import asyncio
import os
import sys
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

async def complete_auth():
    """Завершение процесса авторизации"""
    print("=" * 60)
    print("🔐 ЗАВЕРШЕНИЕ АВТОРИЗАЦИИ TELEGRAM")
    print("=" * 60)
    print(f"📱 Номер: {PHONE}")
    print(f"💾 Сессия: {SESSION_NAME}.session")
    print("=" * 60)
    
    proxy = None
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)
    
    try:
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Уже авторизован!")
            me = await client.get_me()
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            await client.disconnect()
            return True
        
        print("📞 Отправляем код авторизации...")
        await client.send_code_request(PHONE)
        
        print("\n📱 КОД ОТПРАВЛЕН!")
        print("Проверьте:")
        print("- SMS сообщения")
        print("- Telegram приложения на всех устройствах")
        print("- Звонки (код может быть произнесен)")
        print("- Веб-версию telegram.org")
        print("-" * 40)
        
        # Попытки ввода кода
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                code = input(f"✏️  Введите код из Telegram (попытка {attempt + 1}/{max_attempts}): ").strip()
                
                if not code:
                    print("❌ Код не может быть пустым")
                    continue
                
                print(f"🔄 Проверяем код: {code}")
                
                # Пытаемся войти с кодом
                await client.sign_in(PHONE, code)
                print("✅ Код принят!")
                break
                
            except PhoneCodeInvalidError:
                print(f"❌ Неверный код. Осталось попыток: {max_attempts - attempt - 1}")
                if attempt == max_attempts - 1:
                    print("❌ Превышено количество попыток ввода кода")
                    await client.disconnect()
                    return False
                continue
                
            except SessionPasswordNeededError:
                print("\n🔒 Требуется пароль двухфакторной аутентификации")
                
                for pwd_attempt in range(3):
                    try:
                        password = input(f"🔑 Введите пароль 2FA (попытка {pwd_attempt + 1}/3): ").strip()
                        await client.sign_in(password=password)
                        print("✅ Авторизация с 2FA успешна!")
                        break
                    except Exception as e:
                        print(f"❌ Неверный пароль: {e}")
                        if pwd_attempt == 2:
                            await client.disconnect()
                            return False
                break
                
            except Exception as e:
                print(f"❌ Ошибка при вводе кода: {e}")
                continue
        
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
                await client.disconnect()
                return True
                
            except Exception as e:
                print(f"⚠️ Проблема с ботом: {e}")
                print("Но авторизация прошла успешно!")
                await client.disconnect()
                return True
        else:
            print("❌ Авторизация не завершена")
            await client.disconnect()
            return False
            
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(complete_auth())
        
        if success:
            print("\n" + "=" * 60)
            print("🎉 СЕССИЯ НАСТРОЕНА УСПЕШНО!")
            print("Теперь можете запустить бота:")
            print("   python run.py")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("❌ Авторизация не удалась")
            print("Попробуйте еще раз:")
            print("   python complete_auth.py")
            print("=" * 60)
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n❌ Авторизация прервана пользователем")
        sys.exit(1)
