#!/usr/bin/env python3
"""
Авторизация Telegram через SOCKS5 прокси с вводом кода из файла
"""
import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

async def auth_with_proxy():
    """Авторизация через прокси с файловым вводом кода"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM ЧЕРЕЗ PRОKSI SOCKS5")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
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
        print("\n📱 Подключаюсь через прокси...")
        await client.connect()
        
        if client.is_connected():
            print("✅ Подключение к Telegram установлено")
        else:
            print("❌ Не удалось подключиться")
            return
        
        if not await client.is_user_authorized():
            print(f"\n📞 Запрашиваю код авторизации для {ACCOUNT_PHONE}...")
            await client.send_code_request(ACCOUNT_PHONE)
            
            print("\n" + "🔥" * 30)
            print("📱 КОД ОТПРАВЛЕН В TELEGRAM!")
            print("🔥" * 30)
            print("\n📝 ИНСТРУКЦИЯ:")
            print("1. Проверьте Telegram на телефоне")
            print("2. Найдите сообщение с кодом")
            print("3. Создайте файл 'telegram_code.txt'")
            print("4. Поместите код в файл (например: 12345)")
            print("5. Нажмите Enter для продолжения")
            print("\n" + "=" * 50)
            
            input("Создали файл telegram_code.txt с кодом? Нажмите Enter...")
            
            # Читаем код из файла
            code_file = 'telegram_code.txt'
            if os.path.exists(code_file):
                with open(code_file, 'r') as f:
                    code = f.read().strip()
                
                print(f"✅ Код получен: {code}")
                
                try:
                    await client.sign_in(ACCOUNT_PHONE, code)
                    print("✅ Авторизация прошла успешно!")
                    
                    # Удаляем файл с кодом
                    os.remove(code_file)
                    print("🗑️  Файл с кодом удален")
                    
                except Exception as e:
                    if "PASSWORD_HASH_INVALID" in str(e) or "SessionPasswordNeededError" in str(e):
                        print("\n🔒 Требуется пароль двухфакторной аутентификации")
                        print("Создайте файл 'telegram_password.txt' с паролем")
                        input("Создали файл? Нажмите Enter...")
                        
                        with open('telegram_password.txt', 'r') as f:
                            password = f.read().strip()
                        
                        await client.sign_in(password=password)
                        print("✅ Двухфакторная авторизация успешна!")
                        
                        os.remove('telegram_password.txt')
                    else:
                        print(f"❌ Ошибка авторизации: {e}")
                        return
            else:
                print(f"❌ Файл {code_file} не найден")
                return
        
        # Проверяем авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            print("\n" + "🎉" * 30)
            print("✅ АВТОРИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА!")
            print("🎉" * 30)
            print(f"👤 Имя: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            print(f"👤 Username: @{me.username or 'не установлен'}")
        else:
            print("❌ Авторизация не удалась")
            
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        await client.disconnect()
        print("\n📴 Отключение от Telegram")

if __name__ == "__main__":
    asyncio.run(auth_with_proxy())