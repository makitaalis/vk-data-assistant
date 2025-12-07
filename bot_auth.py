#!/usr/bin/env python3
"""
Авторизация через Telegram бота для получения кода
"""
import asyncio
import sys
import os
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Конфигурация
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

async def try_bot_auth():
    """Попытка авторизации через код от бота"""
    print("=" * 60)
    print("🔐 АЛЬТЕРНАТИВНАЯ АВТОРИЗАЦИЯ TELEGRAM")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
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
        print("\n📱 Подключаюсь к Telegram...")
        await client.connect()
        
        if not await client.is_user_authorized():
            print("\n🔄 Пробую альтернативные методы...")
            
            # Метод 1: Попробовать с force_sms
            print("\n📨 Метод 1: Запрос кода через SMS...")
            try:
                await client.send_code_request(ACCOUNT_PHONE, force_sms=True)
                print("✅ Запрос отправлен! Проверьте SMS на телефоне")
                print("\n💬 Код должен прийти в SMS сообщении")
                
                code = input("✏️  Введите код из SMS: ")
                
                await client.sign_in(ACCOUNT_PHONE, code)
                print("✅ Авторизация успешна!")
                
            except Exception as e:
                print(f"❌ SMS метод не сработал: {e}")
                
                # Метод 2: Попробовать через звонок
                print("\n☎️ Метод 2: Запрос кода через звонок...")
                try:
                    # Сбрасываем сессию
                    await client.disconnect()
                    await asyncio.sleep(2)
                    
                    # Удаляем файл сессии для чистого старта
                    session_file = Path(f"{SESSION_NAME}.session")
                    if session_file.exists():
                        session_file.unlink()
                    
                    # Переподключаемся
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
                    await client.connect()
                    
                    # Отправляем запрос с другими параметрами
                    result = await client.send_code_request(ACCOUNT_PHONE)
                    print(f"📞 Тип отправки: {result.type}")
                    print("✅ Запрос отправлен!")
                    
                    # Проверяем разные источники
                    print("\n📱 Проверьте:")
                    print("1. Telegram на другом устройстве")
                    print("2. SMS сообщения")  
                    print("3. Входящие звонки (последние цифры номера)")
                    print("4. Email, привязанный к аккаунту")
                    
                    code = input("\n✏️  Введите код (5-6 цифр): ")
                    
                    await client.sign_in(ACCOUNT_PHONE, code)
                    print("✅ Авторизация успешна!")
                    
                except PhoneCodeInvalidError:
                    print("❌ Неверный код!")
                    
                    # Последняя попытка - попросить переслать код
                    print("\n🔄 Попробуем запросить код повторно...")
                    await client.resend_code(ACCOUNT_PHONE, result.phone_code_hash)
                    print("📨 Код отправлен повторно")
                    
                    code = input("✏️  Введите новый код: ")
                    await client.sign_in(ACCOUNT_PHONE, code)
                    
                except SessionPasswordNeededError:
                    print("\n🔒 Требуется пароль двухфакторной аутентификации")
                    password = input("🔑 Введите пароль 2FA: ")
                    await client.sign_in(password=password)
                    print("✅ Авторизация с 2FA успешна!")
                    
        else:
            print("✅ Сессия уже авторизована!")
        
        # Проверяем подключение
        print("\n📊 Проверяю подключение...")
        me = await client.get_me()
        print(f"✅ Подключен как: {me.first_name} {me.last_name or ''}")
        print(f"📱 Телефон: {me.phone}")
        
        print("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
        await client.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        
        # Диагностика проблемы
        print("\n🔍 ВОЗМОЖНЫЕ РЕШЕНИЯ:")
        print("1. Убедитесь, что номер +380930157086 активен")
        print("2. Проверьте, не заблокирован ли аккаунт")
        print("3. Попробуйте авторизоваться через официальное приложение Telegram")
        print("4. Возможно, нужно подождать перед повторной попыткой (флуд-контроль)")
        
        await client.disconnect()
        return False

if __name__ == "__main__":
    success = asyncio.run(try_bot_auth())
    
    if success:
        print("\n✅ Можно запускать бота: python run.py")
    else:
        print("\n❌ Авторизация не удалась")
        print("\n💡 АЛЬТЕРНАТИВНЫЙ ВАРИАНТ:")
        print("1. Авторизуйтесь в Telegram Desktop или мобильном приложении")
        print("2. Скопируйте файл сессии от другого клиента")
        print("3. Используйте уже авторизованную сессию")
        sys.exit(1)