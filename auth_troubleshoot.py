#!/usr/bin/env python3
"""
Решение проблем с получением кода авторизации Telegram
"""
import asyncio
import os
from telethon import TelegramClient
from telethon.errors import PhoneNumberInvalidError, FloodWaitError
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

async def troubleshoot_auth():
    """Диагностика проблем с авторизацией"""
    print("=" * 60)
    print("🔧 ДИАГНОСТИКА ПРОБЛЕМ С АВТОРИЗАЦИЕЙ")
    print("=" * 60)
    print(f"📱 Номер телефона: {PHONE}")
    print(f"🆔 API ID: {API_ID}")
    print(f"🔑 API Hash: {API_HASH[:10]}...")
    print("=" * 60)
    
    # Проверяем несколько вариантов прокси
    proxy_configs = [
        # Основной прокси
        {
            'proxy_type': 'socks5',
            'addr': '194.31.73.124',
            'port': 60741,
            'username': 'QzYtokLcGL',
            'password': '4MR8FmpoKN',
            'rdns': True
        },
        # Без прокси
        None
    ]
    
    for i, proxy in enumerate(proxy_configs):
        print(f"\n🔄 Попытка {i+1}: {'С прокси' if proxy else 'Без прокси'}")
        
        try:
            client = TelegramClient(f"{SESSION_NAME}_test_{i}", API_ID, API_HASH, proxy=proxy)
            await client.connect()
            
            print(f"✅ Подключение установлено")
            
            # Проверяем авторизацию
            if await client.is_user_authorized():
                print("✅ Уже авторизован!")
                me = await client.get_me()
                print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
                await client.disconnect()
                return True
            
            print(f"📞 Отправляем код на {PHONE}...")
            
            try:
                # Отправляем код
                sent_code = await client.send_code_request(PHONE)
                print(f"✅ Код отправлен! Тип: {sent_code.type}")
                print(f"📱 Проверьте Telegram на устройстве с номером {PHONE}")
                print(f"💬 Код может прийти как:")
                print(f"   - SMS сообщение")
                print(f"   - Звонок с кодом")
                print(f"   - Уведомление в Telegram на другом устройстве")
                print(f"   - Внутреннее сообщение в Telegram")
                
                await client.disconnect()
                return True
                
            except PhoneNumberInvalidError:
                print(f"❌ Неверный номер телефона: {PHONE}")
            except FloodWaitError as e:
                print(f"⏰ Слишком много попыток. Ждите {e.seconds} секунд")
            except Exception as e:
                print(f"❌ Ошибка отправки кода: {e}")
                
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
        
        if client:
            await client.disconnect()
    
    return False

async def check_existing_session():
    """Проверка существующей сессии"""
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА СУЩЕСТВУЮЩЕЙ СЕССИИ")
    print("=" * 60)
    
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
            print(f"✅ Сессия работает!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            
            # Проверяем доступ к боту
            try:
                bot = await client.get_entity(VK_BOT_USERNAME)
                print(f"✅ Бот доступен: {bot.first_name}")
            except Exception as e:
                print(f"⚠️ Проблема с ботом: {e}")
            
            await client.disconnect()
            return True
        else:
            print("❌ Сессия не авторизована")
            await client.disconnect()
            return False
            
    except Exception as e:
        print(f"❌ Ошибка проверки сессии: {e}")
        await client.disconnect()
        return False

async def main():
    """Основная функция диагностики"""
    print("🚀 Запуск диагностики проблем с авторизацией...")
    
    # Сначала проверяем существующую сессию
    if await check_existing_session():
        print("\n🎉 СЕССИЯ УЖЕ РАБОТАЕТ! Можете запускать бота.")
        return
    
    # Если сессия не работает, пробуем получить код
    if await troubleshoot_auth():
        print("\n📋 ИНСТРУКЦИИ ПО ПОЛУЧЕНИЮ КОДА:")
        print(f"1. Проверьте SMS на номере {PHONE}")
        print("2. Проверьте звонки (код может быть произнесен)")
        print("3. Откройте Telegram на всех устройствах")
        print("4. Проверьте раздел 'Devices' в настройках Telegram")
        print("5. Попробуйте через веб-версию telegram.org")
        print("\n💡 АЛЬТЕРНАТИВНЫЕ СПОСОБЫ:")
        print("- Используйте другой номер телефона")
        print("- Проверьте, не заблокирован ли номер")
        print("- Обратитесь в поддержку Telegram")
    else:
        print("\n❌ Не удалось отправить код авторизации")
        print("Проверьте:")
        print("- Правильность номера телефона")
        print("- Доступность Telegram API")
        print("- Настройки прокси")

if __name__ == "__main__":
    asyncio.run(main())
