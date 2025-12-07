#!/usr/bin/env python3
"""
Надежная авторизация с улучшенными настройками
"""
import asyncio
import os
import time
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

async def reliable_auth():
    """Надежная авторизация с повышенной вероятностью получения кода"""
    print("=" * 60)
    print("🔐 НАДЕЖНАЯ АВТОРИЗАЦИЯ TELEGRAM")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print("=" * 60)
    
    # Улучшенные настройки прокси
    proxy = {
        'proxy_type': 'socks5',
        'addr': '194.31.73.124',
        'port': 60741,
        'username': 'QzYtokLcGL',
        'password': '4MR8FmpoKN',
        'rdns': True
    }
    
    # Создаем клиента с дополнительными настройками
    client = TelegramClient(
        SESSION_NAME, 
        API_ID, 
        API_HASH, 
        proxy=proxy,
        connection_retries=5,
        retry_delay=1,
        timeout=30,
        request_retries=5
    )
    
    try:
        print("\n🔌 Подключаюсь через прокси...")
        await client.connect()
        
        if not client.is_connected():
            print("❌ Не удалось подключиться")
            return False
            
        print("✅ Подключение установлено")
        
        # Проверяем авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"\n✅ Уже авторизован как {me.first_name}")
            return True
        
        print(f"\n📞 Запрашиваю код для {ACCOUNT_PHONE}...")
        
        try:
            # Отправляем код с принудительным SMS
            result = await client.send_code_request(
                ACCOUNT_PHONE,
                force_sms=True  # Принудительно через SMS
            )
            print(f"✅ Код отправлен через SMS")
            print(f"📱 Тип: {result.type}")
            
        except FloodWaitError as e:
            print(f"⏳ Flood контроль: ждем {e.seconds} секунд...")
            await asyncio.sleep(e.seconds)
            result = await client.send_code_request(ACCOUNT_PHONE, force_sms=True)
            
        except Exception as e:
            print(f"⚠️ Ошибка отправки SMS, пробую обычный способ: {e}")
            result = await client.send_code_request(ACCOUNT_PHONE)
        
        print("\n🔥" * 25)
        print("📱 КОД ДОЛЖЕН ПРИЙТИ НА ТЕЛЕФОН!")
        print("🔥" * 25)
        print("\n⏱️ Ожидайте до 2 минут")
        print("📱 Проверяйте SMS и Telegram")
        print("💬 Код: 5 цифр (например: 12345)")
        print("\n🔢 Когда получите код - скажите его мне!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return False
    finally:
        await client.disconnect()
        print("\n📴 Соединение закрыто")

if __name__ == "__main__":
    asyncio.run(reliable_auth())