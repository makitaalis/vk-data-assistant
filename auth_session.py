#!/usr/bin/env python3
"""
Интерактивная авторизация Telegram сессии
"""
import asyncio
import sys
import os
from urllib.parse import urlparse
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Конфигурация
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username
PROXY_URL = os.environ.get("PROXY", "").strip()

def build_proxy(proxy_url: str | None):
    """Convert PROXY env string (e.g. socks5://user:pass@host:port) into Telethon dict"""
    if not proxy_url:
        return None

    parsed = urlparse(proxy_url)
    if not parsed.scheme or not parsed.hostname or not parsed.port:
        print(f"⚠️  Игнорирую некорректный PROXY={proxy_url}")
        return None

    proxy = {
        "proxy_type": parsed.scheme,
        "addr": parsed.hostname,
        "port": parsed.port,
        "rdns": True,
    }

    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password

    print(f"🌐 Используется прокси {parsed.scheme}://{parsed.hostname}:{parsed.port}")
    return proxy

async def auth_session():
    """Процесс авторизации с вводом кода"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM СЕССИИ")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print(f"🤖 Бот для поиска: {VK_BOT_USERNAME}")
    print("=" * 60)
    
    proxy = build_proxy(PROXY_URL)
    if not proxy:
        print("🌐 Прокси не используется (прямое подключение)")
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)
    
    print("\n📱 Подключаюсь к Telegram...")
    await client.connect()
    
    if not await client.is_user_authorized():
        print(f"\n📞 Отправляю код авторизации на {ACCOUNT_PHONE}...")
        
        try:
            # Начинаем процесс авторизации
            await client.send_code_request(ACCOUNT_PHONE)
            
            print("\n⚠️  ВАЖНО: Проверьте Telegram на телефоне!")
            print("📱 Вам придет сообщение с кодом от Telegram")
            print("💬 Код будет в формате: 12345 (5 цифр)")
            print("-" * 40)
            
            # Ждем ввода кода
            code = input("✏️  Введите код из Telegram: ")
            
            try:
                # Пытаемся войти с кодом
                await client.sign_in(ACCOUNT_PHONE, code)
                print("✅ Авторизация успешна!")
                
            except SessionPasswordNeededError:
                # Если включена двухфакторная аутентификация
                print("\n🔒 Требуется пароль двухфакторной аутентификации")
                password = input("🔑 Введите пароль: ")
                await client.sign_in(password=password)
                print("✅ Авторизация с 2FA успешна!")
                
        except Exception as e:
            print(f"❌ Ошибка авторизации: {e}")
            await client.disconnect()
            return False
    else:
        print("✅ Сессия уже авторизована!")
    
    # Проверяем подключение
    print("\n📊 Проверяю подключение...")
    me = await client.get_me()
    print(f"✅ Подключен как: {me.first_name} {me.last_name or ''}")
    print(f"📱 Телефон: {me.phone}")
    print(f"🆔 ID: {me.id}")
    
    # Проверяем доступ к боту
    print(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
    try:
        bot = await client.get_entity(VK_BOT_USERNAME)
        print(f"✅ Бот найден: {bot.first_name}")
        
        # Отправляем тестовое сообщение
        print("\n📤 Отправляю тестовое сообщение боту...")
        msg = await client.send_message(bot, "/start")
        print(f"✅ Сообщение отправлено (ID: {msg.id})")
        
        # Ждем ответ
        await asyncio.sleep(2)
        
        # Проверяем ответ
        from telethon.tl.functions.messages import GetHistoryRequest
        messages = await client(GetHistoryRequest(
            peer=bot,
            limit=1,
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        
        if messages.messages:
            last_msg = messages.messages[0]
            if last_msg.id > msg.id:
                print(f"✅ Получен ответ от бота!")
                print(f"   Сообщение: {last_msg.text[:100]}")
        
        print("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
        
    except Exception as e:
        print(f"❌ Ошибка при проверке бота: {e}")
    
    await client.disconnect()
    print("\n👋 Отключено от Telegram")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = asyncio.run(auth_session())
    if success:
        print("\n✅ Теперь можно запускать бота командой:")
        print("   python run.py")
    else:
        print("\n❌ Авторизация не удалась. Попробуйте еще раз.")
        sys.exit(1)
