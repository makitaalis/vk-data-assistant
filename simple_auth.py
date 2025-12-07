#!/usr/bin/env python3
import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
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
    print("=" * 50)
    print("TELEGRAM АВТОРИЗАЦИЯ")
    print("=" * 50)
    
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
    
    if not await client.is_user_authorized():
        print(f"Телефон: {PHONE}")
        print("\nОтправляю код...")
        
        await client.send_code_request(PHONE)
        
        print("\n" + "=" * 50)
        print("КОД ОТПРАВЛЕН!")
        print("Проверьте Telegram на телефоне")
        print("=" * 50)
        
        code = input("\nВведите код из Telegram: ")
        
        try:
            await client.sign_in(PHONE, code)
            print("\n✓ Код принят!")
            
        except SessionPasswordNeededError:
            print("\n" + "=" * 50)
            print("ТРЕБУЕТСЯ ПАРОЛЬ 2FA")
            print("=" * 50)
            
            password = input("\nВведите пароль 2FA: ")
            await client.sign_in(password=password)
            print("\n✓ Пароль принят!")
    
    # Проверка авторизации
    if await client.is_user_authorized():
        me = await client.get_me()
        print("\n" + "=" * 50)
        print("АВТОРИЗАЦИЯ УСПЕШНА!")
        print(f"Пользователь: {me.first_name} {me.last_name or ''}")
        print(f"ID: {me.id}")
        print(f"Телефон: {me.phone}")
        print("=" * 50)
        
        # Тест бота
        print("\nПроверка бота...")
        try:
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✓ Бот доступен: {bot.first_name}")
            await client.send_message(VK_BOT_USERNAME, "/start")
            print("✓ Сообщение отправлено")
        except Exception as e:
            print(f"Ошибка с ботом: {e}")
    else:
        print("\n✗ Авторизация не удалась")
        
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
