#!/usr/bin/env python3
import asyncio
import os
import time
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
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    
    if not await client.is_user_authorized():
        print(f"Телефон: {PHONE}")
        print("\nОтправляю код...")
        
        await client.send_code_request(PHONE)
        
        print("\n" + "=" * 50)
        print("КОД ОТПРАВЛЕН В TELEGRAM!")
        print("=" * 50)
        print("\nСоздайте файл: /home/vkbot/vk-data-assistant/code.txt")
        print("Напишите в нем код из Telegram (только цифры)")
        print("\nЖду код...")
        
        # Ждем файл с кодом
        code = None
        for i in range(60):  # 10 минут
            if os.path.exists("code.txt"):
                with open("code.txt", "r") as f:
                    code = f.read().strip()
                if code:
                    print(f"\n✓ Код получен: {code}")
                    os.remove("code.txt")
                    break
            time.sleep(10)
            if i % 3 == 0 and i > 0:
                print(f"Жду код... ({i*10} сек)")
        
        if not code:
            print("\n✗ Код не получен")
            await client.disconnect()
            return
        
        try:
            await client.sign_in(PHONE, code)
            print("\n✓ Авторизация успешна!")
            
        except SessionPasswordNeededError:
            print("\n" + "=" * 50)
            print("ТРЕБУЕТСЯ ПАРОЛЬ 2FA")
            print("=" * 50)
            print("\nСоздайте файл: /home/vkbot/vk-data-assistant/password.txt")
            print("Напишите в нем ваш пароль 2FA")
            print("\nЖду пароль...")
            
            # Ждем файл с паролем
            password = None
            for i in range(30):  # 5 минут
                if os.path.exists("password.txt"):
                    with open("password.txt", "r") as f:
                        password = f.read().strip()
                    if password:
                        print(f"\n✓ Пароль получен")
                        os.remove("password.txt")
                        break
                time.sleep(10)
                if i % 3 == 0 and i > 0:
                    print(f"Жду пароль... ({i*10} сек)")
            
            if not password:
                print("\n✗ Пароль не получен")
                await client.disconnect()
                return
                
            await client.sign_in(password=password)
            print("\n✓ Авторизация с 2FA успешна!")
    
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
            print("✓ Сообщение отправлено боту")
        except Exception as e:
            print(f"✗ Ошибка с ботом: {e}")
            
        print("\n" + "=" * 50)
        print("СЕССИЯ СОХРАНЕНА!")
        print(f"Файл: {SESSION_NAME}.session")
        print("=" * 50)
    else:
        print("\n✗ Авторизация не удалась")
        
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
