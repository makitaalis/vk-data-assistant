#!/usr/bin/env python3
import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "user_session"
PHONE = "+77086221203"

async def auth():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    await client.connect()

    if not await client.is_user_authorized():
        print(f"Отправляю запрос кода на {PHONE}...")
        await client.send_code_request(PHONE)
        print("Код отправлен! Ожидаю ввод кода...")

        # Ждем код из файла
        code_file = "/home/vkbot/vk-data-assistant/auth_code.txt"
        while not os.path.exists(code_file):
            await asyncio.sleep(1)

        with open(code_file, 'r') as f:
            code = f.read().strip()

        print(f"Получен код: {code}")

        try:
            await client.sign_in(PHONE, code)
            print("Авторизация успешна!")
        except SessionPasswordNeededError:
            print("Требуется пароль 2FA")
            # Используем заранее известный пароль
            password = "5vmoqi3tgjf"
            await client.sign_in(password=password)
            print("Авторизация с 2FA успешна!")
    else:
        print("Уже авторизован!")

    me = await client.get_me()
    print(f"Подключен как: {me.first_name} {me.last_name or ''} ({me.phone})")

    await client.disconnect()

asyncio.run(auth())