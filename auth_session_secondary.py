#!/usr/bin/env python3
"""
Авторизация вторичной Telegram-сессии.
Читает параметры из SESSION_NAME_SECONDARY/ACCOUNT_PHONE_SECONDARY, а при их отсутствии
падает обратно на основные переменные (SESSION_NAME/ACCOUNT_PHONE).
"""

import asyncio
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")

SESSION_NAME = os.environ.get("SESSION_NAME_SECONDARY") or os.environ.get("SESSION_NAME")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE_SECONDARY") or os.environ.get("ACCOUNT_PHONE")

if not SESSION_NAME or not ACCOUNT_PHONE:
    raise SystemExit("Не заданы переменные SESSION_NAME_SECONDARY/ACCOUNT_PHONE_SECONDARY")

SESSION_PATH = Path("data/sessions") / SESSION_NAME
SESSION_PATH.mkdir(parents=True, exist_ok=True)


async def main():
    print("=" * 60)
    print(f"🔐 Авторизация вторичной сессии {SESSION_NAME}")
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print("=" * 60)

    client = TelegramClient(str(SESSION_PATH / SESSION_NAME), API_ID, API_HASH)

    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✅ Уже авторизован как {me.first_name} ({me.phone})")
        await client.disconnect()
        return

    await client.send_code_request(ACCOUNT_PHONE)
    code = input("✏️ Введите код из Telegram: ")

    try:
        await client.sign_in(ACCOUNT_PHONE, code)
    except SessionPasswordNeededError:
        password = input("🔑 Введите пароль 2FA: ")
        await client.sign_in(password=password)

    me = await client.get_me()
    print(f"✅ Успешно авторизован как {me.first_name} {me.last_name or ''} ({me.phone})")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
