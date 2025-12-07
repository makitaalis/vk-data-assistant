#!/usr/bin/env python3
"""
Проверка доступности VK ботов
"""

import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

# Конфигурация
API_ID = int(os.getenv("API_ID", "13801751"))
API_HASH = os.getenv("API_HASH", "ba0fdc4c9c75c16ab3013af244f594e9")
SESSION_NAME = os.getenv("SESSION_NAME", "user_session_15167864134")
ACCOUNT_PHONE = os.getenv("ACCOUNT_PHONE", "+15167864134")

_raw_bot_list = os.getenv(
    "VK_BOT_USERNAMES",
    "sherlock_bot_ne_bot"
)
BOTS_TO_CHECK = [
    bot.strip().lstrip("@")
    for bot in _raw_bot_list.split(",")
    if bot.strip()
]

if not BOTS_TO_CHECK:
    BOTS_TO_CHECK = ["sherlock_bot_ne_bot"]

# Попробуем разные существующие сессии
EXISTING_SESSIONS = [
    f"{SESSION_NAME}.session",
    "new_telegram_session.session",
    "tg_session_new.session",
    "telegram_qr_session.session",
    "user_session_backup_20250921_225736.session"
]

async def check_session_and_bots(session_file):
    """Проверка сессии и ботов"""
    print(f"🔍 Проверка сессии: {session_file}")

    if not os.path.exists(session_file):
        print(f"  ❌ Файл не найден: {session_file}")
        return False, []

    client = TelegramClient(session_file, API_ID, API_HASH)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            print(f"  ❌ Сессия не авторизована")
            return False, []

        # Получаем информацию о пользователе
        me = await client.get_me()
        print(f"  ✅ Авторизован: {me.first_name} {me.last_name or ''} ({me.phone})")

        # Проверяем ботов
        working_bots = []
        print(f"  🤖 Проверка ботов...")

        for bot_username in BOTS_TO_CHECK:
            try:
                bot = await client.get_entity(f"@{bot_username}")
                print(f"    ✅ {bot_username}: {bot.first_name}")
                working_bots.append(bot_username)
                await asyncio.sleep(0.5)  # Пауза между проверками

            except Exception as e:
                print(f"    ❌ {bot_username}: {str(e)[:50]}...")

        print(f"  📊 Работающих ботов: {len(working_bots)}/{len(BOTS_TO_CHECK)}")
        return True, working_bots

    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return False, []

    finally:
        await client.disconnect()

async def main():
    """Основная функция"""
    print("=" * 60)
    print("🔍 ПРОВЕРКА ДОСТУПНОСТИ СЕССИЙ И БОТОВ")
    print("=" * 60)

    best_session = None
    best_bots = []

    for session_file in EXISTING_SESSIONS:
        session_ok, working_bots = await check_session_and_bots(session_file)

        if session_ok and len(working_bots) > len(best_bots):
            best_session = session_file
            best_bots = working_bots

        print("-" * 40)

    print("\n" + "=" * 60)
    print("📊 ИТОГОВЫЙ РЕЗУЛЬТАТ")
    print("=" * 60)

    if best_session:
        print(f"🎯 Лучшая сессия: {best_session}")
        print(f"🤖 Доступных ботов: {len(best_bots)}")

        if best_bots:
            print("✅ Работающие боты:")
            for bot in best_bots:
                print(f"  • @{bot}")

            print(f"\n🔧 Рекомендуемая конфигурация .env:")
            print(f"SESSION_NAME={best_session.replace('.session', '')}")
            print(f"VK_BOT_USERNAMES={','.join(best_bots)}")
            print(f"VK_BOT_USERNAME={best_bots[0]}")
        else:
            print("❌ Ни один бот не доступен")
    else:
        print("❌ Ни одна сессия не работает")
        print("\n📋 Необходимо:")
        print(f"1. Авторизовать новую сессию для {ACCOUNT_PHONE}")
        print("2. Запустить: python auth_session_15167864134.py")

    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
