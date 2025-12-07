#!/usr/bin/env python3
"""
Авторизация Telegram сессии (параметры берутся из .env)
"""

import os
import asyncio
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
API_ID = int(os.getenv("API_ID", "13801751"))
API_HASH = os.getenv("API_HASH", "ba0fdc4c9c75c16ab3013af244f594e9")
PHONE = os.getenv("ACCOUNT_PHONE", "+15167864134")
SESSION_NAME = os.getenv("SESSION_NAME", "user_session_15167864134")

_raw_bot_list = os.getenv(
    "VK_BOT_USERNAMES",
    "sherlock_bot_ne_bot"
)
TEST_BOTS = []
for bot_name in _raw_bot_list.split(","):
    clean_name = bot_name.strip()
    if not clean_name:
        continue
    TEST_BOTS.append(
        clean_name if clean_name.startswith("@") else f"@{clean_name}"
    )

if not TEST_BOTS:
    TEST_BOTS = ["@sherlock_bot_ne_bot"]

# Прокси (если нужен)
PROXY = {
    'proxy_type': 'socks5',
    'addr': '194.31.73.124',
    'port': 60741,
    'username': 'QzYtokLcGL',
    'password': '4MR8FmpoKN',
    'rdns': True
} if os.getenv("USE_PROXY", "").lower() == "true" else None

async def authenticate():
    """Авторизация сессии"""
    print("=" * 60)
    print(f"🔐 АВТОРИЗАЦИЯ TELEGRAM СЕССИИ")
    print("=" * 60)
    print(f"📱 Номер: {PHONE}")
    print(f"🔑 API_ID: {API_ID}")
    print(f"🗝️  API_HASH: {API_HASH[:10]}...")
    print(f"💾 Сессия: {SESSION_NAME}.session")
    print("=" * 60)

    # Создаем клиента
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=PROXY)

    try:
        print("🔗 Подключение к Telegram...")
        await client.connect()

        if await client.is_user_authorized():
            print("✅ Сессия уже авторизована!")
            me = await client.get_me()
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            return True

        print(f"📞 Отправка кода на {PHONE}...")

        try:
            code_request = await client.send_code_request(PHONE)
        except PhoneNumberInvalidError:
            print(f"❌ Неверный номер телефона: {PHONE}")
            return False

        print("📥 Проверьте SMS или Telegram и введите код...")
        code = input("Введите код: ").strip()

        if not code:
            print("❌ Код не введен")
            return False

        try:
            print("🔐 Авторизация...")
            await client.sign_in(PHONE, code)
            print("✅ Авторизация успешна!")

        except SessionPasswordNeededError:
            print("🔒 Требуется пароль двухфакторной аутентификации")
            password = input("Введите пароль 2FA: ").strip()

            if not password:
                print("❌ Пароль не введен")
                return False

            try:
                await client.sign_in(password=password)
                print("✅ Авторизация с 2FA успешна!")
            except Exception as e:
                print(f"❌ Ошибка 2FA: {e}")
                return False

        # Проверяем успешность авторизации
        if await client.is_user_authorized():
            me = await client.get_me()
            print("\n" + "=" * 40)
            print("✅ АВТОРИЗАЦИЯ ЗАВЕРШЕНА!")
            print("=" * 40)
            print(f"👤 Имя: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            print(f"👑 Username: @{me.username}" if me.username else "👑 Username: не установлен")
            print(f"💾 Сессия: {SESSION_NAME}.session")
            print("=" * 40)
            return True
        else:
            print("❌ Авторизация не удалась")
            return False

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

    finally:
        await client.disconnect()

async def test_bots():
    """Тестирование доступа к ботам"""
    bots = TEST_BOTS

    print("\n" + "=" * 50)
    print("🤖 ТЕСТИРОВАНИЕ БОТОВ")
    print("=" * 50)

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=PROXY)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ Сессия не авторизована")
            return False

        working_bots = []

        for bot_username in bots:
            try:
                print(f"🔍 Проверка {bot_username}...")

                # Получаем бота
                bot = await client.get_entity(bot_username)
                print(f"  ✅ Бот найден: {bot.first_name}")

                # Отправляем тестовое сообщение
                test_msg = await client.send_message(bot, "/start")
                print(f"  ✅ Сообщение отправлено (ID: {test_msg.id})")

                working_bots.append(bot_username)

                # Небольшая пауза между запросами
                await asyncio.sleep(1)

            except Exception as e:
                print(f"  ❌ Ошибка с {bot_username}: {e}")

        print("\n" + "-" * 30)
        print(f"✅ Работающих ботов: {len(working_bots)}/{len(bots)}")
        if working_bots:
            print("🤖 Доступные боты:")
            for bot in working_bots:
                print(f"  • {bot}")
        print("-" * 30)

        return len(working_bots) > 0

    except Exception as e:
        print(f"❌ Ошибка тестирования ботов: {e}")
        return False

    finally:
        await client.disconnect()

async def main():
    """Основная функция"""
    # Авторизация
    if await authenticate():
        # Тестирование ботов
        await test_bots()

        print("\n" + "=" * 60)
        print("🎉 НАСТРОЙКА ЗАВЕРШЕНА!")
        print("=" * 60)
        print("✅ Следующие шаги:")
        print("1. Обновите .env файл с новой конфигурацией")
        print("2. Запустите бота: python run.py")
        print("=" * 60)
    else:
        print("\n❌ Авторизация не удалась")
        print("Проверьте номер телефона и попробуйте снова")

if __name__ == "__main__":
    asyncio.run(main())
