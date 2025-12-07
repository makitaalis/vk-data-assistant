#!/usr/bin/env python3
"""
Простая авторизация Telegram сессии для PyCharm
Номер по умолчанию: +15167864134

ВАЖНО: API ID и API Hash ОБЯЗАТЕЛЬНЫ для Telegram API!
Получите их на https://my.telegram.org

После авторизации скопируйте файл user_session.session на сервер
"""

import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# ВАЖНО: Замените на ваши реальные данные с https://my.telegram.org
API_ID = 23690277  # Ваш API ID
API_HASH = "a95df6666bc0bc570891b5114b702cd1"  # Ваш API Hash
SESSION_NAME = os.getenv("SESSION_NAME", "user_session_15167864134")
PHONE = os.getenv("ACCOUNT_PHONE", "+15167864134")

_raw_bot = os.getenv("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot and not _raw_bot.startswith("@"):
    _raw_bot = f"@{_raw_bot}"
PRIMARY_BOT = _raw_bot

print("=" * 60)
print("🔐 АВТОРИЗАЦИЯ TELEGRAM СЕССИИ")
print("=" * 60)
print(f"📱 Номер: {PHONE}")
print(f"🆔 API ID: {API_ID}")
print(f"📂 Файл сессии: {SESSION_NAME}.session")
print("=" * 60)

async def create_session():
    """Создание сессии"""
    
    # Создаем клиент (БЕЗ прокси для локального использования)
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        print("🔗 Подключение к Telegram...")
        await client.connect()
        
        # Проверяем авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Уже авторизован: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            await test_bot(client)
            await client.disconnect()
            print_success()
            return True
        
        # Запрашиваем код
        print(f"📞 Отправляю код на {PHONE}...")
        await client.send_code_request(PHONE)
        print("✅ Код отправлен! Проверьте Telegram")
        
        # Ввод кода
        while True:
            try:
                code = input("\n✏️  Введите код из Telegram: ").strip()
                if not code:
                    print("❌ Введите код!")
                    continue
                    
                await client.sign_in(PHONE, code)
                print("✅ Код принят!")
                break
                
            except PhoneCodeInvalidError:
                print("❌ Неверный код! Попробуйте еще раз")
                continue
                
            except SessionPasswordNeededError:
                print("🔒 Нужен пароль 2FA")
                while True:
                    password = input("🔑 Введите пароль 2FA: ").strip()
                    if not password:
                        continue
                    try:
                        await client.sign_in(password=password)
                        print("✅ 2FA успешно!")
                        break
                    except:
                        print("❌ Неверный пароль 2FA")
                break
        
        # Проверяем результат
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"\n✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            print(f"👤 {me.first_name} {me.last_name or ''}")
            print(f"📱 {me.phone}")
            
            await test_bot(client)
            await client.disconnect()
            print_success()
            return True
        else:
            print("❌ Авторизация не удалась")
            await client.disconnect()
            return False
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await client.disconnect()
        return False

async def test_bot(client):
    """Тест бота"""
    print(f"\n🤖 Тестирую бота {PRIMARY_BOT}...")
    try:
        bot = await client.get_entity(PRIMARY_BOT)
        print(f"✅ Бот найден: {bot.first_name}")
        
        msg = await client.send_message(bot, "/start")
        print(f"✅ Сообщение отправлено (ID: {msg.id})")
        
    except Exception as e:
        print(f"⚠️ Проблема с ботом: {e}")

def print_success():
    """Инструкции после успеха"""
    print("\n" + "=" * 60)
    print("🎉 СЕССИЯ СОЗДАНА УСПЕШНО!")
    print("=" * 60)
    print(f"📂 Файл: {SESSION_NAME}.session")
    print("\n📋 ЧТО ДЕЛАТЬ ДАЛЬШЕ:")
    print("1. Найдите файл user_session.session в папке проекта")
    print("2. Скопируйте его на сервер в папку /home/vkbot/vk-data-assistant/")
    print("3. Замените существующий файл")
    print("4. Запустите бота: python run.py")
    print("\n💡 СПОСОБЫ КОПИРОВАНИЯ:")
    print("- SFTP/SCP")
    print("- Панель управления хостингом")
    print("- Файловый менеджер")
    print("\n⚠️ НЕ публикуйте файл сессии в открытых репозиториях!")
    print("=" * 60)

if __name__ == "__main__":
    print("⚠️ ВАЖНО: Запускайте этот скрипт в PyCharm на локальном компьютере!")
    print("API ID и API Hash ОБЯЗАТЕЛЬНЫ для работы с Telegram API")
    print("Получите их на https://my.telegram.org\n")
    
    try:
        success = asyncio.run(create_session())
        if not success:
            print("\n❌ Не удалось создать сессию")
    except KeyboardInterrupt:
        print("\n❌ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
