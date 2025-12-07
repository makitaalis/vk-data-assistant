#!/usr/bin/env python3
"""
Скрипт для создания Telegram сессии в PyCharm
Запустите этот файл в PyCharm, авторизуйтесь, затем скопируйте файл сессии на сервер

Инструкция:
1. Скопируйте этот файл в PyCharm
2. Создайте .env файл с теми же настройками
3. Запустите скрипт
4. Введите код из Telegram
5. Скопируйте созданный файл user_session.session на сервер
"""

import asyncio
import os
import sys
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Конфигурация из .env файла
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE", "")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

print("=" * 80)
print("🔐 СОЗДАНИЕ TELEGRAM СЕССИИ ДЛЯ PYCHARM")
print("=" * 80)
print(f"📱 Телефон: {ACCOUNT_PHONE}")
print(f"🆔 API ID: {API_ID}")
print(f"📂 Файл сессии: {SESSION_NAME}.session")
print("=" * 80)

if not API_ID or not API_HASH or not ACCOUNT_PHONE:
    print("❌ ОШИБКА: Не найдены настройки в .env файле!")
    print("\nСоздайте файл .env с содержимым:")
    print("""
# Telegram API Configuration
API_ID=ваш_api_id
API_HASH=ваш_api_hash

# Session Configuration
SESSION_NAME=user_session

# Ваш номер телефона
ACCOUNT_PHONE=+380936884294
""")
    sys.exit(1)

async def create_session():
    """Создание и авторизация сессии"""
    
    # Создаем клиент БЕЗ прокси для локальной машины
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        print("\n🔗 Подключение к Telegram...")
        await client.connect()
        
        # Проверяем, авторизован ли уже
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Уже авторизован!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            await test_bot_access(client)
            await client.disconnect()
            
            print(f"\n🎉 СЕССИЯ ГОТОВА!")
            print(f"📂 Файл сессии: {SESSION_NAME}.session")
            print_copy_instructions()
            return True
        
        # Начинаем процесс авторизации
        print(f"\n📞 Отправляю код на {ACCOUNT_PHONE}...")
        sent_code = await client.send_code_request(ACCOUNT_PHONE)
        
        print(f"✅ Код отправлен! Тип: {sent_code.type}")
        print("\n📱 ПРОВЕРЬТЕ TELEGRAM:")
        print("- SMS сообщения")
        print("- Звонки")
        print("- Уведомления в Telegram приложении")
        print("- Веб-версию telegram.org")
        print("-" * 40)
        
        # Запрашиваем код
        while True:
            try:
                code = input("✏️  Введите код из Telegram: ").strip()
                
                if not code:
                    print("❌ Код не может быть пустым!")
                    continue
                
                print(f"🔄 Проверяю код: {code}")
                await client.sign_in(ACCOUNT_PHONE, code)
                print("✅ Код принят!")
                break
                
            except PhoneCodeInvalidError:
                print("❌ Неверный код! Попробуйте еще раз.")
                continue
                
            except SessionPasswordNeededError:
                print("\n🔒 Требуется пароль двухфакторной аутентификации")
                
                while True:
                    try:
                        password = input("🔑 Введите пароль 2FA: ").strip()
                        if not password:
                            print("❌ Пароль не может быть пустым!")
                            continue
                            
                        await client.sign_in(password=password)
                        print("✅ Авторизация с 2FA успешна!")
                        break
                        
                    except Exception as e:
                        print(f"❌ Неверный пароль 2FA: {e}")
                        continue
                break
                
            except Exception as e:
                print(f"❌ Ошибка при авторизации: {e}")
                continue
        
        # Проверяем успешную авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"\n✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            await test_bot_access(client)
            
            print(f"\n🎉 СЕССИЯ СОЗДАНА УСПЕШНО!")
            print(f"📂 Файл сессии: {SESSION_NAME}.session")
            print_copy_instructions()
            
            await client.disconnect()
            return True
        else:
            print("❌ Авторизация не удалась")
            await client.disconnect()
            return False
            
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        await client.disconnect()
        return False

async def test_bot_access(client):
    """Тестирование доступа к боту"""
    print(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
    try:
        bot = await client.get_entity(VK_BOT_USERNAME)
        print(f"✅ Бот найден: {bot.first_name}")
        
        # Отправляем тестовое сообщение
        msg = await client.send_message(bot, "/start")
        print(f"✅ Тестовое сообщение отправлено (ID: {msg.id})")
        
        # Небольшая пауза для получения ответа
        await asyncio.sleep(2)
        
        print("✅ Бот доступен и отвечает!")
        
    except Exception as e:
        print(f"⚠️ Проблема с ботом: {e}")
        print("Но сессия создана успешно!")

def print_copy_instructions():
    """Инструкции по копированию файла сессии"""
    session_file = Path(f"{SESSION_NAME}.session")
    
    print("\n" + "=" * 80)
    print("📋 ИНСТРУКЦИИ ПО КОПИРОВАНИЮ СЕССИИ НА СЕРВЕР:")
    print("=" * 80)
    
    if session_file.exists():
        size = session_file.stat().st_size
        print(f"📂 Файл сессии: {session_file.absolute()}")
        print(f"📏 Размер файла: {size} байт")
        print()
        print("🔄 СПОСОБЫ КОПИРОВАНИЯ:")
        print()
        print("1️⃣ SCP (если есть SSH доступ):")
        print(f"   scp {session_file.name} user@your-server:/home/vkbot/vk-data-assistant/")
        print()
        print("2️⃣ SFTP:")
        print(f"   - Подключитесь к серверу через SFTP")
        print(f"   - Загрузите файл {session_file.name}")
        print(f"   - В папку /home/vkbot/vk-data-assistant/")
        print()
        print("3️⃣ Панель управления хостингом:")
        print(f"   - Загрузите файл {session_file.name} через файловый менеджер")
        print(f"   - В директорию проекта")
        print()
        print("4️⃣ Git (НЕ РЕКОМЕНДУЕТСЯ для продакшена):")
        print(f"   - Добавьте файл в репозиторий (только для тестов!)")
        print(f"   - git add {session_file.name}")
        print(f"   - git commit -m 'Add session file'")
        print(f"   - git push")
        print()
        print("⚠️  ВАЖНО: Файл сессии содержит авторизационные данные!")
        print("   Не публикуйте его в открытых репозиториях!")
        print()
        print("✅ После копирования на сервер запустите: python run.py")
    else:
        print("❌ Файл сессии не найден!")
    
    print("=" * 80)

def main():
    """Основная функция"""
    try:
        success = asyncio.run(create_session())
        
        if success:
            print("\n🎉 ГОТОВО! Сессия создана успешно!")
        else:
            print("\n❌ Создание сессии не удалось")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n❌ Процесс прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
