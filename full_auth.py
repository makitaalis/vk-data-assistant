#!/usr/bin/env python3
"""
Полная авторизация с 2FA и email
"""
import asyncio
import sys
import os
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Конфигурация
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

# Файлы для интерактивного ввода
CODE_FILE = Path("/home/vkbot/vk-data-assistant/auth_code.txt")
PASSWORD_FILE = Path("/home/vkbot/vk-data-assistant/auth_2fa_password.txt")

async def full_auth():
    """Полная авторизация с поддержкой 2FA"""
    print("=" * 60)
    print("🔐 ПОЛНАЯ АВТОРИЗАЦИЯ TELEGRAM")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print("✅ Email установлен")
    print("✅ 2FA включена")
    print("=" * 60)
    
    # Очищаем старые файлы
    for f in [CODE_FILE, PASSWORD_FILE]:
        if f.exists():
            f.unlink()
    
    # Удаляем старую сессию для чистого старта
    session_file = Path(f"{SESSION_NAME}.session")
    if session_file.exists():
        print("🗑 Удаляю старую сессию...")
        session_file.unlink()
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        print("\n📡 Подключаюсь к Telegram...")
        await client.connect()
        print("✅ Подключение установлено")
        
        if await client.is_user_authorized():
            print("✅ Сессия уже авторизована!")
            me = await client.get_me()
            print(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            print(f"📱 Телефон: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            # Проверяем бота
            print("\n🤖 Проверяю доступ к боту...")
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Бот найден: {bot.first_name}")
            
            msg = await client.send_message(bot, "/start")
            print(f"✅ Тестовое сообщение отправлено (ID: {msg.id})")
            
            print("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
            await client.disconnect()
            return True
        
        print(f"\n📤 Отправляю запрос кода на {ACCOUNT_PHONE}...")
        
        # Отправляем запрос кода
        sent_code = await client.send_code_request(ACCOUNT_PHONE)
        
        print("✅ Запрос успешно отправлен!")
        print(f"📊 Тип отправки: {sent_code.type.__class__.__name__}")
        
        # Определяем куда отправлен код
        if 'App' in str(sent_code.type.__class__.__name__):
            print("\n📱 КОД ОТПРАВЛЕН В TELEGRAM!")
            print("Проверьте сообщения от 'Telegram' или '777000'")
        elif 'Sms' in str(sent_code.type.__class__.__name__):
            print("\n💬 КОД ОТПРАВЛЕН ЧЕРЕЗ SMS!")
        elif 'Email' in str(sent_code.type.__class__.__name__):
            print("\n📧 КОД ОТПРАВЛЕН НА EMAIL!")
            print("Проверьте aliensobering@gmail.com")
        else:
            print(f"\n❓ Тип: {sent_code.type.__class__.__name__}")
        
        print("\n" + "=" * 40)
        print("ВВЕДИТЕ КОД АВТОРИЗАЦИИ")
        print("=" * 40)
        
        # Интерактивный режим
        try:
            code = input("✏️  Введите код (5-6 цифр): ")
        except:
            # Если интерактивный ввод не работает, используем файл
            print("\n📝 Создайте файл с кодом:")
            print(f"   {CODE_FILE}")
            print("   Содержимое: только код (например: 12345)")
            
            # Ждем файл
            attempts = 0
            while not CODE_FILE.exists():
                await asyncio.sleep(2)
                attempts += 1
                if attempts % 10 == 0:
                    print(f"⏳ Жду код... (прошло {attempts * 2} сек)")
                if attempts > 150:
                    print("❌ Таймаут ожидания кода")
                    await client.disconnect()
                    return False
            
            with open(CODE_FILE, 'r') as f:
                code = f.read().strip()
        
        print(f"\n🔐 Вхожу с кодом: {code}")
        
        try:
            # Пробуем войти с кодом
            await client.sign_in(ACCOUNT_PHONE, code)
            print("✅ Код принят!")
            
        except SessionPasswordNeededError:
            # Требуется пароль 2FA
            print("\n🔒 ТРЕБУЕТСЯ ПАРОЛЬ 2FA!")
            print("=" * 40)
            
            # Интерактивный ввод пароля
            try:
                password = input("🔑 Введите пароль 2FA: ")
            except:
                # Если интерактивный ввод не работает
                print("\n📝 Создайте файл с паролем:")
                print(f"   {PASSWORD_FILE}")
                print("   Содержимое: ваш пароль 2FA")
                
                # Ждем файл
                attempts = 0
                while not PASSWORD_FILE.exists():
                    await asyncio.sleep(2)
                    attempts += 1
                    if attempts % 10 == 0:
                        print(f"⏳ Жду пароль... (прошло {attempts * 2} сек)")
                    if attempts > 60:
                        print("❌ Таймаут ожидания пароля")
                        await client.disconnect()
                        return False
                
                with open(PASSWORD_FILE, 'r') as f:
                    password = f.read().strip()
            
            print("🔐 Вхожу с паролем 2FA...")
            await client.sign_in(password=password)
            print("✅ Авторизация с 2FA успешна!")
            
        except PhoneCodeInvalidError:
            print("❌ Неверный код! Попробуйте еще раз.")
            await client.disconnect()
            return False
        
        # Проверяем успешную авторизацию
        print("\n📊 Проверяю авторизацию...")
        me = await client.get_me()
        print(f"✅ Авторизован как: {me.first_name} {me.last_name or ''}")
        print(f"📱 Телефон: {me.phone}")
        print(f"🆔 ID: {me.id}")
        
        # Проверяем доступ к боту
        print(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
        try:
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Бот найден: {bot.first_name}")
            
            # Отправляем тестовое сообщение
            msg = await client.send_message(bot, "/start")
            print(f"✅ Тестовое сообщение отправлено (ID: {msg.id})")
            
            await asyncio.sleep(2)
            
            # Проверяем ответ
            from telethon.tl.functions.messages import GetHistoryRequest
            messages = await client(GetHistoryRequest(
                peer=bot,
                limit=3,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            if messages.messages:
                for m in messages.messages:
                    if m.id > msg.id and m.sender_id == bot.id:
                        print(f"✅ Получен ответ от бота: {m.text[:100]}")
                        break
            
        except Exception as e:
            print(f"⚠️ Ошибка при проверке бота: {e}")
        
        print("\n" + "=" * 60)
        print("🎉 АВТОРИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        print("✅ Сессия сохранена")
        print("✅ Можно запускать основного бота")
        
        await client.disconnect()
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        
        # Анализ ошибки
        error_str = str(e).lower()
        if 'flood' in error_str:
            print("\n⏰ ФЛУД-КОНТРОЛЬ!")
            print("Слишком много попыток. Подождите 15-30 минут.")
        elif 'invalid' in error_str:
            print("\n❌ Неверные данные!")
            print("Проверьте код или пароль.")
        
        await client.disconnect()
        return False

if __name__ == "__main__":
    print("🚀 Запуск полной авторизации...")
    print("Убедитесь, что:")
    print("✅ Email настроен в Telegram")
    print("✅ 2FA включена")
    print("✅ У вас есть доступ к коду и паролю")
    print()
    
    success = asyncio.run(full_auth())
    
    if success:
        print("\n✅ Готово! Теперь запустите бота:")
        print("   python run.py")
        sys.exit(0)
    else:
        print("\n❌ Авторизация не удалась")
        print("Проверьте:")
        print("• Правильность кода")
        print("• Правильность пароля 2FA")
        print("• Нет ли флуд-контроля (подождите 15 мин)")
        sys.exit(1)
