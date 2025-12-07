#!/usr/bin/env python3
"""
Авторизация с обработкой email требования
"""
import asyncio
import sys
import os
from telethon import TelegramClient
from telethon.tl.functions.account import SendVerifyEmailCodeRequest, VerifyEmailRequest
from telethon.errors import SessionPasswordNeededError

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Конфигурация
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")
EMAIL = "aliensobering@gmail.com"

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def auth_with_email():
    """Авторизация с установкой email"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ С EMAIL")
    print("=" * 60)
    print(f"📱 Телефон: {ACCOUNT_PHONE}")
    print(f"📧 Email: {EMAIL}")
    print("=" * 60)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        print("\n📡 Подключаюсь к Telegram...")
        await client.connect()
        print("✅ Подключение установлено")
        
        if await client.is_user_authorized():
            print("✅ Сессия уже авторизована!")
            me = await client.get_me()
            print(f"👤 Пользователь: {me.first_name}")
            await client.disconnect()
            return True
        
        print(f"\n📤 Отправляю запрос кода...")
        
        # Отправляем запрос кода
        sent_code = await client.send_code_request(ACCOUNT_PHONE)
        print(f"📊 Тип: {sent_code.type.__class__.__name__}")
        
        # Если требуется email
        if 'EmailRequired' in str(sent_code.type.__class__.__name__):
            print("\n📧 Требуется email. Пытаюсь установить...")
            
            # Пробуем установить email через новый API
            try:
                # Отправляем email для верификации
                print(f"📧 Отправляю запрос верификации на {EMAIL}...")
                email_result = await client(SendVerifyEmailCodeRequest(
                    purpose='login',
                    email=EMAIL
                ))
                print(f"✅ Код верификации отправлен на {EMAIL}")
                print("📨 Проверьте почту и введите код из письма")
                
                email_code = input("✏️ Введите код из email: ")
                
                # Подтверждаем email
                await client(VerifyEmailRequest(
                    purpose='login',
                    verification=email_code
                ))
                print("✅ Email подтвержден!")
                
                # Теперь пробуем получить код еще раз
                sent_code = await client.send_code_request(ACCOUNT_PHONE)
                
            except Exception as e:
                print(f"⚠️ Не удалось установить email через API: {e}")
                print("\n🔄 АЛЬТЕРНАТИВНЫЙ МЕТОД:")
                print("1. Откройте Telegram на телефоне")
                print("2. Перейдите в Настройки")
                print(f"3. Добавьте email: {EMAIL}")
                print("4. Подтвердите его")
                print("5. Затем запустите этот скрипт снова")
                await client.disconnect()
                return False
        
        # Теперь должен прийти обычный код
        print("\n📱 Код должен прийти в Telegram или SMS")
        print("Проверьте:")
        print("• Telegram на другом устройстве")
        print("• SMS сообщения")
        print("• Сообщения от 'Telegram' или '777000'")
        
        code = input("\n✏️ Введите код авторизации: ")
        
        try:
            print(f"\n🔐 Вхожу с кодом {code}...")
            await client.sign_in(ACCOUNT_PHONE, code)
            print("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            
            me = await client.get_me()
            print(f"\n👤 Авторизован как: {me.first_name}")
            print(f"📱 Телефон: {me.phone}")
            
            # Проверяем бота
            print("\n🤖 Проверяю бота...")
            bot = await client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Бот найден: {bot.first_name}")
            
            msg = await client.send_message(bot, "/start")
            print(f"✅ Тестовое сообщение отправлено")
            
            print("\n🎉 ГОТОВО К РАБОТЕ!")
            await client.disconnect()
            return True
            
        except SessionPasswordNeededError:
            print("🔒 Требуется пароль 2FA")
            password = input("🔑 Введите пароль: ")
            await client.sign_in(password=password)
            print("✅ Вход выполнен!")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        
        # Анализ ошибки
        error = str(e).lower()
        if 'email' in error:
            print("\n📧 ТРЕБУЕТСЯ НАСТРОЙКА EMAIL В TELEGRAM!")
            print("Инструкция:")
            print("1. Откройте Telegram на телефоне")
            print("2. Настройки → Конфиденциальность → Email")
            print(f"3. Добавьте: {EMAIL}")
            print("4. Подтвердите email")
            print("5. Запустите скрипт снова")
        elif 'flood' in error:
            print("\n⏰ Слишком много попыток!")
            print("Подождите 15-30 минут")
            
        await client.disconnect()
        return False

if __name__ == "__main__":
    success = asyncio.run(auth_with_email())
    
    if success:
        print("\n✅ Можно запускать бота: python run.py")
    else:
        print("\n❌ Требуется настроить email в Telegram")
        sys.exit(1)
