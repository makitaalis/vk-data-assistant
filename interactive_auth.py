#!/usr/bin/env python3
"""
Интерактивная авторизация с вводом кода через файл
"""
import asyncio
import sys
import os
import time
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

# Файлы для обмена данными
CODE_FILE = Path("/home/vkbot/vk-data-assistant/auth_code.txt")
STATUS_FILE = Path("/home/vkbot/vk-data-assistant/auth_status.txt")

def write_status(message):
    """Записывает статус в файл"""
    with open(STATUS_FILE, 'w') as f:
        f.write(message + "\n")
    print(message)

def wait_for_code():
    """Ждет появления файла с кодом"""
    write_status("⏳ Жду код авторизации...")
    write_status(f"📝 Для ввода кода создайте файл: {CODE_FILE}")
    write_status("   с содержимым: только код (например: 12345)")
    
    # Удаляем старый файл с кодом если есть
    if CODE_FILE.exists():
        CODE_FILE.unlink()
    
    # Ждем появления файла
    attempts = 0
    while not CODE_FILE.exists():
        time.sleep(2)
        attempts += 1
        if attempts % 5 == 0:
            write_status(f"⏳ Жду код... (прошло {attempts * 2} секунд)")
        if attempts > 150:  # 5 минут
            write_status("❌ Таймаут ожидания кода (5 минут)")
            return None
    
    # Читаем код
    time.sleep(0.5)  # Даем время на запись
    with open(CODE_FILE, 'r') as f:
        code = f.read().strip()
    
    write_status(f"✅ Получен код: {code}")
    return code

async def auth_with_code():
    """Процесс авторизации с кодом из файла"""
    write_status("=" * 60)
    write_status("🔐 ИНТЕРАКТИВНАЯ АВТОРИЗАЦИЯ TELEGRAM")
    write_status("=" * 60)
    write_status(f"📱 Телефон: {ACCOUNT_PHONE}")
    write_status(f"🤖 Бот для поиска: {VK_BOT_USERNAME}")
    write_status("=" * 60)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        write_status("\n📱 Подключаюсь к Telegram...")
        await client.connect()
        
        if not await client.is_user_authorized():
            write_status(f"\n📞 Отправляю запрос кода на {ACCOUNT_PHONE}...")
            
            # Отправляем запрос кода
            await client.send_code_request(ACCOUNT_PHONE)
            write_status("✅ Запрос кода отправлен!")
            write_status("\n⚠️  ВАЖНО: Проверьте Telegram на телефоне!")
            write_status("📱 Вам должно прийти сообщение с кодом от Telegram")
            
            # Ждем код из файла
            code = wait_for_code()
            
            if not code:
                write_status("❌ Код не получен")
                await client.disconnect()
                return False
            
            try:
                # Пытаемся войти с кодом
                write_status(f"\n🔑 Пытаюсь войти с кодом {code}...")
                await client.sign_in(ACCOUNT_PHONE, code)
                write_status("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
                
            except PhoneCodeInvalidError:
                write_status("❌ Неверный код! Попробуйте еще раз.")
                await client.disconnect()
                return False
                
            except SessionPasswordNeededError:
                # Если включена двухфакторная аутентификация
                write_status("\n🔒 Требуется пароль двухфакторной аутентификации")
                write_status("📝 Создайте файл /home/vkbot/vk-data-assistant/auth_password.txt с паролем")
                
                # Ждем пароль
                password_file = Path("/home/vkbot/vk-data-assistant/auth_password.txt")
                attempts = 0
                while not password_file.exists():
                    time.sleep(2)
                    attempts += 1
                    if attempts > 60:  # 2 минуты
                        write_status("❌ Таймаут ожидания пароля")
                        await client.disconnect()
                        return False
                
                with open(password_file, 'r') as f:
                    password = f.read().strip()
                
                await client.sign_in(password=password)
                write_status("✅ Авторизация с 2FA успешна!")
                
        else:
            write_status("✅ Сессия уже авторизована!")
        
        # Проверяем подключение
        write_status("\n📊 Проверяю подключение...")
        me = await client.get_me()
        write_status(f"✅ Подключен как: {me.first_name} {me.last_name or ''}")
        write_status(f"📱 Телефон: {me.phone}")
        write_status(f"🆔 ID: {me.id}")
        
        # Проверяем доступ к боту
        write_status(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
        bot = await client.get_entity(VK_BOT_USERNAME)
        write_status(f"✅ Бот найден: {bot.first_name}")
        
        # Отправляем тестовое сообщение
        write_status("\n📤 Отправляю тестовое сообщение боту...")
        msg = await client.send_message(bot, "/start")
        write_status(f"✅ Сообщение отправлено (ID: {msg.id})")
        
        await asyncio.sleep(2)
        
        write_status("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
        write_status("=" * 60)
        
        await client.disconnect()
        return True
        
    except Exception as e:
        write_status(f"❌ Ошибка: {e}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    # Очищаем старые файлы
    if CODE_FILE.exists():
        CODE_FILE.unlink()
    if STATUS_FILE.exists():
        STATUS_FILE.unlink()
    
    success = asyncio.run(auth_with_code())
    
    if success:
        write_status("\n✅ Авторизация завершена успешно!")
        write_status("Теперь можно запускать бота: python run.py")
    else:
        write_status("\n❌ Авторизация не удалась")
        sys.exit(1)
