#!/usr/bin/env python3
"""
Финальная попытка авторизации с обработкой email
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

# Файлы для обмена
CODE_FILE = Path("/home/vkbot/vk-data-assistant/enter_code.txt")
EMAIL_FILE = Path("/home/vkbot/vk-data-assistant/enter_email.txt")
STATUS_FILE = Path("/home/vkbot/vk-data-assistant/auth_progress.txt")

def write_status(msg):
    """Записывает статус в файл и консоль"""
    print(msg)
    with open(STATUS_FILE, 'a') as f:
        f.write(f"{msg}\n")

async def final_auth():
    """Финальная попытка авторизации"""
    write_status("=" * 60)
    write_status("🔐 ФИНАЛЬНАЯ АВТОРИЗАЦИЯ TELEGRAM")
    write_status("=" * 60)
    write_status(f"📱 Телефон: {ACCOUNT_PHONE}")
    write_status("=" * 60)
    
    # Очищаем старые файлы
    for f in [CODE_FILE, EMAIL_FILE, STATUS_FILE]:
        if f.exists():
            f.unlink()
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        write_status("\n📡 Подключаюсь к Telegram...")
        await client.connect()
        write_status("✅ Подключение установлено")
        
        if await client.is_user_authorized():
            write_status("✅ Сессия уже авторизована!")
            me = await client.get_me()
            write_status(f"👤 Пользователь: {me.first_name} {me.last_name or ''}")
            await client.disconnect()
            return True
        
        write_status(f"\n📤 Отправляю запрос кода на {ACCOUNT_PHONE}...")
        
        # Отправляем запрос кода
        sent_code = await client.send_code_request(ACCOUNT_PHONE)
        
        write_status("✅ Запрос отправлен!")
        
        # Анализируем тип ответа
        code_type = sent_code.type.__class__.__name__
        write_status(f"📊 Тип запроса: {code_type}")
        
        # Проверяем, требуется ли email
        if 'EmailRequired' in code_type or 'SetUpEmail' in code_type:
            write_status("\n📧 ТРЕБУЕТСЯ EMAIL!")
            write_status("=" * 40)
            write_status("Telegram требует привязать email к аккаунту.")
            write_status(f"Создайте файл: {EMAIL_FILE}")
            write_status("с вашим email адресом (например: user@gmail.com)")
            write_status("=" * 40)
            
            # Ждем email
            attempts = 0
            while not EMAIL_FILE.exists():
                await asyncio.sleep(2)
                attempts += 1
                if attempts % 10 == 0:
                    write_status(f"⏳ Жду email... (прошло {attempts * 2} сек)")
                if attempts > 150:  # 5 минут
                    write_status("❌ Таймаут ожидания email")
                    await client.disconnect()
                    return False
            
            # Читаем email
            with open(EMAIL_FILE, 'r') as f:
                email = f.read().strip()
            
            write_status(f"📧 Получен email: {email}")
            
            # TODO: Здесь нужно отправить email в Telegram
            # Но это требует специального API вызова
            write_status("⚠️ Настройка email через API пока не реализована")
            write_status("Пожалуйста, настройте email в официальном приложении Telegram")
            
        else:
            # Код должен быть отправлен
            write_status("\n✅ Код отправлен!")
            
            # Определяем куда отправлен код
            if 'App' in code_type:
                write_status("📱 Код отправлен в TELEGRAM на другое устройство")
                write_status("   Проверьте сообщения от 'Telegram' или '777000'")
            elif 'Sms' in code_type:
                write_status("💬 Код отправлен через SMS")
            elif 'Call' in code_type:
                write_status("☎️ Код будет через ЗВОНОК")
            else:
                write_status(f"❓ Тип отправки: {code_type}")
            
            write_status("\n" + "=" * 40)
            write_status(f"Создайте файл: {CODE_FILE}")
            write_status("с кодом авторизации (например: 12345)")
            write_status("=" * 40)
            
            # Ждем код
            attempts = 0
            while not CODE_FILE.exists():
                await asyncio.sleep(2)
                attempts += 1
                if attempts % 10 == 0:
                    write_status(f"⏳ Жду код... (прошло {attempts * 2} сек)")
                if attempts > 150:  # 5 минут
                    write_status("❌ Таймаут ожидания кода")
                    await client.disconnect()
                    return False
            
            # Читаем код
            await asyncio.sleep(0.5)
            with open(CODE_FILE, 'r') as f:
                code = f.read().strip()
            
            write_status(f"\n🔑 Получен код: {code}")
            write_status("🔐 Пытаюсь войти...")
            
            try:
                await client.sign_in(ACCOUNT_PHONE, code)
                write_status("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
                
                me = await client.get_me()
                write_status(f"\n👤 Авторизован как: {me.first_name} {me.last_name or ''}")
                write_status(f"📱 Телефон: {me.phone}")
                write_status(f"🆔 ID: {me.id}")
                
                # Проверяем бота
                write_status(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
                bot = await client.get_entity(VK_BOT_USERNAME)
                write_status(f"✅ Бот найден: {bot.first_name}")
                
                # Тестовое сообщение
                msg = await client.send_message(bot, "/start")
                write_status(f"✅ Тестовое сообщение отправлено (ID: {msg.id})")
                
                await asyncio.sleep(2)
                
                write_status("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
                await client.disconnect()
                return True
                
            except PhoneCodeInvalidError:
                write_status("❌ Неверный код!")
                
            except SessionPasswordNeededError:
                write_status("🔒 Требуется пароль 2FA")
                write_status("Но вы сказали, что отключили 2FA...")
        
        await client.disconnect()
        
    except Exception as e:
        write_status(f"❌ Ошибка: {e}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    success = asyncio.run(final_auth())
    
    if success:
        write_status("\n✅ Авторизация завершена!")
        write_status("Теперь можно запускать бота: python run.py")
    else:
        write_status("\n❌ Авторизация не удалась")
        sys.exit(1)
