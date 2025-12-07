#!/usr/bin/env python3
"""
Авторизация Telegram через QR-код
"""
import asyncio
import sys
import os
import base64
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import qrcode
from io import BytesIO

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Конфигурация
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

# Файлы для QR кода
QR_IMAGE_FILE = Path("/home/vkbot/vk-data-assistant/telegram_qr.png")
QR_TEXT_FILE = Path("/home/vkbot/vk-data-assistant/telegram_qr.txt")

async def auth_with_qr():
    """Авторизация через QR-код"""
    print("=" * 60)
    print("🔐 АВТОРИЗАЦИЯ TELEGRAM ЧЕРЕЗ QR-КОД")
    print("=" * 60)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    try:
        print("📱 Подключаюсь к Telegram...")
        await client.connect()
        
        if not await client.is_user_authorized():
            print("\n📲 Генерирую QR-код для авторизации...")
            
            # Генерируем QR для входа
            qr_login = await client.qr_login()
            
            # Создаем QR-код изображение
            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=4,
            )
            
            # URL для QR кода
            qr_url = qr_login.url
            qr.add_data(qr_url)
            qr.make(fit=True)
            
            # Сохраняем как изображение
            img = qr.make_image(fill_color="black", back_color="white")
            img.save(QR_IMAGE_FILE)
            print(f"✅ QR-код сохранен в: {QR_IMAGE_FILE}")
            
            # Также сохраняем как ASCII для терминала
            from qrcode import console_qr
            qr_ascii = console_qr.qr_terminal(qr_url)
            with open(QR_TEXT_FILE, 'w') as f:
                f.write(qr_ascii)
                f.write(f"\n\nURL: {qr_url}\n")
            
            # Выводим QR в консоль
            print("\n" + "=" * 60)
            print("📲 ОТСКАНИРУЙТЕ QR-КОД В TELEGRAM:")
            print("=" * 60)
            print(qr_ascii)
            print("=" * 60)
            print("\n📱 КАК АВТОРИЗОВАТЬСЯ:")
            print("1. Откройте Telegram на телефоне")  
            print("2. Перейдите в Настройки → Устройства → Подключить устройство")
            print("3. Отсканируйте QR-код выше")
            print("4. Подтвердите вход")
            print("\nИЛИ откройте эту ссылку на телефоне с Telegram:")
            print(qr_url)
            print("\n⏳ Жду подтверждения...")
            
            # Ждем подтверждения входа (максимум 2 минуты)
            import time
            timeout = 120
            start_time = time.time()
            
            while not await client.is_user_authorized():
                await asyncio.sleep(2)
                
                # Проверяем таймаут
                if time.time() - start_time > timeout:
                    print("❌ Таймаут авторизации (2 минуты)")
                    return False
                
                # Пытаемся обновить статус
                try:
                    await qr_login.wait()
                    break
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    if "expired" in str(e).lower():
                        print("❌ QR-код истек. Попробуйте заново.")
                        return False
            
            print("✅ АВТОРИЗАЦИЯ УСПЕШНА!")
            
        else:
            print("✅ Сессия уже авторизована!")
        
        # Проверяем подключение
        print("\n📊 Проверяю подключение...")
        me = await client.get_me()
        print(f"✅ Подключен как: {me.first_name} {me.last_name or ''}")
        print(f"📱 Телефон: {me.phone if me.phone else 'Скрыт'}")
        print(f"🆔 ID: {me.id}")
        
        # Проверяем доступ к боту
        print(f"\n🤖 Проверяю доступ к боту {VK_BOT_USERNAME}...")
        bot = await client.get_entity(VK_BOT_USERNAME)
        print(f"✅ Бот найден: {bot.first_name}")
        
        # Отправляем тестовое сообщение
        print("\n📤 Отправляю тестовое сообщение боту...")
        msg = await client.send_message(bot, "/start")
        print(f"✅ Сообщение отправлено (ID: {msg.id})")
        
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
        
        print("\n🎉 СЕССИЯ ГОТОВА К РАБОТЕ!")
        print("=" * 60)
        
        await client.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    # Очищаем старые файлы
    if QR_IMAGE_FILE.exists():
        QR_IMAGE_FILE.unlink()
    if QR_TEXT_FILE.exists():
        QR_TEXT_FILE.unlink()
    
    success = asyncio.run(auth_with_qr())
    
    if success:
        print("\n✅ Авторизация завершена успешно!")
        print("Теперь можно запускать бота: python run.py")
        sys.exit(0)
    else:
        print("\n❌ Авторизация не удалась")
        sys.exit(1)
