#!/usr/bin/env python3
import asyncio
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_id = 23627963
api_hash = 'dcd16e0a92f2675fa00a9b1ef9e4b147'
phone = '+380930157086'

async def auth_with_retry():
    """Авторизация с повторными попытками"""
    session_file = 'tg_session_new'
    
    # Удаляем старую сессию если есть
    if os.path.exists(session_file + '.session'):
        os.remove(session_file + '.session')
        logger.info("Старая сессия удалена")
    
    client = TelegramClient(session_file, api_id, api_hash)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.info(f"Начинаем авторизацию для {phone}")
            
            # Попытка отправить код
            try:
                await client.send_code_request(phone)
                logger.info("✅ Код успешно отправлен!")
                logger.info("Создайте файл 'auth_code.txt' и введите туда код из Telegram")
                logger.info("Ожидание файла с кодом...")
                
                # Ждем файл с кодом
                while not os.path.exists('auth_code.txt'):
                    await asyncio.sleep(1)
                    print(".", end="", flush=True)
                
                print()  # Новая строка
                
                # Читаем код
                with open('auth_code.txt', 'r') as f:
                    code = f.read().strip()
                
                logger.info(f"Получен код: {code}")
                
                # Пробуем авторизоваться
                try:
                    await client.sign_in(phone, code)
                    logger.info("✅ Авторизация успешна!")
                    
                    # Удаляем файл с кодом
                    os.remove('auth_code.txt')
                    
                except SessionPasswordNeededError:
                    logger.info("⚠️ Требуется двухфакторная аутентификация")
                    logger.info("Создайте файл '2fa_password.txt' с вашим паролем")
                    
                    while not os.path.exists('2fa_password.txt'):
                        await asyncio.sleep(1)
                    
                    with open('2fa_password.txt', 'r') as f:
                        password = f.read().strip()
                    
                    await client.sign_in(password=password)
                    logger.info("✅ Авторизация с 2FA успешна!")
                    os.remove('2fa_password.txt')
                
            except FloodWaitError as e:
                logger.error(f"⚠️ Telegram временно заблокировал отправку кода. Подождите {e.seconds} секунд")
                return False
            except Exception as e:
                logger.error(f"❌ Ошибка при отправке кода: {e}")
                return False
        else:
            logger.info("✅ Уже авторизован!")
        
        # Проверяем авторизацию
        me = await client.get_me()
        logger.info(f"📱 Авторизован как: {me.first_name} {me.last_name or ''}")
        logger.info(f"📞 Телефон: {me.phone}")
        logger.info(f"🆔 User ID: {me.id}")
        if me.username:
            logger.info(f"👤 Username: @{me.username}")
        
        await client.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        await client.disconnect()
        return False

async def main():
    logger.info("=" * 50)
    logger.info("TELEGRAM АВТОРИЗАЦИЯ")
    logger.info("=" * 50)
    
    success = await auth_with_retry()
    
    if success:
        logger.info("\n✅ Сессия успешно создана и сохранена!")
        logger.info("Файл сессии: tg_session_new.session")
    else:
        logger.error("\n❌ Не удалось создать сессию")
        logger.info("\nВозможные решения:")
        logger.info("1. Попробуйте через VPN если код не приходит")
        logger.info("2. Используйте QR-код авторизацию (запустите telegram_auth_methods.py)")
        logger.info("3. Создайте string session на другом устройстве")
        logger.info("4. Проверьте правильность номера телефона")

if __name__ == '__main__':
    asyncio.run(main())