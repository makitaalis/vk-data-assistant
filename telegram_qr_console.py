#!/usr/bin/env python3
import asyncio
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import qrcode
from PIL import Image
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_id = 23627963
api_hash = 'dcd16e0a92f2675fa00a9b1ef9e4b147'
phone = '+380930157086'
session_name = 'telegram_qr_session'

def print_qr_console(data):
    """Выводит QR-код в консоль используя ASCII символы"""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1,
    )
    qr.add_data(data)
    qr.make(fit=True)
    
    # Получаем матрицу QR-кода
    matrix = qr.modules
    
    # Выводим QR-код в консоль
    print("\n" + "=" * 50)
    print("QR-КОД ДЛЯ АВТОРИЗАЦИИ:")
    print("=" * 50 + "\n")
    
    # Используем блочные символы для отображения
    for row in matrix:
        line = ""
        for cell in row:
            if cell:
                line += "██"  # Черный блок
            else:
                line += "  "  # Пробел
        print(line)
    
    print("\n" + "=" * 50)

async def auth_with_qr():
    """QR-код авторизация с выводом в консоль"""
    client = TelegramClient(session_name, api_id, api_hash)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.info("Генерация QR-кода для авторизации...")
            
            # Запрос QR-кода
            qr_login = await client.qr_login()
            
            # Выводим QR-код в консоль
            print_qr_console(qr_login.url)
            
            # Также сохраняем в файл
            qr = qrcode.QRCode(version=1, box_size=10, border=5)
            qr.add_data(qr_login.url)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")
            img.save("telegram_qr.png")
            
            logger.info("QR-код также сохранен в файл: telegram_qr.png")
            logger.info("\n📱 КАК АВТОРИЗОВАТЬСЯ:")
            logger.info("1. Откройте Telegram на телефоне")
            logger.info("2. Перейдите в Настройки → Устройства")
            logger.info("3. Нажмите 'Подключить устройство'")
            logger.info("4. Отсканируйте QR-код выше")
            logger.info("\nОжидание сканирования (60 секунд)...")
            
            # Ожидание авторизации
            try:
                await qr_login.wait(timeout=60)
                logger.info("\n✅ QR авторизация успешна!")
                
                # Получаем информацию о пользователе
                me = await client.get_me()
                logger.info(f"📱 Авторизован как: {me.first_name} {me.last_name or ''}")
                logger.info(f"🆔 User ID: {me.id}")
                if me.username:
                    logger.info(f"👤 Username: @{me.username}")
                logger.info(f"📞 Телефон: {me.phone}")
                
                return True
                
            except asyncio.TimeoutError:
                logger.error("\n❌ Время ожидания истекло (60 секунд)")
                logger.info("Попробуйте запустить скрипт заново")
                return False
                
        else:
            logger.info("✅ Вы уже авторизованы!")
            me = await client.get_me()
            logger.info(f"📱 Авторизован как: {me.first_name} {me.last_name or ''}")
            logger.info(f"🆔 User ID: {me.id}")
            if me.username:
                logger.info(f"👤 Username: @{me.username}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return False
        
    finally:
        await client.disconnect()

async def main():
    logger.info("=" * 50)
    logger.info("TELEGRAM QR-КОД АВТОРИЗАЦИЯ")
    logger.info("=" * 50)
    
    success = await auth_with_qr()
    
    if success:
        logger.info("\n✅ Сессия успешно создана!")
        logger.info(f"📁 Файл сессии: {session_name}.session")
    else:
        logger.info("\n❌ Авторизация не удалась")

if __name__ == '__main__':
    asyncio.run(main())