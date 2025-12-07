#!/usr/bin/env python3
import asyncio
import logging
import sys
import os
import time
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
import qrcode

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_id = 23627963
api_hash = 'dcd16e0a92f2675fa00a9b1ef9e4b147'
phone = '+380930157086'
session_name = 'telegram_session_new'

async def method1_file_code():
    """Метод 1: Авторизация через код в файле"""
    logger.info("=== МЕТОД 1: Авторизация через код в файле ===")
    client = TelegramClient(session_name + '_file', api_id, api_hash)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        logger.info(f"Отправка кода на {phone}...")
        await client.send_code_request(phone)
        
        logger.info("Код отправлен! Создайте файл 'code.txt' и введите туда код")
        logger.info("Ожидание файла с кодом...")
        
        while not os.path.exists('code.txt'):
            await asyncio.sleep(2)
            print(".", end="", flush=True)
        
        with open('code.txt', 'r') as f:
            code = f.read().strip()
        
        try:
            await client.sign_in(phone, code)
            logger.info("Авторизация успешна!")
            os.remove('code.txt')
        except SessionPasswordNeededError:
            logger.info("Требуется 2FA. Введите пароль в файл 'password.txt'")
            while not os.path.exists('password.txt'):
                await asyncio.sleep(2)
            with open('password.txt', 'r') as f:
                password = f.read().strip()
            await client.sign_in(password=password)
            os.remove('password.txt')
            logger.info("Авторизация с 2FA успешна!")
        except PhoneCodeInvalidError:
            logger.error("Неверный код!")
            return False
    else:
        logger.info("Уже авторизован!")
    
    me = await client.get_me()
    logger.info(f"Авторизован как: {me.first_name} (@{me.username or 'без username'})")
    await client.disconnect()
    return True

async def method2_bot_token():
    """Метод 2: Авторизация через бот-токен"""
    logger.info("=== МЕТОД 2: Авторизация через бот ===")
    logger.info("Этот метод работает только если у вас есть бот-токен")
    logger.info("Создайте файл 'bot_token.txt' с токеном вида: 123456:ABC-DEF...")
    
    if not os.path.exists('bot_token.txt'):
        logger.info("Ожидание файла bot_token.txt...")
        while not os.path.exists('bot_token.txt'):
            await asyncio.sleep(2)
    
    with open('bot_token.txt', 'r') as f:
        bot_token = f.read().strip()
    
    client = TelegramClient(session_name + '_bot', api_id, api_hash)
    await client.start(bot_token=bot_token)
    
    me = await client.get_me()
    logger.info(f"Бот авторизован: @{me.username}")
    await client.disconnect()
    return True

async def method3_qr_code():
    """Метод 3: QR-код авторизация"""
    logger.info("=== МЕТОД 3: QR-код авторизация ===")
    client = TelegramClient(session_name + '_qr', api_id, api_hash)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        logger.info("Генерация QR-кода для авторизации...")
        
        # Запрос QR-кода
        qr_login = await client.qr_login()
        
        # Генерация QR-кода
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        
        # Сохранение в файл
        img = qr.make_image(fill_color="black", back_color="white")
        img.save("telegram_qr.png")
        logger.info("QR-код сохранен в telegram_qr.png")
        logger.info("Откройте Telegram на телефоне -> Настройки -> Устройства -> Подключить устройство")
        logger.info("Отсканируйте QR-код")
        
        # Ожидание авторизации
        try:
            await qr_login.wait(timeout=60)
            logger.info("QR авторизация успешна!")
        except asyncio.TimeoutError:
            logger.error("Время ожидания истекло")
            return False
    else:
        logger.info("Уже авторизован!")
    
    me = await client.get_me()
    logger.info(f"Авторизован как: {me.first_name} (@{me.username or 'без username'})")
    await client.disconnect()
    return True

async def method4_string_session():
    """Метод 4: Использование string session"""
    logger.info("=== МЕТОД 4: String Session ===")
    logger.info("Вы можете создать string session на другом устройстве и использовать здесь")
    logger.info("Для генерации используйте: https://replit.com/@GodMrunal/GenerateStringSession")
    logger.info("Сохраните string session в файл 'string_session.txt'")
    
    if os.path.exists('string_session.txt'):
        with open('string_session.txt', 'r') as f:
            string_session = f.read().strip()
        
        client = TelegramClient(StringSession(string_session), api_id, api_hash)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            logger.info(f"Авторизован через string session: {me.first_name}")
            await client.disconnect()
            return True
    else:
        logger.info("Файл string_session.txt не найден")
    return False

async def main():
    logger.info("Доступные методы авторизации:")
    logger.info("1. Через код в файле (code.txt)")
    logger.info("2. Через бот-токен")
    logger.info("3. Через QR-код")
    logger.info("4. Через String Session")
    
    print("\nВыберите метод (1-4): ", end="")
    choice = input().strip()
    
    if choice == '1':
        await method1_file_code()
    elif choice == '2':
        await method2_bot_token()
    elif choice == '3':
        await method3_qr_code()
    elif choice == '4':
        await method4_string_session()
    else:
        logger.error("Неверный выбор")

if __name__ == '__main__':
    try:
        from telethon.sessions import StringSession
    except ImportError:
        StringSession = None
    
    asyncio.run(main())