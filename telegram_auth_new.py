#!/usr/bin/env python3
import asyncio
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_id = 23627963
api_hash = 'dcd16e0a92f2675fa00a9b1ef9e4b147'
phone = '+380930157086'
session_name = 'new_telegram_session'

async def main():
    client = TelegramClient(session_name, api_id, api_hash)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        logger.info(f"Отправка кода авторизации на номер {phone}...")
        await client.send_code_request(phone)
        
        logger.info("Код отправлен! Введите код из Telegram:")
        code = input("Введите код: ")
        
        try:
            await client.sign_in(phone, code)
            logger.info("Авторизация успешна!")
        except SessionPasswordNeededError:
            password = input("Требуется пароль 2FA. Введите пароль: ")
            await client.sign_in(password=password)
            logger.info("Авторизация с 2FA успешна!")
        except Exception as e:
            logger.error(f"Ошибка авторизации: {e}")
            return
    else:
        logger.info("Вы уже авторизованы!")
    
    me = await client.get_me()
    logger.info(f"Авторизован как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
    
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())