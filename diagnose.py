#!/usr/bin/env python3
"""
Скрипт диагностики VK Data Assistant Bot
"""

import sys
import os
import asyncio
import logging
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("diagnose")

async def check_environment():
    """Проверка окружения"""
    print("?? ПРОВЕРКА ОКРУЖЕНИЯ")
    print("=" * 50)
    
    # Проверка Python версии
    print(f"Python версия: {sys.version}")
    
    # Проверка переменных окружения
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("? dotenv загружен")
        
        required_vars = [
            'BOT_TOKEN', 'VK_BOT_USERNAME', 'API_ID', 'API_HASH', 
            'POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD'
        ]
        
        for var in required_vars:
            value = os.getenv(var)
            if value:
                display_value = value[:10] + "..." if len(value) > 10 else value
                print(f"? {var}: {display_value}")
            else:
                print(f"? {var}: НЕ НАЙДЕН")
                
    except Exception as e:
        print(f"? Ошибка загрузки .env: {e}")

async def check_database():
    """Проверка базы данных"""
    print("\n??? ПРОВЕРКА БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    try:
        from database import VKDatabase
        
        db = VKDatabase()
        await db.init()
        print("? PostgreSQL подключение успешно")
        
        # Проверяем таблицы
        async with db.acquire() as conn:
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            
            table_names = [row['table_name'] for row in tables]
            print(f"?? Найдено таблиц: {len(table_names)}")
            
            required_tables = ['vk_results', 'phone_links', 'users']
            for table in required_tables:
                if table in table_names:
                    print(f"? Таблица {table}: найдена")
                else:
                    print(f"? Таблица {table}: НЕ НАЙДЕНА")
        
        await db.close()
        
    except Exception as e:
        print(f"? Ошибка базы данных: {e}")
        import traceback
        traceback.print_exc()

async def check_redis():
    """Проверка Redis"""
    print("\n?? ПРОВЕРКА REDIS")
    print("=" * 50)
    
    try:
        import redis.asyncio as aioredis
        
        redis_client = await aioredis.from_url("redis://localhost:6379")
        await redis_client.ping()
        print("? Redis подключение успешно")
        
        info = await redis_client.info()
        print(f"?? Redis версия: {info.get('redis_version', 'Unknown')}")
        
        await redis_client.close()
        
    except Exception as e:
        print(f"? Ошибка Redis: {e}")

async def check_telegram():
    """Проверка Telegram API"""
    print("\n?? ПРОВЕРКА TELEGRAM API")
    print("=" * 50)
    
    try:
        from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE, VK_BOT_USERNAME
        
        print(f"API_ID: {API_ID}")
        print(f"VK_BOT_USERNAME: {VK_BOT_USERNAME}")
        print(f"SESSION_NAME: {SESSION_NAME}")
        
        # Проверяем сессию
        session_file = Path(f"{SESSION_NAME}.session")
        if session_file.exists():
            print("? Файл сессии существует")
        else:
            print("? Файл сессии НЕ НАЙДЕН - потребуется авторизация")
        
    except Exception as e:
        print(f"? Ошибка Telegram конфигурации: {e}")

async def check_vk_service():
    """Проверка VK сервиса"""
    print("\n?? ПРОВЕРКА VK СЕРВИСА")
    print("=" * 50)
    
    try:
        from services.vk_service import VKService
        from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE
        
        vk = VKService(API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE)
        print("? VKService создан")
        
        # Попытка инициализации (может потребовать ввода кода)
        print("?? Инициализация VK сервиса (может потребовать код авторизации)")
        
    except Exception as e:
        print(f"? Ошибка VK сервиса: {e}")

async def check_bot():
    """Проверка основного бота"""
    print("\n?? ПРОВЕРКА TELEGRAM БОТА")
    print("=" * 50)
    
    try:
        from aiogram import Bot
        from bot.config import BOT_TOKEN
        
        bot = Bot(token=BOT_TOKEN)
        bot_info = await bot.get_me()
        print(f"? Бот подключен: @{bot_info.username}")
        
        await bot.session.close()
        
    except Exception as e:
        print(f"? Ошибка Telegram бота: {e}")

async def main():
    """Основная функция диагностики"""
    print("?? ДИАГНОСТИКА VK DATA ASSISTANT BOT")
    print("=" * 60)
    
    checks = [
        check_environment,
        check_database, 
        check_redis,
        check_telegram,
        check_bot,
        # check_vk_service,  # Комментируем чтобы не требовать код
    ]
    
    for check in checks:
        try:
            await check()
        except Exception as e:
            print(f"? Критическая ошибка в {check.__name__}: {e}")
    
    print("\n" + "=" * 60)
    print("? ДИАГНОСТИКА ЗАВЕРШЕНА")
    print("\nСледующие шаги:")
    print("1. Исправьте найденные ошибки")
    print("2. Запустите бота: python run.py")
    print("3. Проверьте логи в logs/bot.log")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n? Диагностика прервана пользователем")
    except Exception as e:
        print(f"\n? Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()