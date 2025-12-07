#!/usr/bin/env python3
"""Тест множественного поиска через force search"""

import asyncio
import logging
from pathlib import Path
import sys
from datetime import datetime

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).parent))

from services.vk_service import VKService
from database import VKDatabase

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_multiple_force_search():
    """Тестирование force search с несколькими ссылками"""
    
    # Тестовые ссылки
    test_links = [
        "https://vk.com/id1",      # Pavel Durov
        "https://vk.com/id2",      
        "https://vk.com/id5",      
        "https://vk.com/id10",     
        "https://vk.com/id100",    
    ]
    
    # Инициализация VK сервиса
    vk_service = VKService(
        api_id=13801751,
        api_hash="ba0fdc4c9c75c16ab3013af244f594e9",
        session_name="user_session",
        phone="+380930157086"
    )
    
    # Инициализация базы данных  
    db = VKDatabase()
    
    try:
        logger.info("🚀 Начинаем тест множественного force search")
        logger.info(f"📋 Всего ссылок для поиска: {len(test_links)}")
        
        # Инициализация соединений
        await vk_service.initialize()
        await db.init()
        
        results = []
        errors = []
        
        # Обработка каждой ссылки с задержкой
        for i, link in enumerate(test_links, 1):
            try:
                logger.info(f"\n🔍 [{i}/{len(test_links)}] Обрабатываем: {link}")
                
                # Проверяем состояние соединения
                if not vk_service.is_initialized:
                    logger.warning("⚠️ Переинициализация VK сервиса...")
                    await vk_service.initialize()
                
                # Выполняем поиск
                result = await vk_service.search_vk_link(link)
                
                if result:
                    logger.info(f"✅ Получен результат для {link}:")
                    logger.info(f"   Имя: {result.get('full_name', 'Не найдено')}")
                    logger.info(f"   Телефоны: {result.get('phones', 'Не найдено')}")
                    
                    # Сохраняем в БД (обновляем кеш)
                    await db.save_result(link, result, user_id=123456789, source="force_search")
                    
                    results.append((link, result))
                else:
                    logger.warning(f"⚠️ Нет результатов для {link}")
                    results.append((link, None))
                
                # Задержка между запросами (кроме последнего)
                if i < len(test_links):
                    delay = 1.5
                    logger.info(f"⏸ Ждем {delay} секунд перед следующим запросом...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке {link}: {e}")
                errors.append((link, str(e)))
                
                # Увеличенная задержка после ошибки
                if i < len(test_links):
                    delay = 3
                    logger.warning(f"⏸ Ждем {delay} секунд после ошибки...")
                    await asyncio.sleep(delay)
        
        # Итоги
        logger.info("\n" + "="*50)
        logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ:")
        logger.info(f"✅ Успешно обработано: {len([r for r in results if r[1] is not None])}/{len(test_links)}")
        logger.info(f"⚠️ Без результатов: {len([r for r in results if r[1] is None])}")
        logger.info(f"❌ С ошибками: {len(errors)}")
        
        if errors:
            logger.info("\n❌ Ошибки:")
            for link, error in errors:
                logger.info(f"   {link}: {error}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
        
    finally:
        # Закрываем соединения
        if vk_service:
            await vk_service.close()
        if db:
            await db.close()

if __name__ == "__main__":
    # Запуск теста
    results = asyncio.run(test_multiple_force_search())
    
    print("\n" + "="*50)
    print("✅ Тест завершен!")
    print(f"📊 Обработано ссылок: {len(results)}")