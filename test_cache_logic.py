#!/usr/bin/env python3
"""
Тестовый скрипт для проверки логики кеширования результатов
"""

import asyncio
import sys
from database import VKDatabase
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_cache_logic():
    """Тестирует логику работы с кешированными результатами"""
    
    db = VKDatabase()
    await db.init()
    
    # Тестовые ссылки
    test_links = [
        "https://vk.com/id123456",  # Предположим, что эта ссылка уже в БД с данными
        "https://vk.com/id789012",  # Эта ссылка в БД без данных (пустой результат)
        "https://vk.com/id345678",  # Новая ссылка, которой нет в БД
    ]
    
    logger.info("=" * 60)
    logger.info("ТЕСТ ЛОГИКИ КЕШИРОВАНИЯ")
    logger.info("=" * 60)
    
    # 1. Получаем кешированные результаты (включая пустые)
    logger.info("\n1. Получаем кешированные результаты для всех ссылок:")
    cached_results = await db.get_cached_results(test_links)
    
    logger.info(f"   Всего ссылок для проверки: {len(test_links)}")
    logger.info(f"   Найдено в кеше: {len(cached_results)}")
    
    # Анализируем результаты
    for link in test_links:
        if link in cached_results:
            result = cached_results[link]
            has_data = bool(result.get("phones") or result.get("full_name") or result.get("birth_date"))
            logger.info(f"   ✓ {link}: В КЕШЕ (данные: {'ДА' if has_data else 'НЕТ'})")
            if result.get("phones"):
                logger.info(f"      Телефоны: {result['phones']}")
            if result.get("full_name"):
                logger.info(f"      Имя: {result['full_name']}")
        else:
            logger.info(f"   ✗ {link}: НЕ В КЕШЕ (нужна проверка)")
    
    # 2. Определяем какие ссылки нужно отправить боту
    links_to_check = [link for link in test_links if link not in cached_results]
    
    logger.info(f"\n2. Ссылки для отправки боту: {len(links_to_check)}")
    for link in links_to_check:
        logger.info(f"   → {link}")
    
    if not links_to_check:
        logger.info("   ВСЕ ссылки уже проверены ранее! Боту ничего не отправляем.")
    
    # 3. Статистика
    logger.info("\n3. Статистика обработки:")
    
    cached_with_data = sum(1 for r in cached_results.values() 
                          if r.get("phones") or r.get("full_name") or r.get("birth_date"))
    cached_without_data = len(cached_results) - cached_with_data
    
    logger.info(f"   📊 Всего ссылок: {len(test_links)}")
    logger.info(f"   💾 Из кеша всего: {len(cached_results)}")
    logger.info(f"   ✅ Из кеша с данными: {cached_with_data}")
    logger.info(f"   ❌ Из кеша без данных: {cached_without_data}")
    logger.info(f"   🔍 Новых для проверки: {len(links_to_check)}")
    
    # 4. Проверка дубликатов (расширенная)
    logger.info("\n4. Проверка дубликатов:")
    duplicate_check = await db.check_duplicates_extended(test_links)
    
    logger.info(f"   Новых ссылок: {len(duplicate_check['new'])}")
    logger.info(f"   Дубликатов с данными: {len(duplicate_check['duplicates_with_data'])}")
    logger.info(f"   Дубликатов без данных: {len(duplicate_check['duplicates_no_data'])}")
    
    # 5. Тест сохранения пустого результата
    logger.info("\n5. Тест сохранения пустого результата:")
    test_link = "https://vk.com/test_empty_result"
    empty_result = {
        "phones": [],
        "full_name": "",
        "birth_date": ""
    }
    
    await db.save_result(test_link, empty_result, user_id=123456)
    logger.info(f"   Сохранен пустой результат для {test_link}")
    
    # Проверяем, что он теперь в кеше
    cached = await db.get_cached_results([test_link])
    if test_link in cached:
        logger.info(f"   ✓ Ссылка теперь в кеше (пустой результат)")
        logger.info(f"   При повторной проверке НЕ будет отправлена боту")
    else:
        logger.info(f"   ✗ ОШИБКА: Ссылка не сохранилась в кеше")
    
    # Очистка тестовых данных
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM vk_results WHERE link = $1", test_link)
        logger.info(f"\n   🧹 Тестовые данные удалены")
    
    logger.info("\n" + "=" * 60)
    logger.info("ТЕСТ ЗАВЕРШЕН УСПЕШНО")
    logger.info("=" * 60)
    
    await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(test_cache_logic())
    except KeyboardInterrupt:
        logger.info("\n👋 Тест прерван пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        sys.exit(1)