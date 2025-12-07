#!/usr/bin/env python3
"""
Отладка функции force_search_without_cache с исправлениями
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MockMessage:
    def __init__(self):
        self.edit_count = 0
        
    async def edit_text(self, text, reply_markup=None):
        self.edit_count += 1
        logger.info(f"[MOCK MESSAGE EDIT #{self.edit_count}] {text[:100]}...")
        return self

class MockDB:
    def __init__(self):
        self.saved_results = []
        
    async def save_result(self, link, result_data, user_id, source="search"):
        self.saved_results.append({
            'link': link,
            'result_data': result_data,
            'user_id': user_id,
            'source': source
        })
        logger.info(f"[MOCK DB] Сохранен результат для {link}: {result_data}")

class MockVKService:
    def __init__(self):
        self.is_initialized = True
        self.call_count = 0
        
    async def search_vk_link(self, link):
        self.call_count += 1
        # Симулируем разные результаты
        if 'user1' in link:
            return {
                "phones": ["+71234567890"],
                "full_name": "Иван Иванов",
                "birth_date": "1990-01-01"
            }
        elif 'user2' in link:
            return {
                "phones": [],
                "full_name": "Петр Петров",
                "birth_date": ""
            }
        else:
            return {
                "phones": [],
                "full_name": "",
                "birth_date": ""
            }

async def test_force_search():
    """Тестируем исправленную функцию force_search_without_cache"""
    
    logger.info("🧪 Тестирование исправлений в force_search_without_cache")
    
    # Импортируем функцию
    try:
        from bot.handlers.search import force_search_without_cache
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта: {e}")
        return False
    
    # Создаем моки
    message = MockMessage()
    db = MockDB()
    vk_service = MockVKService()
    bot = MagicMock()
    
    # Тестовые данные
    test_links = [
        "https://vk.com/user1",
        "https://vk.com/user2", 
        "https://vk.com/user3"
    ]
    
    # Тестируем без processor
    logger.info("🔄 Запуск теста без processor...")
    
    try:
        await force_search_without_cache(
            message=message,
            links_to_process=test_links,
            processor=None,
            user_id=123,
            db=db,
            vk_service=vk_service,
            bot=bot
        )
        
        # Проверяем результаты
        logger.info(f"✅ Функция выполнилась без ошибок")
        logger.info(f"📊 Количество вызовов VK сервиса: {vk_service.call_count}")
        logger.info(f"💾 Сохранено результатов в БД: {len(db.saved_results)}")
        logger.info(f"🔄 Количество обновлений сообщения: {message.edit_count}")
        
        # Проверяем сохраненные результаты
        for result in db.saved_results:
            logger.info(f"   - {result['link']}: {result['result_data']}")
        
        # Проверки
        if vk_service.call_count == len(test_links):
            logger.info("✅ Все ссылки были обработаны")
        else:
            logger.warning(f"⚠️ Обработано {vk_service.call_count} из {len(test_links)} ссылок")
        
        if len(db.saved_results) == len(test_links):
            logger.info("✅ Все результаты сохранены в БД")
        else:
            logger.warning(f"⚠️ В БД сохранено {len(db.saved_results)} из {len(test_links)} результатов")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении функции: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def test_db_save_method():
    """Тестируем правильность вызова метода БД"""
    
    logger.info("🧪 Тестирование вызова метода БД")
    
    # Проверяем наличие метода save_result в database.py
    try:
        from database import ExtendedVKDatabase
        
        # Создаем экземпляр (не инициализируем, просто проверяем метод)
        db_instance = ExtendedVKDatabase.__new__(ExtendedVKDatabase)
        
        if hasattr(db_instance, 'save_result'):
            logger.info("✅ Метод save_result найден в ExtendedVKDatabase")
        else:
            logger.error("❌ Метод save_result НЕ найден в ExtendedVKDatabase!")
            return False
            
        # Проверяем сигнатуру
        import inspect
        sig = inspect.signature(ExtendedVKDatabase.save_result)
        params = list(sig.parameters.keys())
        logger.info(f"📝 Параметры метода save_result: {params}")
        
        required_params = ['link', 'result_data', 'user_id']
        missing_params = [p for p in required_params if p not in params]
        
        if missing_params:
            logger.error(f"❌ Отсутствуют обязательные параметры: {missing_params}")
            return False
        else:
            logger.info("✅ Все обязательные параметры присутствуют")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке БД: {e}")
        return False

async def main():
    """Запуск всех тестов"""
    
    logger.info("🚀 Начинаю тестирование исправлений")
    logger.info("=" * 60)
    
    tests = [
        ("Метод БД save_result", test_db_save_method),
        ("Функция force_search_without_cache", test_force_search)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Выполняется тест: {test_name}")
        logger.info("-" * 40)
        try:
            result = await test_func()
            results[test_name] = result
            status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
            logger.info(f"   Результат: {status}")
        except Exception as e:
            logger.error(f"❌ Тест '{test_name}' завершился с ошибкой: {e}")
            results[test_name] = False
    
    # Итоги
    logger.info("\n" + "=" * 60)
    logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ:")
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        logger.info(f"   {status} {test_name}")
    
    logger.info(f"\n🎯 Результат: {passed}/{total} тестов пройдено")
    
    if passed == total:
        logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Исправления готовы к использованию.")
    else:
        logger.warning("⚠️ Некоторые тесты не прошли. Требуется дополнительная проверка.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())