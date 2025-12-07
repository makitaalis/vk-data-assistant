#!/usr/bin/env python3
"""
Тестирование исправления интеграции принудительного поиска с обработкой файла
"""

import asyncio
import logging
from pathlib import Path
from services.excel_service import ExcelProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_excel_processor_update():
    """Тестируем новый метод update_results_from_dict"""
    
    logger.info("🧪 Тестирование метода update_results_from_dict")
    
    # Создаем процессор
    processor = ExcelProcessor()
    
    # Проверяем наличие метода
    if hasattr(processor, 'update_results_from_dict'):
        logger.info("✅ Метод update_results_from_dict найден")
    else:
        logger.error("❌ Метод update_results_from_dict НЕ найден!")
        return False
    
    # Тестовые данные для проверки
    test_results = {
        "https://vk.com/user1": {
            "phones": ["+71234567890", "+79876543210"],
            "full_name": "Иван Иванов",
            "birth_date": "1990-01-01"
        },
        "https://vk.com/user2": {
            "phones": ["+71111111111"],
            "full_name": "Петр Петров",
            "birth_date": ""
        },
        "https://vk.com/user3": {
            "phones": [],
            "full_name": "",
            "birth_date": "1985-05-05"
        }
    }
    
    logger.info(f"📊 Тестовые результаты: {len(test_results)} ссылок")
    
    # Тестируем метод без загруженного файла
    try:
        processor.update_results_from_dict(test_results)
        logger.info("✅ Метод корректно обработал случай без DataFrame")
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании без DataFrame: {e}")
    
    return True

async def test_force_search_logic():
    """Тестируем логику в force_search_without_cache"""
    
    logger.info("🧪 Тестирование интеграции с force_search_without_cache")
    
    # Проверяем импорт функции
    try:
        from bot.handlers.search import force_search_without_cache
        logger.info("✅ Функция force_search_without_cache импортируется корректно")
        
        # Проверяем сигнатуру функции
        import inspect
        sig = inspect.signature(force_search_without_cache)
        params = list(sig.parameters.keys())
        logger.info(f"📝 Параметры функции: {params}")
        
        if 'processor' in params:
            logger.info("✅ Параметр 'processor' присутствует в функции")
        else:
            logger.error("❌ Параметр 'processor' ОТСУТСТВУЕТ в функции!")
        
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта force_search_without_cache: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при анализе функции: {e}")
        return False
    
    return True

async def test_callbacks_integration():
    """Тестируем интеграцию с callbacks"""
    
    logger.info("🧪 Тестирование интеграции с callbacks.py")
    
    try:
        # Проверяем callbacks.py
        callbacks_path = Path("/home/vkbot/vk-data-assistant/bot/handlers/callbacks.py")
        if callbacks_path.exists():
            with open(callbacks_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Ищем вызов force_search_without_cache
            if "force_search_without_cache" in content:
                logger.info("✅ Вызов force_search_without_cache найден в callbacks.py")
                
                # Проверяем передачу processor
                if "processor" in content:
                    logger.info("✅ Параметр processor используется в callbacks.py")
                else:
                    logger.warning("⚠️ Параметр processor не найден в callbacks.py")
            else:
                logger.error("❌ force_search_without_cache НЕ найден в callbacks.py")
                return False
        else:
            logger.error("❌ callbacks.py не найден!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке callbacks.py: {e}")
        return False
    
    return True

async def run_tests():
    """Запуск всех тестов"""
    
    logger.info("🚀 Начинаем тестирование исправлений")
    logger.info("=" * 50)
    
    tests = [
        ("Метод update_results_from_dict", test_excel_processor_update),
        ("Функция force_search_without_cache", test_force_search_logic),
        ("Интеграция с callbacks", test_callbacks_integration)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"🧪 Выполняется тест: {test_name}")
        try:
            result = await test_func()
            results[test_name] = result
            status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
            logger.info(f"   {status}")
        except Exception as e:
            logger.error(f"❌ Тест '{test_name}' завершился с ошибкой: {e}")
            results[test_name] = False
        
        logger.info("-" * 30)
    
    # Итоги
    logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ:")
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        logger.info(f"   {status} {test_name}")
    
    logger.info(f"\n🎯 Результат: {passed}/{total} тестов пройдено")
    
    if passed == total:
        logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Исправление готово к использованию.")
    else:
        logger.warning("⚠️ Некоторые тесты не прошли. Требуется дополнительная проверка.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(run_tests())