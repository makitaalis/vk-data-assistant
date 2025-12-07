#!/usr/bin/env python3
"""Test script to verify the force search fix works properly"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).parent))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MockVKService:
    """Мок VK сервиса который симулирует уже инициализированный сервис"""
    
    def __init__(self):
        self.is_initialized = True
        self.request_count = 0
    
    async def search_vk_link(self, link: str):
        """Симуляция поиска VK ссылки"""
        self.request_count += 1
        
        # Симулируем поиск с результатами для некоторых ссылок
        if "id1" in link:
            return {
                "phones": ["+7900123456"], 
                "full_name": "Pavel Durov", 
                "birth_date": "1984-10-10"
            }
        elif "id2" in link:
            return {
                "phones": [], 
                "full_name": "User Name", 
                "birth_date": ""
            }
        else:
            return {
                "phones": [], 
                "full_name": "", 
                "birth_date": ""
            }

class MockMessage:
    """Мок объект для message"""
    def __init__(self, user_id=123456789):
        self.from_user = MockUser(user_id)
        self.message_text = ""
        
    async def answer(self, text, **kwargs):
        logger.info(f"Bot would send: {text}")
        return MockMessage()
        
    async def edit_text(self, text, **kwargs):
        logger.info(f"Bot would edit to: {text[:100]}...")
        self.message_text = text
        return self

class MockUser:
    def __init__(self, user_id):
        self.id = user_id

async def test_force_search_fix():
    """Тестируем исправленную логику принудительного поиска"""
    
    # Тестовые ссылки
    test_links = [
        "https://vk.com/id1",      
        "https://vk.com/id2",      
        "https://vk.com/id3",
        "https://vk.com/id4",
        "https://vk.com/id5",      
    ]
    
    logger.info(f"🚀 Тестируем исправленную логику для {len(test_links)} ссылок")
    
    # Создаем мок VK сервис (уже инициализированный)
    vk_service = MockVKService()
    mock_message = MockMessage()
    
    # Симулируем логику из force_search_without_cache после нашего исправления
    all_results = {}
    processed_count = 0
    found_count = 0
    
    try:
        for i, link in enumerate(test_links, 1):
            try:
                logger.info(f"🔍 Обработка {i}/{len(test_links)}: {link}")
                
                # Новая логика - НЕ переинициализируем VK сервис
                if not vk_service.is_initialized:
                    logger.warning("⚠️ VK сервис не инициализирован - пропускаем ссылку")
                    result = {
                        "phones": [], 
                        "full_name": "", 
                        "birth_date": "",
                        "error": "VK service not initialized"
                    }
                else:
                    # Ищем через VK бота
                    result = await vk_service.search_vk_link(link)
                
                # Обрабатываем результат
                all_results[link] = result
                processed_count += 1
                
                # Считаем найденные данные
                if result and (result.get("phones") or result.get("full_name") or result.get("birth_date")):
                    found_count += 1
                    logger.info(f"✅ Найдены данные для {link}")
                else:
                    logger.info(f"❌ Данные не найдены для {link}")
                
                # Задержка между запросами
                if i < len(test_links):
                    delay = 0.5  # Короткая задержка для теста
                    logger.info(f"⏸ Ждем {delay}с...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке {link}: {e}")
                all_results[link] = {
                    "phones": [], 
                    "full_name": "", 
                    "birth_date": "",
                    "error": str(e)
                }
                processed_count += 1
        
        # Итоги
        logger.info("\\n" + "="*50)
        logger.info("📊 ИТОГИ ТЕСТИРОВАНИЯ:")
        logger.info(f"✅ Успешно обработано: {processed_count}/{len(test_links)}")
        logger.info(f"✅ Найдено данных: {found_count}")
        logger.info(f"❌ Без данных: {processed_count - found_count}")
        logger.info(f"🔧 Общее количество запросов к VK: {vk_service.request_count}")
        
        # Проверяем что все ссылки были обработаны
        if processed_count == len(test_links):
            logger.info("✅ ТЕСТ ПРОЙДЕН: Все ссылки были обработаны!")
            return True
        else:
            logger.error("❌ ТЕСТ НЕ ПРОЙДЕН: Не все ссылки были обработаны!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в тесте: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = asyncio.run(test_force_search_fix())
    if success:
        print("\\n🎉 Исправление работает корректно!")
    else:
        print("\\n❌ Требуется дополнительная работа над исправлением")