#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы бота Sherlock
и диагностики проблемы с сохранением данных
"""

import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.types import InputPeerUser
import os
from dotenv import load_dotenv
import json
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'user_session')
_raw_bot_username = os.getenv('VK_BOT_USERNAME', 'sherlock_bot_ne_bot')
if _raw_bot_username and not _raw_bot_username.startswith('@'):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username
VK_BOT_USERNAME_CLEAN = VK_BOT_USERNAME.lstrip('@')

async def test_bot_search():
    """Тестирование поиска через бота"""
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    
    try:
        # Находим бота
        logger.info(f"🔍 Ищем бота @{VK_BOT_USERNAME_CLEAN}")
        bot_entity = await client.get_entity(VK_BOT_USERNAME)
        logger.info(f"✅ Бот найден: {bot_entity.username} (ID: {bot_entity.id})")
        
        # Тестовые VK ссылки
        test_links = [
            "https://vk.com/id1",  # Павел Дуров
            "https://vk.com/id2",  # Александра Владимирова
            "https://vk.com/id100"  # Тестовый профиль
        ]
        
        results = {}
        
        for link in test_links:
            logger.info(f"\n📤 Отправляем запрос: {link}")
            
            # Отправляем ссылку боту
            await client.send_message(bot_entity, link)
            
            # Ждем ответ
            await asyncio.sleep(3)
            
            # Получаем последние сообщения от бота
            messages = await client.get_messages(bot_entity, limit=5)
            
            # Анализируем ответ
            for msg in messages:
                if msg.text:
                    logger.info(f"📨 Ответ бота:\n{msg.text[:500]}")
                    
                    # Парсим данные из ответа
                    data = parse_bot_response(msg.text)
                    results[link] = data
                    
                    if data['phones'] or data['full_name']:
                        logger.info(f"✅ Найдены данные:")
                        logger.info(f"   Имя: {data['full_name']}")
                        logger.info(f"   Телефоны: {data['phones']}")
                        logger.info(f"   Дата рождения: {data['birth_date']}")
                    else:
                        logger.info(f"❌ Данные не найдены")
                    
                    break
            
            await asyncio.sleep(2)
        
        # Сохраняем результаты для анализа
        with open('/home/vkbot/vk-data-assistant/test_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n📊 Результаты сохранены в test_results.json")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        await client.disconnect()

def parse_bot_response(text):
    """Парсинг ответа бота Sherlock"""
    data = {
        'phones': [],
        'full_name': '',
        'birth_date': '',
        'raw_response': text
    }
    
    if not text:
        return data
    
    # Паттерны для извлечения данных
    phone_patterns = [
        r'📱\s*Телефон[ы]?:\s*([+\d\s\-\(\)]+)',
        r'☎️\s*([+\d\s\-\(\)]+)',
        r'Номер[а]?:\s*([+\d\s\-\(\)]+)',
        r'\+7[\d\s\-\(\)]+',
        r'8[\d\s\-\(\)]{10,}'
    ]
    
    name_patterns = [
        r'👤\s*(?:Имя|ФИО):\s*([^\n]+)',
        r'Имя:\s*([^\n]+)',
        r'ФИО:\s*([^\n]+)',
        r'(?:Фамилия|Имя|Отчество):\s*([^\n]+)'
    ]
    
    birth_patterns = [
        r'🎂\s*(?:Дата рождения|ДР):\s*([^\n]+)',
        r'Дата рождения:\s*([^\n]+)',
        r'Родился:\s*([^\n]+)'
    ]
    
    # Извлекаем телефоны
    for pattern in phone_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Очищаем номер
            phone = re.sub(r'[^\d+]', '', match)
            if len(phone) >= 10:
                data['phones'].append(phone)
    
    # Убираем дубликаты
    data['phones'] = list(set(data['phones']))
    
    # Извлекаем имя
    for pattern in name_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data['full_name'] = match.group(1).strip()
            break
    
    # Извлекаем дату рождения
    for pattern in birth_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data['birth_date'] = match.group(1).strip()
            break
    
    # Проверяем сообщения об отсутствии данных
    no_data_patterns = [
        r'не найден',
        r'нет данных',
        r'информация отсутствует',
        r'не удалось найти',
        r'профиль закрыт',
        r'профиль удален'
    ]
    
    for pattern in no_data_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            logger.info(f"⚠️ Обнаружено сообщение об отсутствии данных: {pattern}")
            break
    
    return data

async def main():
    """Главная функция"""
    logger.info("🚀 Запуск тестирования бота Sherlock")
    logger.info(f"📱 Используем бота: @{VK_BOT_USERNAME_CLEAN}")
    
    results = await test_bot_search()
    
    if results:
        logger.info("\n✅ Тестирование завершено успешно")
        logger.info(f"📊 Проверено ссылок: {len(results)}")
        
        # Анализ результатов
        with_data = sum(1 for r in results.values() if r['phones'] or r['full_name'])
        without_data = len(results) - with_data
        
        logger.info(f"✅ С данными: {with_data}")
        logger.info(f"❌ Без данных: {without_data}")
    else:
        logger.error("❌ Тестирование завершено с ошибками")

if __name__ == "__main__":
    asyncio.run(main())
