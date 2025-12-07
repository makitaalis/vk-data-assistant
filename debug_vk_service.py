#!/usr/bin/env python3
"""
Скрипт для отладки и мониторинга реальных ответов от бота Sherlock
"""

import asyncio
import logging
from telethon import TelegramClient, events
from telethon.tl.types import Message
import os
from dotenv import load_dotenv
import json
from datetime import datetime

# Настройка логирования с детальным выводом
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vk_debug.log'),
        logging.StreamHandler()
    ]
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

# Глобальная переменная для сохранения всех ответов
all_responses = []

async def monitor_bot_responses():
    """Мониторинг ответов от бота в реальном времени"""
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    # Обработчик входящих сообщений
    @client.on(events.NewMessage)
    async def handler(event):
        """Обработчик всех новых сообщений"""
        if event.message.sender_id:
            sender = await event.get_sender()
            
            # Проверяем, что сообщение от нужного бота
            if hasattr(sender, 'username') and sender.username == VK_BOT_USERNAME_CLEAN:
                logger.info(f"\n{'='*60}")
                logger.info(f"📨 НОВОЕ СООБЩЕНИЕ ОТ БОТА @{sender.username}")
                logger.info(f"{'='*60}")
                
                # Сохраняем полное сообщение
                msg_data = {
                    'timestamp': datetime.now().isoformat(),
                    'bot_username': sender.username,
                    'message_id': event.message.id,
                    'text': event.message.text,
                    'raw_text': event.message.raw_text,
                    'entities': str(event.message.entities) if event.message.entities else None,
                    'buttons': None
                }
                
                # Если есть кнопки, сохраняем их
                if event.message.buttons:
                    buttons = []
                    for row in event.message.buttons:
                        for button in row:
                            buttons.append(button.text)
                    msg_data['buttons'] = buttons
                
                # Выводим текст сообщения
                logger.info(f"\n📝 ТЕКСТ СООБЩЕНИЯ:\n{event.message.text}")
                
                # Выводим raw_text если отличается
                if event.message.raw_text != event.message.text:
                    logger.info(f"\n📝 RAW TEXT:\n{event.message.raw_text}")
                
                # Сохраняем в список
                all_responses.append(msg_data)
                
                # Сохраняем в файл
                with open('bot_responses_debug.json', 'w', encoding='utf-8') as f:
                    json.dump(all_responses, f, ensure_ascii=False, indent=2)
                
                logger.info(f"\n✅ Ответ сохранен в bot_responses_debug.json")
                logger.info(f"{'='*60}\n")
    
    await client.start()
    
    try:
        # Находим бота
        logger.info(f"🔍 Подключаемся к боту @{VK_BOT_USERNAME}")
        bot_entity = await client.get_entity(VK_BOT_USERNAME)
        logger.info(f"✅ Бот найден: {bot_entity.username} (ID: {bot_entity.id})")
        
        # Тестовые ссылки для проверки
        test_links = [
            "https://vk.com/id1",        # Павел Дуров
            "https://vk.com/id100",      # ВКонтакте
            "https://vk.com/id500000000" # Случайный ID
        ]
        
        logger.info(f"\n🚀 Начинаю отправку тестовых запросов...")
        
        for link in test_links:
            logger.info(f"\n📤 Отправляю: {link}")
            await client.send_message(bot_entity, link)
            
            # Ждем ответ
            logger.info(f"⏳ Жду ответ от бота (5 секунд)...")
            await asyncio.sleep(5)
        
        # Дополнительное ожидание для получения всех ответов
        logger.info(f"\n⏳ Дополнительное ожидание 10 секунд для сбора всех ответов...")
        await asyncio.sleep(10)
        
        # Выводим итоговую статистику
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        logger.info(f"{'='*60}")
        logger.info(f"Всего получено ответов: {len(all_responses)}")
        
        # Анализируем содержимое ответов
        phones_found = 0
        names_found = 0
        
        for resp in all_responses:
            text = resp['text'] or ''
            if 'Телефон' in text or 'Phone' in text or '📱' in text:
                phones_found += 1
            if 'Полное имя' in text or 'Full name' in text or '👤' in text:
                names_found += 1
        
        logger.info(f"Ответов с телефонами: {phones_found}")
        logger.info(f"Ответов с именами: {names_found}")
        
        # Сохраняем финальный отчет
        report = {
            'test_time': datetime.now().isoformat(),
            'bot_username': VK_BOT_USERNAME,
            'total_responses': len(all_responses),
            'phones_found': phones_found,
            'names_found': names_found,
            'responses': all_responses
        }
        
        with open('vk_debug_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n✅ Полный отчет сохранен в vk_debug_report.json")
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        logger.info(f"\n👋 Завершение работы...")
        await client.disconnect()

async def main():
    """Главная функция"""
    logger.info("🚀 Запуск отладочного мониторинга бота Sherlock")
    logger.info(f"📱 Целевой бот: @{VK_BOT_USERNAME}")
    logger.info(f"🔍 Все ответы будут сохранены в bot_responses_debug.json")
    logger.info(f"{'='*60}\n")
    
    await monitor_bot_responses()

if __name__ == "__main__":
    asyncio.run(main())
