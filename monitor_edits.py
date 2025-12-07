#!/usr/bin/env python3
"""
Мониторинг редактирования сообщений ботом Sherlock
"""

import asyncio
import logging
from telethon import TelegramClient, events
import os
from dotenv import load_dotenv
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'user_session')
_raw_bot_username = os.getenv('VK_BOT_USERNAME', 'sherlock_bot_ne_bot')
if _raw_bot_username and _raw_bot_username.startswith('@'):
    _clean_bot_username = _raw_bot_username.lstrip('@')
else:
    _clean_bot_username = _raw_bot_username or ''
VK_BOT_USERNAME = f"@{_clean_bot_username}" if _clean_bot_username else "@sherlock_bot_ne_bot"
VK_BOT_USERNAME_CLEAN = _clean_bot_username or "sherlock_bot_ne_bot"

all_messages = {}

async def monitor_bot():
    """Мониторинг сообщений и их редактирований"""
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    # Обработчик новых сообщений
    @client.on(events.NewMessage(from_users=VK_BOT_USERNAME_CLEAN))
    async def new_message_handler(event):
        """Обработчик новых сообщений от бота"""
        msg_id = event.message.id
        logger.info(f"\n{'='*60}")
        logger.info(f"📨 НОВОЕ СООБЩЕНИЕ (ID: {msg_id})")
        logger.info(f"Текст: {event.message.text[:100]}...")
        
        all_messages[msg_id] = {
            'type': 'new',
            'time': datetime.now().isoformat(),
            'text': event.message.text,
            'edits': []
        }
    
    # Обработчик редактирования сообщений
    @client.on(events.MessageEdited(from_users=VK_BOT_USERNAME_CLEAN))
    async def edit_handler(event):
        """Обработчик редактирования сообщений"""
        msg_id = event.message.id
        logger.info(f"\n{'='*60}")
        logger.info(f"✏️ СООБЩЕНИЕ ОТРЕДАКТИРОВАНО (ID: {msg_id})")
        logger.info(f"Новый текст:\n{event.message.text}")
        
        if msg_id in all_messages:
            all_messages[msg_id]['edits'].append({
                'time': datetime.now().isoformat(),
                'text': event.message.text
            })
        else:
            all_messages[msg_id] = {
                'type': 'edited',
                'time': datetime.now().isoformat(),
                'text': event.message.text,
                'edits': []
            }
        
        # Проверяем наличие телефонов в тексте
        if 'Телефон' in event.message.text or '📱' in event.message.text:
            logger.info("✅ НАЙДЕНЫ ТЕЛЕФОНЫ В СООБЩЕНИИ!")
        
        # Сохраняем все сообщения
        with open('messages_monitor.json', 'w', encoding='utf-8') as f:
            json.dump(all_messages, f, ensure_ascii=False, indent=2)
    
    await client.start()
    
    try:
        logger.info(f"🔍 Мониторинг бота @{VK_BOT_USERNAME}")
        bot_entity = await client.get_entity(VK_BOT_USERNAME)
        logger.info(f"✅ Бот найден: {bot_entity.username}")
        
        # Отправляем тестовые запросы
        test_links = [
            "https://vk.com/id1",
            "https://vk.com/id2",
            "https://vk.com/id100"
        ]
        
        for link in test_links:
            logger.info(f"\n📤 Отправляю: {link}")
            await client.send_message(bot_entity, link)
            await asyncio.sleep(2)
        
        # Ждем ответы и редактирования
        logger.info(f"\n⏳ Жду 30 секунд для получения всех ответов и редактирований...")
        await asyncio.sleep(30)
        
        # Анализ результатов
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 РЕЗУЛЬТАТЫ МОНИТОРИНГА")
        logger.info(f"{'='*60}")
        
        for msg_id, data in all_messages.items():
            logger.info(f"\nСообщение ID: {msg_id}")
            logger.info(f"  Тип: {data['type']}")
            logger.info(f"  Редактирований: {len(data['edits'])}")
            
            if data['edits']:
                last_edit = data['edits'][-1]
                logger.info(f"  Последний текст: {last_edit['text'][:200]}...")
        
        logger.info(f"\n✅ Результаты сохранены в messages_monitor.json")
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(monitor_bot())
