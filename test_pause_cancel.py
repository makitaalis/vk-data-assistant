#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы паузы и отмены
"""

import asyncio
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("test_pause_cancel")

async def simulate_status_check(user_id: int, session_state: dict):
    """Симуляция callback функции проверки статуса"""
    
    # Проверяем флаг отмены
    if session_state.get('cancelled', False):
        logger.info(f"🛑 Обработка отменена для пользователя {user_id}")
        raise asyncio.CancelledError("Processing cancelled by user")
    
    # Проверяем флаг паузы
    while session_state.get('paused', False):
        logger.info(f"⏸ Обработка приостановлена для пользователя {user_id}")
        await asyncio.sleep(1)
        
        # Проверяем отмену во время паузы
        if session_state.get('cancelled', False):
            raise asyncio.CancelledError("Processing cancelled by user")

async def process_with_pause_cancel(items: list, session_state: dict, user_id: int):
    """Обработка с поддержкой паузы и отмены"""
    
    processed_count = 0
    
    for i, item in enumerate(items, 1):
        # Проверяем статус перед обработкой каждого элемента
        await simulate_status_check(user_id, session_state)
        
        # Симулируем обработку
        logger.info(f"📝 Обработка элемента {i}/{len(items)}: {item}")
        await asyncio.sleep(0.5)  # Имитация работы
        
        processed_count += 1
        
        # Проверяем статус после обработки
        await simulate_status_check(user_id, session_state)
    
    return processed_count

async def test_pause_resume():
    """Тест паузы и возобновления"""
    logger.info("=== Тест паузы и возобновления ===")
    
    items = [f"item_{i}" for i in range(1, 11)]
    session_state = {'paused': False, 'cancelled': False}
    user_id = 123
    
    # Создаем задачу обработки
    process_task = asyncio.create_task(
        process_with_pause_cancel(items, session_state, user_id)
    )
    
    # Через 2 секунды ставим на паузу
    await asyncio.sleep(2)
    logger.info("🔴 Устанавливаем паузу...")
    session_state['paused'] = True
    
    # Ждем 3 секунды
    await asyncio.sleep(3)
    logger.info("🟢 Снимаем паузу...")
    session_state['paused'] = False
    
    # Ждем завершения
    try:
        result = await process_task
        logger.info(f"✅ Обработка завершена. Обработано: {result} элементов")
    except asyncio.CancelledError:
        logger.info("❌ Обработка была отменена")

async def test_cancel_during_processing():
    """Тест отмены во время обработки"""
    logger.info("\n=== Тест отмены во время обработки ===")
    
    items = [f"item_{i}" for i in range(1, 11)]
    session_state = {'paused': False, 'cancelled': False}
    user_id = 456
    
    # Создаем задачу обработки
    process_task = asyncio.create_task(
        process_with_pause_cancel(items, session_state, user_id)
    )
    
    # Через 2 секунды отменяем
    await asyncio.sleep(2)
    logger.info("🔴 Отменяем обработку...")
    session_state['cancelled'] = True
    
    # Ждем завершения
    try:
        result = await process_task
        logger.info(f"✅ Обработка завершена. Обработано: {result} элементов")
    except asyncio.CancelledError:
        logger.info("✅ Обработка успешно отменена")

async def test_cancel_during_pause():
    """Тест отмены во время паузы"""
    logger.info("\n=== Тест отмены во время паузы ===")
    
    items = [f"item_{i}" for i in range(1, 11)]
    session_state = {'paused': False, 'cancelled': False}
    user_id = 789
    
    # Создаем задачу обработки
    process_task = asyncio.create_task(
        process_with_pause_cancel(items, session_state, user_id)
    )
    
    # Через 1 секунду ставим на паузу
    await asyncio.sleep(1)
    logger.info("🔴 Устанавливаем паузу...")
    session_state['paused'] = True
    
    # Через 2 секунды отменяем
    await asyncio.sleep(2)
    logger.info("🔴 Отменяем обработку во время паузы...")
    session_state['cancelled'] = True
    
    # Ждем завершения
    try:
        result = await process_task
        logger.info(f"✅ Обработка завершена. Обработано: {result} элементов")
    except asyncio.CancelledError:
        logger.info("✅ Обработка успешно отменена во время паузы")

async def main():
    """Запуск всех тестов"""
    logger.info("🚀 Запуск тестов паузы и отмены\n")
    
    # Тест 1: Пауза и возобновление
    await test_pause_resume()
    
    # Тест 2: Отмена во время обработки
    await test_cancel_during_processing()
    
    # Тест 3: Отмена во время паузы
    await test_cancel_during_pause()
    
    logger.info("\n✅ Все тесты завершены")

if __name__ == "__main__":
    asyncio.run(main())