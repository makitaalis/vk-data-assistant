#!/usr/bin/env python3
"""
Исправление ложного срабатывания лимитов в VK сервисе
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_limits")


def fix_vk_service():
    """Исправляет обработку сообщений о лимитах"""

    vk_service_file = Path("services/vk_service.py")

    if not vk_service_file.exists():
        logger.error("Файл services/vk_service.py не найден!")
        return False

    with open(vk_service_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Создаем резервную копию
    backup_file = Path("services/vk_service.py.backup_limits")
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)

    # Находим метод _process_message и обновляем логику проверки лимитов
    old_limit_check = '''        # Проверка на лимит
        if any(phrase in text for phrase in ["Лимит запросов исчерпан", "Too many requests", "limit"]):
            logger.error("⚠️ Достигнут лимит запросов!")
            self.current_result = {"error": "limit_reached"}
            self.result_event.set()
            return'''

    new_limit_check = '''        # Проверка на лимит - более точная
        # Добавляем логирование для отладки
        if len(text) < 200:  # Короткие сообщения логируем полностью
            logger.debug(f"📨 Получено сообщение от бота: {text}")

        # Более точные фразы для определения лимита
        limit_phrases = [
            "Лимит запросов исчерпан",
            "Too many requests",
            "Превышен лимит",
            "Достигнут лимит",
            "исчерпали лимит"
        ]

        # Проверяем точные фразы, а не просто слово "limit"
        if any(phrase.lower() in text.lower() for phrase in limit_phrases):
            logger.error(f"⚠️ Бот сообщил о лимите! Полный текст: {text[:500]}")
            self.current_result = {"error": "limit_reached"}
            self.result_event.set()
            return'''

    content = content.replace(old_limit_check, new_limit_check)

    # Также обновим обработку ошибок, чтобы логировать полный текст
    old_error_check = '''        # Проверка на сообщение об ошибке
        if any(phrase in text for phrase in ["не найден", "ошибка", "error", "Попробуйте позже"]):
            logger.warning(f"⚠️ Бот вернул ошибку для {self.current_link}")
            self.current_result = {"phones": [], "full_name": "", "birth_date": ""}
            self.result_event.set()
            return'''

    new_error_check = '''        # Проверка на сообщение об ошибке
        error_phrases = ["не найден", "ошибка", "error", "Попробуйте позже", "недоступен"]
        if any(phrase in text.lower() for phrase in error_phrases):
            logger.warning(f"⚠️ Бот вернул ошибку для {self.current_link}. Текст: {text[:200]}")
            self.current_result = {"phones": [], "full_name": "", "birth_date": ""}
            self.result_event.set()
            return'''

    content = content.replace(old_error_check, new_error_check)

    # Добавим дополнительное логирование в начало _process_message
    process_method_start = '''async def _process_message(self, text: str, message_id: int):
        """Обработка сообщения от бота"""
        if not text:
            return'''

    new_process_method_start = '''async def _process_message(self, text: str, message_id: int):
        """Обработка сообщения от бота"""
        if not text:
            return

        # Логируем все сообщения для отладки
        logger.debug(f"[MSG {message_id}] Длина: {len(text)}, начало: {text[:100]}...")'''

    content = content.replace(process_method_start, new_process_method_start)

    # Также увеличим таймауты
    content = content.replace("MESSAGE_TIMEOUT = 15.0", "MESSAGE_TIMEOUT = 30.0")
    content = content.replace("INITIAL_DELAY = 2.0", "INITIAL_DELAY = 3.0")

    # Сохраняем изменения
    with open(vk_service_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ Файл services/vk_service.py обновлен")
    return True


def add_anti_flood_delay():
    """Добавляет задержку между запросами для предотвращения флуда"""

    vk_service_file = Path("services/vk_service.py")

    with open(vk_service_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Находим метод process_queue
    old_process = '''                try:
                    result = await self.search_vk_link(link)
                    await result_callback(link, result)
                    processed += 1'''

    new_process = '''                try:
                    result = await self.search_vk_link(link)
                    await result_callback(link, result)
                    processed += 1

                    # Добавляем небольшую задержку между запросами
                    # чтобы избежать блокировки за флуд
                    await asyncio.sleep(0.5)'''

    content = content.replace(old_process, new_process)

    # Сохраняем
    with open(vk_service_file, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info("✅ Добавлена антифлуд задержка")
    return True


def main():
    """Основная функция"""
    logger.info("🚀 Исправление проблемы с ложными лимитами")
    logger.info("=" * 60)

    # Применяем исправления
    if fix_vk_service():
        logger.info("✅ Обновлена логика определения лимитов")

    if add_anti_flood_delay():
        logger.info("✅ Добавлена защита от флуда")

    logger.info("\n" + "=" * 60)
    logger.info("✅ Исправления применены!")
    logger.info("\n📋 Что было сделано:")
    logger.info("1. Улучшена проверка сообщений о лимитах (теперь ищутся точные фразы)")
    logger.info("2. Добавлено детальное логирование всех сообщений от бота")
    logger.info("3. Увеличены таймауты ожидания ответа (с 15 до 30 секунд)")
    logger.info("4. Добавлена задержка 0.5 сек между запросами для предотвращения флуда")
    logger.info("\n🔄 Теперь:")
    logger.info("1. Перезапустите бота")
    logger.info("2. Попробуйте обработать файл снова")
    logger.info("3. В логах будут видны точные тексты сообщений от VK бота")
    logger.info("\n💡 Если проблема повторится, в логах будет виден полный текст сообщения от бота")


if __name__ == "__main__":
    main()