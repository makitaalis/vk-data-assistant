#!/usr/bin/env python3
"""
VK Data Assistant Bot
Точка входа для запуска бота
"""

import sys
import asyncio
import logging
from pathlib import Path

# Добавляем корневую директорию в путь Python
sys.path.insert(0, str(Path(__file__).parent))

from bot.main import main
from services.logger_service import setup_logger

def setup_logging():
    """Настройка логирования"""
    # Создаем директорию для логов если её нет
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Настройка форматирования
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Настройка логирования в файл
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Вывод в консоль
            logging.StreamHandler(sys.stdout),
            # Запись в файл
            logging.FileHandler(
                log_dir / "bot.log",
                mode='a',
                encoding='utf-8'
            )
        ]
    )

    # Уменьшаем уровень логирования для некоторых библиотек
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def print_banner():
    """Вывод баннера при запуске"""
    banner = """
╔══════════════════════════════════════════╗
║        VK Data Assistant Bot             ║
║        Version: 2.0                      ║
║        Author: Your Team                 ║
╚══════════════════════════════════════════╝
    """
    print(banner)


if __name__ == "__main__":
    # Настройка логирования
    setup_logging()

    # Вывод баннера
    print_banner()

    # Получаем logger
    logger = logging.getLogger("run")

    try:
        # Проверка версии Python
        if sys.version_info < (3, 10):
            logger.error("Требуется Python 3.10 или выше!")
            sys.exit(1)

        logger.info("🚀 Запуск VK Data Assistant Bot...")

        # Запуск бота
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("⏹ Бот остановлен пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)