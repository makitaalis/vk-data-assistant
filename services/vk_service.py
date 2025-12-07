"""
VKService - обратная совместимость
Этот файл теперь импортирует VKMultiBotService для поддержки ротации ботов
"""

from services.vk_multibot_service import VKMultiBotService as VKService

# Для обратной совместимости экспортируем все необходимое
__all__ = ["VKService"]