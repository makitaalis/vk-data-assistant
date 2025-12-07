"""Сервисы для работы с внешними системами"""

from .vk_service import VKService
from .excel_service import ExcelProcessor
from .analysis_service import FileAnalyzer
from .task_queue_service import TaskQueueService

__all__ = ["VKService", "ExcelProcessor", "FileAnalyzer", "TaskQueueService"]
