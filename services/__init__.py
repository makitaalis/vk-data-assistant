"""Сервисы для работы с внешними системами"""

from .vk_service import VKService
from .excel_service import ExcelProcessor
from .analysis_service import FileAnalyzer

__all__ = ["VKService", "ExcelProcessor", "FileAnalyzer"]