"""Вспомогательные функции"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

from aiogram import types
from aiogram.exceptions import TelegramBadRequest

from bot.config import VK_LINK_PATTERN, TEMP_DIR

logger = logging.getLogger("helpers")


def create_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Создает визуальный прогресс-бар"""
    if total == 0:
        return "⬜" * length

    filled = int((current / total) * length)
    empty = length - filled

    bar = "🟩" * filled + "⬜" * empty
    return bar


def format_time() -> str:
    """Форматирует текущее время"""
    return datetime.now().strftime("%H:%M:%S")


def format_datetime(dt: datetime) -> str:
    """Форматирует дату и время"""
    return dt.strftime("%d.%m.%Y %H:%M")


def validate_vk_link(link: str) -> bool:
    """Валидация ссылки VK"""
    if not re.match(VK_LINK_PATTERN, link):
        return False

    # Дополнительные проверки
    if len(link) > 200:  # Слишком длинная ссылка
        return False

    # Проверка на недопустимые символы
    if any(char in link for char in ['<', '>', '"', "'", '\n', '\r']):
        return False

    return True


def extract_vk_links(text: str) -> List[str]:
    """Извлекает и валидирует ссылки VK из текста"""
    if not text or len(text) > 10000:  # Защита от слишком больших сообщений
        return []

    links = re.findall(VK_LINK_PATTERN, text)
    # Валидируем и удаляем дубликаты
    valid_links = []
    seen = set()
    for link in links:
        if link not in seen and validate_vk_link(link):
            valid_links.append(link)
            seen.add(link)

    return valid_links[:100]  # Максимум 100 ссылок за раз


def normalize_phone(phone: str) -> Optional[str]:
    """Нормализует телефонный номер к единому формату"""
    # Очищаем от всех нецифровых символов
    digits = re.sub(r'[^\d]', '', phone)

    # Проверяем различные форматы
    if len(digits) == 11 and digits.startswith('7'):
        return digits
    elif len(digits) == 11 and digits.startswith('8'):
        return '7' + digits[1:]
    elif len(digits) == 10 and digits.startswith('9'):
        return '7' + digits

    return None


def validate_phone(phone: str) -> bool:
    """Проверяет валидность телефонного номера"""
    normalized = normalize_phone(phone)
    return normalized is not None and len(normalized) == 11 and normalized.startswith('7')


async def safe_edit_message(message: types.Message, text: str, reply_markup=None):
    """Безопасно обновляет сообщение"""
    try:
        if reply_markup:
            await message.edit_text(text, reply_markup=reply_markup)
        elif message.text != text:
            await message.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка при обновлении сообщения: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обновлении сообщения: {e}")


async def safe_answer_callback(callback: types.CallbackQuery, text: str = None, show_alert: bool = False):
    """Безопасно отвечает на callback"""
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception as e:
        logger.error(f"Ошибка при ответе на callback: {e}")


def truncate_text(text: str, max_length: int = 4096) -> str:
    """Обрезает текст до максимальной длины"""
    if len(text) <= max_length:
        return text

    return text[:max_length - 3] + "..."


def escape_html(text: str) -> str:
    """Экранирует HTML символы"""
    if not text:
        return ""

    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#39;'))


def format_phone_list(phones: List[str], max_phones: int = 4) -> str:
    """Форматирует список телефонов для отображения"""
    if not phones:
        return "Нет"

    display_phones = phones[:max_phones]
    formatted = ", ".join(display_phones)

    if len(phones) > max_phones:
        formatted += f" (и еще {len(phones) - max_phones})"

    return formatted


def calculate_eta(processed: int, total: int, elapsed_seconds: float) -> int:
    """Вычисляет примерное время до завершения в секундах"""
    if processed == 0 or elapsed_seconds == 0:
        return 0

    speed = processed / elapsed_seconds
    remaining = total - processed

    if speed > 0:
        return int(remaining / speed)

    return 0


def create_temp_dir(prefix: str = "tmp") -> Path:
    """Создает уникальную временную директорию в каталоге TEMP_DIR"""
    for _ in range(5):
        dir_path = TEMP_DIR / f"{prefix}_{uuid4().hex}"
        try:
            dir_path.mkdir(parents=True, exist_ok=False)
            return dir_path
        except FileExistsError:
            continue

    raise RuntimeError("Не удалось создать временную директорию")


def prepare_temp_file(filename: str, prefix: str = "tmp") -> Path:
    """Возвращает путь к временному файлу в каталоге TEMP_DIR"""
    base_name = Path(filename).name or "tmp_file"
    temp_dir = create_temp_dir(prefix)
    return temp_dir / base_name


def format_file_size(size_bytes: int) -> str:
    """Форматирует размер файла в человекочитаемый вид"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0

    return f"{size_bytes:.1f} TB"
