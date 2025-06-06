"""Валидаторы для проверки данных"""

import re
from typing import List, Optional

from bot.config import VK_LINK_PATTERN, PHONE_PATTERN


def validate_vk_link(link: str) -> bool:
    """
    Валидация ссылки VK

    Args:
        link: Ссылка для проверки

    Returns:
        True если ссылка валидна, False в противном случае
    """
    if not link or not isinstance(link, str):
        return False

    # Проверка по регулярному выражению
    if not re.match(VK_LINK_PATTERN, link):
        return False

    # Дополнительные проверки
    if len(link) > 200:  # Слишком длинная ссылка
        return False

    # Проверка на недопустимые символы
    if any(char in link for char in ['<', '>', '"', "'", '\n', '\r', '\t']):
        return False

    # Проверка на дубли слешей
    if '//' in link[8:]:  # Пропускаем https://
        return False

    return True


def extract_vk_links(text: str) -> List[str]:
    """
    Извлекает и валидирует ссылки VK из текста

    Args:
        text: Текст для поиска ссылок

    Returns:
        Список валидных уникальных ссылок
    """
    if not text or not isinstance(text, str):
        return []

    # Защита от слишком больших сообщений
    if len(text) > 10000:
        return []

    # Находим все ссылки
    links = re.findall(VK_LINK_PATTERN, text)

    # Валидируем и удаляем дубликаты
    valid_links = []
    seen = set()

    for link in links:
        if link not in seen and validate_vk_link(link):
            valid_links.append(link)
            seen.add(link)

            # Ограничение на количество ссылок
            if len(valid_links) >= 100:
                break

    return valid_links


def normalize_phone(phone: str) -> Optional[str]:
    """
    Нормализует телефонный номер к единому формату

    Args:
        phone: Номер телефона в любом формате

    Returns:
        Нормализованный номер (11 цифр начиная с 7) или None
    """
    if not phone:
        return None

    # Очищаем от всех нецифровых символов
    digits = re.sub(r'[^\d]', '', str(phone))

    # Проверяем различные форматы
    if len(digits) == 11 and digits.startswith('7'):
        return digits
    elif len(digits) == 11 and digits.startswith('8'):
        return '7' + digits[1:]
    elif len(digits) == 10 and digits.startswith('9'):
        return '7' + digits
    elif len(digits) == 10 and not digits.startswith('7'):
        # Возможно, номер без кода страны
        return '7' + digits

    return None


def validate_phone(phone: str) -> bool:
    """
    Проверяет валидность телефонного номера

    Args:
        phone: Номер телефона для проверки

    Returns:
        True если номер валиден, False в противном случае
    """
    normalized = normalize_phone(phone)
    return normalized is not None and len(normalized) == 11 and normalized.startswith('7')


def extract_phones(text: str) -> List[str]:
    """
    Извлекает телефонные номера из текста

    Args:
        text: Текст для поиска номеров

    Returns:
        Список нормализованных уникальных номеров
    """
    if not text or not isinstance(text, str):
        return []

    phones = []
    seen = set()

    # Ищем по паттерну
    matches = re.findall(PHONE_PATTERN, text)

    for match in matches:
        normalized = normalize_phone(match)
        if normalized and normalized not in seen:
            phones.append(normalized)
            seen.add(normalized)

            # Ограничение на количество
            if len(phones) >= 20:
                break

    return phones


def validate_excel_filename(filename: str) -> bool:
    """
    Проверяет, является ли файл Excel файлом

    Args:
        filename: Имя файла

    Returns:
        True если это Excel файл
    """
    if not filename:
        return False

    return filename.lower().endswith(('.xlsx', '.xls'))


def validate_user_id(user_id: int) -> bool:
    """
    Проверяет валидность Telegram user ID

    Args:
        user_id: ID пользователя

    Returns:
        True если ID валиден
    """
    return isinstance(user_id, int) and user_id > 0


def sanitize_filename(filename: str) -> str:
    """
    Очищает имя файла от недопустимых символов

    Args:
        filename: Исходное имя файла

    Returns:
        Безопасное имя файла
    """
    # Удаляем недопустимые символы
    safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)

    # Ограничиваем длину
    name, ext = safe_name.rsplit('.', 1) if '.' in safe_name else (safe_name, '')
    if len(name) > 100:
        name = name[:100]

    return f"{name}.{ext}" if ext else name


def validate_date_format(date_str: str) -> bool:
    """
    Проверяет формат даты рождения

    Args:
        date_str: Строка с датой

    Returns:
        True если формат корректен
    """
    if not date_str:
        return False

    # Поддерживаемые форматы
    patterns = [
        r'^\d{1,2}\.\d{1,2}\.\d{4}$',  # 12.08.2003
        r'^\d{1,2}\.\d{1,2}\.\d{2}$',  # 12.08.03
        r'^\d{4}-\d{2}-\d{2}$',  # 2003-08-12
        r'^\d{1,2}/\d{1,2}/\d{4}$',  # 12/08/2003
    ]

    return any(re.match(pattern, date_str.strip()) for pattern in patterns)