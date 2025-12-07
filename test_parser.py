#!/usr/bin/env python3
"""
Тест парсера для проверки извлечения данных из ответа бота Sherlock
"""

import re

# Тестовый ответ от бота Sherlock
test_response = """🔎 **Вконтакте**
**ID:** `1`
**Имя:** `Павел`
**Фамилия:** `Дуров`
**Полное имя:** `Павел Дуров`
**Логин:** `durov`
**Дата рождения:** `10.10.1984`
**Город:** `Санкт-Петербург`

**Телефоны:**
 - `79161925982`
 - `79219550020`
 - `79539048549`
 - `447408857600`

👁 **Интересовались этим:** `6818`"""

def extract_phones(text: str) -> list[str]:
    """Извлечение телефонов из текста"""
    phones = []
    seen = set()
    
    # Метод 1: Поиск телефонов в Markdown формате (для бота Sherlock)
    # Формат: - `79161925982`
    markdown_phone_pattern = r'[-\s]+`(\d{10,})`'
    markdown_matches = re.findall(markdown_phone_pattern, text)
    
    for phone in markdown_matches:
        clean_phone = re.sub(r'[^\d]', '', phone)
        if len(clean_phone) >= 10:
            # Добавляем 7 если телефон начинается с 9 и имеет 10 цифр
            if len(clean_phone) == 10 and clean_phone.startswith('9'):
                clean_phone = '7' + clean_phone
            # Проверяем что это российский номер
            if len(clean_phone) == 11 and clean_phone.startswith('7'):
                if clean_phone not in seen:
                    phones.append(clean_phone)
                    seen.add(clean_phone)
            # Или международный номер
            elif len(clean_phone) > 11:
                if clean_phone not in seen:
                    phones.append(clean_phone)
                    seen.add(clean_phone)
    
    return phones

def parse_vk_data(text: str) -> dict:
    """Парсинг данных VK из ответа бота"""
    result = {
        "phones": [],
        "full_name": "",
        "birth_date": ""
    }
    
    # Извлечение телефонов
    result["phones"] = extract_phones(text)
    
    # Извлечение полного имени
    name_patterns = [
        r'\*\*Полное имя:\*\*\s*`([^`]+)`',
        r'Полное имя:\s*[`*]?(.*?)(?:[`*\n]|$)',
    ]
    
    for pattern in name_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            name = match.group(1).strip()
            # Очищаем от Markdown форматирования
            name = re.sub(r'\*\*([^*]+)\*\*', r'\1', name)
            name = re.sub(r'[`*_~]', '', name)
            if name:
                result["full_name"] = name.strip()
                break
    
    # Извлечение даты рождения
    birth_patterns = [
        r'\*\*Дата рождения:\*\*\s*`([^`]+)`',
        r'Дата рождения:\s*[`*]?(.*?)(?:[`*\n]|$)',
    ]
    
    for pattern in birth_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            birth = match.group(1).strip()
            birth = re.sub(r'[`*_~]', '', birth)
            if birth:
                result["birth_date"] = birth.strip()
                break
    
    return result

# Парсим данные
result = parse_vk_data(test_response)

print("Результат парсинга:")
print(f"Телефоны: {result['phones']}")
print(f"Имя: {result['full_name']}")
print(f"Дата рождения: {result['birth_date']}")

# Тестируем отдельно функцию извлечения телефонов
phones = extract_phones(test_response)
print(f"\nИзвлеченные телефоны: {phones}")
print(f"\nИзвлеченные телефоны: {phones}")