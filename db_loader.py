import pandas as pd
import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import json
import asyncio

logger = logging.getLogger("db_loader")

# Регулярное выражение для поиска ссылок VK
VK_LINK_PATTERN = r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.]+)'

# Регулярное выражение для поиска телефонов
PHONE_PATTERN = r'(?<!\d)(?:7|8|9)\d{10}(?!\d)'


class DatabaseLoader:
    """Класс для загрузки данных из Excel файлов в базу данных"""

    def __init__(self, database):
        self.db = database

    def normalize_phone(self, phone: str) -> Optional[str]:
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

    def extract_data_from_row(self, row: pd.Series) -> Dict[str, Any]:
        """
        Извлекает все VK ссылки и телефоны из строки, независимо от структуры

        Returns:
            Dict с найденными данными: {"vk_links": [...], "phones": [...]}
        """
        result = {
            "vk_links": [],
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }

        seen_links = set()
        seen_phones = set()

        # Проходим по всем ячейкам в строке
        for value in row:
            if pd.isna(value):
                continue

            str_value = str(value).strip()
            if not str_value:
                continue

            # Ищем VK ссылки
            vk_matches = re.findall(VK_LINK_PATTERN, str_value)
            for link in vk_matches:
                if link not in seen_links:
                    result["vk_links"].append(link)
                    seen_links.add(link)

            # Ищем телефоны
            phone_matches = re.findall(PHONE_PATTERN, str_value)
            for phone in phone_matches:
                normalized = self.normalize_phone(phone)
                if normalized and normalized not in seen_phones:
                    result["phones"].append(normalized)
                    seen_phones.add(normalized)

            # Пытаемся определить имя (строка без ссылок и телефонов, длиной от 5 до 50 символов)
            if not result["full_name"] and 5 <= len(str_value) <= 50:
                # Проверяем, что это не ссылка и не телефон
                if not re.search(VK_LINK_PATTERN, str_value) and not re.search(PHONE_PATTERN, str_value):
                    # Проверяем, что это похоже на имя (содержит буквы)
                    if re.search(r'[а-яА-Яa-zA-Z]', str_value):
                        result["full_name"] = str_value

            # Пытаемся определить дату рождения
            if not result["birth_date"]:
                # Паттерны для дат
                date_patterns = [
                    r'\d{1,2}\.\d{1,2}\.\d{4}',  # 12.08.2003
                    r'\d{1,2}\.\d{1,2}\.\d{2}',  # 12.08.03
                    r'\d{4}-\d{2}-\d{2}',  # 2003-08-12
                ]

                for pattern in date_patterns:
                    date_match = re.search(pattern, str_value)
                    if date_match:
                        result["birth_date"] = date_match.group()
                        break

        return result

    async def check_duplicates_in_batch(self, records: List[Dict[str, Any]]) -> Tuple[
        List[Dict[str, Any]], Dict[str, Any]]:
        """
        Проверяет все записи на дубликаты и возвращает только уникальные

        Returns:
            Tuple[List[Dict], Dict]: (уникальные записи, статистика дубликатов)
        """
        # Собираем все ссылки и телефоны для проверки
        all_links = []
        all_phones = []

        for record in records:
            if record["link"] and not record["link"].startswith("phone:"):
                all_links.append(record["link"])
            all_phones.extend(record.get("phones", []))

        # Убираем дубликаты в списках
        unique_links = list(set(all_links))
        unique_phones = list(set(all_phones))

        # Проверяем дубликаты в БД
        duplicate_data = await self.db.check_both_duplicates(unique_links, unique_phones)
        duplicate_links = duplicate_data["duplicate_links"]
        duplicate_phones = duplicate_data["duplicate_phones"]

        # Фильтруем записи
        unique_records = []
        duplicate_stats = {
            "total_checked": len(records),
            "duplicate_by_link": 0,
            "duplicate_by_phone": 0,
            "duplicate_by_both": 0,
            "unique": 0
        }

        for record in records:
            link = record["link"]
            phones = record.get("phones", [])

            # Проверяем дубликаты
            is_duplicate_link = link in duplicate_links
            is_duplicate_phone = any(phone in duplicate_phones for phone in phones)

            if is_duplicate_link and is_duplicate_phone:
                duplicate_stats["duplicate_by_both"] += 1
                logger.info(f"🔄 Дубликат по ссылке И телефону: {link}")
            elif is_duplicate_link:
                duplicate_stats["duplicate_by_link"] += 1
                logger.info(f"🔄 Дубликат по ссылке: {link}")
            elif is_duplicate_phone:
                duplicate_stats["duplicate_by_phone"] += 1
                logger.info(f"🔄 Дубликат по телефону: {link} - телефоны {phones}")
            else:
                # Это уникальная запись
                unique_records.append(record)
                duplicate_stats["unique"] += 1

        return unique_records, duplicate_stats

    def process_excel_file(self, file_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        Обрабатывает Excel файл и извлекает данные

        Returns:
            Tuple[List[Dict], Dict]: (список записей, статистика)
        """
        stats = {
            "total_rows": 0,
            "rows_with_vk_links": 0,
            "rows_with_phones": 0,
            "total_vk_links": 0,
            "total_phones": 0,
            "unique_vk_links": 0,
            "unique_phones": 0
        }

        all_records = []
        vk_link_to_data = {}  # {vk_link: {"phones": [], "full_name": "", "birth_date": ""}}
        phone_to_vk_links = {}  # {phone: [vk_links]}

        try:
            # Читаем файл, пробуем с заголовками и без
            try:
                df = pd.read_excel(file_path, dtype=str)
            except:
                df = pd.read_excel(file_path, dtype=str, header=None)

            stats["total_rows"] = len(df)
            logger.info(f"📊 Загружен файл: {stats['total_rows']} строк, {len(df.columns)} столбцов")

            # Обрабатываем каждую строку
            for idx, row in df.iterrows():
                try:
                    # Извлекаем все данные из строки
                    row_data = self.extract_data_from_row(row)

                    if row_data["vk_links"]:
                        stats["rows_with_vk_links"] += 1
                        stats["total_vk_links"] += len(row_data["vk_links"])

                    if row_data["phones"]:
                        stats["rows_with_phones"] += 1
                        stats["total_phones"] += len(row_data["phones"])

                    # Связываем VK ссылки с данными
                    for vk_link in row_data["vk_links"]:
                        if vk_link not in vk_link_to_data:
                            vk_link_to_data[vk_link] = {
                                "phones": [],
                                "full_name": row_data["full_name"],
                                "birth_date": row_data["birth_date"]
                            }

                        # Добавляем телефоны к ссылке
                        for phone in row_data["phones"]:
                            if phone not in vk_link_to_data[vk_link]["phones"]:
                                vk_link_to_data[vk_link]["phones"].append(phone)

                        # Обновляем имя и дату если они пустые
                        if not vk_link_to_data[vk_link]["full_name"] and row_data["full_name"]:
                            vk_link_to_data[vk_link]["full_name"] = row_data["full_name"]

                        if not vk_link_to_data[vk_link]["birth_date"] and row_data["birth_date"]:
                            vk_link_to_data[vk_link]["birth_date"] = row_data["birth_date"]

                    # Связываем телефоны с VK ссылками
                    for phone in row_data["phones"]:
                        if phone not in phone_to_vk_links:
                            phone_to_vk_links[phone] = []

                        for vk_link in row_data["vk_links"]:
                            if vk_link not in phone_to_vk_links[phone]:
                                phone_to_vk_links[phone].append(vk_link)

                except Exception as e:
                    logger.error(f"Ошибка при обработке строки {idx}: {e}")
                    continue

            # Теперь обрабатываем телефоны без VK ссылок
            # Создаем записи для телефонов, у которых нет прямой связи с VK
            orphan_phones = {}  # Телефоны без прямых VK ссылок

            for idx, row in df.iterrows():
                row_data = self.extract_data_from_row(row)

                # Если есть телефоны, но нет VK ссылок
                if row_data["phones"] and not row_data["vk_links"]:
                    for phone in row_data["phones"]:
                        if phone not in orphan_phones:
                            orphan_phones[phone] = {
                                "full_name": row_data["full_name"],
                                "birth_date": row_data["birth_date"]
                            }

            # Формируем финальные записи
            # 1. Записи с VK ссылками
            for vk_link, data in vk_link_to_data.items():
                record = {
                    "link": vk_link,
                    "phones": data["phones"],
                    "full_name": data["full_name"],
                    "birth_date": data["birth_date"]
                }
                all_records.append(record)

            # 2. Записи для телефонов без VK ссылок (сохраняем как специальные записи)
            for phone, data in orphan_phones.items():
                # Проверяем, нет ли этого телефона уже в базе
                # Это будет делать основной код через check_phone_duplicates
                record = {
                    "link": f"phone:{phone}",  # Специальный формат для телефонов без ссылок
                    "phones": [phone],
                    "full_name": data["full_name"],
                    "birth_date": data["birth_date"]
                }
                all_records.append(record)

            stats["unique_vk_links"] = len(vk_link_to_data)
            stats["unique_phones"] = len(phone_to_vk_links)

            logger.info(f"📊 Итоги обработки:")
            logger.info(f"   Уникальных VK ссылок: {stats['unique_vk_links']}")
            logger.info(f"   Уникальных телефонов: {stats['unique_phones']}")
            logger.info(f"   Телефонов без VK ссылок: {len(orphan_phones)}")

        except Exception as e:
            logger.error(f"❌ Ошибка при чтении файла: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return all_records, stats

    async def load_from_excel(self, file_path: Path, user_id: int) -> Dict[str, int]:
        """
        Загружает данные из Excel файла в базу данных

        Args:
            file_path: Путь к Excel файлу
            user_id: ID пользователя, загружающего данные

        Returns:
            Dict со статистикой загрузки
        """
        logger.info(f"🔄 Начинаю загрузку файла: {file_path.name}")

        # Обрабатываем файл
        records, file_stats = self.process_excel_file(file_path)

        logger.info(f"📊 Обработано: {len(records)} записей")

        if not records:
            return {"added": 0, "updated": 0, "errors": 0, "duplicates": 0}

        # Фильтруем записи с настоящими VK ссылками для сохранения
        vk_records = [r for r in records if not r["link"].startswith("phone:")]

        # Проверяем дубликаты перед сохранением
        unique_records, duplicate_stats = await self.check_duplicates_in_batch(vk_records)

        logger.info(f"📊 Статистика дубликатов:")
        logger.info(f"   Всего проверено: {duplicate_stats['total_checked']}")
        logger.info(f"   Дубликаты по ссылке: {duplicate_stats['duplicate_by_link']}")
        logger.info(f"   Дубликаты по телефону: {duplicate_stats['duplicate_by_phone']}")
        logger.info(f"   Дубликаты по обоим: {duplicate_stats['duplicate_by_both']}")
        logger.info(f"   Уникальных записей: {duplicate_stats['unique']}")

        # Сохраняем в базу данных только уникальные записи
        if unique_records:
            db_stats = await self.db.batch_save_results(unique_records, user_id, source="import")
            logger.info(f"✅ Загружено в БД: добавлено {db_stats['added']}, обновлено {db_stats['updated']}")
        else:
            db_stats = {"added": 0, "updated": 0, "errors": 0}
            logger.info("⚠️ Нет уникальных записей для загрузки")

        # Добавляем информацию о дубликатах в статистику
        db_stats["duplicates"] = duplicate_stats['total_checked'] - duplicate_stats['unique']

        return db_stats

    def analyze_excel_structure(self, file_path: Path) -> Dict[str, Any]:
        """Анализирует структуру Excel файла для отладки"""
        try:
            # Читаем файл
            try:
                df = pd.read_excel(file_path, dtype=str)
            except:
                df = pd.read_excel(file_path, dtype=str, header=None)

            analysis = {
                "file_name": file_path.name,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "data_preview": []
            }

            # Анализируем первые 5 строк
            for idx, row in df.head().iterrows():
                row_data = self.extract_data_from_row(row)
                analysis["data_preview"].append({
                    "row": idx + 1,
                    "vk_links": row_data["vk_links"],
                    "phones": row_data["phones"],
                    "full_name": row_data["full_name"],
                    "birth_date": row_data["birth_date"]
                })

            # Общая статистика
            all_vk_links = set()
            all_phones = set()

            for idx, row in df.iterrows():
                row_data = self.extract_data_from_row(row)
                all_vk_links.update(row_data["vk_links"])
                all_phones.update(row_data["phones"])

            analysis["total_unique_vk_links"] = len(all_vk_links)
            analysis["total_unique_phones"] = len(all_phones)

            return analysis

        except Exception as e:
            logger.error(f"Ошибка при анализе файла: {e}")
            return {"error": str(e)}

    def find_all_related_data(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Находит все связанные данные по телефонам и ссылкам

        Returns:
            Dict с полной картиной связей
        """
        phone_network = {}  # {phone: {"vk_links": [...], "names": [...], "birth_dates": [...]}}
        vk_network = {}  # {vk_link: {"phones": [...], "related_vk_links": [...]}}

        # Строим сеть связей
        for record in records:
            vk_link = record["link"]
            phones = record["phones"]

            # Обновляем VK сеть
            if vk_link not in vk_network:
                vk_network[vk_link] = {"phones": [], "related_vk_links": set()}
            vk_network[vk_link]["phones"] = phones

            # Обновляем телефонную сеть
            for phone in phones:
                if phone not in phone_network:
                    phone_network[phone] = {"vk_links": [], "names": [], "birth_dates": []}

                if vk_link not in phone_network[phone]["vk_links"]:
                    phone_network[phone]["vk_links"].append(vk_link)

                if record["full_name"] and record["full_name"] not in phone_network[phone]["names"]:
                    phone_network[phone]["names"].append(record["full_name"])

                if record["birth_date"] and record["birth_date"] not in phone_network[phone]["birth_dates"]:
                    phone_network[phone]["birth_dates"].append(record["birth_date"])

        # Находим связанные VK через общие телефоны
        for phone, data in phone_network.items():
            vk_links = data["vk_links"]
            # Связываем все VK ссылки, имеющие общий телефон
            for i, vk1 in enumerate(vk_links):
                for vk2 in vk_links[i + 1:]:
                    vk_network[vk1]["related_vk_links"].add(vk2)
                    vk_network[vk2]["related_vk_links"].add(vk1)

        # Конвертируем sets в lists для JSON
        for vk_link in vk_network:
            vk_network[vk_link]["related_vk_links"] = list(vk_network[vk_link]["related_vk_links"])

        return {
            "phone_network": phone_network,
            "vk_network": vk_network,
            "stats": {
                "total_phones": len(phone_network),
                "phones_with_multiple_vk": sum(1 for p in phone_network.values() if len(p["vk_links"]) > 1),
                "total_vk_links": len(vk_network),
                "vk_with_multiple_phones": sum(1 for v in vk_network.values() if len(v["phones"]) > 1)
            }
        }