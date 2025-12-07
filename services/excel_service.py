"""Сервис для работы с Excel файлами с поддержкой анализа дубликатов"""

import pandas as pd
import re
import logging
from typing import List, Dict, Tuple, Optional, Any, Set
from pathlib import Path
import json
from collections import Counter, OrderedDict
from openpyxl.styles import Alignment

from bot.config import VK_LINK_PATTERN

logger = logging.getLogger("excel_service")


class ExcelProcessor:
    """Класс для интеллектуальной обработки Excel файлов с анализом дубликатов"""

    def __init__(self):
        self.original_df = None
        self.vk_column_index = None
        self.vk_column_name = None
        self.vk_links_mapping = {}  # {link: [row_index, ...]}
        self.all_links_found = []  # Все найденные ссылки (с дубликатами)
        self.duplicate_analysis = None  # Результаты анализа дубликатов

    def find_vk_column(self, df: pd.DataFrame) -> Optional[Tuple[int, str]]:
        """
        Автоматически находит столбец со ссылками VK
        Возвращает (индекс_столбца, имя_столбца) или None
        """
        logger.info("🔍 Поиск столбца со ссылками VK...")

        # Проверяем каждый столбец
        for col_idx, col_name in enumerate(df.columns):
            vk_links_count = 0
            total_non_empty = 0

            # Проверяем значения в столбце
            for value in df[col_name].dropna():
                str_value = str(value).strip()
                if str_value:
                    total_non_empty += 1
                    if re.match(VK_LINK_PATTERN, str_value):
                        vk_links_count += 1

            # Если более 50% непустых значений - это VK ссылки
            if total_non_empty > 0 and (vk_links_count / total_non_empty) > 0.5:
                logger.info(f"✅ Найден столбец со ссылками: '{col_name}' (индекс {col_idx})")
                logger.info(f"   Содержит {vk_links_count} VK ссылок из {total_non_empty} значений")
                return col_idx, col_name

        logger.warning("❌ Столбец со ссылками VK не найден")
        return None

    def load_excel_file(self, file_path: Path) -> Tuple[List[str], List[int], bool]:
        """
        Загружает Excel файл и извлекает ссылки VK

        Возвращает:
        - Список уникальных ссылок VK (в порядке первого появления)
        - Список индексов строк для каждой ссылки
        - Успешность операции
        """
        try:
            # Читаем файл БЕЗ принудительного преобразования в строки
            self.original_df = pd.read_excel(file_path)

            # Но для столбца с VK ссылками нужно убедиться что они строки
            df_for_search = self.original_df.astype(str)

            logger.info(f"📊 Загружен файл: {self.original_df.shape[0]} строк, {self.original_df.shape[1]} столбцов")

            # Ищем столбец со ссылками в строковой версии
            column_info = self._find_vk_column_in_df(df_for_search)
            if not column_info:
                return [], [], False

            self.vk_column_index, self.vk_column_name = column_info

            # Извлекаем ВСЕ ссылки (включая дубликаты) и запоминаем их позиции
            self.all_links_found = []
            links_with_rows = []  # [(link, row_index), ...]
            self.vk_links_mapping = {}

            for idx, row in self.original_df.iterrows():
                # Для VK ссылок используем строковое представление
                value = str(row[self.vk_column_name]).strip()

                # Ищем ВСЕ VK ссылки в ячейке (может быть несколько)
                matches = re.findall(VK_LINK_PATTERN, value)
                for match in matches:
                    self.all_links_found.append(match)
                    links_with_rows.append((match, idx))
                    self.vk_links_mapping.setdefault(match, []).append(idx)

            # Создаем упорядоченный список уникальных ссылок (сохраняя порядок первого появления)
            seen = set()
            unique_links = []
            row_indices = []
            for link, row_idx in links_with_rows:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)
                    row_indices.append(row_idx)

            # Анализируем дубликаты
            self.duplicate_analysis = self._analyze_duplicates()

            logger.info(f"✅ Найдено {len(self.all_links_found)} VK ссылок (включая дубликаты)")
            logger.info(f"✅ Уникальных ссылок: {len(unique_links)}")

            return unique_links, row_indices, True

        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке файла: {e}")
            return [], [], False

    def _find_vk_column_in_df(self, df: pd.DataFrame) -> Optional[Tuple[int, str]]:
        """
        Вспомогательный метод для поиска столбца с VK ссылками
        """
        # Проверяем каждый столбец
        for col_idx, col_name in enumerate(df.columns):
            vk_links_count = 0
            total_non_empty = 0

            # Проверяем значения в столбце
            for value in df[col_name].dropna():
                str_value = str(value).strip()
                if str_value and str_value != 'nan':
                    total_non_empty += 1
                    # Проверяем есть ли VK ссылка в значении
                    if re.search(VK_LINK_PATTERN, str_value):
                        vk_links_count += 1

            # Если более 50% непустых значений содержат VK ссылки
            if total_non_empty > 0 and (vk_links_count / total_non_empty) > 0.5:
                logger.info(f"✅ Найден столбец со ссылками: '{col_name}' (индекс {col_idx})")
                logger.info(f"   Содержит VK ссылки в {vk_links_count} ячейках из {total_non_empty}")
                return col_idx, col_name

        logger.warning("❌ Столбец со ссылками VK не найден")
        return None

    def _analyze_duplicates(self) -> Dict[str, Any]:
        """
        Анализирует дубликаты в найденных ссылках
        """
        if not self.all_links_found:
            return {
                'total_links': 0,
                'unique_links': 0,
                'duplicate_count': 0,
                'duplicate_percent': 0,
                'duplicates': {},
                'duplicate_rows': {}
            }

        # Подсчет частоты каждой ссылки
        link_counter = Counter(self.all_links_found)

        # Находим дубликаты (ссылки встречающиеся более 1 раза)
        duplicates = {link: count for link, count in link_counter.items() if count > 1}

        # Находим строки с дубликатами
        duplicate_rows = {}
        for idx, row in self.original_df.iterrows():
            value = str(row[self.vk_column_name]).strip()
            matches = re.findall(VK_LINK_PATTERN, value)

            for match in matches:
                if match in duplicates:
                    if match not in duplicate_rows:
                        duplicate_rows[match] = []
                    duplicate_rows[match].append(idx + 2)  # +2 для Excel (1-based + заголовок)

        total = len(self.all_links_found)
        unique = len(link_counter)
        duplicate_count = total - unique
        duplicate_percent = (duplicate_count / total * 100) if total > 0 else 0

        return {
            'total_links': total,
            'unique_links': unique,
            'duplicate_count': duplicate_count,
            'duplicate_percent': duplicate_percent,
            'duplicates': duplicates,
            'duplicate_rows': duplicate_rows,
            'top_duplicates': sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:10] if duplicates else []
        }

    def get_duplicate_analysis(self) -> Dict[str, Any]:
        """
        Возвращает результаты анализа дубликатов
        """
        if self.duplicate_analysis is None:
            self.duplicate_analysis = self._analyze_duplicates()
        return self.duplicate_analysis

    def remove_duplicates_keep_first(self) -> Tuple[List[str], List[int]]:
        """
        Удаляет дубликаты, оставляя первое вхождение каждой ссылки

        Возвращает:
        - Список уникальных ссылок
        - Список индексов строк
        """
        seen = set()
        unique_links = []
        row_indices = []

        for link in self.all_links_found:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
                if link in self.vk_links_mapping and self.vk_links_mapping[link]:
                    row_indices.append(self.vk_links_mapping[link][0])

        return unique_links, row_indices

    def get_links_without_duplicates(self) -> List[str]:
        """
        Возвращает список уникальных ссылок без дубликатов
        """
        # Используем OrderedDict для сохранения порядка
        return list(OrderedDict.fromkeys(self.all_links_found))

    def save_results_with_original_data(
            self,
            results: Dict[str, Dict[str, Any]],
            output_path: Path
    ) -> bool:
        """
        Сохраняет результаты, добавляя найденные телефоны к исходным данным без их перезаписи
        """
        try:
            if self.original_df is None:
                logger.error("❌ Нет загруженного файла")
                return False

            result_df = self.original_df.copy()

            target_column = "Найденные телефоны"
            if target_column not in result_df.columns:
                result_df[target_column] = ""
            else:
                result_df[target_column] = result_df[target_column].fillna("")

            for link, data in results.items():
                rows = self.vk_links_mapping.get(link)
                if rows is None:
                    continue

                if not isinstance(rows, list):
                    rows = [rows]

                phones = self._extract_phone_list(data)
                if not phones:
                    continue
                phones = phones[:2]
                if not phones:
                    continue

                for row_idx in rows:
                    if row_idx >= len(result_df):
                        continue
                    existing_value = result_df.at[row_idx, target_column]
                    existing_items = []
                    if pd.notna(existing_value):
                        existing_items = [
                            item.strip()
                            for item in str(existing_value).replace("\n", ",").split(",")
                            if item.strip()
                        ]

                    combined = list(OrderedDict.fromkeys(existing_items + phones))[:2]
                    result_df.at[row_idx, target_column] = ", ".join(combined)

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Результаты')
                worksheet = writer.sheets['Результаты']

                header_row = 1

                for column in worksheet.columns:
                    column_letter = column[0].column_letter
                    header_value = worksheet.cell(row=header_row, column=column[0].column)

                    if header_value.value == target_column:
                        worksheet.column_dimensions[column_letter].width = 30
                        for cell in column:
                            cell.alignment = Alignment(wrap_text=True, vertical="top")
                            cell.number_format = "@"
                    else:
                        max_length = 0
                        for cell in column:
                            try:
                                if cell.value is not None:
                                    max_length = max(max_length, len(str(cell.value)))
                            except Exception:
                                continue
                        worksheet.column_dimensions[column_letter].width = min(max_length + 2, 50)

            logger.info(f"✅ Результаты сохранены в {output_path} с исходными данными")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результатов: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def get_file_info(self) -> Dict[str, Any]:
        """Возвращает информацию о загруженном файле"""
        if self.original_df is None:
            return {}

        duplicate_info = self.get_duplicate_analysis()

        return {
            "total_rows": len(self.original_df),
            "total_columns": len(self.original_df.columns),
            "vk_column": self.vk_column_name,
            "vk_links_count": len(self.vk_links_mapping),
            "total_links_found": duplicate_info['total_links'],
            "duplicate_count": duplicate_info['duplicate_count'],
            "duplicate_percent": duplicate_info['duplicate_percent'],
            "columns": list(self.original_df.columns)
        }

    def _extract_phone_list(self, result_data: Dict[str, Any]) -> List[str]:
        """Вытаскивает список телефонов из результата и приводит к строковому виду"""
        phones = result_data.get("phones", [])

        if phones is None:
            phones = []
        elif isinstance(phones, str):
            if phones.startswith('['):
                try:
                    phones = json.loads(phones)
                except Exception:
                    phones = []
            else:
                phones = [phones] if phones else []
        elif not isinstance(phones, list):
            phones = []

        normalized = OrderedDict()
        for phone in phones:
            candidate = str(phone).strip()
            if candidate:
                normalized[candidate] = None

        return list(normalized.keys())

    def update_results_from_dict(self, results: Dict[str, Dict[str, Any]]):
        """
        Обновляет исходный DataFrame результатами поиска
        Добавляет колонки для телефонов, имен и дат рождения
        """
        if self.original_df is None:
            logger.error("❌ Нет загруженного DataFrame для обновления")
            return

        try:
            # Создаем копию для работы
            df = self.original_df.copy()
            
            # Определяем максимальное количество телефонов для создания колонок
            max_phones = max(
                len(data.get('phones', [])) 
                for data in results.values() 
                if isinstance(data.get('phones'), list)
            ) if results else 0
            max_phones = min(max_phones, 2)
            
            # Создаем колонки для телефонов если нужно
            phone_columns = []
            for i in range(max_phones):
                col_name = f"Phone_{i+1}" if i > 0 else "Phone"
                if col_name not in df.columns:
                    df[col_name] = ""
                phone_columns.append(col_name)
            
            # Создаем колонки для других данных если нужно
            if "Full_Name" not in df.columns:
                df["Full_Name"] = ""
            if "Birth_Date" not in df.columns:
                df["Birth_Date"] = ""
            
            # Обновляем данные для каждой ссылки
            updated_count = 0
            for link, data in results.items():
                if link not in self.vk_links_mapping:
                    continue

                row_indices = self.vk_links_mapping.get(link) or []
                if not isinstance(row_indices, list):
                    row_indices = [row_indices]

                # Обновляем телефоны
                phones = data.get('phones', [])
                phone_values = phones if isinstance(phones, list) else []
                phone_values = phone_values[:2]

                # Обновляем имя/дату
                full_name = data.get('full_name', '').strip()
                birth_date = data.get('birth_date', '').strip()

                for row_idx in row_indices:
                    for i, phone in enumerate(phone_values[:max_phones]):
                        if i < len(phone_columns):
                            df.at[row_idx, phone_columns[i]] = phone

                    if full_name:
                        df.at[row_idx, "Full_Name"] = full_name
                    if birth_date:
                        df.at[row_idx, "Birth_Date"] = birth_date

                updated_count += 1
            
            # Обновляем исходный DataFrame
            self.original_df = df
            logger.info(f"✅ DataFrame обновлен результатами для {updated_count} ссылок")
            logger.info(f"   Добавлено колонок: Phone({max_phones}), Full_Name, Birth_Date")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении DataFrame: {e}")
            import traceback
            logger.error(traceback.format_exc())
