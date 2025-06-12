"""Сервис для работы с Excel файлами"""

import pandas as pd
import re
import logging
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
import json

from bot.config import VK_LINK_PATTERN

logger = logging.getLogger("excel_service")


class ExcelProcessor:
    """Класс для интеллектуальной обработки Excel файлов"""

    def __init__(self):
        self.original_df = None
        self.vk_column_index = None
        self.vk_column_name = None
        self.vk_links_mapping = {}  # {link: row_index}

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
        - Список ссылок VK
        - Список индексов строк для каждой ссылки
        - Успешность операции
        """
        try:
            # Читаем файл БЕЗ принудительного преобразования в строки
            # Это позволит pandas автоматически определить типы данных
            self.original_df = pd.read_excel(file_path)

            # Но для столбца с VK ссылками нужно убедиться что они строки
            # Преобразуем все столбцы в строки только для поиска VK ссылок
            df_for_search = self.original_df.astype(str)

            logger.info(f"📊 Загружен файл: {self.original_df.shape[0]} строк, {self.original_df.shape[1]} столбцов")

            # Ищем столбец со ссылками в строковой версии
            column_info = self._find_vk_column_in_df(df_for_search)
            if not column_info:
                return [], [], False

            self.vk_column_index, self.vk_column_name = column_info

            # Извлекаем ссылки и запоминаем их позиции
            links = []
            row_indices = []
            self.vk_links_mapping = {}

            for idx, row in self.original_df.iterrows():
                # Для VK ссылок используем строковое представление
                value = str(row[self.vk_column_name]).strip()
                if re.match(VK_LINK_PATTERN, value):
                    links.append(value)
                    row_indices.append(idx)
                    self.vk_links_mapping[value] = idx

            logger.info(f"✅ Извлечено {len(links)} VK ссылок")
            return links, row_indices, True

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
                    if re.match(VK_LINK_PATTERN, str_value):
                        vk_links_count += 1

            # Если более 50% непустых значений - это VK ссылки
            if total_non_empty > 0 and (vk_links_count / total_non_empty) > 0.5:
                logger.info(f"✅ Найден столбец со ссылками: '{col_name}' (индекс {col_idx})")
                logger.info(f"   Содержит {vk_links_count} VK ссылок из {total_non_empty} значений")
                return col_idx, col_name

        logger.warning("❌ Столбец со ссылками VK не найден")
        return None

    def save_results_with_original_data(
            self,
            results: Dict[str, Dict[str, Any]],
            output_path: Path
    ) -> bool:
        """
        Сохраняет результаты, добавляя ТОЛЬКО телефоны к исходным данным
        """
        try:
            if self.original_df is None:
                logger.error("❌ Нет загруженного файла")
                return False

            # Создаем копию оригинального DataFrame
            result_df = self.original_df.copy()

            # Определяем максимальное количество телефонов
            max_phones = 0
            for data in results.values():
                phones = data.get('phones', [])
                if isinstance(phones, list):
                    max_phones = max(max_phones, len(phones))

            # Если телефонов нет, добавляем хотя бы один столбец
            if max_phones == 0:
                max_phones = 1

            # Добавляем столбцы для телефонов
            for i in range(max_phones):
                col_name = f'Телефон{i + 1}'
                result_df[col_name] = ''

            # Заполняем данные для каждой строки
            for link, data in results.items():
                if link in self.vk_links_mapping:
                    row_idx = self.vk_links_mapping[link]

                    # Извлекаем телефоны
                    phones = data.get('phones', [])
                    if phones is None:
                        phones = []
                    elif isinstance(phones, str):
                        if phones.startswith('['):
                            try:
                                phones = json.loads(phones)
                            except:
                                phones = []
                        else:
                            phones = [phones] if phones else []
                    elif not isinstance(phones, list):
                        phones = []

                    # Убедимся что элементы списка - строки
                    phones = [str(p) for p in phones if p]

                    # Добавляем телефоны в соответствующие столбцы
                    for i, phone in enumerate(phones):
                        if i < max_phones:
                            col_name = f'Телефон{i + 1}'
                            result_df.at[row_idx, col_name] = phone

            # Сохраняем результат с правильным форматированием
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Результаты')

                # Получаем worksheet
                worksheet = writer.sheets['Результаты']

                # Проходим по всем ячейкам и устанавливаем формат
                for row in worksheet.iter_rows():
                    for cell in row:
                        # Если это заголовок, пропускаем
                        if cell.row == 1:
                            continue

                        # Для телефонных столбцов устанавливаем текстовый формат
                        if cell.column_letter and worksheet.cell(1, cell.column).value and 'Телефон' in str(
                                worksheet.cell(1, cell.column).value):
                            # Устанавливаем формат как текст
                            cell.number_format = '@'
                            # Если значение есть, преобразуем в строку
                            if cell.value:
                                cell.value = str(cell.value)
                        else:
                            # Для остальных ячеек пытаемся сохранить исходный тип
                            if cell.value is not None:
                                # Пробуем преобразовать в число если это возможно
                                try:
                                    # Проверяем, является ли значение числом
                                    if str(cell.value).replace('.', '').replace(',', '').replace('-', '').isdigit():
                                        # Если нет букв, пробуем преобразовать
                                        if '.' in str(cell.value) or ',' in str(cell.value):
                                            cell.value = float(str(cell.value).replace(',', '.'))
                                        else:
                                            # Проверяем, не телефон ли это (11 цифр начиная с 7 или 8)
                                            if len(str(cell.value)) == 11 and str(cell.value)[0] in ['7', '8']:
                                                cell.number_format = '@'  # Текстовый формат для телефонов
                                            else:
                                                cell.value = int(str(cell.value))
                                except:
                                    # Если не получилось преобразовать, оставляем как есть
                                    pass

                # Автоподбор ширины столбцов
                for column in worksheet.columns:
                    max_length = 0
                    column_cells = [cell for cell in column]

                    for cell in column_cells:
                        try:
                            if cell.value:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass

                    # Устанавливаем ширину
                    column_letter = column_cells[0].column_letter
                    if column_cells[0].value and 'Телефон' in str(column_cells[0].value):
                        worksheet.column_dimensions[column_letter].width = 15
                    else:
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

            logger.info(f"✅ Результаты сохранены в {output_path} с исходными данными")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результатов: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def save_results_to_excel(
            self,
            results: Dict[str, Dict[str, Any]],
            output_path: Path,
            keep_only_with_data: bool = False
    ) -> bool:
        """
        Сохраняет результаты обратно в Excel, добавляя новые столбцы

        Args:
            results: Словарь {ссылка: {phones, full_name, birth_date}}
            output_path: Путь для сохранения файла
            keep_only_with_data: Если True, сохраняет только строки с найденными данными
        """
        try:
            if self.original_df is None:
                logger.error("❌ Нет загруженного файла")
                return False

            # Создаем копию оригинального DataFrame
            result_df = self.original_df.copy()

            # Определяем позицию для новых столбцов (после последнего существующего)
            insert_position = len(result_df.columns)

            # Подготавливаем новые столбцы
            new_columns = {
                'Телефон 1': {},
                'Телефон 2': {},
                'Телефон 3': {},
                'Телефон 4': {},
                'Полное имя': {},
                'Дата рождения': {}
            }

            # Заполняем данные для каждой строки
            rows_with_data = set()

            for link, data in results.items():
                if link in self.vk_links_mapping:
                    row_idx = self.vk_links_mapping[link]

                    # Телефоны
                    phones = data.get('phones', [])
                    for i in range(4):
                        col_name = f'Телефон {i + 1}'
                        new_columns[col_name][row_idx] = phones[i] if i < len(phones) else ''

                    # Полное имя и дата рождения
                    new_columns['Полное имя'][row_idx] = data.get('full_name', '')
                    new_columns['Дата рождения'][row_idx] = data.get('birth_date', '')

                    # Отмечаем строки с данными
                    if phones or data.get('full_name') or data.get('birth_date'):
                        rows_with_data.add(row_idx)

            # Добавляем новые столбцы к DataFrame
            for col_name in ['Телефон 1', 'Телефон 2', 'Телефон 3', 'Телефон 4', 'Полное имя', 'Дата рождения']:
                # Создаем пустой столбец
                result_df[col_name] = ''

                # Заполняем данными где они есть
                col_data = new_columns[col_name]
                for row_idx, value in col_data.items():
                    if row_idx in result_df.index:
                        result_df.at[row_idx, col_name] = value

            # Если нужно оставить только строки с данными
            if keep_only_with_data and rows_with_data:
                result_df = result_df.loc[result_df.index.isin(rows_with_data)]
                logger.info(f"📊 Оставлено {len(result_df)} строк с найденными данными")

            # Сохраняем результат
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False)

                # Автоподбор ширины столбцов
                worksheet = writer.sheets['Sheet1']
                for column in worksheet.columns:
                    max_length = 0
                    column = [cell for cell in column]
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

            logger.info(f"✅ Результаты сохранены в {output_path}")
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

        return {
            "total_rows": len(self.original_df),
            "total_columns": len(self.original_df.columns),
            "vk_column": self.vk_column_name,
            "vk_links_count": len(self.vk_links_mapping),
            "columns": list(self.original_df.columns)
        }