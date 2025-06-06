"""Функции для экспорта результатов в различные форматы"""

import pandas as pd
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple

from bot.config import EXPORT_DATE_FORMAT, EXPORT_COLUMN_WIDTHS
from bot.utils.messages import MESSAGES

logger = logging.getLogger("export")


async def create_excel_from_results(
        all_results: Dict[str, Dict[str, Any]],
        links_order: List[str]
) -> List[Tuple[Path, str]]:
    """
    Создает Excel файл из результатов поиска

    Args:
        all_results: Словарь с результатами {link: {phones, full_name, birth_date}}
        links_order: Порядок ссылок для сохранения

    Returns:
        Список кортежей (путь_к_файлу, описание)
    """
    temp_dir = Path(tempfile.mkdtemp())
    ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
    path_result = temp_dir / f"vk_data_{ts}.xlsx"

    files_to_return = []

    try:
        # Подготавливаем данные для DataFrame
        data_for_df = []

        for link in links_order:
            result_data = all_results.get(link, {})

            # Извлекаем данные
            phones = result_data.get("phones", [])
            full_name = result_data.get("full_name", "")
            birth_date = result_data.get("birth_date", "")

            # Создаем словарь для строки
            row_data = {
                "Ссылка VK": link,
                "Телефон 1": phones[0] if len(phones) > 0 else "",
                "Телефон 2": phones[1] if len(phones) > 1 else "",
                "Телефон 3": phones[2] if len(phones) > 2 else "",
                "Телефон 4": phones[3] if len(phones) > 3 else "",
                "Полное имя": full_name,
                "Дата рождения": birth_date
            }

            data_for_df.append(row_data)

        # Создаем DataFrame из списка словарей
        df = pd.DataFrame(data_for_df)

        # Если DataFrame пустой, создаем с правильными колонками
        if len(df) == 0:
            df = pd.DataFrame(columns=[
                "Ссылка VK", "Телефон 1", "Телефон 2", "Телефон 3",
                "Телефон 4", "Полное имя", "Дата рождения"
            ])

        # Сохраняем в Excel
        with pd.ExcelWriter(path_result, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты')

            # Автоподбор ширины столбцов
            worksheet = writer.sheets['Результаты']

            # Устанавливаем предопределенные ширины
            for column in worksheet.columns:
                column_letter = column[0].column_letter
                column_title = column[0].value

                if column_title in EXPORT_COLUMN_WIDTHS:
                    worksheet.column_dimensions[column_letter].width = EXPORT_COLUMN_WIDTHS[column_title]
                else:
                    # Автоподбор для остальных колонок
                    max_length = 0
                    for cell in column:
                        try:
                            if cell.value and len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except Exception:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    if adjusted_width > 0:
                        worksheet.column_dimensions[column_letter].width = adjusted_width

        logger.info(f"✅ Сохранен файл с данными: {path_result}")

        # Правильный подсчет статистики
        found_count = 0
        not_found_count = 0

        for link in links_order:
            data = all_results.get(link, {})
            # Проверяем есть ли хоть какие-то данные
            has_phones = bool(data.get("phones", []))
            has_name = bool(data.get("full_name", ""))
            has_birth = bool(data.get("birth_date", ""))

            if has_phones or has_name or has_birth:
                found_count += 1
            else:
                not_found_count += 1

        caption = MESSAGES["file_ready"].format(
            total=len(links_order),
            found=found_count,
            not_found=not_found_count
        )

        files_to_return.append((path_result, caption))

        # Если есть результаты, создаем также файл только с найденными данными
        if found_count > 0:
            path_found_only = temp_dir / f"vk_data_found_only_{ts}.xlsx"

            # Фильтруем только записи с данными
            found_data = []
            for link in links_order:
                data = all_results.get(link, {})
                if data.get("phones") or data.get("full_name") or data.get("birth_date"):
                    phones = data.get("phones", [])
                    row_data = {
                        "Ссылка VK": link,
                        "Телефон 1": phones[0] if len(phones) > 0 else "",
                        "Телефон 2": phones[1] if len(phones) > 1 else "",
                        "Телефон 3": phones[2] if len(phones) > 2 else "",
                        "Телефон 4": phones[3] if len(phones) > 3 else "",
                        "Полное имя": data.get("full_name", ""),
                        "Дата рождения": data.get("birth_date", "")
                    }
                    found_data.append(row_data)

            df_found = pd.DataFrame(found_data)

            # Сохраняем файл только с найденными
            with pd.ExcelWriter(path_found_only, engine='openpyxl') as writer:
                df_found.to_excel(writer, index=False, sheet_name='Найденные данные')

                # Форматирование
                worksheet = writer.sheets['Найденные данные']
                for column in worksheet.columns:
                    column_letter = column[0].column_letter
                    column_title = column[0].value

                    if column_title in EXPORT_COLUMN_WIDTHS:
                        worksheet.column_dimensions[column_letter].width = EXPORT_COLUMN_WIDTHS[column_title]

            caption_found = f"📋 Файл только с найденными данными ({found_count} записей)"
            files_to_return.append((path_found_only, caption_found))

    except Exception as e:
        logger.error(f"Ошибка при создании Excel файла: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return files_to_return


async def create_json_report(data: Dict[str, Any], filename_prefix: str = "report") -> Path:
    """
    Создает JSON отчет

    Args:
        data: Данные для сохранения
        filename_prefix: Префикс имени файла

    Returns:
        Path к созданному файлу
    """
    import json

    temp_dir = Path(tempfile.mkdtemp())
    ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
    json_path = temp_dir / f"{filename_prefix}_{ts}.json"

    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ Создан JSON отчет: {json_path}")
        return json_path

    except Exception as e:
        logger.error(f"Ошибка при создании JSON отчета: {e}")
        raise


async def export_statistics_report(stats: Dict[str, Any]) -> Path:
    """
    Экспортирует статистику в Excel

    Args:
        stats: Словарь со статистикой

    Returns:
        Path к файлу
    """
    temp_dir = Path(tempfile.mkdtemp())
    ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
    path = temp_dir / f"statistics_{ts}.xlsx"

    try:
        # Создаем несколько DataFrame для разных листов
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            # Общая статистика
            general_stats = pd.DataFrame([{
                "Метрика": "Всего проверено ссылок",
                "Значение": stats.get("total_checked", 0)
            }, {
                "Метрика": "Найдено данных",
                "Значение": stats.get("found_data_count", 0)
            }, {
                "Метрика": "Без данных",
                "Значение": stats.get("without_data_count", 0)
            }, {
                "Метрика": "Эффективность (%)",
                "Значение": stats.get("efficiency", 0)
            }])

            general_stats.to_excel(writer, sheet_name="Общая статистика", index=False)

            # Форматирование
            worksheet = writer.sheets["Общая статистика"]
            worksheet.column_dimensions['A'].width = 30
            worksheet.column_dimensions['B'].width = 15

        logger.info(f"✅ Экспортирована статистика: {path}")
        return path

    except Exception as e:
        logger.error(f"Ошибка при экспорте статистики: {e}")
        raise