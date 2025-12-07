"""Функции для экспорта результатов в различные форматы"""
import json

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

from bot.config import EXPORT_DATE_FORMAT, EXPORT_COLUMN_WIDTHS
from bot.utils.messages import MESSAGES
from bot.utils.helpers import create_temp_dir
from services.excel_service import ExcelProcessor

logger = logging.getLogger("export")


def restore_processor_from_session(session: Optional[dict]) -> Optional[ExcelProcessor]:
    """
    Переинициализирует ExcelProcessor по данным из пользовательской сессии.
    Возвращает None, если исходный файл недоступен или испорчен.
    """
    if not session:
        return None

    temp_file = session.get("temp_file")
    if not temp_file:
        return None

    file_path = Path(temp_file)
    if not file_path.exists():
        logger.warning("Исходный файл %s недоступен", file_path)
        return None

    processor = ExcelProcessor()
    links, _, success = processor.load_excel_file(file_path)
    if not success or not links:
        logger.warning("Не удалось загрузить файл %s для повторного экспорта", file_path)
        return None

    mapping = session.get("vk_links_mapping")
    if isinstance(mapping, dict):
        normalized = {}
        for link, rows in mapping.items():
            if rows is None:
                continue
            if isinstance(rows, list):
                cleaned = []
                for row in rows:
                    try:
                        cleaned.append(int(row))
                    except (TypeError, ValueError):
                        continue
                if cleaned:
                    normalized[link] = cleaned
            else:
                try:
                    normalized[link] = [int(rows)]
                except (TypeError, ValueError):
                    continue
        if normalized:
            processor.vk_links_mapping = normalized

    return processor


async def create_excel_from_results(
        all_results: Dict[str, Dict[str, Any]],
        links_order: List[str]
) -> List[Tuple[Path, str]]:
    """
    Создает Excel файл из результатов поиска
    """
    temp_dir = create_temp_dir(prefix="export")
    ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
    path_result = temp_dir / f"vk_data_{ts}.xlsx"

    files_to_return = []

    try:
        # Получаем процессор из сессии для доступа к исходному файлу
        from bot.utils.session_manager import get_user_session
        import asyncio

        # Получаем ID пользователя из контекста
        # Это временное решение - нужно передавать user_id в функцию
        session = None
        processor = None

        # Если есть исходный файл, используем его структуру
        if hasattr(asyncio, '_current_task'):
            # Попытка получить данные из текущего контекста
            # В реальности нужно передавать processor в функцию
            pass

        # Подготавливаем данные для DataFrame
        data_for_df = []

        # Определяем максимальное количество телефонов (но выводим не более двух)
        max_phones = 0
        for result_data in all_results.values():
            phones = result_data.get("phones", [])
            if isinstance(phones, list):
                max_phones = max(max_phones, len(phones))
        max_phones = min(max_phones, 2) if max_phones else 2

        # Создаем данные для каждой ссылки
        for link in links_order:
            result_data = all_results.get(link, {})

            # Извлекаем телефоны
            phones = result_data.get("phones", [])
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

            phones = [str(p) for p in phones if p][:2]

            # Извлекаем ФИО и дату рождения
            full_name = result_data.get("full_name", "")
            birth_date = result_data.get("birth_date", "")

            # Создаем строку с ссылкой и телефонами
            row_data = {"Ссылка VK": link}
            row_data["ФИО"] = full_name
            row_data["Дата рождения"] = birth_date

            # Добавляем телефоны в отдельные столбцы
            for i in range(max_phones):
                col_name = f"Телефон{i + 1}"
                row_data[col_name] = phones[i] if i < len(phones) else ""

            data_for_df.append(row_data)

        # Создаем DataFrame
        df = pd.DataFrame(data_for_df)

        # Сохраняем в Excel
        with pd.ExcelWriter(path_result, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты')

            # Автоподбор ширины столбцов
            worksheet = writer.sheets['Результаты']
            for column in worksheet.columns:
                column_letter = column[0].column_letter
                column_title = str(column[0].value)

                if column_title == "Ссылка VK":
                    worksheet.column_dimensions[column_letter].width = 50
                elif column_title == "ФИО":
                    worksheet.column_dimensions[column_letter].width = 30
                elif column_title == "Дата рождения":
                    worksheet.column_dimensions[column_letter].width = 15
                elif column_title.startswith("Телефон"):
                    worksheet.column_dimensions[column_letter].width = 15

        logger.info(f"✅ Сохранен файл с данными: {path_result}")

        # Подсчет статистики
        found_count = sum(1 for data in all_results.values() if data.get("phones"))
        not_found_count = len(links_order) - found_count

        caption = MESSAGES["file_ready"].format(
            total=len(links_order),
            found=found_count,
            not_found=not_found_count
        )

        files_to_return.append((path_result, caption))

    except Exception as e:
        logger.error(f"Ошибка при создании Excel файла: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return files_to_return


async def prepare_result_files(
        processor,
        results: Dict[str, Dict[str, Any]],
        links_order: List[str]
) -> List[Tuple[Path, str]]:
    """
    Создает список файлов с результатами, используя оригинальный Excel, если он доступен.
    """
    # Стараемся сохранить оригинальную структуру файла, если загруженный Excel доступен
    try:
        if processor and getattr(processor, "original_df", None) is not None:
            temp_dir = create_temp_dir(prefix="export")
            ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
            output_path = temp_dir / f"vk_data_complete_{ts}.xlsx"

            success = processor.save_results_with_original_data(results, output_path)
            if success:
                found_count = sum(1 for data in results.values() if data.get("phones"))
                not_found_count = len(links_order) - found_count

                caption = (
                    "📊 Файл с результатами готов!\n\n"
                    f"✅ Обработано: {len(links_order)} ссылок\n"
                    f"📱 Найдены телефоны: {found_count}\n"
                    f"❌ Без телефонов: {not_found_count}\n\n"
                    "💾 Все исходные данные сохранены!"
                )
                return [(output_path, caption)]
            else:
                logger.warning("Не удалось сохранить результаты с исходными данными, используем стандартный экспорт.")
    except Exception as exc:
        logger.error(f"Ошибка при формировании файла с исходными данными: {exc}")

    # Фолбэк: стандартный экспорт без исходных данных
    return await create_excel_from_results(results, links_order)


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

    temp_dir = create_temp_dir(prefix="export")
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
    temp_dir = create_temp_dir(prefix="export")
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


# В bot/utils/export.py добавьте новую функцию
async def create_excel_with_original_data(
        all_results: Dict[str, Dict[str, Any]],
        links_order: List[str],
        processor: Optional['ExcelProcessor'] = None
) -> List[Tuple[Path, str]]:
    """
    Создает Excel файл с исходными данными и добавленными телефонами
    """
    temp_dir = create_temp_dir(prefix="export")
    ts = datetime.now().strftime(EXPORT_DATE_FORMAT)
    files_to_return = []

    try:
        # Если есть процессор с исходным файлом
        if processor and processor.original_df is not None:
            path_result = temp_dir / f"vk_data_complete_{ts}.xlsx"

            # Используем метод процессора для сохранения с исходными данными
            success = processor.save_results_with_original_data(
                all_results,
                path_result
            )

            if success:
                # Подсчет статистики
                found_count = sum(1 for data in all_results.values() if data.get("phones"))
                not_found_count = len(links_order) - found_count

                caption = f"""📊 Файл с результатами готов!

✅ Обработано: {len(links_order)} ссылок
📱 Найдены телефоны: {found_count}
❌ Без телефонов: {not_found_count}

💾 Все исходные данные сохранены!"""

                files_to_return.append((path_result, caption))
        else:
            # Если нет исходного файла, используем старый метод
            return await create_excel_from_results(all_results, links_order)

    except Exception as e:
        logger.error(f"Ошибка при создании Excel файла: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return files_to_return
