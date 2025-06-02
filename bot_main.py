import asyncio
import datetime
import json
import logging
import os
import pathlib
import re
import tempfile
from collections import defaultdict, OrderedDict
from typing import Dict, Any, List, Set, Tuple, Optional

import pandas as pd
import redis.asyncio as redis
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (CallbackQuery, FSInputFile,
                           InlineKeyboardButton, InlineKeyboardMarkup)
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from aiogram.types.bot_command import BotCommand

from vk_worker import VKWorker, init_project_structure
from database import VKDatabase
from excel_processor import ExcelProcessor
from db_loader import DatabaseLoader

# ───────────────────────────  Настройка логов  ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bot_main")

# ───────────────────────────  Константы и пути  ────────────────────────────
DATA_DIR = pathlib.Path("data")
PENDING_FILE = DATA_DIR / "pending_links.json"
TEMP_RESULTS_FILE = DATA_DIR / "temp_results.json"

# Периодичность сохранения промежуточных результатов
SAVE_INTERVAL = 5  # Сохранять каждые N обработанных ссылок

# Регулярное выражение для поиска ссылок VK
VK_LINK_PATTERN = r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.]+)'

# Инициализация базы данных
db = VKDatabase()

# Redis для хранения сессий
redis_client: Optional[redis.Redis] = None

# ───────────────────────────  Загрузка конфигурации  ─────────────────────────
load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
ADMIN_IDS = [int(id) for id in os.environ.get("ADMIN_IDS", "").split(",") if id]

# ───────────────────────────  Современные сообщения  ─────────────────────────

MESSAGES = {
    "welcome": """
🚀 <b>Добро пожаловать в VK Data Assistant!</b>

Я помогу вам быстро найти контактные данные по ссылкам VK.

<b>Что я умею:</b>
📎 Обрабатывать Excel файлы со ссылками
🔗 Искать данные по отдельным ссылкам  
📊 Формировать удобные отчеты

<b>Как начать:</b>
- Отправьте мне <code>.xlsx</code> файл со ссылками
- Или просто вставьте ссылки в сообщение

Готов к работе! 💫
""",

    "disclaimer": """
⚠️ <b>ВАЖНОЕ ПРЕДУПРЕЖДЕНИЕ</b> ⚠️

Данный бот предназначен исключительно для <b>законного использования</b>.

<b>Используя этот бот, вы подтверждаете, что:</b>

✓ Будете соблюдать законодательство вашей страны
✓ Не будете использовать полученные данные в незаконных целях
✓ Берете на себя полную ответственность за использование бота
✓ Понимаете риски, связанные с использованием автоматизированных инструментов

<b>Администрация не несет ответственности за:</b>
- Незаконное использование бота
- Возможные блокировки аккаунтов
- Любые последствия использования полученных данных

<b>Продолжая использование, вы соглашаетесь с данными условиями.</b>

Подтвердите ваше согласие:
""",

    "help": """
📚 <b>Руководство пользователя</b>

<b>🔹 Поддерживаемые форматы:</b>
- Excel файлы (.xlsx) - ссылки в первом столбце
- Прямые ссылки VK в сообщениях
- Несколько ссылок в одном сообщении

<b>🔹 Доступные команды:</b>
/start - Начать работу
/help - Показать это руководство
/status - Проверить текущий прогресс
/export - Получить результаты
/stats - Статистика использования
/findphone - Поиск по телефону

<b>🔹 Полезные советы:</b>
- Бот автоматически сохраняет прогресс
- При лимите можно продолжить позже
- Результаты доступны в течение сессии
- База данных проверяет дубликаты по ссылкам и телефонам

Есть вопросы? Просто начните работу! 🎯
""",

    "no_session": "🤷 У вас пока нет активной сессии. Отправьте мне ссылки для начала работы!",

    "processing_status": """
⚡ <b>Обработка данных</b>

{progress_bar}
<b>Прогресс:</b> {processed}/{total} ({percent}%)

📊 <b>Статистика:</b>
✅ Найдено данных: {found}
⏳ В обработке: {pending}  
❌ Без результата: {not_found}

<i>Обновлено: {time}</i>
""",

    "processing_with_cache": """
⚡ <b>Обработка данных</b>

{progress_bar}
<b>Прогресс:</b> {processed}/{total} ({percent}%)

📊 <b>Статистика:</b>
✅ Найдено данных: {found}
💾 Из кеша: {from_cache}
🔍 Новых проверок: {new_checks}
❌ Без результата: {not_found}

<i>Обновлено: {time}</i>
""",

    "limit_reached": """
⚠️ <b>Достигнут лимит запросов</b>

Обработано: {processed} из {total} ссылок

Не волнуйтесь! Ваш прогресс сохранен. 
Вы можете продолжить в любое время.

💡 <i>Совет: подождите 5-10 минут перед продолжением</i>
""",

    "session_complete": """
🎉 <b>Обработка завершена!</b>

📊 <b>Итоговая статистика:</b>
- Всего обработано: {total}
- Найдено данных: {found} ✅
- Без результата: {not_found} ❌

Ваши результаты готовы к экспорту!
""",

    "file_ready": """
📦 <b>Файл с результатами</b>

📋 Обработано ссылок: {total}
✅ С данными: {found}
❌ Без данных: {not_found}

<i>Файл содержит все найденные данные в удобном формате</i>
""",

    "duplicate_analysis": """
📊 <b>Анализ файла завершен!</b>

📁 Файл: <code>{filename}</code>
🔍 Режим: <b>Автоматическое определение данных</b>

<b>Статистика VK ссылок:</b>
- Всего ссылок: {total}
✅ Новых: {new_count}
🔄 Уже проверенных: {duplicate_count}
  └ С данными: {with_data_count}
  └ Без данных: {no_data_count}

<b>Что делать с дубликатами?</b>
""",

    "phone_duplicates_found": """
📱 <b>Найдены дубликаты по телефонам!</b>

{phone_duplicates}

Эти телефоны уже есть в базе данных.
""",

    "db_load_mode": """
🗄 <b>Режим загрузки базы данных</b>

Отправьте мне Excel файлы с уже проверенными данными.
Можно отправить несколько файлов одним сообщением.

<b>Формат файла должен содержать колонки:</b>
- Ссылка VK
- Телефоны (или отдельные колонки Телефон 1, 2, 3, 4)
- Полное имя (опционально)
- Дата рождения (опционально)

<i>Ожидаю файлы для загрузки...</i>
""",

    "file_action_prompt": """
📁 <b>Файл загружен!</b>

Файл: <code>{filename}</code>
Размер: {size} строк

<b>Что вы хотите сделать?</b>
""",

    "analysis_in_progress": """
🔄 <b>Анализирую файл...</b>

✅ Чтение структуры файла
{vk_status} Поиск VK ссылок
{phone_status} Поиск телефонов
{network_status} Анализ связей
{duplicate_status} Проверка дубликатов

<i>Это может занять несколько секунд...</i>
""",

    "analysis_complete": """
📊 <b>Анализ файла завершен!</b>

📁 Файл: <code>{filename}</code>

<b>🔍 Найдено в файле:</b>
• VK ссылок: {vk_links} (уникальных)
• Телефонов: {phones} (уникальных)
• Строк с данными: {data_rows}

<b>🔗 Связи между данными:</b>
• Телефонов с несколькими VK: {phones_multiple_vk}
• VK с несколькими телефонами: {vk_multiple_phones}

<b>📋 Дубликаты в базе:</b>
• VK уже в базе: {duplicate_vk} ({duplicate_vk_with_data} с данными)
• Телефонов уже в базе: {duplicate_phones}

{recommendations}

<b>Что дальше?</b>
""",

    "analysis_details": """
📊 <b>Детальный анализ связей</b>

{details}

<i>Показаны первые 10 записей</i>
""",

    "recommendations": """
💡 <b>Рекомендации:</b>
{items}
""",

    "db_load_complete": """
✅ <b>Загрузка базы данных завершена!</b>

📊 <b>Статистика загрузки:</b>
- Файлов обработано: {files_count}
- Записей добавлено: {added}
- Записей обновлено: {updated}
- Ошибок: {errors}

💾 <b>Общая статистика БД:</b>
- Всего записей: {total_records}
- С данными: {with_data}
- Без данных: {without_data}
""",

    "user_stats": """
📊 <b>Ваша статистика</b>

👤 ID: <code>{user_id}</code>

📈 <b>Использование:</b>
- Проверено ссылок: {total_checked}
- Найдено данных: {found_data_count}
- Дней активности: {days_active}

🏆 <b>Эффективность:</b> {efficiency}%
"""
}


# ───────────────────────────  Inline клавиатуры  ─────────────────────────────

def disclaimer_kb() -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения условий использования"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Я согласен", callback_data="accept_disclaimer"),
                InlineKeyboardButton(text="❌ Отказаться", callback_data="reject_disclaimer")
            ]
        ]
    )


def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    """Главное меню бота"""
    keyboard = [
        [
            InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file"),
            InlineKeyboardButton(text="🔗 Отправить ссылки", callback_data="send_links")
        ],
        [
            InlineKeyboardButton(text="📊 Мои результаты", callback_data="my_results"),
            InlineKeyboardButton(text="📚 Помощь", callback_data="help")
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск по телефону", callback_data="search_phone"),
            InlineKeyboardButton(text="📈 Статистика", callback_data="user_stats")
        ]
    ]

    # Добавляем кнопку загрузки БД только для админов
    if user_id in ADMIN_IDS:
        keyboard.append([
            InlineKeyboardButton(text="🗄 Загрузить БД ВК", callback_data="load_database")
        ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def processing_menu_kb() -> InlineKeyboardMarkup:
    """Меню во время обработки"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏸ Пауза", callback_data="pause"),
                InlineKeyboardButton(text="📊 Статистика", callback_data="stats")
            ],
            [
                InlineKeyboardButton(text="🚫 Отменить", callback_data="cancel")
            ]
        ]
    )


def continue_kb() -> InlineKeyboardMarkup:
    """Кнопка продолжения после лимита"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Продолжить обработку", callback_data="continue")
            ],
            [
                InlineKeyboardButton(text="📊 Получить текущие результаты", callback_data="export_current"),
                InlineKeyboardButton(text="🚫 Отменить", callback_data="cancel")
            ]
        ]
    )


def finish_kb() -> InlineKeyboardMarkup:
    """Меню завершения обработки"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Скачать результаты", callback_data="download_results")
            ],
            [
                InlineKeyboardButton(text="➕ Добавить еще ссылки", callback_data="add_more"),
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="main_menu")
            ]
        ]
    )


def upload_file_menu_kb() -> InlineKeyboardMarkup:
    """Меню для работы с файлами"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📤 Обработать файл", callback_data="process_file"),
                InlineKeyboardButton(text="🔍 Анализ файла", callback_data="analyze_file")
            ],
            [
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
            ]
        ]
    )


def duplicate_actions_kb() -> InlineKeyboardMarkup:
    """Клавиатура для работы с дубликатами"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🗑 Удалить дубликаты", callback_data="remove_duplicates"),
                InlineKeyboardButton(text="📋 Оставить все", callback_data="keep_all")
            ],
            [
                InlineKeyboardButton(text="📊 Обновить данные дубликатов", callback_data="update_duplicates")
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_processing")
            ]
        ]
    )


def file_action_menu_kb() -> InlineKeyboardMarkup:
    """Меню действий с файлом"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔍 Анализировать", callback_data="analyze_only"),
                InlineKeyboardButton(text="📤 Обработать", callback_data="process_only")
            ],
            [
                InlineKeyboardButton(text="📊 Анализ + Обработка", callback_data="analyze_and_process")
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
            ]
        ]
    )


def analysis_results_kb() -> InlineKeyboardMarkup:
    """Меню после анализа файла"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📤 Обработать файл", callback_data="process_after_analysis"),
                InlineKeyboardButton(text="📋 Детали", callback_data="analysis_details")
            ],
            [
                InlineKeyboardButton(text="💾 Скачать отчет", callback_data="export_analysis"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_file")
            ]
        ]
    )


# ───────────────────────────  Хелперы для Redis  ────────────────────────────

async def init_redis():
    """Инициализация подключения к Redis"""
    global redis_client
    try:
        redis_client = await redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("✅ Redis подключен успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Redis: {e}")
        logger.warning("⚠️ Работаем без Redis (сессии в памяти)")
        redis_client = None


async def get_user_session(user_id: int) -> Dict[str, Any]:
    """Получение сессии пользователя из Redis или памяти"""
    session_key = f"session:{user_id}"

    if redis_client:
        try:
            session_data = await redis_client.get(session_key)
            if session_data:
                return json.loads(session_data)
        except Exception as e:
            logger.error(f"Ошибка чтения из Redis: {e}")

    return {}


async def save_user_session(user_id: int, session_data: Dict[str, Any]):
    """Сохранение сессии пользователя в Redis или память"""
    session_key = f"session:{user_id}"

    if redis_client:
        try:
            await redis_client.setex(
                session_key,
                86400,  # TTL 24 часа
                json.dumps(session_data, ensure_ascii=False)
            )
        except Exception as e:
            logger.error(f"Ошибка записи в Redis: {e}")


async def clear_user_session(user_id: int):
    """Очистка сессии пользователя"""
    session_key = f"session:{user_id}"

    if redis_client:
        try:
            await redis_client.delete(session_key)
        except Exception as e:
            logger.error(f"Ошибка удаления из Redis: {e}")


async def check_user_accepted_disclaimer(user_id: int) -> bool:
    """Проверка, принял ли пользователь условия использования"""
    if redis_client:
        try:
            accepted = await redis_client.get(f"disclaimer:{user_id}")
            return accepted == "1"
        except:
            pass
    return db.check_user_accepted_disclaimer(user_id)


async def set_user_accepted_disclaimer(user_id: int):
    """Отметка о принятии условий использования"""
    if redis_client:
        try:
            await redis_client.setex(f"disclaimer:{user_id}", 2592000, "1")  # 30 дней
        except:
            pass
    db.set_user_accepted_disclaimer(user_id)


def db_load_menu_kb() -> InlineKeyboardMarkup:
    """Меню режима загрузки БД"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="❌ Отменить загрузку", callback_data="cancel_db_load")
            ]
        ]
    )


# ───────────────────────────  Хелперы  ────────────────────────────────────

def create_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Анализ файла внутри бота"""
    from db_loader import DatabaseLoader
    loader = DatabaseLoader(db)

    # Базовый анализ структуры
    analysis = loader.analyze_excel_structure(file_path)

    # Полная обработка для получения данных
    records, stats = loader.process_excel_file(file_path)

    # Анализ связей
    network = loader.find_all_related_data(records)

    # Извлекаем VK ссылки и телефоны для проверки дубликатов
    all_vk_links = [r['link'] for r in records if not r['link'].startswith('phone:')]
    all_phones = set()
    for r in records:
        all_phones.update(r.get('phones', []))

    # Проверка дубликатов
    duplicate_vk = db.check_duplicates(all_vk_links) if all_vk_links else {"new": [], "duplicates_with_data": {},
                                                                           "duplicates_no_data": []}
    duplicate_phones = db.check_phone_duplicates(list(all_phones)) if all_phones else {}

    # Генерация рекомендаций
    recommendations = generate_analysis_recommendations(stats, network, duplicate_vk, duplicate_phones)

    return {
        "basic": analysis,
        "stats": stats,
        "network": network,
        "records": records,
        "duplicates": {
            "vk": duplicate_vk,
            "phones": duplicate_phones
        },
        "recommendations": recommendations
    }


def generate_analysis_recommendations(stats: Dict, network: Dict, duplicate_vk: Dict, duplicate_phones: Dict) -> List[
    str]:
    """Генерирует рекомендации на основе анализа"""
    recommendations = []

    # Рекомендации по дубликатам
    total_vk = len(duplicate_vk.get("new", [])) + len(duplicate_vk.get("duplicates_with_data", {})) + len(
        duplicate_vk.get("duplicates_no_data", []))
    if total_vk > 0:
        duplicate_percent = ((len(duplicate_vk.get("duplicates_with_data", {})) + len(
            duplicate_vk.get("duplicates_no_data", []))) / total_vk) * 100
        if duplicate_percent > 50:
            recommendations.append(f"🔄 {int(duplicate_percent)}% ссылок уже в базе - рекомендую удалить дубликаты")

    # Рекомендации по телефонам
    if network['stats']['phones_with_multiple_vk'] > 5:
        recommendations.append(
            f"📱 Найдено {network['stats']['phones_with_multiple_vk']} телефонов с несколькими VK - возможны связанные аккаунты")

    # Рекомендации по качеству данных
    if stats.get('unique_phones', 0) > stats.get('unique_vk_links', 0):
        recommendations.append("☎️ Телефонов больше чем VK ссылок - можно найти дополнительные профили")

    # Рекомендации по обработке
    if len(duplicate_phones) > 10:
        recommendations.append(f"🔍 {len(duplicate_phones)} телефонов уже в базе - проверьте связанные профили")

    if not recommendations:
        recommendations.append("✅ Файл готов к обработке")

    return recommendations


async def format_analysis_message(analysis: Dict) -> str:
    """Форматирование результатов анализа для Telegram"""
    stats = analysis['stats']
    network = analysis['network']['stats']
    duplicates = analysis['duplicates']

    # Подсчет дубликатов
    duplicate_vk_count = len(duplicates['vk'].get('duplicates_with_data', {})) + len(
        duplicates['vk'].get('duplicates_no_data', []))
    duplicate_vk_with_data = len(duplicates['vk'].get('duplicates_with_data', {}))
    duplicate_phones_count = len(duplicates['phones'])

    # Форматирование рекомендаций
    recommendations_text = ""
    if analysis['recommendations']:
        recommendations_text = MESSAGES["recommendations"].format(
            items="\n".join(f"• {rec}" for rec in analysis['recommendations'])
        )

    return MESSAGES["analysis_complete"].format(
        filename=analysis['basic']['file_name'],
        vk_links=stats.get('unique_vk_links', 0),
        phones=stats.get('unique_phones', 0),
        data_rows=stats.get('rows_with_vk_links', 0) + stats.get('rows_with_phones', 0),
        phones_multiple_vk=network.get('phones_with_multiple_vk', 0),
        vk_multiple_phones=network.get('vk_with_multiple_phones', 0),
        duplicate_vk=duplicate_vk_count,
        duplicate_vk_with_data=duplicate_vk_with_data,
        duplicate_phones=duplicate_phones_count,
        recommendations=recommendations_text
    )


async def format_analysis_details(analysis: Dict) -> str:
    """Форматирование детального анализа"""
    network = analysis['network']
    details = []

    # Показываем телефоны с несколькими VK
    if network['stats']['phones_with_multiple_vk'] > 0:
        details.append("<b>📱 Телефоны с несколькими VK профилями:</b>")
        count = 0
        for phone, data in network['phone_network'].items():
            if len(data['vk_links']) > 1:
                details.append(f"\n☎️ <code>{phone}</code> ({len(data['vk_links'])} профилей)")
                for vk in data['vk_links'][:3]:
                    details.append(f"  └ {vk}")
                if len(data['vk_links']) > 3:
                    details.append(f"  └ ... и еще {len(data['vk_links']) - 3}")
                count += 1
                if count >= 5:
                    details.append("\n... и другие")
                    break
        details.append("")

    # Показываем VK с несколькими телефонами
    if network['stats']['vk_with_multiple_phones'] > 0:
        details.append("<b>🔗 VK профили с несколькими телефонами:</b>")
        count = 0
        for vk, data in network['vk_network'].items():
            if len(data['phones']) > 1 and not vk.startswith('phone:'):
                details.append(f"\n👤 {vk}")
                details.append(f"  📱 Телефонов: {len(data['phones'])}")
                count += 1
                if count >= 5:
                    details.append("\n... и другие")
                    break

    if not details:
        details.append("📊 Нет сложных связей между данными")

    return MESSAGES["analysis_details"].format(details="\n".join(details))


async def export_analysis_json(analysis: Dict, chat_id: int, bot: Bot) -> bool:
    """Экспорт полного отчета анализа в JSON"""
    try:
        # Создаем временный файл
        temp_dir = pathlib.Path(tempfile.mkdtemp())
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = temp_dir / f"analysis_report_{timestamp}.json"

        # Подготавливаем данные для экспорта
        export_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "file_info": analysis['basic'],
            "statistics": analysis['stats'],
            "network_analysis": {
                "stats": analysis['network']['stats'],
                "phones_with_multiple_vk": {
                    phone: data
                    for phone, data in list(analysis['network']['phone_network'].items())[:20]
                    if len(data['vk_links']) > 1
                },
                "vk_with_multiple_phones": {
                    vk: {"phones": data['phones'], "related_count": len(data['related_vk_links'])}
                    for vk, data in list(analysis['network']['vk_network'].items())[:20]
                    if len(data['phones']) > 1 and not vk.startswith('phone:')
                }
            },
            "duplicates": {
                "vk_summary": {
                    "new": len(analysis['duplicates']['vk'].get('new', [])),
                    "with_data": len(analysis['duplicates']['vk'].get('duplicates_with_data', {})),
                    "without_data": len(analysis['duplicates']['vk'].get('duplicates_no_data', []))
                },
                "phones_count": len(analysis['duplicates']['phones'])
            },
            "recommendations": analysis['recommendations']
        }

        # Сохраняем в JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)

        # Отправляем файл
        await bot.send_document(
            chat_id,
            FSInputFile(json_path),
            caption="📊 Полный отчет анализа файла"
        )

        return True

    except Exception as e:
        logger.error(f"Ошибка при экспорте анализа: {e}")
        return False


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
    return datetime.datetime.now().strftime("%H:%M:%S")


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


async def create_excel_from_results(all_results: Dict[str, Dict[str, Any]], links_order: List[str]):
    """Создает Excel файл из результатов поиска"""
    temp_dir = pathlib.Path(tempfile.mkdtemp())
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path_result = temp_dir / f"vk_data_{ts}.xlsx"

    files_to_return = []

    try:
        rows = []

        for link in links_order:
            data = all_results.get(link, {})

            # Создаем строку с правильным порядком полей
            row = OrderedDict()
            row["link"] = link

            # Добавляем телефоны
            phones = data.get("phones", [])
            for i in range(4):
                if i < len(phones):
                    row[f"phone_{i + 1}"] = phones[i]
                else:
                    row[f"phone_{i + 1}"] = ""

            # Добавляем имя и дату рождения
            row["full_name"] = data.get("full_name", "")
            row["birth_date"] = data.get("birth_date", "")

            rows.append(row)

        # Создаем DataFrame с явным указанием столбцов
        columns = ["link", "phone_1", "phone_2", "phone_3", "phone_4", "full_name", "birth_date"]

        # Создаем пустой DataFrame с нужными столбцами
        df = pd.DataFrame(columns=columns)

        # Добавляем строки
        for row in rows:
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

        # Переименовываем столбцы на русские названия
        df.columns = ["Ссылка VK", "Телефон 1", "Телефон 2", "Телефон 3", "Телефон 4", "Полное имя", "Дата рождения"]

        # Сохраняем в Excel
        with pd.ExcelWriter(path_result, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты')

            # Автоподбор ширины столбцов
            worksheet = writer.sheets['Результаты']
            for column in worksheet.columns:
                max_length = 0
                column_cells = list(column)
                for cell in column_cells:
                    try:
                        if cell.value and len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                if adjusted_width > 0:
                    worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width

        logger.info(f"Сохранен файл с данными: {path_result}")

        found_count = sum(1 for link in links_order if all_results.get(link, {}).get("phones"))
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


async def setup_bot_commands(bot: Bot):
    """Настройка меню команд бота"""
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="📚 Руководство пользователя"),
        BotCommand(command="status", description="📊 Текущий прогресс"),
        BotCommand(command="export", description="📥 Получить результаты"),
        BotCommand(command="stats", description="📈 Моя статистика"),
        BotCommand(command="findphone", description="🔍 Поиск по телефону"),
        BotCommand(command="cancel", description="🚫 Отменить обработку"),
    ]
    await bot.set_my_commands(commands)
    logger.info("✅ Команды бота настроены")


# ───────────────────────────  Инициализация бота  ────────────────────────────

from aiogram.client.default import DefaultBotProperties

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
router = Router()
dp.include_router(router)


# ───────────────────────────  Обработчики команд  ────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: types.Message):
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    await msg.answer(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id))


@router.message(Command("help"))
async def cmd_help(msg: types.Message):
    await msg.answer(MESSAGES["help"], reply_markup=back_to_menu_kb())


@router.message(Command("stats"))
async def cmd_user_stats(msg: types.Message):
    user_id = msg.from_user.id
    stats = db.get_user_statistics(user_id)

    efficiency = 0
    if stats["total_checked"] > 0:
        efficiency = int((stats["found_data_count"] / stats["total_checked"]) * 100)

    stats_text = MESSAGES["user_stats"].format(
        user_id=user_id,
        total_checked=stats["total_checked"],
        found_data_count=stats["found_data_count"],
        days_active=stats["days_active"],
        efficiency=efficiency
    )

    await msg.answer(stats_text, reply_markup=back_to_menu_kb())


@router.message(Command("status"))
async def cmd_status(msg: types.Message):
    user_id = msg.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("links"):
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    total = len(session["links"])
    processed = len(session.get("results", {}))
    found = sum(1 for data in session.get("results", {}).values() if data.get("phones"))
    not_found = processed - found
    pending = total - processed

    progress_bar = create_progress_bar(processed, total)
    percent = int((processed / total) * 100) if total > 0 else 0

    status_text = MESSAGES["processing_status"].format(
        progress_bar=progress_bar,
        processed=processed,
        total=total,
        percent=percent,
        found=found,
        pending=pending,
        not_found=not_found,
        time=format_time()
    )

    await msg.answer(status_text, reply_markup=processing_menu_kb() if pending > 0 else finish_kb())


@router.message(Command("export"))
async def cmd_export(msg: types.Message):
    user_id = msg.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    all_results = session.get("results", {})
    links_order = session.get("links_order", [])

    if not links_order:
        await msg.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    # Генерируем файл с результатами
    files = await create_excel_from_results(all_results, links_order)

    for file_path, caption in files:
        try:
            await bot.send_document(
                msg.chat.id,
                FSInputFile(file_path),
                caption=caption
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            await msg.answer(f"⚠️ Не удалось отправить файл: {str(e)}")

    await msg.answer("Готово! Выберите дальнейшее действие:", reply_markup=finish_kb())


@router.message(Command("cancel"))
async def cmd_cancel(msg: types.Message):
    user_id = msg.from_user.id

    await clear_user_session(user_id)
    await msg.answer("🚫 Обработка отменена. Все данные очищены.", reply_markup=main_menu_kb(user_id))


@router.message(Command("findphone"))
async def cmd_find_phone(msg: types.Message):
    """Поиск по номеру телефона"""
    # Извлекаем номер из команды
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer(
            "🔍 <b>Поиск по телефону</b>\n\n"
            "Использование:\n"
            "<code>/findphone 79001234567</code>\n\n"
            "Введите 11-значный номер телефона, начинающийся с 7",
            reply_markup=back_to_menu_kb()
        )
        return

    # Очищаем номер от всех символов кроме цифр
    phone = re.sub(r'[^\d]', '', parts[1])

    # Валидация номера
    if len(phone) != 11 or not phone.startswith('7'):
        await msg.answer(
            "❌ Неверный формат номера\n\n"
            "Номер должен состоять из 11 цифр и начинаться с 7\n"
            "Пример: <code>79001234567</code>",
            reply_markup=back_to_menu_kb()
        )
        return

    # Поиск в базе
    results = db.find_links_by_phone(phone)

    if not results:
        await msg.answer(
            f"❌ Номер <code>{phone}</code> не найден в базе данных",
            reply_markup=back_to_menu_kb()
        )
        return

    # Формируем ответ
    response = f"📱 <b>Результаты поиска для номера {phone}:</b>\n\n"
    response += f"Найдено профилей: {len(results)}\n\n"

    for i, result in enumerate(results[:10], 1):  # Показываем максимум 10
        response += f"{i}. <a href='{result['link']}'>{result['link']}</a>\n"
        if result['full_name']:
            response += f"   👤 {result['full_name']}\n"
        if result['birth_date']:
            response += f"   🎂 {result['birth_date']}\n"

        # Показываем все телефоны профиля
        other_phones = [p for p in result['phones'] if p != phone]
        if other_phones:
            response += f"   📞 Другие телефоны: {', '.join(other_phones)}\n"

        response += "\n"

    if len(results) > 10:
        response += f"... и еще {len(results) - 10} профилей"

    await msg.answer(response, reply_markup=back_to_menu_kb(), disable_web_page_preview=True)


# ───────────────────────────  Обработчики callback  ────────────────────────────

@router.callback_query(F.data == "accept_disclaimer")
async def on_accept_disclaimer(call: CallbackQuery):
    await call.answer("✅ Условия приняты")
    user_id = call.from_user.id

    await set_user_accepted_disclaimer(user_id)
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id))


@router.callback_query(F.data == "reject_disclaimer")
async def on_reject_disclaimer(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text(
        "❌ Вы отказались от условий использования.\n\n"
        "К сожалению, без согласия с условиями вы не можете использовать бота.\n\n"
        "Если передумаете - используйте команду /start"
    )


@router.callback_query(F.data == "main_menu")
async def on_main_menu(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id))


@router.callback_query(F.data == "help")
async def on_help(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text(MESSAGES["help"], reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "user_stats")
async def on_user_stats(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id
    stats = db.get_user_statistics(user_id)

    efficiency = 0
    if stats["total_checked"] > 0:
        efficiency = int((stats["found_data_count"] / stats["total_checked"]) * 100)

    stats_text = MESSAGES["user_stats"].format(
        user_id=user_id,
        total_checked=stats["total_checked"],
        found_data_count=stats["found_data_count"],
        days_active=stats["days_active"],
        efficiency=efficiency
    )

    await call.message.edit_text(stats_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "upload_file")
async def on_upload_file(call: CallbackQuery):
    await call.answer()
    upload_text = """
📤 <b>Загрузка файла</b>

Отправьте мне Excel файл (.xlsx) со ссылками VK.

<b>Поддерживается:</b>
- Автоматический поиск VK ссылок
- Автоматический поиск телефонов
- Любая структура файла
- Файлы до 50 МБ

<i>Ожидаю ваш файл...</i>
"""
    await call.message.edit_text(upload_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "send_links")
async def on_send_links(call: CallbackQuery):
    await call.answer()
    links_text = """
🔗 <b>Отправка ссылок</b>

Вы можете отправить мне ссылки VK прямо в сообщении.

<b>Примеры:</b>
- https://vk.com/id123456
- https://vk.com/username
- Несколько ссылок через пробел или с новой строки

<i>Жду ваши ссылки...</i>
"""
    await call.message.edit_text(links_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "load_database")
async def on_load_database(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id

    # Проверка прав администратора
    if user_id not in ADMIN_IDS:
        await call.answer("⛔ У вас нет прав для этой операции", show_alert=True)
        return

    # Устанавливаем режим загрузки БД
    session = {"db_load_mode": True}
    await save_user_session(user_id, session)

    await call.message.edit_text(MESSAGES["db_load_mode"], reply_markup=db_load_menu_kb())


@router.callback_query(F.data == "cancel_db_load")
async def on_cancel_db_load(call: CallbackQuery):
    await call.answer("❌ Загрузка отменена")
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(MESSAGES["welcome"], reply_markup=main_menu_kb(user_id))


@router.callback_query(F.data == "search_phone")
async def on_search_phone(call: CallbackQuery):
    await call.answer()
    search_text = """
🔍 <b>Поиск по телефону</b>

Отправьте номер телефона для поиска в базе.

<b>Формат:</b>
- 11 цифр, начиная с 7
- Можно с разделителями или без

<b>Примеры:</b>
<code>79001234567</code>
<code>7 900 123-45-67</code>
<code>+7(900)123-45-67</code>

<i>Отправьте номер телефона...</i>
"""
    await call.message.edit_text(search_text, reply_markup=back_to_menu_kb())

    # Устанавливаем режим ожидания телефона
    session = {"waiting_phone": True}
    await save_user_session(call.from_user.id, session)


@router.callback_query(F.data == "my_results")
async def on_my_results(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get("results"):
        await call.message.edit_text(
            "📭 У вас пока нет сохраненных результатов.\n\n"
            "Начните с загрузки файла или отправки ссылок!",
            reply_markup=main_menu_kb(user_id)
        )
        return

    # Перенаправляем на команду статуса
    await cmd_status(call.message)


@router.callback_query(F.data == "stats")
async def on_stats(call: CallbackQuery):
    await call.answer("📊 Обновляю статистику...")
    # Обновляем сообщение со статистикой
    user_id = call.from_user.id
    session = await get_user_session(user_id)
    if session:
        # Имитируем сообщение для вызова cmd_status
        await cmd_status(call.message)


@router.callback_query(F.data == "pause")
async def on_pause(call: CallbackQuery):
    await call.answer("⏸ Обработка приостановлена")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if session:
        session["paused"] = True
        await save_user_session(user_id, session)

    pause_text = """
⏸ <b>Обработка приостановлена</b>

Ваш прогресс сохранен. Вы можете:
- Продолжить обработку в любое время
- Скачать текущие результаты
- Отменить и начать заново
"""
    await call.message.edit_text(pause_text, reply_markup=continue_kb())


@router.callback_query(F.data == "cancel")
async def on_cancel_button(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(
        "🚫 Обработка отменена. Все данные очищены.",
        reply_markup=main_menu_kb(user_id)
    )


@router.callback_query(F.data == "download_results")
async def on_download_results(call: CallbackQuery):
    await call.answer("📥 Подготавливаю файл...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    all_results = session.get("results", {})
    links_order = session.get("links_order", [])

    if not links_order:
        await call.message.answer(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    # Генерируем файл
    files = await create_excel_from_results(all_results, links_order)

    for file_path, caption in files:
        try:
            await bot.send_document(
                call.message.chat.id,
                FSInputFile(file_path),
                caption=caption
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            await call.message.answer(f"⚠️ Не удалось отправить файл: {str(e)}")


@router.callback_query(F.data == "add_more")
async def on_add_more(call: CallbackQuery):
    await call.answer()
    add_more_text = """
➕ <b>Добавление ссылок</b>

Вы можете добавить еще ссылки к текущей сессии.

Отправьте мне:
- Новый Excel файл
- Или ссылки в сообщении

<i>Все новые результаты будут добавлены к существующим.</i>
"""
    await call.message.edit_text(add_more_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "export_current")
async def on_export_current(call: CallbackQuery):
    await call.answer("📊 Экспортирую текущие результаты...")
    # Вызываем экспорт
    await cmd_export(call.message)


@router.callback_query(F.data == "continue")
async def on_continue(call: CallbackQuery):
    await call.answer("▶️ Продолжаю обработку...")

    # TODO: Реализовать продолжение обработки из Redis
    await call.message.edit_text(
        "⚠️ Функция в разработке",
        reply_markup=back_to_menu_kb()
    )


# ───────────────────────────  Обработчики callback для анализа  ────────────────────────────

@router.callback_query(F.data == "analyze_only")
async def on_analyze_only(call: CallbackQuery):
    await call.answer("🔍 Начинаю анализ...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('temp_file'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id))
        return

    # Обновляем статус анализа
    progress_msg = await call.message.edit_text(
        MESSAGES["analysis_in_progress"].format(
            vk_status="🔄",
            phone_status="⏳",
            network_status="⏳",
            duplicate_status="⏳"
        )
    )

    try:
        # Выполняем анализ
        file_path = pathlib.Path(session['temp_file'])

        # Шаг 1: Поиск VK
        await progress_msg.edit_text(
            MESSAGES["analysis_in_progress"].format(
                vk_status="✅",
                phone_status="🔄",
                network_status="⏳",
                duplicate_status="⏳"
            )
        )

        analysis = await analyze_file_inline(file_path, db)

        # Шаг 2: Поиск телефонов
        await progress_msg.edit_text(
            MESSAGES["analysis_in_progress"].format(
                vk_status="✅",
                phone_status="✅",
                network_status="🔄",
                duplicate_status="⏳"
            )
        )

        # Шаг 3: Анализ связей
        await progress_msg.edit_text(
            MESSAGES["analysis_in_progress"].format(
                vk_status="✅",
                phone_status="✅",
                network_status="✅",
                duplicate_status="🔄"
            )
        )

        # Сохраняем результаты анализа в сессию
        session['analysis_result'] = analysis
        session['file_mode'] = 'analyzed'
        await save_user_session(user_id, session)

        # Показываем результаты
        result_text = await format_analysis_message(analysis)
        await progress_msg.edit_text(result_text, reply_markup=analysis_results_kb())

    except Exception as e:
        logger.error(f"Ошибка при анализе файла: {e}")
        await progress_msg.edit_text(
            f"❌ Ошибка при анализе файла: {str(e)}",
            reply_markup=back_to_menu_kb()
        )


@router.callback_query(F.data == "process_only")
async def on_process_only(call: CallbackQuery):
    await call.answer("📤 Начинаю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('temp_file'):
        await call.message.edit_text("❌ Файл не найден", reply_markup=main_menu_kb(user_id))
        return

    # Перенаправляем на существующую логику обработки
    file_path = pathlib.Path(session['temp_file'])

    # Используем ExcelProcessor для совместимости
    processor = ExcelProcessor()
    links, row_indices, success = processor.load_excel_file(file_path)

    if not success or not links:
        await call.message.edit_text(
            "❌ Не удалось найти VK ссылки в файле",
            reply_markup=main_menu_kb(user_id)
        )
        return

    # Запускаем обработку
    await call.message.edit_text(f"📤 Начинаю обработку {len(links)} ссылок...")

    # Проверяем дубликаты
    duplicate_check = db.check_duplicates(links)

    # Запускаем обработку
    await start_processing(call.message, links, processor, duplicate_check, user_id)


@router.callback_query(F.data == "analyze_and_process")
async def on_analyze_and_process(call: CallbackQuery):
    await call.answer("📊 Анализ и обработка...")

    # Сначала выполняем анализ
    await on_analyze_only(call)

    # Затем автоматически запускаем обработку
    # (обработка запустится через callback process_after_analysis)


@router.callback_query(F.data == "process_after_analysis")
async def on_process_after_analysis(call: CallbackQuery):
    await call.answer("📤 Начинаю обработку...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.edit_text("❌ Результаты анализа не найдены", reply_markup=main_menu_kb(user_id))
        return

    analysis = session['analysis_result']
    file_path = pathlib.Path(session['temp_file'])

    # Извлекаем VK ссылки из анализа
    vk_links = [r['link'] for r in analysis['records'] if not r['link'].startswith('phone:')]

    if not vk_links:
        await call.message.edit_text(
            "❌ В файле не найдено VK ссылок для обработки",
            reply_markup=main_menu_kb(user_id)
        )
        return

    # Используем ExcelProcessor для совместимости
    processor = ExcelProcessor()
    processor.load_excel_file(file_path)  # Загружаем для сохранения результатов

    # Используем уже полученную информацию о дубликатах
    duplicate_check = analysis['duplicates']['vk']

    await call.message.edit_text(f"📤 Начинаю обработку {len(vk_links)} ссылок...")

    # Запускаем обработку
    await start_processing(call.message, vk_links, processor, duplicate_check, user_id)


@router.callback_query(F.data == "analysis_details")
async def on_analysis_details(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.answer("❌ Результаты анализа не найдены")
        return

    analysis = session['analysis_result']
    details_text = await format_analysis_details(analysis)

    # Отправляем новое сообщение с деталями
    await call.message.answer(details_text, reply_markup=back_to_menu_kb())


@router.callback_query(F.data == "export_analysis")
async def on_export_analysis(call: CallbackQuery):
    await call.answer("💾 Экспортирую отчет...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session or not session.get('analysis_result'):
        await call.message.answer("❌ Результаты анализа не найдены")
        return

    analysis = session['analysis_result']

    # Экспортируем в JSON
    success = await export_analysis_json(analysis, call.message.chat.id, bot)

    if success:
        await call.message.answer("✅ Отчет успешно экспортирован!")
    else:
        await call.message.answer("❌ Ошибка при экспорте отчета")


@router.callback_query(F.data == "cancel_file")
async def on_cancel_file(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id

    # Очищаем сессию
    await clear_user_session(user_id)

    await call.message.edit_text(
        "❌ Операция отменена",
        reply_markup=main_menu_kb(user_id)
    )


# ───────────────────────────  Обработчики дубликатов  ────────────────────────────

@router.callback_query(F.data == "remove_duplicates")
async def on_remove_duplicates(call: CallbackQuery):
    await call.answer("🗑 Удаляю дубликаты...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    duplicate_check = session["duplicate_check"]

    # Оставляем только новые ссылки
    links_to_process = duplicate_check["new"]

    if not links_to_process:
        await call.message.edit_text(
            "ℹ️ Все ссылки уже были проверены ранее.\n"
            "Нет новых ссылок для обработки.",
            reply_markup=main_menu_kb(user_id)
        )
        return

    await call.message.edit_text(
        f"✅ Дубликаты удалены!\n\n"
        f"Будет обработано: {len(links_to_process)} новых ссылок"
    )

    # Запускаем обработку только новых ссылок
    await start_processing(call.message, links_to_process, session["processor"], duplicate_check, user_id)


@router.callback_query(F.data == "keep_all")
async def on_keep_all(call: CallbackQuery):
    await call.answer("📋 Обрабатываю все ссылки...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    await call.message.edit_text(
        f"✅ Начинаю обработку всех {len(session['all_links'])} ссылок\n\n"
        f"<i>Данные из кеша будут использованы автоматически</i>"
    )

    # Запускаем обработку всех ссылок
    await start_processing(
        call.message,
        session["all_links"],
        session["processor"],
        session["duplicate_check"],
        user_id
    )


@router.callback_query(F.data == "update_duplicates")
async def on_update_duplicates(call: CallbackQuery):
    await call.answer("🔄 Обновляю данные...")
    user_id = call.from_user.id
    session = await get_user_session(user_id)

    if not session:
        await call.message.edit_text(MESSAGES["no_session"], reply_markup=main_menu_kb(user_id))
        return

    duplicate_check = session["duplicate_check"]

    # Будем перепроверять только дубликаты без данных
    links_to_update = duplicate_check["duplicates_no_data"]

    if not links_to_update:
        await call.message.edit_text(
            "ℹ️ Нет дубликатов для обновления.\n"
            "Все существующие дубликаты уже имеют данные.",
            reply_markup=main_menu_kb(user_id)
        )
        return

    await call.message.edit_text(
        f"🔄 Обновляю данные для {len(links_to_update)} ссылок без результатов"
    )

    # Запускаем обработку
    await start_processing(call.message, links_to_update, session["processor"], duplicate_check, user_id)


@router.callback_query(F.data == "cancel_processing")
async def on_cancel_processing(call: CallbackQuery):
    await call.answer()
    user_id = call.from_user.id

    await clear_user_session(user_id)
    await call.message.edit_text(
        "🚫 Обработка отменена.",
        reply_markup=main_menu_kb(user_id)
    )


# ───────────────────────────  Функции обработки  ────────────────────────────

async def start_processing(
        message: types.Message,
        links_to_process: List[str],
        processor: ExcelProcessor,
        duplicate_check: Dict,
        user_id: int
):
    """Запускает обработку ссылок с учетом кеша"""

    # Получаем закешированные результаты
    cached_results = db.get_cached_results(links_to_process)

    # Определяем, какие ссылки нужно проверить через VK
    links_to_check = [link for link in links_to_process if link not in cached_results]

    # Статус-сообщение
    total = len(links_to_process)
    from_cache = len(cached_results)
    to_check = len(links_to_check)

    progress_bar = create_progress_bar(from_cache, total)
    status_text = MESSAGES["processing_with_cache"].format(
        progress_bar=progress_bar,
        processed=from_cache,
        total=total,
        percent=int((from_cache / total) * 100) if total > 0 else 0,
        found=sum(1 for r in cached_results.values() if r.get("phones")),
        from_cache=from_cache,
        new_checks=0,
        not_found=0,
        time=format_time()
    )

    status = await message.answer(status_text, reply_markup=processing_menu_kb())

    # Начинаем с результатов из кеша
    all_results = dict(cached_results)

    # Если все результаты из кеша
    if not links_to_check:
        await finish_processing(message, all_results, processor, links_to_process, user_id)
        return

    # Создаем очередь для новых проверок
    queue: asyncio.Queue[str] = asyncio.Queue()
    for link in links_to_check:
        await queue.put(link)

    new_checks_count = 0
    last_status_text = ""

    async def result_cb(link: str, result_data: Dict[str, Any]):
        nonlocal new_checks_count, last_status_text

        # Сохраняем результат
        all_results[link] = result_data

        # Сохраняем в базу данных
        db.save_result(link, result_data, user_id)

        new_checks_count += 1
        processed = len(all_results)

        found_count = sum(1 for data in all_results.values() if data.get("phones"))
        not_found_count = processed - found_count

        progress_bar = create_progress_bar(processed, total)
        percent = int((processed / total) * 100)

        new_status_text = MESSAGES["processing_with_cache"].format(
            progress_bar=progress_bar,
            processed=processed,
            total=total,
            percent=percent,
            found=found_count,
            from_cache=from_cache,
            new_checks=new_checks_count,
            not_found=not_found_count,
            time=format_time()
        )

        if new_status_text != last_status_text:
            await safe_edit_message(status, new_status_text, reply_markup=processing_menu_kb())
            last_status_text = new_status_text

    async def limit_cb():
        # Сохраняем прогресс при достижении лимита
        session = await get_user_session(user_id)
        session["partial_results"] = all_results
        await save_user_session(user_id, session)

        limit_message = MESSAGES["limit_reached"].format(
            processed=len(all_results),
            total=total
        )

        await status.edit_text(limit_message, reply_markup=continue_kb())

    # Запускаем VK Worker
    worker = VKWorker(queue, result_cb, limit_cb)
    await worker.start()
    await queue.join()

    if not worker.limit_reached.is_set():
        # Обработка завершена успешно
        await finish_processing(message, all_results, processor, links_to_process, user_id)


async def finish_processing(
        message: types.Message,
        results: Dict[str, Dict],
        processor: ExcelProcessor,
        links_order: List[str],
        user_id: int
):
    """Завершает обработку и отправляет результаты"""

    # Генерируем имя файла
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = pathlib.Path(tempfile.mkdtemp()) / f"vk_data_results_{ts}.xlsx"

    # Сохраняем результаты в Excel с сохранением структуры
    success = processor.save_results_to_excel(results, output_path)

    if success:
        # Подсчитываем статистику
        found_count = sum(1 for data in results.values() if data.get("phones"))
        not_found_count = len(results) - found_count

        # Отправляем сообщение о завершении
        complete_text = MESSAGES["session_complete"].format(
            total=len(results),
            found=found_count,
            not_found=not_found_count
        )

        await message.answer(complete_text, reply_markup=finish_kb())

        # Отправляем файл
        caption = MESSAGES["file_ready"].format(
            total=len(results),
            found=found_count,
            not_found=not_found_count
        )

        try:
            await bot.send_document(
                message.chat.id,
                FSInputFile(output_path),
                caption=caption
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            await message.answer(f"⚠️ Не удалось отправить файл: {str(e)}")
    else:
        await message.answer(
            "❌ Произошла ошибка при создании файла с результатами.\n"
            "Попробуйте еще раз или обратитесь к администратору.",
            reply_markup=main_menu_kb(user_id)
        )

    # Очищаем сессию
    await clear_user_session(user_id)


# ───────────────────────────  Обработчики файлов и текста  ────────────────────────────

@router.message(F.document)
async def on_document(msg: types.Message):
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    # Проверяем сессию пользователя
    session = await get_user_session(user_id)

    # Если включен режим загрузки БД
    if session.get("db_load_mode"):
        # Проверка прав администратора
        if user_id not in ADMIN_IDS:
            await msg.answer("⛔ У вас нет прав для этой операции")
            return

        # Собираем все документы из сообщения
        documents = []
        if msg.document:
            documents.append(msg.document)

        # Если есть медиа-группа, собираем все документы
        if msg.media_group_id:
            # TODO: Обработка медиа-группы требует дополнительной логики
            pass

        # Обрабатываем документы
        loader = DatabaseLoader(db)
        total_stats = {
            "files_count": 0,
            "added": 0,
            "updated": 0,
            "errors": 0
        }

        status_msg = await msg.answer("🔄 Начинаю загрузку файлов в базу данных...")

        for doc in documents:
            if not doc.file_name.endswith('.xlsx'):
                continue

            try:
                # Скачиваем файл
                temp_dir = pathlib.Path(tempfile.mkdtemp())
                file_path = temp_dir / doc.file_name
                await bot.download(doc.file_id, destination=file_path)

                # Загружаем в БД
                stats = await loader.load_from_excel(file_path, user_id)

                # Анализируем связи (для информации)
                records, _ = loader.process_excel_file(file_path)
                network_data = loader.find_all_related_data(records)

                # Обновляем общую статистику
                total_stats["files_count"] += 1
                total_stats["added"] += stats["added"]
                total_stats["updated"] += stats["updated"]
                total_stats["errors"] += stats["errors"]

                # Обновляем статус с информацией о связях
                status_text = f"🔄 Обработано файлов: {total_stats['files_count']}\n"
                status_text += f"Добавлено: {total_stats['added']}, Обновлено: {total_stats['updated']}\n\n"
                status_text += f"📊 Найдено связей:\n"
                status_text += f"Телефонов с несколькими VK: {network_data['stats']['phones_with_multiple_vk']}\n"
                status_text += f"VK с несколькими телефонами: {network_data['stats']['vk_with_multiple_phones']}"

                await status_msg.edit_text(status_text)

            except Exception as e:
                logger.error(f"Ошибка при загрузке файла {doc.file_name}: {e}")
                total_stats["errors"] += 1

        # Получаем общую статистику БД
        db_stats = db.get_database_statistics()

        # Показываем итоговый результат
        complete_text = MESSAGES["db_load_complete"].format(
            files_count=total_stats["files_count"],
            added=total_stats["added"],
            updated=total_stats["updated"],
            errors=total_stats["errors"],
            total_records=db_stats["total_records"],
            with_data=db_stats["with_data"],
            without_data=db_stats["without_data"]
        )

        await status_msg.edit_text(complete_text, reply_markup=back_to_menu_kb())

        # Очищаем режим загрузки БД
        await clear_user_session(user_id)
        return

    # Обычная обработка Excel файла для поиска
    if msg.document.file_name.endswith(".xlsx"):
        await on_excel(msg)
    else:
        await msg.answer(
            "❌ Поддерживаются только Excel файлы (.xlsx)",
            reply_markup=main_menu_kb(user_id)
        )


async def on_excel(msg: types.Message):
    user_id = msg.from_user.id

    # Создаем временную папку и скачиваем файл
    temp_dir = pathlib.Path(tempfile.mkdtemp())
    path_in = temp_dir / msg.document.file_name
    await bot.download(msg.document.file_id, destination=path_in)

    # Сохраняем информацию о файле в сессию
    session = {
        'temp_file': str(path_in),
        'file_name': msg.document.file_name,
        'file_mode': 'pending'
    }
    await save_user_session(user_id, session)

    # Быстрая проверка размера файла
    try:
        df = pd.read_excel(path_in, nrows=1)
        total_rows = len(pd.read_excel(path_in))
    except:
        total_rows = "неизвестно"

    # Показываем меню действий
    prompt_text = MESSAGES["file_action_prompt"].format(
        filename=msg.document.file_name,
        size=total_rows
    )

    await msg.answer(prompt_text, reply_markup=file_action_menu_kb())


@router.message(F.text)
async def on_text_message(msg: types.Message):
    """Обработка текстовых сообщений с ссылками или телефонами"""
    user_id = msg.from_user.id

    # Проверяем, принял ли пользователь условия
    if not await check_user_accepted_disclaimer(user_id):
        await msg.answer(MESSAGES["disclaimer"], reply_markup=disclaimer_kb())
        return

    # Проверяем сессию
    session = await get_user_session(user_id)

    # Если ждем номер телефона
    if session.get("waiting_phone"):
        # Очищаем номер от всех символов кроме цифр
        phone = re.sub(r'[^\d]', '', msg.text)

        # Валидация номера
        if len(phone) == 11 and phone.startswith('7'):
            # Поиск в базе
            results = db.find_links_by_phone(phone)

            if not results:
                await msg.answer(
                    f"❌ Номер <code>{phone}</code> не найден в базе данных",
                    reply_markup=main_menu_kb(user_id)
                )
            else:
                # Формируем ответ
                response = f"📱 <b>Результаты поиска для номера {phone}:</b>\n\n"
                response += f"Найдено профилей: {len(results)}\n\n"

                for i, result in enumerate(results[:10], 1):  # Показываем максимум 10
                    response += f"{i}. <a href='{result['link']}'>{result['link']}</a>\n"
                    if result['full_name']:
                        response += f"   👤 {result['full_name']}\n"
                    if result['birth_date']:
                        response += f"   🎂 {result['birth_date']}\n"

                    # Показываем все телефоны профиля
                    other_phones = [p for p in result['phones'] if p != phone]
                    if other_phones:
                        response += f"   📞 Другие телефоны: {', '.join(other_phones)}\n"

                    response += "\n"

                if len(results) > 10:
                    response += f"... и еще {len(results) - 10} профилей"

                await msg.answer(response, reply_markup=main_menu_kb(user_id), disable_web_page_preview=True)
        else:
            await msg.answer(
                "❌ Неверный формат номера\n\n"
                "Номер должен состоять из 11 цифр и начинаться с 7\n"
                "Пример: <code>79001234567</code>",
                reply_markup=back_to_menu_kb()
            )

        # Очищаем режим ожидания
        await clear_user_session(user_id)
        return

    # Извлекаем ссылки
    links = extract_vk_links(msg.text)

    if not links:
        # Проверяем, не команда ли это из inline меню
        if msg.text in ["📤 Загрузить файл", "🔗 Отправить ссылки", "📊 Мои результаты", "📚 Помощь"]:
            await msg.answer("Пожалуйста, используйте кнопки меню ☝️", reply_markup=main_menu_kb(user_id))
        else:
            await msg.answer(
                "🔍 Не нашел ссылок VK в вашем сообщении.\n\n"
                "Отправьте ссылки в формате:\n"
                "<code>https://vk.com/id123456</code>",
                reply_markup=main_menu_kb(user_id)
            )
        return

    # TODO: Реализовать обработку текстовых ссылок
    await msg.answer(
        f"✅ Найдено {len(links)} ссылок\n\n"
        "⚠️ Функция обработки текстовых ссылок в разработке",
        reply_markup=main_menu_kb(user_id)
    )


# ───────────────────────────  Обработка ошибок  ─────────────────────────────

@dp.error()
async def error_handler(event: types.ErrorEvent):
    """Глобальный обработчик ошибок"""
    logger.error(f"Произошла ошибка: {event.exception}", exc_info=True)

    # Отправляем уведомление админам
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 Ошибка в боте:\n\n"
                f"<code>{str(event.exception)[:1000]}</code>"
            )
        except:
            pass


# ───────────────────────────  Запуск  ─────────────────────────────────────

async def on_startup():
    """Действия при запуске бота"""
    # Инициализация структуры проекта
    init_project_structure()

    # Выполняем миграцию БД если требуется
    logger.info("🔄 Проверка и миграция базы данных...")
    db.migrate_database()

    # Подключение к Redis
    await init_redis()

    # Настройка команд бота
    await setup_bot_commands(bot)

    # Уведомление админов о запуске
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                "✅ Бот запущен и готов к работе!"
            )
        except:
            pass

    logger.info("✅ Бот успешно запущен")


async def on_shutdown():
    """Действия при остановке бота"""
    # Закрываем соединение с Redis
    if redis_client:
        await redis_client.close()

    logger.info("👋 Бот остановлен")


async def main():
    # Регистрируем хендлеры жизненного цикла
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())