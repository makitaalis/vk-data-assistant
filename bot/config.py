import os
import pathlib
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# ===== Telegram Bot Configuration =====
BOT_TOKEN = os.environ["BOT_TOKEN"]

# ===== VK Bot Configuration =====
VK_BOT_USERNAME = os.environ.get("VK_BOT_USERNAME", "vk_memosimo_3_bot")

# ===== Telethon Configuration =====
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE")

# ===== Database Configuration =====
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "vk_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# ===== Redis Configuration =====
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

# ===== Admin Configuration =====
ADMIN_IDS = [int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "").split(",") if admin_id]

# ===== Proxy Configuration =====
PROXY = os.environ.get("PROXY", None)

# ===== Paths =====
BASE_DIR = pathlib.Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DEBUG_DIR = BASE_DIR / "debug"
TEMP_DIR = DATA_DIR / "temp"

# Create directories if not exist
DATA_DIR.mkdir(exist_ok=True)
DEBUG_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

# ===== Files =====
PENDING_FILE = DATA_DIR / "pending_links.json"
TEMP_RESULTS_FILE = DATA_DIR / "temp_results.json"

# ===== OPTIMIZED Processing Configuration =====
# Оптимизированные параметры для быстрой работы с редактируемыми сообщениями
SAVE_INTERVAL = 10  # Сохранять каждые N обработанных ссылок
MAX_LINKS_PER_FILE = 5000  # Максимум ссылок в одном файле
MAX_LINKS_PER_MESSAGE = 100  # Максимум ссылок в одном сообщении

# ОПТИМИЗИРОВАННЫЕ ТАЙМИНГИ
MESSAGE_TIMEOUT = 5.0  # Уменьшен с 15 до 5 секунд - бот обычно отвечает за 2-4 сек
INITIAL_DELAY = 0.5   # Уменьшена с 2 до 0.5 сек - быстрая первая проверка
RETRY_DELAY = 0.3     # Уменьшена с 1 до 0.3 сек - быстрые повторы
MAX_RETRIES = 1       # Уменьшено с 3 до 1 - меньше повторов для скорости

# ===== Patterns =====
VK_LINK_PATTERN = r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.]+)'
PHONE_PATTERN = r'(?<!\d)7\d{10}(?!\d)'

# ===== Redis Keys =====
REDIS_SESSION_PREFIX = "session:"
REDIS_DISCLAIMER_PREFIX = "disclaimer:"
REDIS_SESSION_TTL = 86400  # 24 часа
REDIS_DISCLAIMER_TTL = 2592000  # 30 дней

# ===== Feature Flags =====
USE_REDIS = True  # Использовать Redis для сессий
ENABLE_DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

# ===== Rate Limiting =====
RATE_LIMIT_MESSAGES = 30  # Максимум сообщений в минуту от пользователя
RATE_LIMIT_WINDOW = 60  # Окно в секундах

# ===== Export Configuration =====
EXPORT_DATE_FORMAT = "%Y%m%d_%H%M%S"
EXPORT_COLUMN_WIDTHS = {
    "Ссылка VK": 50,
    "Телефон": 15,
    "Полное имя": 30,
    "Дата рождения": 15
}

# ===== PERFORMANCE OPTIMIZATION FLAGS =====
# Флаги для тонкой настройки производительности
PARALLEL_PROCESSING = False  # Параллельная обработка (экспериментально)
SKIP_BALANCE_CHECK = False  # Пропускать проверку баланса во время работы
AGGRESSIVE_MODE = True      # Агрессивный режим с минимальными задержками
USE_MESSAGE_TRACKING = True # Отслеживание ID сообщений "Идёт поиск"

# ===== Monitoring =====
LOG_PERFORMANCE_STATS = True  # Логировать статистику производительности
STATS_INTERVAL = 10          # Логировать статистику каждые N запросов

# ===== Bot Response Patterns =====
# Паттерны для быстрого определения типа сообщения от бота
SEARCHING_PATTERNS = [
    "идёт поиск", "идет поиск", "searching", "ищу",
    "пожалуйста, подождите", "обработка", "processing"
]

RESULT_INDICATORS = [
    "id:", "👁", "телефон", "phone", "вконтакте", "vk.com"
]

ERROR_PATTERNS = [
    "не найден", "not found", "ошибка", "error",
    "недоступен", "приватн", "private", "удален", "deleted"
]

LIMIT_PATTERNS = [
    "лимит запросов исчерпан", "too many requests",
    "превышен лимит", "достигнут лимит"
]

# ===== VK Batch Processing Configuration =====
VK_BATCH_PROCESSING_ENABLED = True  # Включить пакетную обработку
VK_BATCH_SIZE = 3  # Размер пакета (количество одновременных запросов)
VK_BATCH_DELAY = 0.1  # Задержка между запросами в пакете (секунды)
VK_INTER_BATCH_DELAY = 1.3  # Задержка между пакетами (секунды)
VK_BATCH_TIMEOUT = 20.0  # Таймаут ожидания результатов пакета (секунды)
VK_MIN_LINKS_FOR_BATCH = 10  # Минимальное количество ссылок для включения пакетной обработки