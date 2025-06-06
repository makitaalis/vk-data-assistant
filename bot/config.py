import os
import pathlib
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# ===== Telegram Bot Configuration =====
BOT_TOKEN = os.environ["BOT_TOKEN"]

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

# ===== Processing Configuration =====
SAVE_INTERVAL = 5  # Сохранять каждые N обработанных ссылок
MAX_LINKS_PER_FILE = 5000  # Максимум ссылок в одном файле
MAX_LINKS_PER_MESSAGE = 100  # Максимум ссылок в одном сообщении
MESSAGE_TIMEOUT = 15.0  # Таймаут ожидания ответа от VK бота

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
USE_BOT_POOL = False  # Использовать пул ботов (отключено - работаем с 1 ботом)
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