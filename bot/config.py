import os
import pathlib
import shutil
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

# Р—Р°РіСЂСѓР·РєР° РїРµСЂРµРјРµРЅРЅС‹С… РѕРєСЂСѓР¶РµРЅРёСЏ
load_dotenv()


@dataclass(frozen=True)
class TelegramSessionConfig:
    """Описание отдельной Telegram-сессии."""

    name: str
    phone: str
    enabled: bool
    storage_dir: pathlib.Path


def _clean_env(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

# ===== Telegram Bot Configuration =====
BOT_TOKEN = os.environ["BOT_TOKEN"]

# ===== VK Bot Configuration =====
# Поддержка списка ботов для ротации
VK_BOT_USERNAMES_STR = os.environ.get("VK_BOT_USERNAMES", "")
if VK_BOT_USERNAMES_STR:
    VK_BOT_USERNAMES = [bot.strip() for bot in VK_BOT_USERNAMES_STR.split(",") if bot.strip()]
else:
    # Обратная совместимость со старой настройкой
    single_bot = os.environ.get("VK_BOT_USERNAME", "").strip()
    VK_BOT_USERNAMES = [single_bot] if single_bot else []

# Для обратной совместимости оставляем старую переменную
VK_BOT_USERNAME = VK_BOT_USERNAMES[0] if VK_BOT_USERNAMES else ""

# ===== Telethon Configuration =====
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")

LEGACY_SESSION_NAME = _clean_env(os.environ.get("SESSION_NAME"))
LEGACY_ACCOUNT_PHONE = _clean_env(os.environ.get("ACCOUNT_PHONE"))

PRIMARY_SESSION_NAME = _clean_env(os.environ.get("SESSION_NAME_PRIMARY")) or LEGACY_SESSION_NAME
PRIMARY_ACCOUNT_PHONE = _clean_env(os.environ.get("ACCOUNT_PHONE_PRIMARY")) or LEGACY_ACCOUNT_PHONE
SECONDARY_SESSION_NAME = _clean_env(os.environ.get("SESSION_NAME_SECONDARY"))
SECONDARY_ACCOUNT_PHONE = _clean_env(os.environ.get("ACCOUNT_PHONE_SECONDARY"))

ENABLE_PRIMARY = _env_bool("ENABLE_PRIMARY", True)
ENABLE_SECONDARY = _env_bool("ENABLE_SECONDARY", False)
SESSION_MODE = os.environ.get("SESSION_MODE", "primary").strip().lower() or "primary"
BALANCE_LIMIT_CHECK_ENABLED = _env_bool("BALANCE_LIMIT_CHECK_ENABLED", True)

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

# ===== Session Storage Configuration =====
SESSION_STORAGE_MODE = os.environ.get("SESSION_STORAGE_MODE", "string").lower()
SESSION_BASE_DIR = pathlib.Path(os.environ.get("SESSION_DIR", str(DATA_DIR / "sessions")))
SESSION_BASE_DIR.mkdir(parents=True, exist_ok=True)
# SESSION_DIR оставляем для обратной совместимости
SESSION_DIR = SESSION_BASE_DIR

TELEGRAM_SESSIONS: List[TelegramSessionConfig] = []


def _register_session(name: str | None, phone: str | None, enabled: bool):
    if not name or not phone:
        return
    storage_dir = SESSION_BASE_DIR / name
    TELEGRAM_SESSIONS.append(
        TelegramSessionConfig(
            name=name,
            phone=phone,
            enabled=enabled,
            storage_dir=storage_dir,
        )
    )


def _ensure_session_storage(session: TelegramSessionConfig):
    session.storage_dir.mkdir(parents=True, exist_ok=True)

    def _move_matching(directory: pathlib.Path, pattern: str):
        for candidate in directory.glob(pattern):
            if not candidate.is_file():
                continue
            destination = session.storage_dir / candidate.name
            if destination.exists():
                continue
            if destination.resolve() == candidate.resolve():
                continue
            shutil.move(str(candidate), destination)

    for directory in (BASE_DIR, SESSION_BASE_DIR):
        for pattern in (
            f"{session.name}.session",
            f"{session.name}.session_string",
            f"{session.name}_*.session",
            f"{session.name}_*.session_string",
        ):
            _move_matching(directory, pattern)


_register_session(PRIMARY_SESSION_NAME, PRIMARY_ACCOUNT_PHONE, ENABLE_PRIMARY)
_register_session(SECONDARY_SESSION_NAME, SECONDARY_ACCOUNT_PHONE, ENABLE_SECONDARY)

if not TELEGRAM_SESSIONS:
    fallback_name = LEGACY_SESSION_NAME or "user_session"
    fallback_phone = LEGACY_ACCOUNT_PHONE or "+10000000000"
    TELEGRAM_SESSIONS.append(
        TelegramSessionConfig(
            name=fallback_name,
            phone=fallback_phone,
            enabled=True,
            storage_dir=SESSION_BASE_DIR / fallback_name,
        )
    )

for session in TELEGRAM_SESSIONS:
    _ensure_session_storage(session)

# Обратная совместимость: сохраняем переменные первой активной сессии
PRIMARY_ACTIVE_SESSION = TELEGRAM_SESSIONS[0]
SESSION_NAME = PRIMARY_ACTIVE_SESSION.name
ACCOUNT_PHONE = PRIMARY_ACTIVE_SESSION.phone

# ===== Files =====
PENDING_FILE = DATA_DIR / "pending_links.json"
TEMP_RESULTS_FILE = DATA_DIR / "temp_results.json"

# ===== OPTIMIZED Processing Configuration =====
# РћРїС‚РёРјРёР·РёСЂРѕРІР°РЅРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹ РґР»СЏ Р±С‹СЃС‚СЂРѕР№ СЂР°Р±РѕС‚С‹ СЃ СЂРµРґР°РєС‚РёСЂСѓРµРјС‹РјРё СЃРѕРѕР±С‰РµРЅРёСЏРјРё
SAVE_INTERVAL = 10  # РЎРѕС…СЂР°РЅСЏС‚СЊ РєР°Р¶РґС‹Рµ N РѕР±СЂР°Р±РѕС‚Р°РЅРЅС‹С… СЃСЃС‹Р»РѕРє
MAX_LINKS_PER_FILE = 20000  # РњР°РєСЃРёРјСѓРј СЃСЃС‹Р»РѕРє РІ РѕРґРЅРѕРј С„Р°Р№Р»Рµ
MAX_LINKS_PER_MESSAGE = 100  # РњР°РєСЃРёРјСѓРј СЃСЃС‹Р»РѕРє РІ РѕРґРЅРѕРј СЃРѕРѕР±С‰РµРЅРёРё

# РћРџРўРРњРР—РР РћР’РђРќРќР«Р• РўРђР™РњРРќР“Р
MESSAGE_TIMEOUT = 5.0  # РЈРјРµРЅСЊС€РµРЅ СЃ 15 РґРѕ 5 СЃРµРєСѓРЅРґ - Р±РѕС‚ РѕР±С‹С‡РЅРѕ РѕС‚РІРµС‡Р°РµС‚ Р·Р° 2-4 СЃРµРє
INITIAL_DELAY = 0.5   # РЈРјРµРЅСЊС€РµРЅР° СЃ 2 РґРѕ 0.5 СЃРµРє - Р±С‹СЃС‚СЂР°СЏ РїРµСЂРІР°СЏ РїСЂРѕРІРµСЂРєР°
RETRY_DELAY = 0.3     # РЈРјРµРЅСЊС€РµРЅР° СЃ 1 РґРѕ 0.3 СЃРµРє - Р±С‹СЃС‚СЂС‹Рµ РїРѕРІС‚РѕСЂС‹
MAX_RETRIES = 1       # РЈРјРµРЅСЊС€РµРЅРѕ СЃ 3 РґРѕ 1 - РјРµРЅСЊС€Рµ РїРѕРІС‚РѕСЂРѕРІ РґР»СЏ СЃРєРѕСЂРѕСЃС‚Рё
INTER_REQUEST_DELAY = 0.8  # РџР°СѓР·Р° РјРµР¶РґСѓ Р·Р°РїСЂРѕСЃР°РјРё Рє VK Р±РѕС‚Р°Рј (СЃРµРєСѓРЅРґС‹)
INTER_REQUEST_DELAY_MIN = float(os.environ.get("INTER_REQUEST_DELAY_MIN", 3.5))  # Минимальная случайная задержка после запроса
INTER_REQUEST_DELAY_MAX = float(os.environ.get("INTER_REQUEST_DELAY_MAX", 7.0))  # Максимальная случайная задержка после запроса
SUCCESS_DELAY_BONUS = 0.5  # Extra pause after delivering data (seconds)
EXTRA_INTER_REQUEST_DELAY_MIN = float(os.environ.get("EXTRA_INTER_REQUEST_DELAY_MIN", 3.5))
EXTRA_INTER_REQUEST_DELAY_MAX = float(os.environ.get("EXTRA_INTER_REQUEST_DELAY_MAX", 7.0))
EXTRA_INTER_REQUEST_DELAY_BIAS = float(os.environ.get("EXTRA_INTER_REQUEST_DELAY_BIAS", 0.0))
MAX_WORKERS_PER_JOB = int(os.environ.get("MAX_WORKERS_PER_JOB", 4))
PROGRESS_HEARTBEAT_SECONDS = float(os.environ.get("PROGRESS_HEARTBEAT_SECONDS", 6.0))
QUEUE_PRESSURE_MEDIUM = int(os.environ.get("QUEUE_PRESSURE_MEDIUM", 500))
QUEUE_PRESSURE_HIGH = int(os.environ.get("QUEUE_PRESSURE_HIGH", 1500))
SINGLE_SESSION_DELAY_CAP = float(os.environ.get("SINGLE_SESSION_DELAY_CAP", 0.5))
AUTO_ENABLE_SESSION_THRESHOLD = int(os.environ.get("AUTO_ENABLE_SESSION_THRESHOLD", 2000))
AUTO_SESSION_MANAGEMENT_ENABLED = os.environ.get("AUTO_SESSION_MANAGEMENT_ENABLED", "false").lower() == "true"
TIMEOUT_RETRY_LIMIT = int(os.environ.get("TIMEOUT_RETRY_LIMIT", 2))
WATCHDOG_INTERVAL_SECONDS = float(os.environ.get("WATCHDOG_INTERVAL_SECONDS", 5.0))
WATCHDOG_STALL_SECONDS = float(os.environ.get("WATCHDOG_STALL_SECONDS", 35.0))
QUOTA_BALANCER_ENABLED = os.environ.get("QUOTA_BALANCER_ENABLED", "false").lower() == "true"
VK_BOT_DEFAULT_QUOTA = int(os.environ.get("VK_BOT_DEFAULT_QUOTA", "0") or 0)  # 0 = без лимита
VK_BOT_QUOTA_RESET_HOURS = int(os.environ.get("VK_BOT_QUOTA_RESET_HOURS", "24") or 24)
DB_TASK_QUEUE_ENABLED = os.environ.get("DB_TASK_QUEUE_ENABLED", "false").lower() == "true"
DB_TASK_QUEUE_BATCH = int(os.environ.get("DB_TASK_QUEUE_BATCH", "20") or 20)
DB_TASK_QUEUE_STALE_MINUTES = int(os.environ.get("DB_TASK_QUEUE_STALE_MINUTES", "60") or 60)
SEARCH_STUCK_RETRY_SECONDS = float(os.environ.get("SEARCH_STUCK_RETRY_SECONDS", 60.0))
BOT_HOLD_DURATION_SECONDS = int(os.environ.get("BOT_HOLD_DURATION_SECONDS", 120))

STUCK_RESEND_INTERVAL_SECONDS = int(os.environ.get("STUCK_RESEND_INTERVAL_SECONDS", 300))
STUCK_RESEND_MAX_ATTEMPTS = int(os.environ.get("STUCK_RESEND_MAX_ATTEMPTS", 10))

FORCED_PAUSE_EVERY_REQUESTS = int(os.environ.get("FORCED_PAUSE_EVERY_REQUESTS", 400))
FORCED_PAUSE_DURATION_SECONDS = float(os.environ.get("FORCED_PAUSE_DURATION_SECONDS", 120))

# ===== Patterns =====
# РЈР›РЈР§РЁР•РќРќР«Р™ РїР°С‚С‚РµСЂРЅ РґР»СЏ VK СЃСЃС‹Р»РѕРє СЃ РїРѕРґРґРµСЂР¶РєРѕР№ РІСЃРµС… С„РѕСЂРјР°С‚РѕРІ
VK_LINK_PATTERN = r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com|vkontakte\.ru)/(?:id\d+|[a-zA-Z0-9_\.\-]+)(?:[?#][^\s<>"\'\n\r]*)?'

# РђР»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Рµ РїР°С‚С‚РµСЂРЅС‹ РґР»СЏ СЂР°Р·РЅС‹С… СЃР»СѓС‡Р°РµРІ
VK_LINK_PATTERNS = {
    'standard': r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.\-]+)',
    'with_params': r'https?://(?:www\.)?(?:vk\.com|m\.vk\.com)/(?:id\d+|[a-zA-Z0-9_\.\-]+)(?:\?[^\s<>"\'\n\r]*)?',
    'old_domain': r'https?://(?:www\.)?vkontakte\.ru/(?:id\d+|[a-zA-Z0-9_\.\-]+)',
    'flexible': VK_LINK_PATTERN
}

PHONE_PATTERN = r'(?<!\d)(?:7|8|9)\d{10}(?!\d)'

# ===== Redis Keys =====
REDIS_SESSION_PREFIX = "session:"
REDIS_DISCLAIMER_PREFIX = "disclaimer:"
REDIS_SESSION_TTL = 86400  # 24 С‡Р°СЃР°
REDIS_DISCLAIMER_TTL = 2592000  # 30 РґРЅРµР№

# ===== Feature Flags =====
USE_REDIS = True  # РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Redis РґР»СЏ СЃРµСЃСЃРёР№
ENABLE_DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
ENABLE_DUPLICATE_REMOVAL = True  # Р'РєР»СЋС‡РёС‚СЊ С„СѓРЅРєС†РёСЋ СѓРґР°Р»РµРЅРёСЏ РґСѓР±Р»РёРєР°С‚РѕРІ
USE_CACHE = False  # РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєРµС€ РёР· БД (True = СЃСЃС‹Р»РєРё РёР· БД РЅРµ РїСЂРѕРІРµСЂСЏСЋС‚СЃСЏ РїРѕРІС‚РѕСЂРЅРѕ)
ADMIN_USE_CACHE = False  # Администраторы используют кеш (True = админы видят статистику кеша)

# ===== Rate Limiting =====
RATE_LIMIT_MESSAGES = 30  # РњР°РєСЃРёРјСѓРј СЃРѕРѕР±С‰РµРЅРёР№ РІ РјРёРЅСѓС‚Сѓ РѕС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
RATE_LIMIT_WINDOW = 60  # РћРєРЅРѕ РІ СЃРµРєСѓРЅРґР°С…

# ===== Export Configuration =====
EXPORT_DATE_FORMAT = "%Y%m%d_%H%M%S"
EXPORT_COLUMN_WIDTHS = {
    "РЎСЃС‹Р»РєР° VK": 50,
    "РўРµР»РµС„РѕРЅ": 15,
    "РџРѕР»РЅРѕРµ РёРјСЏ": 30,
    "Р”Р°С‚Р° СЂРѕР¶РґРµРЅРёСЏ": 15
}

# ===== PERFORMANCE OPTIMIZATION FLAGS =====
# Р¤Р»Р°РіРё РґР»СЏ С‚РѕРЅРєРѕР№ РЅР°СЃС‚СЂРѕР№РєРё РїСЂРѕРёР·РІРѕРґРёС‚РµР»СЊРЅРѕСЃС‚Рё
PARALLEL_PROCESSING = False  # РџР°СЂР°Р»Р»РµР»СЊРЅР°СЏ РѕР±СЂР°Р±РѕС‚РєР° (СЌРєСЃРїРµСЂРёРјРµРЅС‚Р°Р»СЊРЅРѕ)
SKIP_BALANCE_CHECK = False  # РџСЂРѕРїСѓСЃРєР°С‚СЊ РїСЂРѕРІРµСЂРєСѓ Р±Р°Р»Р°РЅСЃР° РІРѕ РІСЂРµРјСЏ СЂР°Р±РѕС‚С‹
AGGRESSIVE_MODE = True      # РђРіСЂРµСЃСЃРёРІРЅС‹Р№ СЂРµР¶РёРј СЃ РјРёРЅРёРјР°Р»СЊРЅС‹РјРё Р·Р°РґРµСЂР¶РєР°РјРё
USE_MESSAGE_TRACKING = True # РћС‚СЃР»РµР¶РёРІР°РЅРёРµ ID СЃРѕРѕР±С‰РµРЅРёР№ "РРґС‘С‚ РїРѕРёСЃРє"

# ===== Monitoring =====
LOG_PERFORMANCE_STATS = True  # Р›РѕРіРёСЂРѕРІР°С‚СЊ СЃС‚Р°С‚РёСЃС‚РёРєСѓ РїСЂРѕРёР·РІРѕРґРёС‚РµР»СЊРЅРѕСЃС‚Рё
STATS_INTERVAL = 10          # Р›РѕРіРёСЂРѕРІР°С‚СЊ СЃС‚Р°С‚РёСЃС‚РёРєСѓ РєР°Р¶РґС‹Рµ N Р·Р°РїСЂРѕСЃРѕРІ

# ===== Bot Response Patterns =====
# РџР°С‚С‚РµСЂРЅС‹ РґР»СЏ Р±С‹СЃС‚СЂРѕРіРѕ РѕРїСЂРµРґРµР»РµРЅРёСЏ С‚РёРїР° СЃРѕРѕР±С‰РµРЅРёСЏ РѕС‚ Р±РѕС‚Р°
SEARCHING_PATTERNS = [
    "РёРґС‘С‚ РїРѕРёСЃРє", "РёРґРµС‚ РїРѕРёСЃРє", "searching", "РёС‰Сѓ",
    "РїРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРґРѕР¶РґРёС‚Рµ", "РѕР±СЂР°Р±РѕС‚РєР°", "processing"
]

RESULT_INDICATORS = [
    "id:", "рџ‘Ѓ", "С‚РµР»РµС„РѕРЅ", "phone", "РІРєРѕРЅС‚Р°РєС‚Рµ", "vk.com"
]

ERROR_PATTERNS = [
    "РЅРµ РЅР°Р№РґРµРЅ", "not found", "РѕС€РёР±РєР°", "error",
    "РЅРµРґРѕСЃС‚СѓРїРµРЅ", "РїСЂРёРІР°С‚РЅ", "private", "СѓРґР°Р»РµРЅ", "deleted"
]

LIMIT_PATTERNS = [
    "Р»РёРјРёС‚ Р·Р°РїСЂРѕСЃРѕРІ РёСЃС‡РµСЂРїР°РЅ", "too many requests",
    "РїСЂРµРІС‹С€РµРЅ Р»РёРјРёС‚", "РґРѕСЃС‚РёРіРЅСѓС‚ Р»РёРјРёС‚"
]

# ===== VK Batch Processing Configuration =====
VK_BATCH_PROCESSING_ENABLED = False  # Р’РєР»СЋС‡РёС‚СЊ РїР°РєРµС‚РЅСѓСЋ РѕР±СЂР°Р±РѕС‚РєСѓ
VK_BATCH_SIZE = 3  # Р Р°Р·РјРµСЂ РїР°РєРµС‚Р° (РєРѕР»РёС‡РµСЃС‚РІРѕ РѕРґРЅРѕРІСЂРµРјРµРЅРЅС‹С… Р·Р°РїСЂРѕСЃРѕРІ)
VK_BATCH_DELAY = 0.1  # Р—Р°РґРµСЂР¶РєР° РјРµР¶РґСѓ Р·Р°РїСЂРѕСЃР°РјРё РІ РїР°РєРµС‚Рµ (СЃРµРєСѓРЅРґС‹)
VK_INTER_BATCH_DELAY = 1.2  # Р—Р°РґРµСЂР¶РєР° РјРµР¶РґСѓ РїР°РєРµС‚Р°РјРё (СЃРµРєСѓРЅРґС‹)
VK_BATCH_TIMEOUT = 3.0  # РўР°Р№РјР°СѓС‚ РѕР¶РёРґР°РЅРёСЏ СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ РїР°РєРµС‚Р° (СЃРµРєСѓРЅРґС‹)
VK_MIN_LINKS_FOR_BATCH = 10  # РњРёРЅРёРјР°Р»СЊРЅРѕРµ РєРѕР»РёС‡РµСЃС‚РІРѕ СЃСЃС‹Р»РѕРє РґР»СЏ РІРєР»СЋС‡РµРЅРёСЏ РїР°РєРµС‚РЅРѕР№ РѕР±СЂР°Р±РѕС‚РєРё
