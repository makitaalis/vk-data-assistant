"""
Сервис для работы с несколькими VK ботами с ротацией запросов
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Set

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

from bot.config import (
    VK_BOT_USERNAMES,
    VK_BOT_USERNAME,
    TELEGRAM_SESSIONS,
    SESSION_MODE,
    SESSION_STORAGE_MODE,
    TelegramSessionConfig,
    INTER_REQUEST_DELAY,
    EXTRA_INTER_REQUEST_DELAY_MIN,
    EXTRA_INTER_REQUEST_DELAY_MAX,
    EXTRA_INTER_REQUEST_DELAY_BIAS,
    QUEUE_PRESSURE_MEDIUM,
    QUEUE_PRESSURE_HIGH,
    SINGLE_SESSION_DELAY_CAP,
    AUTO_ENABLE_SESSION_THRESHOLD,
    AUTO_SESSION_MANAGEMENT_ENABLED,
    TIMEOUT_RETRY_LIMIT,
    MESSAGE_TIMEOUT as CONFIG_MESSAGE_TIMEOUT,
    WATCHDOG_INTERVAL_SECONDS,
    WATCHDOG_STALL_SECONDS,
    SEARCH_STUCK_RETRY_SECONDS,
    BOT_HOLD_DURATION_SECONDS,
    FORCED_PAUSE_EVERY_REQUESTS,
    FORCED_PAUSE_DURATION_SECONDS,
    QUOTA_BALANCER_ENABLED,
    VK_BOT_DEFAULT_QUOTA,
    VK_BOT_QUOTA_RESET_HOURS,
)
from services.search_stats_service import SearchStatsManager
PRIMARY_BOT_USERNAME = (VK_BOT_USERNAME or (VK_BOT_USERNAMES[0] if VK_BOT_USERNAMES else "")).lstrip("@")

logger = logging.getLogger("vk_multibot_service")

# Константы
MESSAGE_TIMEOUT_DEFAULT = CONFIG_MESSAGE_TIMEOUT or 5.0
MESSAGE_TIMEOUT = max(
    5.0,
    float(os.environ.get("MESSAGE_TIMEOUT", MESSAGE_TIMEOUT_DEFAULT)),
)  # Основной таймаут ожидания
INITIAL_DELAY = 0.1  # Минимальная задержка для быстрых ответов
RETRY_DELAY = 1.0
MAX_RETRIES = 1
BOT_FAILURE_THRESHOLD = 5  # Подряд неудачных попыток до hold
BOT_HOLD_DURATION = BOT_HOLD_DURATION_SECONDS
SESSION_LOW_COUNT_DELAY_MIN = float(os.environ.get("SESSION_LOW_COUNT_DELAY_MIN", 3.5))
SESSION_LOW_COUNT_DELAY_MAX = float(os.environ.get("SESSION_LOW_COUNT_DELAY_MAX", 7.0))
SESSION_DELAY_STEP_MIN = float(os.environ.get("SESSION_DELAY_STEP_MIN", 1.0))
SESSION_DELAY_STEP_MAX = float(os.environ.get("SESSION_DELAY_STEP_MAX", 1.0))
SESSION_DELAY_MAX_CAP = float(os.environ.get("SESSION_DELAY_MAX_CAP", 12.0))
GLOBAL_PAUSE_THRESHOLD = int(os.environ.get("GLOBAL_PAUSE_THRESHOLD", 200) or 200)
GLOBAL_PAUSE_MIN = 5.2
GLOBAL_PAUSE_MAX = 7.8
FATAL_ERROR_MARKERS = (
    "user was deleted",
    "user deleted",
    "username invalid",
    "user is not a participant",
    "user banned",
    "user deactivated",
    "peer id invalid",
)

# Паттерны для парсинга
PHONE_PATTERN = re.compile(r'(?<!\d)7\d{10}(?!\d)')


@dataclass
class BotState:
    """Состояние отдельного бота"""
    username: str
    session_name: str
    session_dir: Path
    client: Optional[TelegramClient] = None
    entity: Optional[Any] = None
    is_initialized: bool = False
    is_available: bool = True
    last_used: Optional[float] = None
    requests_count: int = 0
    errors_count: int = 0
    limit_reached: bool = False
    balance: Optional[int] = None
    hold_until: Optional[float] = None  # timestamp до которого бот на холде
    is_removed: bool = False
    session_path: Optional[Path] = None
    lock: Optional[asyncio.Lock] = None
    alert_limit_sent: bool = False
    alert_hold_sent: bool = False
    pending_request: Optional["RequestContext"] = None
    is_busy: bool = False
    next_allowed_request: float = 0.0
    quota_limit: Optional[int] = None
    quota_used: int = 0
    quota_window_start: Optional[float] = None
    quota_reset_seconds: int = 24 * 3600
    floodwait_strikes: int = 0
    last_floodwait_ts: float = 0.0


@dataclass
class RequestContext:
    """Контекст одиночного запроса к боту"""
    link: str
    future: asyncio.Future
    message_id: int
    created_at: float
    request_id: int


class RetrySearch(Exception):
    """Внутренний сигнал для повторной попытки поиска."""
    pass


@dataclass
class SessionState:
    """Состояние набора VK ботов, работающих от конкретной Telegram-сессии"""

    config: TelegramSessionConfig
    bots: List[BotState]
    assigned_bots: List[str] = field(default_factory=list)
    current_bot_index: int = 0
    base_session_string: Optional[str] = None
    authorization_completed: bool = False
    is_enabled: bool = True
    next_allowed_request: float = 0.0


class VKMultiBotService:
    """Сервис для работы с несколькими VK ботами с ротацией"""
    
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_base_name: Optional[str] = None,
        phone: Optional[str] = None,
        *,
        sessions: Optional[List[TelegramSessionConfig]] = None,
        session_mode: Optional[str] = None,
        session_storage_mode: Optional[str] = None,
        session_dir: Optional[Path] = None,
        stats_manager: Optional["SearchStatsManager"] = None,
        session_bot_assignments: Optional[Dict[str, List[str]]] = None,
        config_service=None,
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_storage_mode = (session_storage_mode or SESSION_STORAGE_MODE or "string").lower()
        self.session_dir = Path(session_dir) if session_dir else Path(".")
        self.session_dir.mkdir(parents=True, exist_ok=True)

        configured_sessions: List[TelegramSessionConfig] = sessions or TELEGRAM_SESSIONS or []

        if not configured_sessions and session_base_name and phone:
            storage_dir = self.session_dir / session_base_name
            storage_dir.mkdir(parents=True, exist_ok=True)
            configured_sessions = [
                TelegramSessionConfig(
                    name=session_base_name,
                    phone=phone,
                    enabled=True,
                    storage_dir=storage_dir,
                )
            ]

        if not configured_sessions:
            raise RuntimeError("Не заданы Telegram-сессии для VKMultiBotService")

        self.session_mode = (session_mode or SESSION_MODE or "primary").strip().lower() or "primary"

        self.sessions: Dict[str, SessionState] = {}
        self.session_order: List[str] = []
        self._session_cursor = 0
        self.session_slots: Dict[str, Optional[str]] = {"slot_a": None, "slot_b": None}
        self.alert_handler = None
        self._request_seq = 0
        self._session_lock = asyncio.Lock()
        self.stats_manager = stats_manager
        self._load_total_links: int = 0
        self._load_active_workers: int = 1
        self._last_wait_hint_ts: float = 0.0
        self._last_scale_hint_ts: float = 0.0
        self._last_scale_hint_text: str = ""
        self.config_service = config_service
        self._requests_since_pause: int = 0
        self._forced_pause_counter: int = 0
        self._last_no_bot_alert_ts: float = 0.0
        self._last_activity_ts: float = time.time()
        self._last_auto_manage_ts: float = 0.0
        self._auto_manage_lock = asyncio.Lock()
        self._session_reinit_at: Dict[str, float] = {}
        self._session_reinit_cooldown: float = 30.0
        self._session_reinit_attempts: Dict[str, int] = {}
        self._session_reinit_max_attempts: int = 3
        self._session_init_locks: Dict[str, asyncio.Lock] = {}
        self._default_proxy: Optional[Dict[str, Any]] = None
        self._vk_bot_pool: List[str] = self._prepare_vk_bot_pool()
        if not self._vk_bot_pool:
            raise RuntimeError("Не заданы VK боты для VKMultiBotService (VK_BOT_USERNAMES пуст)")
        self._vk_bot_pool_set: Set[str] = set(self._vk_bot_pool)
        self._session_bot_overrides: Dict[str, List[str]] = {}
        if session_bot_assignments:
            for session_name, usernames in session_bot_assignments.items():
                override = self._resolve_session_assignment(usernames)
                if override:
                    self._session_bot_overrides[session_name] = override
        self._session_alias_map: Dict[str, str] = {}
        self._session_alias_reverse: Dict[str, str] = {}
        self._session_alias_seq: int = 0

        for session_config in configured_sessions:
            session_config.storage_dir.mkdir(parents=True, exist_ok=True)
            bot_usernames = self._get_session_bot_list(session_config.name)
            bots = self._build_bot_states(session_config, bot_usernames)
            session_state = SessionState(
                config=session_config,
                bots=bots,
                assigned_bots=list(bot_usernames),
                is_enabled=session_config.enabled,
            )
            self.sessions[session_config.name] = session_state
            self.session_order.append(session_config.name)
            self._assign_session_alias(session_config.name)

        # Плоский список ботов для старых метрик
        self._refresh_flat_bot_list()
        self.primary_session_name = self.session_order[0] if self.session_order else None

        logger.info(
            "🤖 Инициализирован мульти-бот сервис: %s сессий, %s ботов",
            len(self.sessions),
            len(self.bots),
        )
        
        # Общие счетчики
        self.total_processed = 0
        self.total_errors = 0
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_stop: Optional[asyncio.Event] = None

    async def initialize(self):
        """Инициализация всех ботов"""
        logger.info(
            "🔄 Инициализация %s ботов в %s сессиях (режим: %s)",
            len(self.bots),
            len(self.sessions),
            self.session_storage_mode,
        )

        proxy = self._default_proxy
        self._default_proxy = proxy
        initialized_count = 0

        enabled_session_names = [
            name
            for name in self.session_order
            if self.sessions.get(name) and self.sessions[name].is_enabled
        ]

        if not enabled_session_names:
            logger.warning(
                "⚠️ Нет активных Telegram-сессий — VKMultiBotService запущен в режиме ожидания."
            )
            return 0

        for session_name in enabled_session_names:
            session_state = self.sessions.get(session_name)
            if not session_state:
                continue
            initialized = await self._initialize_session(session_state, proxy)
            initialized_count += initialized
            if initialized == 0:
                # Помечаем сессию неактивной, чтобы очередь не залипала на неё
                session_state.is_enabled = False
                session_state.authorization_completed = False
                try:
                    if self.config_service:
                        await self.config_service.set_session_enabled(session_name, False)
                except Exception as exc:
                    logger.warning("Не удалось обновить статус сессии %s в ConfigService: %s", session_name, exc)
                await self._clear_slots_for_session(session_name)
                logger.warning(
                    "⚠️ Сессия %s не авторизована или не инициализирована — отключена до повторной авторизации.",
                    session_name,
                )
                self._notify_async(
                    f"⚠️ Сессия {session_name} не авторизована/не инициализирована и отключена."
                )

        if initialized_count == 0:
            logger.error(
                "⚠️ Не удалось инициализировать ни одного бота. "
                "Бот продолжит работу, ожидая авторизации через /session_auth."
            )
            return 0

        logger.info(f"✅ Успешно инициализировано {initialized_count}/{len(self.bots)} ботов")
        if not self._watchdog_task:
            self._start_watchdog()
        return initialized_count
    
    async def initialize_with_session_auth(self):
        """Инициализация с авторизацией через сессию (для совместимости)"""
        return await self.initialize()

    def _normalize_vk_username(self, username: Optional[str]) -> Optional[str]:
        if not username:
            return None
        cleaned = username.strip()
        if not cleaned:
            return None
        return cleaned.lstrip("@")

    def _prepare_vk_bot_pool(self) -> List[str]:
        pool: List[str] = []
        seen: Set[str] = set()
        for raw in VK_BOT_USERNAMES:
            normalized = self._normalize_vk_username(raw)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            pool.append(normalized)
        return pool

    def _resolve_session_assignment(self, usernames: Optional[List[str]]) -> List[str]:
        if not usernames:
            return []
        resolved: List[str] = []
        seen: Set[str] = set()
        for raw in usernames:
            normalized = self._normalize_vk_username(raw)
            if not normalized:
                continue
            if self._vk_bot_pool_set and normalized not in self._vk_bot_pool_set:
                continue
            if normalized in seen:
                continue
            resolved.append(normalized)
            seen.add(normalized)
        return resolved

    def _get_session_bot_list(self, session_name: str) -> List[str]:
        override = self._session_bot_overrides.get(session_name)
        if override:
            return list(override)
        return list(self._vk_bot_pool)

    async def sync_vk_pool_from_config(self):
        """Подтягивает динамический пул VK ботов из ConfigService и переинициализирует сессии."""
        if not self.config_service:
            return
        dynamic_pool = await self.config_service.get_vk_bot_pool()
        if not dynamic_pool:
            return
        normalized = self._resolve_session_assignment(dynamic_pool)
        if not normalized:
            return

        previous_pool = list(self._vk_bot_pool)
        if previous_pool == normalized:
            return

        self._vk_bot_pool = normalized
        self._vk_bot_pool_set = set(normalized)

        # Обновляем overrides из ConfigService с учётом нового пула
        if self.config_service:
            try:
                assignments = await self.config_service.get_all_session_bots()
                self._bot_quotas = await self.config_service.get_vk_bot_quotas()
            except Exception as exc:
                logger.warning("Не удалось загрузить overrides VK ботов: %s", exc)
                assignments = {}
                self._bot_quotas = {}
            self._session_bot_overrides = {}
            for name, override in assignments.items():
                resolved = self._resolve_session_assignment(override)
                if resolved:
                    self._session_bot_overrides[name] = resolved

        # Пересобираем ботов для сессий
        for session_name, session_state in self.sessions.items():
            target_list = self._get_session_bot_list(session_name)
            await self._reconfigure_session_bots(session_state, list(target_list))

    async def add_vk_bot(self, username: str) -> bool:
        """Добавляет VK бота в пул и пересобирает сессии без overrides."""
        normalized = self._normalize_vk_username(username)
        if not normalized:
            return False
        if normalized in self._vk_bot_pool_set:
            return True

        self._vk_bot_pool.append(normalized)
        self._vk_bot_pool_set.add(normalized)
        if self.config_service:
            await self.config_service.add_vk_bot(normalized)

        for session_name, session_state in self.sessions.items():
            if session_name in self._session_bot_overrides:
                continue
            await self._reconfigure_session_bots(session_state, list(self._vk_bot_pool))
        return True

    async def remove_vk_bot(self, username: str) -> bool:
        """Удаляет VK бота из пула и обновляет сессии."""
        normalized = self._normalize_vk_username(username)
        if not normalized or normalized not in self._vk_bot_pool_set:
            return False
        if len(self._vk_bot_pool_set) <= 1:
            logger.warning("Нельзя удалить последнего VK бота из пула")
            return False

        self._vk_bot_pool = [b for b in self._vk_bot_pool if b != normalized]
        self._vk_bot_pool_set = set(self._vk_bot_pool)
        if self.config_service:
            await self.config_service.remove_vk_bot(normalized)

        for session_name, session_state in self.sessions.items():
            # Удаляем из overrides
            if session_name in self._session_bot_overrides:
                override = [b for b in self._session_bot_overrides[session_name] if b != normalized]
                self._session_bot_overrides[session_name] = override
                target_list = override or list(self._vk_bot_pool)
            else:
                target_list = list(self._vk_bot_pool)
            await self._reconfigure_session_bots(session_state, target_list)
        return True

    def _build_bot_states(self, session_config: TelegramSessionConfig, bot_usernames: List[str]) -> List[BotState]:
        bots: List[BotState] = []
        quota_limit = VK_BOT_DEFAULT_QUOTA if QUOTA_BALANCER_ENABLED and VK_BOT_DEFAULT_QUOTA > 0 else None
        quota_reset_seconds = VK_BOT_QUOTA_RESET_HOURS * 3600
        for bot_username in bot_usernames:
            per_bot_quota = None
            if QUOTA_BALANCER_ENABLED and getattr(self, "_bot_quotas", None):
                per_bot_quota = self._bot_quotas.get(bot_username.lstrip("@"))
            limit_to_use = per_bot_quota if per_bot_quota is not None else quota_limit
            bot_state = BotState(
                username=bot_username,
                session_name=session_config.name,
                session_dir=session_config.storage_dir,
                quota_limit=limit_to_use,
                quota_reset_seconds=quota_reset_seconds,
            )
            bot_state.lock = asyncio.Lock()
            bots.append(bot_state)
        return bots

    def _refresh_flat_bot_list(self):
        combined: List[BotState] = []
        for name in self.session_order:
            state = self.sessions.get(name)
            if not state:
                continue
            combined.extend(state.bots)
        self.bots = combined

    def _assign_session_alias(self, session_name: str) -> str:
        existing = self._session_alias_map.get(session_name)
        if existing:
            return existing
        alias = f"s{self._session_alias_seq}"
        self._session_alias_seq += 1
        self._session_alias_map[session_name] = alias
        self._session_alias_reverse[alias] = session_name
        return alias

    def get_session_alias(self, session_name: str) -> str:
        return self._assign_session_alias(session_name)

    def resolve_session_alias(self, alias: Optional[str]) -> Optional[str]:
        if not alias:
            return None
        return self._session_alias_reverse.get(alias)

    def clear_session_alias(self, session_name: str):
        alias = self._session_alias_map.pop(session_name, None)
        if alias:
            self._session_alias_reverse.pop(alias, None)

    def get_available_bot_usernames(self) -> List[str]:
        return list(self._vk_bot_pool)

    def get_session_assigned_bots(self, session_name: str) -> List[str]:
        state = self.sessions.get(session_name)
        if not state:
            return []
        if state.assigned_bots:
            return list(state.assigned_bots)
        return [bot.username for bot in state.bots]

    def get_session_bot_overrides(self) -> Dict[str, List[str]]:
        return {name: list(bots) for name, bots in self._session_bot_overrides.items()}

    async def update_session_bots(self, session_name: str, usernames: List[str]) -> bool:
        session_state = self.sessions.get(session_name)
        if not session_state:
            return False
        normalized = self._resolve_session_assignment(usernames)
        if normalized:
            self._session_bot_overrides[session_name] = normalized
        else:
            self._session_bot_overrides.pop(session_name, None)
            normalized = list(self._vk_bot_pool)

        current = [bot.username for bot in session_state.bots]
        if current == normalized:
            session_state.assigned_bots = list(normalized)
            return True

        await self._reconfigure_session_bots(session_state, normalized)
        return True

    async def _reconfigure_session_bots(self, session_state: SessionState, bot_usernames: List[str]):
        session_name = session_state.config.name
        was_enabled = session_state.is_enabled
        await self.disable_session(session_name, disconnect_clients=True, reason="bot list update")

        async with self._session_lock:
            session_state.bots = self._build_bot_states(session_state.config, bot_usernames)
            session_state.assigned_bots = list(bot_usernames)
            self._refresh_flat_bot_list()

        if was_enabled:
            await self.enable_session(session_name, reason="bot list update")
    
    def _setup_handlers(self, bot: BotState):
        """Настройка обработчиков для конкретного бота"""
        client = bot.client
        if not client:
            return

        @client.on(events.NewMessage())
        async def handle_new_message(event):
            if not event.message or not event.message.text:
                return
            
            if getattr(event.message, "out", False):
                return
            
            chat_id = getattr(event, "chat_id", None)
            if bot.entity and chat_id is not None and chat_id != bot.entity.id:
                return
            
            if bot.pending_request:
                await self._process_message(event.message.text, event.message.id, bot)

        @client.on(events.MessageEdited())
        async def handle_edited_message(event):
            if not event.message or not event.message.text:
                return
            
            if getattr(event.message, "out", False):
                return
            
            chat_id = getattr(event, "chat_id", None)
            if bot.entity and chat_id is not None and chat_id != bot.entity.id:
                return
            
            if bot.pending_request:
                await self._process_message(event.message.text, event.message.id, bot)

    async def _initialize_session(self, session_state: SessionState, proxy: Optional[Dict[str, Any]]) -> int:
        """Инициализация всех ботов конкретной Telegram-сессии"""
        lock = self._session_init_locks.setdefault(session_state.config.name, asyncio.Lock())
        async with lock:
            try:
                base_session_string = await self._load_base_session_string(session_state, proxy)
            except Exception as exc:
                logger.warning("⚠️ Базовая сессия %s недоступна: %s", session_state.config.name, exc)
                return 0
            initialized_count = 0
            total = len(session_state.bots)

            for index, bot in enumerate(session_state.bots, start=1):
                try:
                    await self._initialize_bot_with_string_session(
                        session_state,
                        bot,
                        proxy,
                        base_session_string,
                        index=index,
                        total=total,
                    )
                    initialized_count += 1
                except Exception as e:
                    logger.error(
                        "❌ Ошибка инициализации бота %s/%s (@%s) в сессии %s: %s",
                        index,
                        total,
                        bot.username,
                        session_state.config.name,
                        e,
                    )
                    bot.is_available = False
                    bot.is_initialized = False
                    if bot.client:
                        try:
                            await bot.client.disconnect()
                        except Exception:
                            pass
                        bot.client = None

        return initialized_count

    async def _load_base_session_string(
        self,
        session_state: SessionState,
        proxy: Optional[Dict[str, Any]],
    ) -> str:
        """Загрузка StringSession для конкретной Telegram-сессии"""
        if session_state.base_session_string is not None:
            return session_state.base_session_string

        session_name = session_state.config.name
        storage_paths = [
            session_state.config.storage_dir / f"{session_name}.session_string",
            self.session_dir / f"{session_name}.session_string",
        ]

        for path in storage_paths:
            if path.exists():
                session_state.base_session_string = path.read_text().strip()
                # Синхронизируем в основное хранилище, если файл найден в legacy пути
                target_path = session_state.config.storage_dir / path.name
                if path != target_path:
                    target_path.write_text(session_state.base_session_string)
                return session_state.base_session_string

        sqlite_candidates = [
            session_state.config.storage_dir / f"{session_name}.session",
            self.session_dir / f"{session_name}.session",
            Path(f"{session_name}.session"),
        ]

        for sqlite_path in sqlite_candidates:
            if sqlite_path.exists():
                temp_client = TelegramClient(
                    str(sqlite_path),
                    self.api_id,
                    self.api_hash,
                    proxy=proxy,
                )
                await temp_client.connect()
                if not await temp_client.is_user_authorized():
                    phone = session_state.config.phone
                    if not phone:
                        raise RuntimeError(
                            f"Не удалось авторизоваться для сессии {session_name}: номер телефона не задан"
                        )
                    logger.warning(
                        "⚠️ Базовая сессия %s не авторизована, пробуем авторизоваться...",
                        session_name,
                    )
                    await temp_client.start(phone=phone)

                session_state.base_session_string = StringSession.save(temp_client.session)
                await temp_client.disconnect()

                target_path = session_state.config.storage_dir / f"{session_name}.session_string"
                target_path.write_text(session_state.base_session_string)
                return session_state.base_session_string

        # Если базовая сессия отсутствует — считаем сессию неавторизованной
        raise RuntimeError(f"Базовая сессия {session_name} не найдена (.session/.session_string)")

    async def _initialize_bot_with_string_session(
        self,
        session_state: SessionState,
        bot: BotState,
        proxy: Optional[Dict[str, Any]],
        base_session_string: str,
        index: int,
        total: int,
    ):
        """Создание отдельного клиента на основе StringSession"""
        session_file = bot.session_dir / f"{bot.session_name}_{bot.username}.session"
        bot.session_path = session_file

        if session_file.exists():
            session_data = session_file.read_text().strip()
        else:
            session_data = base_session_string

        string_session = StringSession(session_data or None)
        client = TelegramClient(string_session, self.api_id, self.api_hash, proxy=proxy)
        bot.client = client

        await client.connect()
        if not await client.is_user_authorized():
            phone = session_state.config.phone
            if not phone:
                raise RuntimeError(
                    f"Не удалось авторизовать бота @{bot.username}: не указан ACCOUNT_PHONE"
                )
            if session_state.authorization_completed:
                raise RuntimeError(
                    f"Не удалось авторизовать всех ботов в сессии {session_state.config.name}: "
                    "требуется подготовить сессии заранее."
                )
            logger.warning(
                "⚠️ Сессия %s для бота @%s не авторизована, выполняем авторизацию...",
                session_state.config.name,
                bot.username,
            )
            await client.start(phone=phone)
            session_state.authorization_completed = True

        async with bot.lock:
            bot.entity = await client.get_entity(bot.username)

        async with bot.lock:
            await client.send_message(bot.entity, "/start")

        await asyncio.sleep(1)

        bot.is_initialized = True
        bot.is_available = True
        bot.quota_used = 0
        bot.quota_window_start = time.time()

        self._persist_bot_session(bot)
        self._setup_handlers(bot)

        logger.info(
            "✅ Бот %s/%s (@%s) инициализирован в сессии %s",
            index,
            total,
            bot.username,
            session_state.config.name,
        )

    def _persist_bot_session(self, bot: BotState):
        """Сохранение StringSession бота на диск"""
        if not bot.client or not bot.session_path:
            return

        try:
            saved_session = bot.client.session.save()
            bot.session_path.write_text(saved_session)

            # Обновляем базовую строку для текущей Telegram-сессии
            base_string_path = bot.session_dir / f"{bot.session_name}.session_string"
            try:
                base_string_path.write_text(saved_session)
            except Exception:
                pass

            session_state = self.sessions.get(bot.session_name)
            if session_state:
                session_state.base_session_string = saved_session
        except Exception as e:
            logger.error(f"⚠️ Не удалось сохранить сессию для @{bot.username}: {e}")
    
    def _get_session_for_balance(self) -> Optional[SessionState]:
        for session_name in self.session_order:
            session_state = self.sessions.get(session_name)
            if session_state and session_state.is_enabled and session_state.bots:
                return session_state
        return None

    def _get_primary_bot_for_session(self, session_state: SessionState) -> Optional[BotState]:
        if not session_state.bots:
            return None
        for bot in session_state.bots:
            if bot.username == PRIMARY_BOT_USERNAME:
                return bot
        return session_state.bots[0]

    def set_session_mode(self, mode: str):
        mode_normalized = (mode or "primary").strip().lower()
        if mode_normalized not in {"primary", "secondary", "both"}:
            raise ValueError(f"Некорректный режим сессии: {mode}")
        self.session_mode = mode_normalized

    async def disable_session(
        self,
        session_name: str,
        disconnect_clients: bool = False,
        reason: Optional[str] = None,
    ) -> bool:
        session_state = self.sessions.get(session_name)
        if not session_state:
            return False
        session_state.is_enabled = False
        if disconnect_clients:
            for bot in session_state.bots:
                if bot.client:
                    try:
                        self._persist_bot_session(bot)
                        async with bot.lock:
                            await bot.client.disconnect()
                    except Exception:
                        pass
                    finally:
                        bot.client = None
                bot.is_initialized = False
                bot.is_available = False
        note = f" ({reason})" if reason else ""
        self._notify_async(f"♻️ Сессия {session_name} отключена{note}.")
        return True

    async def enable_session(
        self,
        session_name: str,
        proxy: Optional[Dict[str, Any]] = None,
        reason: Optional[str] = None,
    ) -> bool:
        session_state = self.sessions.get(session_name)
        if not session_state:
            return False
        session_state.is_enabled = True
        for bot in session_state.bots:
            bot.hold_until = None
            bot.alert_hold_sent = False
            bot.limit_reached = False
            bot.is_available = True
        # Если клиенты уже инициализированы, ничего не делаем
        if all(bot.is_initialized for bot in session_state.bots if bot.client):
            return True
        await self._initialize_session(session_state, proxy)
        note = f" ({reason})" if reason else ""
        self._notify_async(f"✅ Сессия {session_name} активирована{note}.")
        return True

    async def register_session(self, session_name: str, phone: Optional[str], enabled: bool = True) -> bool:
        """Добавляет новую Telegram-сессию в сервис и при необходимости запускает её."""
        async with self._session_lock:
            if session_name in self.sessions:
                return False
            storage_dir = self.session_dir / session_name
            storage_dir.mkdir(parents=True, exist_ok=True)

            session_config = TelegramSessionConfig(
                name=session_name,
                phone=phone or "",
                enabled=enabled,
                storage_dir=storage_dir,
            )
            bot_usernames = self._get_session_bot_list(session_name)
            bots = self._build_bot_states(session_config, bot_usernames)

            session_state = SessionState(
                config=session_config,
                bots=bots,
                assigned_bots=list(bot_usernames),
                is_enabled=False,
            )
            self.sessions[session_name] = session_state
            self.session_order.append(session_name)
            self._assign_session_alias(session_name)
            self._refresh_flat_bot_list()

        if enabled:
            await self.enable_session(session_name, reason="register")
        return True

    def _session_is_ready(self, state: Optional["SessionState"]) -> bool:
        if not state or not state.is_enabled or not state.bots:
            return False
        return any(bot.is_initialized and not bot.is_removed for bot in state.bots)

    def get_active_session_names(self) -> List[str]:
        return [
            name
            for name in self.session_order
            if self._session_is_ready(self.sessions.get(name))
        ]

    def _get_slot_session_list(self) -> List[str]:
        ordered: List[str] = []
        for slot in ("slot_a", "slot_b"):
            name = self.session_slots.get(slot)
            if name and name in self.sessions:
                ordered.append(name)
        return ordered

    def _active_session_count(self) -> int:
        return sum(
            1
            for state in self.sessions.values()
            if state.is_enabled and state.bots
        )

    def get_slot_assignments(self) -> Dict[str, Optional[str]]:
        self._ensure_slot_health()
        return dict(self.session_slots)

    def _ensure_slot_health(self):
        dirty = False
        for slot, name in list(self.session_slots.items()):
            if not name:
                continue
            state = self.sessions.get(name)
            if not self._session_is_ready(state):
                logger.warning("Slot %s содержит недоступную сессию %s — очищаем", slot, name)
                self.session_slots[slot] = None
                if self.config_service:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self.config_service.set_session_slot(slot, None))
                    except Exception as exc:
                        logger.error("Не удалось очистить слот %s через ConfigService: %s", slot, exc)
                dirty = True
        if dirty:
            self._notify_async("⚠️ Обновлены слоты: удалены неактивные сессии")

    async def _sync_session_enable_state(self, desired_active: List[str]):
        desired_set = {name for name in desired_active if name in self.sessions}
        current_active = {name for name, state in self.sessions.items() if state.is_enabled}

        to_disable = current_active - desired_set
        to_enable = desired_set - current_active

        for name in to_disable:
            await self.disable_session(name, disconnect_clients=False, reason="slot update")

        for name in to_enable:
            await self.enable_session(name, reason="slot update")

    async def apply_slot_assignments(self, slots: Dict[str, Optional[str]]):
        normalized: Dict[str, Optional[str]] = {"slot_a": None, "slot_b": None}
        seen: set[str] = set()

        for slot in ("slot_a", "slot_b"):
            candidate = (slots or {}).get(slot)
            state = self.sessions.get(candidate) if candidate else None
            if candidate and state and self._session_is_ready(state) and candidate not in seen:
                normalized[slot] = candidate
                seen.add(candidate)

        self.session_slots = normalized
        desired = [name for name in normalized.values() if name]
        if desired:
            await self._sync_session_enable_state(desired)
        await self._emit_slot_update_alert()

    def set_alert_handler(self, handler):
        """Устанавливает корутину для отправки алертов администраторам."""
        self.alert_handler = handler

    async def update_load_metrics(self, total_links: int, active_workers: int):
        """Получает информацию о текущей очереди и активных воркерах."""
        self._load_total_links = max(0, total_links)
        self._load_active_workers = max(1, active_workers or 1)
        if not AUTO_SESSION_MANAGEMENT_ENABLED:
            return

        now = time.time()
        if now - self._last_auto_manage_ts < 5.0:
            return

        async with self._auto_manage_lock:
            self._last_auto_manage_ts = time.time()
            if self._load_total_links >= AUTO_ENABLE_SESSION_THRESHOLD:
                await self._auto_enable_backup_session()
            elif self._load_total_links <= max(100, QUEUE_PRESSURE_MEDIUM // 2):
                await self._maybe_disable_extra_session()

    def _start_watchdog(self):
        if self._watchdog_task:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._watchdog_stop = asyncio.Event()
        self._watchdog_task = loop.create_task(self._watchdog_loop())

    def _schedule_session_reinit(self, session_name: str, delay: float = 0.0, reason: str = ""):
        """Планирует переинициализацию сессии с антиспамом."""
        if session_name not in self.sessions:
            return
        now = time.time()
        last = self._session_reinit_at.get(session_name, 0)
        if now - last < self._session_reinit_cooldown:
            return
        attempts = self._session_reinit_attempts.get(session_name, 0)
        if attempts >= self._session_reinit_max_attempts:
            logger.warning("⏸ Превышен лимит re-init для %s, требуется ручная проверка", session_name)
            self._notify_async(
                f"⚠️ Сессия {session_name}: достигнут лимит автоматических перезапусков. Проверьте авторизацию/лимиты."
            )
            asyncio.create_task(self._clear_slots_for_session(session_name))
            return
        self._session_reinit_at[session_name] = now
        self._session_reinit_attempts[session_name] = attempts + 1

        async def _do_reinit():
            if delay > 0:
                await asyncio.sleep(delay)
            state = self.sessions.get(session_name)
            if not state or not state.is_enabled:
                return
            try:
                await self._initialize_session(state, proxy=self._default_proxy)
                logger.info("🔄 Сессия %s переинициализирована после %s", session_name, reason or "reinit")
                self._session_reinit_attempts[session_name] = 0
            except Exception as exc:
                logger.warning("Не удалось переинициализировать %s: %s", session_name, exc)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_do_reinit())
        except RuntimeError:
            asyncio.run(_do_reinit())

    async def _watchdog_loop(self):
        interval = max(1.0, WATCHDOG_INTERVAL_SECONDS)
        logger.info("🛡️ Watchdog запущен (интервал %.1f с)", interval)
        try:
            while True:
                if self._watchdog_stop and self._watchdog_stop.is_set():
                    break
                await asyncio.sleep(interval)
                await self._watchdog_scan()
        except asyncio.CancelledError:
            logger.info("🛡️ Watchdog остановлен")
            raise
        finally:
            self._watchdog_task = None

    async def _watchdog_scan(self):
        if not self.bots:
            return
        stall_threshold = max(WATCHDOG_STALL_SECONDS, MESSAGE_TIMEOUT * 2)
        now = time.time()
        for bot in self.bots:
            request = bot.pending_request
            if not request:
                continue
            future = request.future
            if not future or future.done():
                continue
            elapsed = now - request.created_at
            if elapsed < stall_threshold:
                continue
            if not bot.is_busy:
                continue
            logger.warning(
                "🛡️ Watchdog: req#%s у @%s (%s) завис на %.1fс — инициируем повтор",
                request.request_id,
                bot.username,
                bot.session_name,
                elapsed,
            )
            logger.info(
                "🛡️ Watchdog auto-cancel req#%s (%s) link=%s",
                request.request_id,
                bot.session_name,
                request.link,
            )
            try:
                async with bot.lock:
                    if bot.client and bot.entity:
                        await bot.client.send_message(bot.entity, "/cancel")
            except Exception as exc:
                logger.debug("Watchdog cancel для @%s завершился с ошибкой: %s", bot.username, exc)
            payload = {
                "phones": [],
                "full_name": "",
                "birth_date": "",
                "error": "watchdog_timeout",
                "stall_seconds": round(elapsed, 2),
                "auto_cancel_reason": "watchdog",
            }
            payload = self._attach_metadata(bot, payload)
            try:
                future.set_result(payload)
            except asyncio.InvalidStateError:
                continue
            bot.pending_request = None
            bot.is_busy = False
            bot.is_initialized = False
            bot.is_available = False
            if bot.client:
                try:
                    async with bot.lock:
                        await bot.client.disconnect()
                except Exception:
                    pass
                bot.client = None
            self._register_failure(bot, reason="watchdog_timeout")
            self._notify_async(
                f"🛡️ Watchdog перезапустил @{bot.username} (сессия {bot.session_name}), ожидание {elapsed:.1f}с."
            )
            self._schedule_session_reinit(bot.session_name, delay=1.0, reason="watchdog_timeout")

    async def _emit_slot_update_alert(self):
        if not self.alert_handler:
            return
        slots = ", ".join(
            f"{slot.upper()} -> {name or 'не назначен'}"
            for slot, name in self.session_slots.items()
        )
        await self.alert_handler(f"🔁 Слоты обновлены: {slots}")

    def _notify_async(self, message: str):
        if not self.alert_handler:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.alert_handler(message))
        except RuntimeError:
            asyncio.run(self.alert_handler(message))

    async def _clear_slots_for_session(self, session_name: str):
        """Удаляет сессию из слотов A/B и синхронизирует с ConfigService."""
        changed = False
        for slot, assigned in list(self.session_slots.items()):
            if assigned == session_name:
                self.session_slots[slot] = None
                changed = True
                if self.config_service:
                    try:
                        await self.config_service.set_session_slot(slot, None)
                    except Exception as exc:
                        logger.warning("Не удалось очистить слот %s в ConfigService: %s", slot, exc)
        if changed:
            self._notify_async(f"♻️ Сессия {session_name} удалена из слотов.")

    def _summarize_bot_pool(self) -> Dict[str, Any]:
        """Собирает агрегированную информацию о состояниях ботов для диагностики."""
        now = time.time()
        summary = {
            "total": len(self.bots),
            "available": 0,
            "busy": 0,
            "on_hold": 0,
            "limited": 0,
            "removed": 0,
            "pending": 0,
            "min_hold_seconds": None,
        }
        hold_details: List[str] = []
        for bot in self.bots:
            if bot.is_removed:
                summary["removed"] += 1
                continue
            if bot.pending_request:
                summary["pending"] += 1
            if bot.is_busy:
                summary["busy"] += 1
            if bot.hold_until:
                remaining = max(0.0, bot.hold_until - now)
                if remaining > 0:
                    summary["on_hold"] += 1
                    hold_details.append(
                        f"@{bot.username}/{bot.session_name}:{int(remaining)}s"
                    )
                    if summary["min_hold_seconds"] is None or remaining < summary["min_hold_seconds"]:
                        summary["min_hold_seconds"] = remaining
            if bot.limit_reached:
                summary["limited"] += 1
            if bot.is_initialized and bot.is_available and not bot.limit_reached and not bot.hold_until:
                summary["available"] += 1
        if hold_details:
            summary["hold_details"] = hold_details[:5]
        else:
            summary.pop("min_hold_seconds", None)
        return summary

    def get_wait_hint(self) -> Optional[str]:
        now = time.time()
        if now - self._last_wait_hint_ts < max(5.0, INTER_REQUEST_DELAY * 4):
            return "⏳ Все боты заняты, ждём освобождения…"
        if self._last_scale_hint_text and now - self._last_scale_hint_ts < 30:
            return self._last_scale_hint_text
        return None

    def _slot_label(self, session_name: str) -> str:
        for slot, name in self.session_slots.items():
            if name == session_name:
                return slot.upper()
        return "RESERVE"

    async def _auto_enable_backup_session(self):
        active = [
            name for name, state in self.sessions.items()
            if state.is_enabled and state.bots
        ]
        if len(active) >= 2:
            return

        for candidate in self.session_order:
            state = self.sessions.get(candidate)
            if not state or state.is_enabled:
                continue
            enabled = await self.enable_session(candidate, reason="auto_scale")
            if enabled:
                self._last_scale_hint_ts = time.time()
                self._last_scale_hint_text = (
                    f"⚙️ Подключаем резервную сессию {candidate}, очередь {self._load_total_links} ссылок."
                )
                self._notify_async(
                    f"⚠️ Автоподнята резервная сессия {candidate} (очередь {self._load_total_links} ссылок)."
                )
            break

    async def _maybe_disable_extra_session(self):
        active = [
            name for name, state in self.sessions.items()
            if state.is_enabled and state.bots
        ]
        desired = {name for name in self.session_slots.values() if name}
        if len(active) <= 1:
            return
        for candidate in active:
            if candidate == self.session_order[0]:
                continue
            if candidate in desired:
                continue
            await self.disable_session(candidate, disconnect_clients=False, reason="auto_scale")
            self._last_scale_hint_ts = time.time()
            self._last_scale_hint_text = f"ℹ️ Сессия {candidate} отключена, очередь стабилизировалась."
            self._notify_async(
                f"ℹ️ Сессия {candidate} отключена (очередь снизилась до {self._load_total_links})."
            )
            break

    async def _respect_session_delay(self, session_state: Optional[SessionState]):
        if not session_state:
            return
        now = time.time()
        if session_state.next_allowed_request > now:
            await asyncio.sleep(session_state.next_allowed_request - now)

    def _schedule_next_session_delay(self, session_state: Optional[SessionState]):
        if not session_state:
            return
        delay = self._random_delay_for_active_sessions(
            base_min=SESSION_LOW_COUNT_DELAY_MIN,
            base_max=SESSION_LOW_COUNT_DELAY_MAX,
        )
        delay = self._apply_adaptive_delay(delay)
        session_state.next_allowed_request = time.time() + delay

    async def _respect_bot_delay(self, bot: Optional[BotState]):
        if not bot:
            return
        now = time.time()
        if bot.next_allowed_request > now:
            await asyncio.sleep(bot.next_allowed_request - now)

    def _schedule_next_bot_delay(self, bot: Optional[BotState]):
        if not bot:
            return
        delay = self._random_delay_for_active_sessions(
            base_min=EXTRA_INTER_REQUEST_DELAY_MIN,
            base_max=EXTRA_INTER_REQUEST_DELAY_MAX,
        )
        delay += EXTRA_INTER_REQUEST_DELAY_BIAS / 2
        delay = self._apply_adaptive_delay(delay, is_bot_level=True)
        bot.next_allowed_request = time.time() + delay

    async def _register_request_completion(self):
        self._requests_since_pause += 1
        forced_pause = False
        if FORCED_PAUSE_EVERY_REQUESTS > 0:
            self._forced_pause_counter += 1
            if self._forced_pause_counter >= FORCED_PAUSE_EVERY_REQUESTS:
                forced_pause = True
                self._forced_pause_counter = 0

        if self._requests_since_pause >= GLOBAL_PAUSE_THRESHOLD:
            pause = random.uniform(GLOBAL_PAUSE_MIN, GLOBAL_PAUSE_MAX)
            logger.info(
                "⏳ Глобальная пауза %.1f с после %s запросов",
                pause,
                GLOBAL_PAUSE_THRESHOLD,
            )
            await asyncio.sleep(pause)
            self._requests_since_pause = 0

        if forced_pause and FORCED_PAUSE_DURATION_SECONDS > 0:
            duration = FORCED_PAUSE_DURATION_SECONDS
            logger.info(
                "😴 Человеческая пауза %.1f с после %s запросов",
                duration,
                FORCED_PAUSE_EVERY_REQUESTS,
            )
            await asyncio.sleep(duration)

    async def _finalize_result(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        await self._register_request_completion()
        self._last_activity_ts = time.time()
        return payload

    def _register_quota_usage(self, bot: BotState):
        if not QUOTA_BALANCER_ENABLED:
            return
        now = time.time()
        if bot.quota_limit:
            if bot.quota_window_start is None or (now - bot.quota_window_start) >= bot.quota_reset_seconds:
                bot.quota_window_start = now
                bot.quota_used = 0
            bot.quota_used += 1

    def get_last_activity_ts(self) -> float:
        return self._last_activity_ts

    def has_pending_requests(self) -> bool:
        return any(bot.pending_request for bot in self.bots if bot.pending_request)

    def _apply_adaptive_delay(self, base_delay: float, *, is_bot_level: bool = False) -> float:
        delay = max(0.05, base_delay)
        queue = self._load_total_links
        workers = max(1, self._load_active_workers)

        if workers <= 1 and queue >= QUEUE_PRESSURE_HIGH:
            delay = min(delay, SINGLE_SESSION_DELAY_CAP)
        elif workers <= 2 and queue >= QUEUE_PRESSURE_MEDIUM:
            delay *= 0.5 if is_bot_level else 0.7

        return max(0.05, delay)

    def _random_delay_for_active_sessions(self, base_min: float, base_max: float) -> float:
        """
        Случайная задержка с учётом числа активных сессий.
        Базовый диапазон растягивается на SESSION_DELAY_STEP_MIN/MAX
        за каждую сессию сверх двух.
        """
        active = max(1, self._active_session_count())
        extra = max(0, active - 2)
        min_delay = base_min + SESSION_DELAY_STEP_MIN * extra
        max_delay = base_max + SESSION_DELAY_STEP_MAX * extra
        if SESSION_DELAY_MAX_CAP > 0:
            min_delay = min(min_delay, SESSION_DELAY_MAX_CAP)
            max_delay = min(max_delay, SESSION_DELAY_MAX_CAP)
        if max_delay < min_delay:
            max_delay = min_delay
        return random.uniform(min_delay, max_delay)

    def _get_allowed_session_names(self, preferred_sessions: Optional[List[str]] = None) -> List[str]:
        slot_names = self._get_slot_session_list()
        names: List[str] = []
        if slot_names:
            if self.session_mode == "primary":
                candidates = slot_names[:1]
            elif self.session_mode == "secondary":
                candidates = slot_names[1:2]
            else:
                candidates = slot_names
            names = [
                name for name in candidates
                if self._session_is_ready(self.sessions.get(name))
            ]

        # Фоллбек: если слоты заданы, но сессии не готовы/отсутствуют, используем доступные в порядке инициализации
        if not names:
            for idx, session_name in enumerate(self.session_order):
                session_state = self.sessions.get(session_name)
                if not self._session_is_ready(session_state):
                    continue
                names.append(session_name)

        # Последний фоллбек: если в режиме primary/secondary не нашли ни одной готовой,
        # снимаем ограничение по режиму и используем любые готовые сессии.
        if not names:
            names = [
                session_name
                for session_name in self.session_order
                if self._session_is_ready(self.sessions.get(session_name))
            ]

        if preferred_sessions:
            preferred_set = {name for name in preferred_sessions if name in self.sessions}
            filtered = [name for name in names if name in preferred_set]
            if filtered:
                return filtered
            return [name for name in preferred_sessions if name in self.sessions] or names

        if not names:
            for session_name in self.session_order:
                session_state = self.sessions.get(session_name)
                if session_state and session_state.is_enabled and session_state.bots:
                    names.append(session_name)
        return names

    def _restore_bot_if_hold_finished(self, bot: BotState, now: float):
        if bot.hold_until and now >= bot.hold_until:
            was_alerted = bot.alert_hold_sent
            bot.hold_until = None
            bot.is_available = True
            bot.errors_count = 0
            bot.alert_hold_sent = False
            bot.limit_reached = False
            logger.info("✅ Бот @%s возвращен в ротацию после hold", bot.username)
            if was_alerted:
                slot_label = self._slot_label(bot.session_name)
                self._notify_async(
                    f"✅ @{bot.username} (сессия {bot.session_name}, {slot_label}) вернулся после HOLD."
                )

    def _get_next_bot_from_session(self, session_state: SessionState) -> Optional[BotState]:
        now = time.time()
        total = len(session_state.bots)
        if total == 0:
            return None

        attempts = 0
        # Сбор кандидатов
        candidates: List[BotState] = []
        for _ in range(total):
            bot = session_state.bots[session_state.current_bot_index]
            session_state.current_bot_index = (session_state.current_bot_index + 1) % total
            if bot.is_removed or bot.is_busy:
                continue
            self._restore_bot_if_hold_finished(bot, now)
            if bot.hold_until:
                continue
            if not bot.is_initialized or not bot.is_available or bot.limit_reached:
                continue
            candidates.append(bot)

        if not candidates:
            # Попытка мягко сбросить лимиты
            for bot in session_state.bots:
                if bot.is_initialized:
                    self._restore_bot_if_hold_finished(bot, now)
                    if not bot.hold_until:
                        bot.limit_reached = False
                        bot.is_available = True
            candidates = [
                bot for bot in session_state.bots
                if not bot.is_removed and bot.is_initialized and bot.is_available and not bot.hold_until
            ]

        if not candidates:
            return None

        if not QUOTA_BALANCER_ENABLED:
            return candidates[0]

        # Выбор бота с максимальной оставшейся квотой
        def quota_remaining(b: BotState) -> float:
            if not b.quota_limit:
                return float("inf")
            # Сброс окна, если истекло
            if not b.quota_window_start or (now - b.quota_window_start) >= b.quota_reset_seconds:
                b.quota_window_start = now
                b.quota_used = 0
            return max(0, b.quota_limit - b.quota_used)

        best = max(candidates, key=quota_remaining)
        if best.quota_limit and quota_remaining(best) <= 0:
            # Исчерпана квота у всех
            for bot in candidates:
                bot.hold_until = now + bot.quota_reset_seconds
                bot.is_available = False
                bot.limit_reached = True
                bot.alert_hold_sent = False
            return None

        return best

    def _get_next_available_bot(self, preferred_sessions: Optional[List[str]] = None) -> Optional[BotState]:
        allowed_sessions = self._get_allowed_session_names(preferred_sessions)
        # Если слоты/режим не дали ни одной сессии, используем полный пул ботов как фоллбек
        if not allowed_sessions and self.bots:
            for bot in self.bots:
                if bot.is_initialized and not bot.is_removed and not bot.hold_until and bot.is_available:
                    return bot

        if not allowed_sessions:
            return None

        total_sessions = len(allowed_sessions)
        self._session_cursor %= total_sessions
        checked = 0

        while checked < total_sessions:
            session_name = allowed_sessions[self._session_cursor]
            self._session_cursor = (self._session_cursor + 1) % total_sessions
            session_state = self.sessions.get(session_name)
            if not session_state or not session_state.is_enabled:
                checked += 1
                continue

            bot = self._get_next_bot_from_session(session_state)
            if bot:
                return bot

            checked += 1

        return None

    def _mark_bot_removed(self, bot: BotState, reason: str):
        """Исключаем бота из пула до ручной проверки"""
        bot.is_available = False
        bot.is_initialized = False
        bot.limit_reached = True
        bot.hold_until = None
        bot.errors_count = 0
        bot.is_removed = True
        logger.error(f"🚫 Бот @{bot.username} исключен из пула: {reason}. Требуется ручная проверка.")
        self._notify_async(
            f"🚨 @{bot.username} исключён из пула (сессия {bot.session_name}). Причина: {reason}"
        )

    def _attach_metadata(self, bot: Optional[BotState], payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return payload
        payload = payload.copy()
        if bot:
            payload.setdefault("session_name", bot.session_name)
            payload.setdefault("bot_username", bot.username)
        else:
            payload.setdefault("session_name", None)
            payload.setdefault("bot_username", None)
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        return payload

    def _complete_pending_request(self, bot: BotState, payload: Dict[str, Any]):
        """Передает результат текущему запросу конкретного бота"""
        request = bot.pending_request
        if not request:
            return
        payload = self._attach_metadata(bot, payload)
        future = request.future
        if future and not future.done():
            future.set_result(payload)
        bot.pending_request = None

    def _next_request_id(self) -> int:
        self._request_seq += 1
        return self._request_seq

    def _log_request(self, request: RequestContext, bot: BotState, stage: str, extra: str = ""):
        """Упрощённая точка логирования статуса запроса"""
        message = (
            f"{stage} | req#{request.request_id} | @{bot.username} ({bot.session_name}) | "
            f"{request.link} | msg_id={request.message_id}"
        )
        if extra:
            message = f"{message} | {extra}"
        logger.info(message)
    
    async def search_vk_data(self, link: str, preferred_session: Optional[str] = None) -> Dict[str, Any]:
        """Поиск данных по VK ссылке с автоматическим ретраем при таймаутах"""

        preferred = [preferred_session] if preferred_session else None
        last_timeout_payload: Optional[Dict[str, Any]] = None
        last_blockers_summary: Optional[Dict[str, Any]] = None
        max_attempts = max(1, TIMEOUT_RETRY_LIMIT)

        for attempt in range(1, max_attempts + 1):
            should_retry = False
            bot = None
            wait_elapsed = 0.0
            base_wait = max(0.2, min(INTER_REQUEST_DELAY, 1.0))
            max_wait = max(3.0, INTER_REQUEST_DELAY * 5)
            while wait_elapsed < max_wait:
                bot = self._get_next_available_bot(preferred)
                if bot:
                    break
                wait_next = min(base_wait, max_wait - wait_elapsed)
                wait_elapsed += wait_next
                self._last_wait_hint_ts = time.time()
                logger.info(
                    "⏳ Все боты заняты, ожидаем %.1f с (накоплено %.1f/%.1f)",
                    wait_next,
                    wait_elapsed,
                    max_wait,
                )
                await asyncio.sleep(wait_next)

            # Если по предпочтениям бота не нашли — пробуем без привязки к сессии
            if not bot:
                bot = self._get_next_available_bot(preferred_sessions=None)

            if not bot:
                last_blockers_summary = self._summarize_bot_pool()
                logger.error(
                    "❌ Нет доступных ботов для обработки (даже после ожидания) — %s",
                    last_blockers_summary,
                )
                now = time.time()
                if now - self._last_no_bot_alert_ts >= 30:
                    self._last_no_bot_alert_ts = now
                    summary_text = ""
                    if last_blockers_summary:
                        summary_text = f" Состояние: {last_blockers_summary}"
                    self._notify_async(
                        "⚠️ Нет доступных Telegram-сессий для обработки. "
                        "Очередь поставлена на паузу до освобождения ботов."
                        f"{summary_text}"
                    )
                # Если квоты исчерпаны у всех — ставим hold на окно и возвращаем ошибку
                if QUOTA_BALANCER_ENABLED:
                    for session_state in self.sessions.values():
                        for bot_state in session_state.bots:
                            if bot_state.quota_limit:
                                bot_state.hold_until = time.time() + bot_state.quota_reset_seconds
                                bot_state.is_available = False
                                bot_state.limit_reached = True
                    self._notify_async("⚠️ Все квоты исчерпаны, пауза до сброса окна.")
                no_bot_payload = {
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": "no_available_bots",
                    "details": last_blockers_summary,
                }
                return await self._finalize_result(self._attach_metadata(None, no_bot_payload))

            logger.info(
                "🔄 Используем бота @{bot} из сессии {session} для {link} (попытка {attempt}/{limit})".format(
                    bot=bot.username,
                    session=bot.session_name,
                    link=link,
                    attempt=attempt,
                    limit=max_attempts,
                )
            )
            self._register_quota_usage(bot)

            bot.is_busy = True
            response_future: Optional[asyncio.Future] = None
            request_ctx: Optional[RequestContext] = None
            try:
                loop = asyncio.get_running_loop()
                response_future = loop.create_future()
                request_ctx = RequestContext(
                    link=link,
                    future=response_future,
                    message_id=0,
                    created_at=time.time(),
                    request_id=self._next_request_id(),
                )
                bot.pending_request = request_ctx

                session_state = self.sessions.get(bot.session_name)
                await self._respect_session_delay(session_state)
                await self._respect_bot_delay(bot)
                await asyncio.sleep(random.uniform(0.8, 1.2))

                async with bot.lock:
                    msg = await bot.client.send_message(bot.entity, link)
                if self.stats_manager:
                    await self.stats_manager.record_link_sent(bot.session_name, bot.username)
                    await self.stats_manager.record_bot_request(bot.session_name, bot.username)
                current_request = bot.pending_request
                if current_request:
                    current_request.message_id = msg.id
                    self._log_request(current_request, bot, "➡️ SEND")
                else:
                    logger.debug(
                        "⚠️ Pending request исчез до логирования SEND для %s (бот @%s)",
                        link,
                        bot.username,
                    )

                self._schedule_next_session_delay(session_state)
                self._schedule_next_bot_delay(bot)
                bot.last_used = time.time()
                bot.requests_count += 1

                await asyncio.sleep(INITIAL_DELAY)

                stuck_retry = False
                if not response_future.done():
                    try:
                        await asyncio.wait_for(response_future, timeout=MESSAGE_TIMEOUT)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "⏱ Таймаут ожидания ответа от @%s для %s (req#%s)",
                            bot.username,
                            link,
                            request_ctx.request_id if request_ctx else "n/a",
                        )
                        async with bot.lock:
                            last_messages = await bot.client.get_messages(bot.entity, limit=3)
                        is_searching = any(
                            m.text and "🔎" in m.text and "идёт поиск" in m.text.lower()
                            for m in last_messages
                        )
                        if is_searching:
                            logger.info(
                                "🔄 Бот @%s все еще ищет (req#%s), ждем до %ss перед повтором...",
                                bot.username,
                                request_ctx.request_id if request_ctx else "n/a",
                                SEARCH_STUCK_RETRY_SECONDS,
                            )
                            try:
                                await asyncio.wait_for(response_future, timeout=SEARCH_STUCK_RETRY_SECONDS)
                            except asyncio.TimeoutError:
                                logger.error(
                                    "❌ Поиск завис у @%s (req#%s) > %ss, перезапускаем запрос",
                                    bot.username,
                                    request_ctx.request_id if request_ctx else "n/a",
                                    SEARCH_STUCK_RETRY_SECONDS,
                                )
                                async with bot.lock:
                                    await bot.client.send_message(bot.entity, "/cancel")
                                await asyncio.sleep(1)
                                stuck_retry = True
                        if not is_searching or (is_searching and not response_future.done() and not stuck_retry):
                            # обычный повторный таймаут; после /cancel продолжим ниже
                            async with bot.lock:
                                await bot.client.send_message(bot.entity, "/cancel")
                            await asyncio.sleep(1)
                            stuck_retry = True

                if stuck_retry and not response_future.done():
                    timeout_payload = {
                        "phones": [],
                        "full_name": "",
                        "birth_date": "",
                        "error": "search_stuck" if is_searching else "timeout",
                        "retry_attempt": attempt,
                    }
                    timeout_payload = self._attach_metadata(bot, timeout_payload)
                    response_future.set_result(timeout_payload)
                    bot.pending_request = None
                    self._register_failure(bot, reason=timeout_payload["error"])
                    if request_ctx:
                        self._log_request(
                            request_ctx,
                            bot,
                            "⏱ TIMEOUT",
                            extra=f"waited {time.time() - request_ctx.created_at:.2f}s ({timeout_payload['error']})",
                        )
                    last_timeout_payload = timeout_payload
                    if attempt < max_attempts:
                        logger.info(
                            "↩️ Повторная попытка для %s после статуса '%s': следующая попытка %s/%s",
                            link,
                            timeout_payload["error"],
                            attempt + 1,
                            max_attempts,
                        )
                        should_retry = True
                        raise RetrySearch()
                    return await self._finalize_result(timeout_payload)

                if not response_future.done():
                    timeout_payload = {
                        "phones": [],
                        "full_name": "",
                        "birth_date": "",
                        "error": "timeout",
                        "retry_attempt": attempt,
                    }
                    timeout_payload = self._attach_metadata(bot, timeout_payload)
                    response_future.set_result(timeout_payload)
                    bot.pending_request = None
                    self._register_failure(bot, reason="timeout")
                    if request_ctx:
                        self._log_request(
                            request_ctx,
                            bot,
                            "⏱ TIMEOUT",
                            extra=f"waited {time.time() - request_ctx.created_at:.2f}s",
                        )
                    last_timeout_payload = timeout_payload
                    if attempt < max_attempts:
                        logger.info(
                            "↩️ Повторная попытка для %s: предыдущий бот @%s, следующая попытка %s/%s",
                            link,
                            bot.username,
                            attempt + 1,
                            max_attempts,
                        )
                        should_retry = True
                        raise RetrySearch()
                    return await self._finalize_result(timeout_payload)

                result_payload = response_future.result()
                if result_payload and result_payload.get("error") == "watchdog_timeout":
                    logger.info(
                        "🛡️ Watchdog перезапустил запрос %s, сразу пробуем следующего бота (попытка %s/%s)",
                        link,
                        attempt + 1,
                        max_attempts,
                    )
                    last_timeout_payload = result_payload
                    should_retry = True
                    raise RetrySearch()
                bot.errors_count = 0
                bot.hold_until = None
                bot.is_available = True
                bot.limit_reached = False
                bot.alert_limit_sent = False
                self.total_processed += 1

                duration = time.time() - request_ctx.created_at if request_ctx else 0.0
                if result_payload is None:
                    result_payload = {
                        "phones": [],
                        "full_name": "",
                        "birth_date": "",
                        "error": None,
                    }
                    result_payload = self._attach_metadata(bot, result_payload)
                elif "session_name" not in result_payload:
                    result_payload = self._attach_metadata(bot, result_payload)
                if attempt > 1:
                    result_payload.setdefault("retry_attempt", attempt)

                phones_count = len(result_payload.get("phones") or [])
                self._log_request(
                    request_ctx,
                    bot,
                    "✅ DONE",
                    extra=f"{duration:.2f}s | phones={phones_count} | error={result_payload.get('error')}",
                )
                return await self._finalize_result(result_payload)

            except RetrySearch:
                pass

            except FloodWaitError as e:
                wait_seconds = getattr(e, "seconds", 0)
                now = time.time()
                logger.error(
                    "⏳ FloodWaitError у бота @%s: требуется подождать %s секунд",
                    bot.username,
                    wait_seconds,
                )
                self.total_errors += 1

                # Мягкий backoff: усиливаем паузу при повторяющихся мелких FloodWait
                if now - bot.last_floodwait_ts > 600:
                    bot.floodwait_strikes = 0
                bot.floodwait_strikes += 1
                bot.last_floodwait_ts = now
                backoff_extra = min(180.0, 15.0 * bot.floodwait_strikes)

                hold_for = max(wait_seconds + backoff_extra, BOT_HOLD_DURATION)
                bot.hold_until = now + hold_for
                bot.next_allowed_request = bot.hold_until
                bot.is_available = False
                bot.limit_reached = True
                bot.is_initialized = False
                # Планируем мягкий реинициализационный backoff, чтобы вернуть бота после большого FloodWait
                if wait_seconds >= 300 or (bot.floodwait_strikes >= 3 and wait_seconds > 0):
                    self._notify_async(
                        f"⚠️ @{bot.username} получил FloodWait на {wait_seconds}с. "
                        f"Сессия {bot.session_name} поставлена на паузу."
                    )
                    delay = min(max(hold_for, wait_seconds), 600)
                    self._schedule_session_reinit(bot.session_name, delay=delay, reason="flood_wait")

                error_payload = {
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": "flood_wait",
                    "wait_seconds": wait_seconds,
                }
                return await self._finalize_result(self._attach_metadata(bot, error_payload))

            except Exception as e:
                logger.error(f"❌ Ошибка при поиске через @{bot.username}: {e}")
                self._register_failure(bot, reason=str(e))
                error_payload = {
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": str(e),
                }
                return await self._finalize_result(self._attach_metadata(bot, error_payload))
            finally:
                if response_future:
                    request = bot.pending_request
                    if request and request.future is response_future and not response_future.done():
                        response_future.cancel()
                        bot.pending_request = None
                if request_ctx and bot.pending_request is None:
                    self._log_request(request_ctx, bot, "⚠️ FINISH", extra="Future cleaned up")
                bot.is_busy = False
                self._persist_bot_session(bot)
            if not should_retry:
                if last_timeout_payload:
                    return await self._finalize_result(last_timeout_payload)
                final_payload = {
                    "phones": [],
                    "full_name": "",
                    "birth_date": "",
                    "error": "no_bots_available",
                }
                if last_blockers_summary:
                    final_payload["blockers"] = last_blockers_summary
                return await self._finalize_result(final_payload)

    async def _process_message(self, text: str, msg_id: int, bot: BotState):
        """Обработка сообщения от конкретного бота"""
        request = bot.pending_request
        if not request:
            logger.warning(
                "📨 Получено сообщение от @%s без активного запроса (msg_id=%s)",
                bot.username,
                msg_id,
            )
            return

        if request.message_id and request.message_id != msg_id:
            logger.warning(
                "📨 Сообщение не совпадает с ожидаемым (req#%s, ожидаемый msg_id=%s, получен=%s)",
                request.request_id,
                request.message_id,
                msg_id,
            )
            self._log_request(request, bot, "⚠️ MISMATCH")
        else:
            self._log_request(request, bot, "📨 RECEIVE")
        
        if "🔎" in text and "идёт поиск" in text.lower():
            logger.info(f"🔍 Бот @{bot.username} начал поиск...")
            return
        
        text_lower = text.lower()
        if any(phrase in text_lower for phrase in ["лимит запросов исчерпан", "too many requests", "превышен лимит"]):
            logger.warning(f"⚠️ Бот @{bot.username} достиг лимита!")
            previously_limited = bot.limit_reached
            bot.limit_reached = True
            bot.is_available = False
            bot.hold_until = None
            if not previously_limited and not bot.alert_limit_sent:
                bot.alert_limit_sent = True
                slot_label = self._slot_label(bot.session_name)
                self._notify_async(
                    f"⚠️ @{bot.username} (сессия {bot.session_name}, {slot_label}) достиг лимита поисков. "
                    "Сессия временно остановлена."
                )

            payload = {
                "phones": [],
                "full_name": "",
                "birth_date": "",
                "error": "limit_reached",
                "limit_reason": "vk_text_limit",
            }
            self._log_request(request, bot, "🚫 LIMIT")
            self._complete_pending_request(bot, payload)
            return
        
        if self._is_result_message(text):
            payload = self._parse_result(text)
            phones_count = len(payload.get("phones") or [])
            self._log_request(
                request,
                bot,
                "📦 RESULT",
                extra=f"phones={phones_count}, full_name={'yes' if payload.get('full_name') else 'no'}",
            )
            logger.info(
                "📞 Детали парсинга (req#%s): phones=%s, snippet=%s",
                request.request_id,
                payload.get("phones"),
                text.replace("\n", "\\n")[:120],
            )
            if phones_count == 0:
                snippet = text.replace("\n", "\\n")[:200]
                logger.warning(
                    "📭 Парсер не нашел телефоны (req#%s, @%s). Фрагмент: %s",
                    request.request_id,
                    bot.username,
                    snippet,
                )
            self._complete_pending_request(bot, payload)
            logger.info(f"✅ Получен результат от @{bot.username}")
            return
        
        if any(phrase in text_lower for phrase in ["не найден", "ошибка", "error", "недоступен", "не удалось найти"]):
            payload = {
                "phones": [],
                "full_name": "",
                "birth_date": "",
                "error": "not_found"
            }
            bot.alert_limit_sent = False
            self._log_request(request, bot, "ℹ️ EMPTY/ERROR", extra=text.splitlines()[0] if text else "")
            self._complete_pending_request(bot, payload)
            logger.info(f"ℹ️ Данные не найдены (@{bot.username})")
    
    def _register_failure(self, bot: BotState, reason: str):
        """Регистрирует сбой бота и при необходимости отправляет его в холд"""
        bot.errors_count += 1
        self.total_errors += 1
        reason_lower = (reason or "").lower()

        if any(marker in reason_lower for marker in FATAL_ERROR_MARKERS):
            self._mark_bot_removed(bot, reason)
            return


        if bot.errors_count >= BOT_FAILURE_THRESHOLD:
            hold_until = time.time() + BOT_HOLD_DURATION
            bot.hold_until = hold_until
            bot.is_available = False
            bot.limit_reached = False
            bot.errors_count = 0  # сбрасываем счетчик после помещения в hold
            if not bot.alert_hold_sent:
                bot.alert_hold_sent = True
                slot_label = self._slot_label(bot.session_name)
                human_time = datetime.fromtimestamp(hold_until).strftime("%H:%M:%S")
                self._notify_async(
                    f"🚫 @{bot.username} (сессия {bot.session_name}, {slot_label}) "
                    f"переведен в HOLD до {human_time}. Причина: {reason}"
                )

            hold_until_human = datetime.fromtimestamp(hold_until).strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(
                f"🚫 Бот @{bot.username} переведен в hold до {hold_until_human} "
                f"(последняя причина: {reason})"
            )
        else:
            logger.debug(
                "⚠️ Ошибка #%s для бота @%s (причина: %s)",
                bot.errors_count,
                bot.username,
                reason
            )

    def _is_result_message(self, text: str) -> bool:
        """Проверка, является ли сообщение результатом поиска"""
        indicators = ["👁", "телефон", "phone", "id:", "vk.com", "родился", "дата рождения"]
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in indicators)
    
    def _parse_result(self, text: str) -> Dict[str, Any]:
        """Парсинг результата из сообщения бота"""
        result = {
            "phones": [],
            "full_name": "",
            "birth_date": ""
        }
        
        lines = text.split('\n')
        
        # Флаг для отслеживания секции телефонов
        in_phone_section = False
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            # Проверяем начало секции телефонов
            if 'телефон' in line_lower or 'phone' in line_lower:
                in_phone_section = True
                # Ищем телефоны в текущей строке
                phones = PHONE_PATTERN.findall(line)
                result["phones"].extend(phones)
                continue
            
            # Если мы в секции телефонов, ищем телефоны в следующих строках
            if in_phone_section:
                # Проверяем, не началась ли новая секция
                if any(marker in line_lower for marker in ['имя:', 'фамилия:', 'город:', 'интересовались', '👁']):
                    in_phone_section = False
                else:
                    # Ищем телефоны в строке (могут быть с префиксом " - ")
                    phones = PHONE_PATTERN.findall(line)
                    result["phones"].extend(phones)
            
            # Парсинг полного имени (ищем в формате "**Полное имя:** `...`")
            if 'полное имя' in line_lower or 'full name' in line_lower:
                # Извлекаем значение между обратными кавычками
                import re
                match = re.search(r'`([^`]+)`', line)
                if match:
                    result["full_name"] = match.group(1)
            
            # Парсинг даты рождения
            if 'дата рождения' in line_lower or 'родился' in line_lower or 'birth' in line_lower:
                # Извлекаем значение между обратными кавычками
                import re
                match = re.search(r'`([^`]+)`', line)
                if match:
                    result["birth_date"] = match.group(1)
        
        # Убираем дубликаты телефонов
        result["phones"] = list(set(result["phones"]))
        
        return result
    

    async def _get_balance_for_session(self, session_state: SessionState) -> Optional[Dict[str, Any]]:
        main_bot = self._get_primary_bot_for_session(session_state)
        if not main_bot:
            logger.error("Нет доступного бота для сессии %s", session_state.config.name)
            return None

        if not main_bot.is_initialized:
            logger.error(
                "Сессия %s: основной бот @%s не инициализирован",
                session_state.config.name,
                main_bot.username,
            )
            return None

        try:
            logger.info(
                "🔄 Проверка баланса через @%s (сессия %s)...",
                main_bot.username,
                session_state.config.name,
            )

            async with main_bot.lock:
                await main_bot.client.send_message(main_bot.entity, "/start")
            await asyncio.sleep(1.5)

            async with main_bot.lock:
                messages = await main_bot.client.get_messages(main_bot.entity, limit=1)

            if not messages or not messages[0].buttons:
                logger.error("Сессия %s: не получено меню от бота", session_state.config.name)
                return None

            profile_button = None
            for row in messages[0].buttons:
                for button in row:
                    button_text = button.text.lower()
                    if "профиль" in button_text or "profile" in button_text or "баланс" in button_text:
                        profile_button = button
                        break
                if profile_button:
                    break

            if not profile_button:
                logger.error("Сессия %s: кнопка профиля не найдена", session_state.config.name)
                return None

            async with main_bot.lock:
                await messages[0].click(data=profile_button.data)
            await asyncio.sleep(2)

            async with main_bot.lock:
                messages = await main_bot.client.get_messages(main_bot.entity, limit=1)

            if not messages or not messages[0].text:
                logger.error("Сессия %s: не получен ответ с профилем", session_state.config.name)
                return None

            profile_text = messages[0].text
            import re

            searches_match = re.search(r'\*?\*?Доступно поисков:?\*?\*?\s*`?([0-9,]+)`?', profile_text)
            if not searches_match:
                searches_match = re.search(r'Available searches:?\s*`?([0-9,]+)`?', profile_text)

            if not searches_match:
                logger.error("Сессия %s: не удалось распарсить количество поисков", session_state.config.name)
                logger.debug("Текст профиля: %s", profile_text)
                return None

            searches_count = int(searches_match.group(1).replace(',', ''))

            for bot in session_state.bots:
                bot.balance = searches_count

            balance_info = [
                f"💰 <b>Баланс сессии {session_state.config.name}</b>",
                f"🔍 <b>Доступно поисков:</b> {searches_count:,}",
                "",
                "📊 <b>Статус VK ботов:</b>",
            ]
            for bot in session_state.bots:
                status = "✅" if bot.is_available and not bot.limit_reached else "⚠️"
                balance_info.append(f"{status} @{bot.username}")

            activity_match = re.search(r'Поисковая активность\s*—?\s*([0-9,]+)', profile_text)
            if activity_match:
                activity_count = int(activity_match.group(1).replace(',', ''))
                balance_info.append(f"🔍 <i>Всего выполнено поисков: {activity_count:,}</i>")

            daily_limit_match = re.search(r'Ежедневно лимит обновляется до (\d+)', profile_text)
            if daily_limit_match:
                balance_info.append(f"📅 <i>Ежедневный лимит: {daily_limit_match.group(1)} поисков</i>")

            return {
                "session": session_state.config.name,
                "count": searches_count,
                "text": "\n".join(balance_info),
            }
        except Exception as exc:
            logger.error("Ошибка при проверке баланса сессии %s: %s", session_state.config.name, exc)
            return None
        finally:
            self._persist_bot_session(main_bot)

    async def check_balance(self) -> Optional[int]:
        session_state = self._get_session_for_balance()
        if not session_state:
            logger.error("Нет активных сессий для проверки баланса")
            return None

        info = await self._get_balance_for_session(session_state)
        return info["count"] if info else None

    async def get_balance_info(self) -> Optional[str]:
        session_state = self._get_session_for_balance()
        if not session_state:
            return None
        info = await self._get_balance_for_session(session_state)
        return info["text"] if info else None

    async def get_balance_overview(self) -> str:
        sections: List[str] = []
        for session_name in self.session_order:
            session_state = self.sessions.get(session_name)
            if not session_state:
                continue

            if not session_state.is_enabled:
                sections.append(f"⚪️ <b>{session_name}</b>\nСессия отключена")
                continue

            info = await self._get_balance_for_session(session_state)
            if info and info.get("text"):
                sections.append(info["text"])
            else:
                sections.append(f"⚠️ <b>{session_name}</b>\nНе удалось получить баланс")

        if not sections:
            return "❌ Нет активных Telegram-сессий"
        return "\n\n".join(sections)

    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики по всем ботам"""
        available_bots = sum(1 for bot in self.bots if bot.is_available and bot.is_initialized)
        total_requests = sum(bot.requests_count for bot in self.bots)
        
        bots_stats = []
        for i, bot in enumerate(self.bots):
            bots_stats.append({
                "index": i + 1,
                "username": bot.username,
                "session": bot.session_name,
                "initialized": bot.is_initialized,
                "available": bot.is_available,
                "requests": bot.requests_count,
                "errors": bot.errors_count,
                "limit_reached": bot.limit_reached,
                "balance": bot.balance,
                "quota_limit": bot.quota_limit,
                "quota_used": bot.quota_used,
            })

        sessions_stats = []
        for name in self.session_order:
            session_state = self.sessions.get(name)
            if not session_state:
                continue
            session_bots = session_state.bots
            requests_total = sum(bot.requests_count for bot in session_bots)
            errors_total = sum(bot.errors_count for bot in session_bots)
            bots_on_hold = sum(1 for bot in session_bots if bot.hold_until)
            bots_limited = sum(1 for bot in session_bots if bot.limit_reached)
            bots_removed = sum(1 for bot in session_bots if bot.is_removed)
            slot_label = None
            for slot_name, slot_session in self.session_slots.items():
                if slot_session == name:
                    slot_label = slot_name
                    break

            sessions_stats.append({
                "name": name,
                "alias": self.get_session_alias(name),
                "enabled": session_state.is_enabled,
                "configured": session_state.config.enabled,
                "bots_total": len(session_bots),
                "bots_available": sum(1 for bot in session_bots if bot.is_available and not bot.limit_reached),
                "configured_bots": list(session_state.assigned_bots or []),
                "bot_names": [bot.username for bot in session_bots],
                "requests": requests_total,
                "errors": errors_total,
                "bots_on_hold": bots_on_hold,
                "bots_limited": bots_limited,
                "bots_removed": bots_removed,
                "slot": slot_label,
            })
        
        search_stats = None
        if self.stats_manager:
            snapshot = await self.stats_manager.get_snapshot()
            today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_bucket = (snapshot.get("per_day") or {}).get(today_key, {})
            search_stats = {
                "total_links_sent": (snapshot.get("totals") or {}).get("links_sent", 0),
                "total_bot_requests": (snapshot.get("totals") or {}).get("bot_requests", 0),
                "today_links_sent": today_bucket.get("links_sent", 0),
                "today_bot_requests": today_bucket.get("bot_requests", 0),
                "sessions_total": snapshot.get("sessions", {}),
                "sessions_today": today_bucket.get("sessions", {}),
                "bots_total": snapshot.get("bots", {}),
                "bots_today": today_bucket.get("bots", {}),
                "updated_at": snapshot.get("updated_at"),
            }

        return {
            "total_bots": len(self.bots),
            "available_bots": available_bots,
            "total_requests": total_requests,
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "session_mode": self.session_mode,
            "current_rotation_index": self._session_cursor,
            "bots": bots_stats,
            "sessions": sessions_stats,
            "slots": self.get_slot_assignments(),
            "search_stats": search_stats,
        }
    
    async def close(self):
        """Закрытие соединения"""
        if self._watchdog_task:
            if self._watchdog_stop:
                self._watchdog_stop.set()
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None

        for bot in self.bots:
            if bot.client:
                try:
                    self._persist_bot_session(bot)
                    async with bot.lock:
                        await bot.client.disconnect()
                except Exception:
                    pass
                finally:
                    bot.client = None
        
        logger.info("✅ Соединение закрыто")


# Для обратной совместимости создаем алиас
VKService = VKMultiBotService
