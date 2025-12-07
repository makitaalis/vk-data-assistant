"""
Сервис для динамического управления настройками бота через Redis
Автор: Claude Code
Дата: 2025-10-16
"""

import json
import logging
from typing import Optional, List, Dict, Any
from redis import asyncio as aioredis

logger = logging.getLogger(__name__)


class ConfigService:
    """
    Сервис для управления динамическими настройками бота через Redis.
    Позволяет изменять настройки без перезапуска бота.
    """

    # Ключи Redis для хранения настроек
    KEY_USE_CACHE = "bot:config:use_cache"
    KEY_ADMIN_USE_CACHE = "bot:config:admin_use_cache"
    KEY_ENABLE_DUPLICATE_REMOVAL = "bot:config:enable_duplicate_removal"
    KEY_SESSION_MODE = "bot:session:mode"
    KEY_SESSION_ENABLED_TEMPLATE = "bot:session:enabled:{name}"
    KEY_SESSION_SLOT_TEMPLATE = "bot:session:slot:{slot}"
    KEY_SESSION_REGISTRY = "bot:session:registry"
    KEY_SESSION_BOTS = "bot:session:bots"
    KEY_VK_BOT_POOL = "bot:vk_bot_pool"
    KEY_VK_BOT_QUOTAS = "bot:vk_bot_quotas"
    SESSION_SLOTS = ("slot_a", "slot_b")

    def __init__(self, redis_client: aioredis.Redis):
        """
        Инициализация сервиса

        Args:
            redis_client: Клиент Redis для хранения настроек
        """
        self.redis = redis_client
        logger.info("🔧 ConfigService инициализирован")

    async def initialize_defaults(self, use_cache: bool, admin_use_cache: bool, enable_duplicate_removal: bool):
        """
        Инициализация настроек по умолчанию из config.py (только при первом запуске)

        Args:
            use_cache: Значение USE_CACHE из config.py
            admin_use_cache: Значение ADMIN_USE_CACHE из config.py
            enable_duplicate_removal: Значение ENABLE_DUPLICATE_REMOVAL из config.py
        """
        # Проверяем, существуют ли уже настройки
        existing_use_cache = await self.redis.get(self.KEY_USE_CACHE)

        if existing_use_cache is None:
            # Первый запуск - сохраняем дефолтные значения из config.py
            await self.set_use_cache(use_cache)
            await self.set_admin_use_cache(admin_use_cache)
            await self.set_enable_duplicate_removal(enable_duplicate_removal)
            await self.set_session_mode("primary")
            logger.info(
                f"✅ Настройки инициализированы: "
                f"USE_CACHE={use_cache}, "
                f"ADMIN_USE_CACHE={admin_use_cache}, "
                f"ENABLE_DUPLICATE_REMOVAL={enable_duplicate_removal}, "
                f"SESSION_MODE=primary"
            )
        else:
            current_settings = await self.get_all_settings()
            logger.info(f"ℹ️ Настройки уже существуют в Redis: {current_settings}")

    # ==================== USE_CACHE ====================

    async def get_use_cache(self) -> bool:
        """Получить текущее значение USE_CACHE"""
        value = await self.redis.get(self.KEY_USE_CACHE)
        return value == b'1' if value else True  # Default: True

    async def set_use_cache(self, enabled: bool):
        """Установить значение USE_CACHE"""
        await self.redis.set(self.KEY_USE_CACHE, '1' if enabled else '0')
        logger.info(f"⚙️ USE_CACHE изменен на: {enabled}")

    async def toggle_use_cache(self) -> bool:
        """Переключить USE_CACHE (вкл/выкл). Возвращает новое значение"""
        current = await self.get_use_cache()
        new_value = not current
        await self.set_use_cache(new_value)
        return new_value

    # ==================== ADMIN_USE_CACHE ====================

    async def get_admin_use_cache(self) -> bool:
        """Получить текущее значение ADMIN_USE_CACHE"""
        value = await self.redis.get(self.KEY_ADMIN_USE_CACHE)
        return value == b'1' if value else True  # Default: True

    async def set_admin_use_cache(self, enabled: bool):
        """Установить значение ADMIN_USE_CACHE"""
        await self.redis.set(self.KEY_ADMIN_USE_CACHE, '1' if enabled else '0')
        logger.info(f"⚙️ ADMIN_USE_CACHE изменен на: {enabled}")

    async def toggle_admin_use_cache(self) -> bool:
        """Переключить ADMIN_USE_CACHE (вкл/выкл). Возвращает новое значение"""
        current = await self.get_admin_use_cache()
        new_value = not current
        await self.set_admin_use_cache(new_value)
        return new_value

    # ==================== ENABLE_DUPLICATE_REMOVAL ====================

    async def get_enable_duplicate_removal(self) -> bool:
        """Получить текущее значение ENABLE_DUPLICATE_REMOVAL"""
        value = await self.redis.get(self.KEY_ENABLE_DUPLICATE_REMOVAL)
        return value == b'1' if value else True  # Default: True

    async def set_enable_duplicate_removal(self, enabled: bool):
        """Установить значение ENABLE_DUPLICATE_REMOVAL"""
        await self.redis.set(self.KEY_ENABLE_DUPLICATE_REMOVAL, '1' if enabled else '0')
        logger.info(f"⚙️ ENABLE_DUPLICATE_REMOVAL изменен на: {enabled}")

    async def toggle_enable_duplicate_removal(self) -> bool:
        """Переключить ENABLE_DUPLICATE_REMOVAL (вкл/выкл). Возвращает новое значение"""
        current = await self.get_enable_duplicate_removal()
        new_value = not current
        await self.set_enable_duplicate_removal(new_value)
        return new_value

    # ==================== SESSION MODE ====================

    async def get_session_mode(self) -> str:
        value = await self.redis.get(self.KEY_SESSION_MODE)
        if not value:
            return "primary"
        return value.decode("utf-8")

    async def set_session_mode(self, mode: str):
        if mode not in {"primary", "secondary", "both"}:
            raise ValueError(f"Некорректный режим сессии: {mode}")
        await self.redis.set(self.KEY_SESSION_MODE, mode)
        logger.info(f"⚙️ SESSION_MODE изменен на: {mode}")

    async def set_session_enabled(self, session_name: str, enabled: bool):
        key = self.KEY_SESSION_ENABLED_TEMPLATE.format(name=session_name)
        await self.redis.set(key, '1' if enabled else '0')
        logger.info("⚙️ SESSION %s -> %s", session_name, "ON" if enabled else "OFF")

    async def get_session_enabled(self, session_name: str, default: bool = True) -> bool:
        key = self.KEY_SESSION_ENABLED_TEMPLATE.format(name=session_name)
        value = await self.redis.get(key)
        if value is None:
            return default
        return value == b'1'

    # ==================== SESSION SLOTS ====================

    async def get_session_slot(self, slot: str) -> Optional[str]:
        slot_key = slot.lower()
        if slot_key not in self.SESSION_SLOTS:
            raise ValueError(f"Неизвестный слот: {slot}")
        key = self.KEY_SESSION_SLOT_TEMPLATE.format(slot=slot_key)
        value = await self.redis.get(key)
        return value.decode("utf-8") if value else None

    async def set_session_slot(self, slot: str, session_name: Optional[str]):
        slot_key = slot.lower()
        if slot_key not in self.SESSION_SLOTS:
            raise ValueError(f"Неизвестный слот: {slot}")
        key = self.KEY_SESSION_SLOT_TEMPLATE.format(slot=slot_key)
        if session_name:
            await self.redis.set(key, session_name)
            logger.info("⚙️ Slot %s назначен на %s", slot_key, session_name)
        else:
            await self.redis.delete(key)
            logger.info("⚙️ Slot %s очищен", slot_key)

    async def get_session_slots(self) -> dict:
        slots = {}
        for slot in self.SESSION_SLOTS:
            slots[slot] = await self.get_session_slot(slot)
        return slots

    # ==================== SESSION REGISTRY ====================

    async def list_registered_sessions(self) -> List[Dict[str, Any]]:
        """Возвращает список динамически добавленных сессий."""
        raw = await self.redis.hgetall(self.KEY_SESSION_REGISTRY)
        sessions: List[Dict[str, Any]] = []
        for name_bytes, payload_bytes in raw.items():
            try:
                name = name_bytes.decode("utf-8")
            except Exception:
                continue
            try:
                payload = json.loads(payload_bytes)
            except Exception:
                payload = {}
            payload.setdefault("name", name)
            sessions.append(payload)
        return sessions

    async def upsert_session_definition(self, name: str, phone: Optional[str], enabled: bool = True):
        if not name:
            raise ValueError("Имя сессии не может быть пустым")
        payload = json.dumps(
            {
                "name": name,
                "phone": phone,
                "enabled": bool(enabled),
            },
            ensure_ascii=False,
        )
        await self.redis.hset(self.KEY_SESSION_REGISTRY, name, payload)
        logger.info("💾 Сессия %s сохранена в реестре (enabled=%s)", name, enabled)

    async def remove_session_definition(self, name: str):
        if not name:
            return
        await self.redis.hdel(self.KEY_SESSION_REGISTRY, name)
        logger.info("🗑️ Сессия %s удалена из реестра", name)

    async def clear_session_registry(self):
        """Полностью очищает реестр сессий (слоты не трогает)."""
        await self.redis.delete(self.KEY_SESSION_REGISTRY)
        logger.info("🧹 Реестр сессий очищен")

    # ==================== SESSION BOT ASSIGNMENTS ====================

    async def get_session_bots(self, session_name: str) -> List[str]:
        if not session_name:
            return []
        raw = await self.redis.hget(self.KEY_SESSION_BOTS, session_name)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            return [bot for bot in data if isinstance(bot, str) and bot]
        except Exception:
            logger.warning("⚠️ Не удалось прочитать список ботов для %s", session_name)
            return []

    async def set_session_bots(self, session_name: str, usernames: List[str]):
        if not session_name:
            return
        clean = [bot.lstrip("@") for bot in usernames if bot]
        if clean:
            payload = json.dumps(clean, ensure_ascii=False)
            await self.redis.hset(self.KEY_SESSION_BOTS, session_name, payload)
            logger.info("🤖 Для %s назначены VK боты: %s", session_name, ", ".join(clean))
        else:
            await self.redis.hdel(self.KEY_SESSION_BOTS, session_name)
            logger.info("🤖 Для %s используется полный список VK ботов", session_name)

    async def get_all_session_bots(self) -> Dict[str, List[str]]:
        raw = await self.redis.hgetall(self.KEY_SESSION_BOTS)
        result: Dict[str, List[str]] = {}
        for name_bytes, payload_bytes in raw.items():
            try:
                name = name_bytes.decode("utf-8")
            except Exception:
                continue
            try:
                payload = json.loads(payload_bytes)
                bots = [bot for bot in payload if isinstance(bot, str) and bot]
            except Exception:
                bots = []
            if bots:
                result[name] = bots
        return result

    async def clear_all_session_bots(self):
        """Удаляет все назначения VK ботов для сессий."""
        await self.redis.delete(self.KEY_SESSION_BOTS)
        logger.info("🧹 Назначения VK ботов очищены")

    # ==================== VK BOT QUOTAS ====================

    async def get_vk_bot_quotas(self) -> Dict[str, int]:
        raw = await self.redis.hgetall(self.KEY_VK_BOT_QUOTAS)
        quotas: Dict[str, int] = {}
        for name_bytes, payload_bytes in raw.items():
            try:
                name = name_bytes.decode("utf-8").lstrip("@")
                limit = int(payload_bytes.decode("utf-8"))
                if limit > 0:
                    quotas[name] = limit
            except Exception:
                continue
        return quotas

    async def set_vk_bot_quota(self, username: str, limit: int):
        username = username.lstrip("@")
        if not username:
            return
        if limit <= 0:
            await self.redis.hdel(self.KEY_VK_BOT_QUOTAS, username)
            return
        await self.redis.hset(self.KEY_VK_BOT_QUOTAS, username, str(limit))

    async def remove_vk_bot_quota(self, username: str):
        username = username.lstrip("@")
        if not username:
            return
        await self.redis.hdel(self.KEY_VK_BOT_QUOTAS, username)

    # ==================== VK BOT POOL ====================

    async def get_vk_bot_pool(self) -> List[str]:
        raw = await self.redis.get(self.KEY_VK_BOT_POOL)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            return [bot.lstrip("@") for bot in data if isinstance(bot, str) and bot.strip()]
        except Exception:
            logger.warning("⚠️ Не удалось прочитать VK бот-пул из Redis")
            return []

    async def set_vk_bot_pool(self, usernames: List[str]):
        clean = [bot.lstrip("@") for bot in usernames if bot and bot.strip()]
        if clean:
            payload = json.dumps(clean, ensure_ascii=False)
            await self.redis.set(self.KEY_VK_BOT_POOL, payload)
            logger.info("🤖 VK бот-пул обновлён: %s", ", ".join(clean))
        else:
            await self.redis.delete(self.KEY_VK_BOT_POOL)
            logger.info("🤖 VK бот-пул очищен")

    async def add_vk_bot(self, username: str) -> List[str]:
        bots = await self.get_vk_bot_pool()
        normalized = username.lstrip("@")
        if normalized and normalized not in bots:
            bots.append(normalized)
            await self.set_vk_bot_pool(bots)
        return bots

    async def remove_vk_bot(self, username: str) -> List[str]:
        bots = await self.get_vk_bot_pool()
        normalized = username.lstrip("@")
        filtered = [b for b in bots if b != normalized]
        if filtered != bots:
            await self.set_vk_bot_pool(filtered)
        return filtered

    # ==================== Утилиты ====================

    async def get_all_settings(self) -> dict:
        """Получить все настройки одним запросом"""
        base = {
            "use_cache": await self.get_use_cache(),
            "admin_use_cache": await self.get_admin_use_cache(),
            "enable_duplicate_removal": await self.get_enable_duplicate_removal()
        }
        base["session_mode"] = await self.get_session_mode()
        return base

    async def get_settings_display(self) -> str:
        """Получить человекочитаемое отображение всех настроек"""
        settings = await self.get_all_settings()

        def status_icon(enabled: bool) -> str:
            return "✅ ВКЛ" if enabled else "❌ ВЫКЛ"

        return (
            f"⚙️ **Текущие настройки:**\n\n"
            f"🗄️ **Кеш для всех пользователей:** {status_icon(settings['use_cache'])}\n"
            f"   └─ Использовать данные из БД без повторной проверки\n\n"
            f"👑 **Кеш для администраторов:** {status_icon(settings['admin_use_cache'])}\n"
            f"   └─ Админы видят статистику кеша и используют кеш\n\n"
            f"🔍 **Проверка дубликатов:** {status_icon(settings['enable_duplicate_removal'])}\n"
            f"   └─ Удаление повторяющихся ссылок и телефонов\n\n"
            f"📡 **Режим Telegram-сессий:** {settings['session_mode']}"
        )


# Глобальный экземпляр сервиса (будет инициализирован в main.py)
config_service: Optional[ConfigService] = None


def get_config_service() -> ConfigService:
    """Получить глобальный экземпляр ConfigService"""
    if config_service is None:
        raise RuntimeError("ConfigService не инициализирован. Вызовите initialize_config_service() в main.py")
    return config_service


async def initialize_config_service(redis_client: aioredis.Redis, use_cache: bool, admin_use_cache: bool, enable_duplicate_removal: bool):
    """
    Инициализация глобального ConfigService

    Args:
        redis_client: Клиент Redis
        use_cache: Начальное значение USE_CACHE из config.py
        admin_use_cache: Начальное значение ADMIN_USE_CACHE из config.py
        enable_duplicate_removal: Начальное значение ENABLE_DUPLICATE_REMOVAL из config.py
    """
    global config_service
    config_service = ConfigService(redis_client)
    await config_service.initialize_defaults(use_cache, admin_use_cache, enable_duplicate_removal)
    return config_service
