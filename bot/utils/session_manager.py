"""Управление пользовательскими сессиями"""

import json
import logging
from typing import Dict, Any, Optional
import redis.asyncio as redis

from bot.config import (
    REDIS_URL,
    REDIS_SESSION_PREFIX,
    REDIS_DISCLAIMER_PREFIX,
    REDIS_SESSION_TTL,
    REDIS_DISCLAIMER_TTL,
    USE_REDIS
)
from db_module import VKDatabase

logger = logging.getLogger("session_manager")

# Глобальный клиент Redis
redis_client: Optional[redis.Redis] = None

# Локальное хранилище для случаев когда Redis недоступен
local_sessions: Dict[int, Dict[str, Any]] = {}
local_disclaimers: Dict[int, bool] = {}


async def init_redis():
    """Инициализация подключения к Redis"""
    global redis_client

    if not USE_REDIS:
        logger.info("Redis отключен в конфигурации")
        return

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
    session_key = f"{REDIS_SESSION_PREFIX}{user_id}"

    if redis_client:
        try:
            session_data = await redis_client.get(session_key)
            if session_data:
                return json.loads(session_data)
        except Exception as e:
            logger.error(f"Ошибка чтения из Redis: {e}")

    # Fallback на локальное хранилище
    return local_sessions.get(user_id, {})


async def save_user_session(user_id: int, session_data: Dict[str, Any]):
    """Сохранение сессии пользователя в Redis или память"""
    session_key = f"{REDIS_SESSION_PREFIX}{user_id}"

    if redis_client:
        try:
            await redis_client.setex(
                session_key,
                REDIS_SESSION_TTL,
                json.dumps(session_data, ensure_ascii=False)
            )
            return
        except Exception as e:
            logger.error(f"Ошибка записи в Redis: {e}")

    # Fallback на локальное хранилище
    local_sessions[user_id] = session_data


async def clear_user_session(user_id: int):
    """Очистка сессии пользователя"""
    session_key = f"{REDIS_SESSION_PREFIX}{user_id}"

    if redis_client:
        try:
            await redis_client.delete(session_key)
        except Exception as e:
            logger.error(f"Ошибка удаления из Redis: {e}")

    # Также очищаем локальное хранилище
    if user_id in local_sessions:
        del local_sessions[user_id]


async def check_user_accepted_disclaimer(user_id: int) -> bool:
    """Проверка, принял ли пользователь условия использования"""
    # Сначала проверяем Redis/память
    if redis_client:
        try:
            accepted = await redis_client.get(f"{REDIS_DISCLAIMER_PREFIX}{user_id}")
            if accepted:
                return accepted == "1"
        except Exception:
            pass
    elif user_id in local_disclaimers:
        return local_disclaimers[user_id]

    # Если нет в кеше, проверяем БД (будет передана через dependency injection)
    return False


async def set_user_accepted_disclaimer(user_id: int, user_data: Optional[Dict] = None, db: Optional[VKDatabase] = None):
    """Отметка о принятии условий использования"""
    # Сохраняем в Redis/память
    if redis_client:
        try:
            await redis_client.setex(
                f"{REDIS_DISCLAIMER_PREFIX}{user_id}",
                REDIS_DISCLAIMER_TTL,
                "1"
            )
        except Exception:
            pass
    else:
        local_disclaimers[user_id] = True

    # Сохраняем в БД
    if db:
        await db.set_user_accepted_disclaimer(user_id, user_data)


async def get_session_value(user_id: int, key: str, default: Any = None) -> Any:
    """Получение конкретного значения из сессии"""
    session = await get_user_session(user_id)
    return session.get(key, default)


async def set_session_value(user_id: int, key: str, value: Any):
    """Установка конкретного значения в сессии"""
    session = await get_user_session(user_id)
    session[key] = value
    await save_user_session(user_id, session)


async def delete_session_value(user_id: int, key: str):
    """Удаление конкретного значения из сессии"""
    session = await get_user_session(user_id)
    if key in session:
        del session[key]
        await save_user_session(user_id, session)


async def close_redis():
    """Закрытие соединения с Redis"""
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None