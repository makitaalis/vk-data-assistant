"""
Улучшенный сервис кеширования с поддержкой TTL и автоматической очисткой
"""

import json
import redis.asyncio as aioredis
from typing import Any, Optional, List, Dict
from datetime import datetime, timedelta
import logging
import asyncio
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class ImprovedCacheService:
    """Улучшенный сервис для работы с Redis кешем"""

    # Константы времени жизни кеша (в секундах)
    TTL_SEARCH_RESULTS = 3600  # 1 час для результатов поиска
    TTL_USER_DATA = 86400  # 24 часа для данных пользователей
    TTL_PROCESSING = 300  # 5 минут для временных данных обработки
    TTL_FILE_INFO = 7200  # 2 часа для информации о файлах
    TTL_STATISTICS = 1800  # 30 минут для статистики

    # Префиксы ключей
    PREFIX_SEARCH = "vk_data:search:"
    PREFIX_USER = "vk_data:user:"
    PREFIX_PROCESSING = "processing:"
    PREFIX_FILE = "vk_data:file:"
    PREFIX_STATS = "vk_data:stats:"
    PREFIX_TEMP = "temp:"

    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or "redis://localhost:6379/0"
        self.redis: Optional[aioredis.Redis] = None
        self._lock_prefix = "lock:"
        self._connected = False

    async def connect(self):
        """Подключение к Redis"""
        try:
            self.redis = await aioredis.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=10
            )
            await self.redis.ping()
            self._connected = True
            logger.info("✅ Подключено к Redis")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Redis: {e}")
            self._connected = False
            raise

    async def disconnect(self):
        """Отключение от Redis"""
        if self.redis:
            await self.redis.close()
            self._connected = False
            logger.info("Отключено от Redis")

    def _ensure_connected(self):
        """Проверка подключения"""
        if not self._connected or not self.redis:
            raise RuntimeError("Redis не подключен. Вызовите connect() сначала.")

    async def set_with_ttl(self, key: str, value: Any, ttl: int = None) -> bool:
        """Сохранение значения с временем жизни"""
        self._ensure_connected()
        try:
            serialized = json.dumps(value, ensure_ascii=False, default=str)
            if ttl:
                return await self.redis.setex(key, ttl, serialized)
            else:
                return await self.redis.set(key, serialized)
        except Exception as e:
            logger.error(f"Ошибка при сохранении в кеш: {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из кеша"""
        self._ensure_connected()
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except json.JSONDecodeError:
            logger.error(f"Ошибка декодирования JSON для ключа: {key}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при чтении из кеша: {e}")
            return None

    async def delete(self, key: str) -> bool:
        """Удаление ключа"""
        self._ensure_connected()
        try:
            return bool(await self.redis.delete(key))
        except Exception as e:
            logger.error(f"Ошибка при удалении из кеша: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """Удаление всех ключей по паттерну"""
        self._ensure_connected()
        try:
            keys = []
            async for key in self.redis.scan_iter(match=pattern):
                keys.append(key)

            if keys:
                return await self.redis.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Ошибка при удалении по паттерну: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """Проверка существования ключа"""
        self._ensure_connected()
        try:
            return bool(await self.redis.exists(key))
        except Exception as e:
            logger.error(f"Ошибка при проверке существования ключа: {e}")
            return False

    async def get_ttl(self, key: str) -> int:
        """Получение оставшегося времени жизни ключа"""
        self._ensure_connected()
        try:
            ttl = await self.redis.ttl(key)
            return ttl if ttl > 0 else 0
        except Exception as e:
            logger.error(f"Ошибка при получении TTL: {e}")
            return 0

    async def extend_ttl(self, key: str, ttl: int) -> bool:
        """Продление времени жизни ключа"""
        self._ensure_connected()
        try:
            return await self.redis.expire(key, ttl)
        except Exception as e:
            logger.error(f"Ошибка при продлении TTL: {e}")
            return False

    # Методы для работы с результатами поиска
    async def save_search_results(self, search_key: str, results: List[Dict], ttl: int = None) -> bool:
        """Сохранение результатов поиска"""
        key = f"{self.PREFIX_SEARCH}{search_key}"
        ttl = ttl or self.TTL_SEARCH_RESULTS
        return await self.set_with_ttl(key, results, ttl)

    async def get_search_results(self, search_key: str) -> Optional[List[Dict]]:
        """Получение результатов поиска"""
        key = f"{self.PREFIX_SEARCH}{search_key}"
        return await self.get(key)

    # Методы для работы с данными пользователей
    async def save_user_data(self, user_id: int, data: Dict, ttl: int = None) -> bool:
        """Сохранение данных пользователя"""
        key = f"{self.PREFIX_USER}{user_id}"
        ttl = ttl or self.TTL_USER_DATA
        return await self.set_with_ttl(key, data, ttl)

    async def get_user_data(self, user_id: int) -> Optional[Dict]:
        """Получение данных пользователя"""
        key = f"{self.PREFIX_USER}{user_id}"
        return await self.get(key)

    # Методы для работы с обработкой
    async def set_processing_status(self, task_id: str, status: Dict) -> bool:
        """Установка статуса обработки"""
        key = f"{self.PREFIX_PROCESSING}{task_id}"
        return await self.set_with_ttl(key, status, self.TTL_PROCESSING)

    async def get_processing_status(self, task_id: str) -> Optional[Dict]:
        """Получение статуса обработки"""
        key = f"{self.PREFIX_PROCESSING}{task_id}"
        return await self.get(key)

    # Методы для работы с файлами
    async def save_file_info(self, file_id: str, info: Dict) -> bool:
        """Сохранение информации о файле"""
        key = f"{self.PREFIX_FILE}{file_id}"
        return await self.set_with_ttl(key, info, self.TTL_FILE_INFO)

    async def get_file_info(self, file_id: str) -> Optional[Dict]:
        """Получение информации о файле"""
        key = f"{self.PREFIX_FILE}{file_id}"
        return await self.get(key)

    # Методы для работы со статистикой
    async def save_statistics(self, stat_type: str, data: Dict) -> bool:
        """Сохранение статистики"""
        key = f"{self.PREFIX_STATS}{stat_type}"
        return await self.set_with_ttl(key, data, self.TTL_STATISTICS)

    async def get_statistics(self, stat_type: str) -> Optional[Dict]:
        """Получение статистики"""
        key = f"{self.PREFIX_STATS}{stat_type}"
        return await self.get(key)

    # Методы для работы с блокировками
    @asynccontextmanager
    async def lock(self, resource: str, timeout: int = 10):
        """Контекстный менеджер для блокировок"""
        lock_key = f"{self._lock_prefix}{resource}"
        lock_value = f"{datetime.now().timestamp()}"

        # Пытаемся установить блокировку
        acquired = await self.redis.set(lock_key, lock_value, nx=True, ex=timeout)

        try:
            if acquired:
                yield True
            else:
                yield False
        finally:
            if acquired:
                # Удаляем блокировку только если она наша
                current_value = await self.redis.get(lock_key)
                if current_value == lock_value:
                    await self.redis.delete(lock_key)

    # Методы очистки
    async def clear_expired(self) -> Dict[str, int]:
        """Очистка истекших ключей (Redis делает это автоматически)"""
        stats = {
            "search": 0,
            "processing": 0,
            "temp": 0
        }

        # Очистка временных ключей старше 1 часа
        temp_keys = []
        async for key in self.redis.scan_iter(match=f"{self.PREFIX_TEMP}*"):
            ttl = await self.redis.ttl(key)
            if ttl == -1:  # Ключ без TTL
                temp_keys.append(key)

        if temp_keys:
            stats["temp"] = await self.redis.delete(*temp_keys)

        return stats

    async def clear_by_prefix(self, prefix: str) -> int:
        """Очистка всех ключей с заданным префиксом"""
        return await self.delete_pattern(f"{prefix}*")

    async def get_memory_usage(self) -> Dict[str, Any]:
        """Получение информации об использовании памяти"""
        self._ensure_connected()
        try:
            info = await self.redis.info("memory")
            return {
                "used_memory": info.get("used_memory_human", "N/A"),
                "used_memory_peak": info.get("used_memory_peak_human", "N/A"),
                "total_keys": await self.redis.dbsize()
            }
        except Exception as e:
            logger.error(f"Ошибка при получении информации о памяти: {e}")
            return {}

    async def get_keys_by_pattern(self, pattern: str) -> List[str]:
        """Получение списка ключей по паттерну"""
        self._ensure_connected()
        keys = []
        try:
            async for key in self.redis.scan_iter(match=pattern):
                keys.append(key)
        except Exception as e:
            logger.error(f"Ошибка при получении ключей: {e}")
        return keys

    # Batch операции
    async def mget(self, keys: List[str]) -> List[Optional[Any]]:
        """Получение нескольких значений одновременно"""
        self._ensure_connected()
        try:
            values = await self.redis.mget(keys)
            return [json.loads(v) if v else None for v in values]
        except Exception as e:
            logger.error(f"Ошибка при batch чтении: {e}")
            return [None] * len(keys)

    async def mset(self, mapping: Dict[str, Any], ttl: int = None) -> bool:
        """Установка нескольких значений одновременно"""
        self._ensure_connected()
        try:
            # Сериализуем все значения
            serialized = {k: json.dumps(v, ensure_ascii=False, default=str)
                          for k, v in mapping.items()}

            # Устанавливаем значения
            await self.redis.mset(serialized)

            # Если нужно TTL, устанавливаем для каждого ключа
            if ttl:
                pipe = self.redis.pipeline()
                for key in serialized:
                    pipe.expire(key, ttl)
                await pipe.execute()

            return True
        except Exception as e:
            logger.error(f"Ошибка при batch записи: {e}")
            return False


# Пример использования
async def example_usage():
    """Пример использования улучшенного кеш-сервиса"""
    cache = ImprovedCacheService()
    await cache.connect()

    try:
        # Сохранение результатов поиска
        search_results = [
            {"id": 1, "name": "User 1", "birth_year": 1990},
            {"id": 2, "name": "User 2", "birth_year": 1991}
        ]
        await cache.save_search_results("search_1990_moscow", search_results)

        # Работа с блокировками
        async with cache.lock("process_file_123") as acquired:
            if acquired:
                print("Получена блокировка для обработки файла")
                # Выполняем обработку
                await asyncio.sleep(1)
            else:
                print("Не удалось получить блокировку")

        # Получение статистики памяти
        memory_info = await cache.get_memory_usage()
        print(f"Использование памяти: {memory_info}")

    finally:
        await cache.disconnect()


if __name__ == "__main__":
    asyncio.run(example_usage())