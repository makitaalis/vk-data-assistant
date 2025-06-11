import asyncpg
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set, Any
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv

logger = logging.getLogger("database")

# Загрузка конфигурации
load_dotenv()

# PostgreSQL конфигурация
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "vk_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}


class VKDatabase:
    """Класс для работы с PostgreSQL базой данных результатов VK"""

    def __init__(self):
        self._pool = None
        self._initialized = False

    async def init(self):
        """Инициализация пула соединений и структуры БД"""
        if self._initialized:
            return

        try:
            # Создаем пул соединений с меньшим количеством подключений
            self._pool = await asyncpg.create_pool(
                **DB_CONFIG,
                min_size=2,
                max_size=10,
                command_timeout=60
            )

            # Инициализируем структуру БД
            await self._init_db()
            self._initialized = True
            logger.info("✅ PostgreSQL база данных инициализирована")
        except asyncpg.exceptions.TooManyConnectionsError:
            logger.error("❌ Превышен лимит подключений к PostgreSQL")
            logger.error("Проверьте настройки max_connections в postgresql.conf")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            if self._pool:
                await self._pool.close()
            raise

    async def close(self):
        """Закрытие пула соединений"""
        if self._pool:
            await self._pool.close()
            self._initialized = False

    @asynccontextmanager
    async def acquire(self):
        """Получение соединения из пула"""
        if not self._initialized:
            raise RuntimeError("Database not initialized. Call init() first.")
        async with self._pool.acquire() as connection:
            yield connection

    async def _init_db(self):
        """Инициализация структуры базы данных"""
        # Используем прямое подключение из пула, а не acquire()
        async with self._pool.acquire() as conn:
            # Таблица результатов VK
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS vk_results (
                    link TEXT PRIMARY KEY,
                    phones JSONB DEFAULT '[]'::jsonb,
                    full_name TEXT DEFAULT '',
                    birth_date TEXT DEFAULT '',
                    checked_at TIMESTAMP DEFAULT NOW(),
                    checked_by_user_id BIGINT,
                    found_data BOOLEAN DEFAULT FALSE,
                    source TEXT DEFAULT 'search',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Индексы для vk_results
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_vk_user_id 
                ON vk_results(checked_by_user_id)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_vk_checked_at 
                ON vk_results(checked_at)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_vk_found_data 
                ON vk_results(found_data)
            """)

            # Таблица для связи телефонов и ссылок
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS phone_links (
                    phone TEXT,
                    link TEXT REFERENCES vk_results(link) ON DELETE CASCADE,
                    PRIMARY KEY (phone, link)
                )
            """)

            # Индекс для быстрого поиска по телефону
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_phone 
                ON phone_links(phone)
            """)

            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    accepted_disclaimer BOOLEAN DEFAULT FALSE,
                    accepted_at TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_activity TIMESTAMP DEFAULT NOW()
                )
            """)

            # Таблица логов действий
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS action_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """)

            # Индекс для логов
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_logs_user_id 
                ON action_logs(user_id)
            """)

            # Создаем функцию для автоматического обновления updated_at
            await conn.execute("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
            """)

            # Создаем триггер для vk_results
            await conn.execute("""
                DROP TRIGGER IF EXISTS update_vk_results_updated_at ON vk_results;
                CREATE TRIGGER update_vk_results_updated_at 
                BEFORE UPDATE ON vk_results 
                FOR EACH ROW 
                EXECUTE FUNCTION update_updated_at_column();
            """)

    async def check_duplicates_extended(self, links: List[str]) -> Dict[str, Any]:
        """
        Проверяет список ссылок на наличие в базе

        Возвращает словарь:
        {
            "new": ["link1", "link2"],  # Новые ссылки
            "duplicates_with_data": {  # Дубликаты с найденными данными
                "link3": {"phones": [...], "full_name": "...", ...}
            },
            "duplicates_no_data": ["link4", "link5"]  # Дубликаты без данных
        }
        """
        result = {
            "new": [],
            "duplicates_with_data": {},
            "duplicates_no_data": []
        }

        if not links:
            return result

        async with self.acquire() as conn:
            # Получаем все существующие ссылки одним запросом
            rows = await conn.fetch("""
                SELECT link, phones, full_name, birth_date, found_data 
                FROM vk_results 
                WHERE link = ANY($1::text[])
            """, links)

            existing_links = {}
            for row in rows:
                # Обработка phones из JSONB
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                link_data = {
                    "link": row["link"],
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "found_data": row["found_data"]
                }
                existing_links[row["link"]] = link_data

        # Классифицируем ссылки
        for link in links:
            if link not in existing_links:
                result["new"].append(link)
            else:
                data = existing_links[link]
                if data["found_data"]:
                    result["duplicates_with_data"][link] = data
                else:
                    result["duplicates_no_data"].append(link)

        return result

    async def check_phone_duplicates(self, phones: List[str]) -> Dict[str, List[Dict[str, str]]]:
        """
        Проверяет телефоны на наличие в базе

        Returns:
            Dict: {phone: [{"link": "...", "full_name": "...", "birth_date": "..."}, ...]}
        """
        if not phones:
            return {}

        results = {}
        async with self.acquire() as conn:
            # Получаем все связи телефонов одним запросом
            rows = await conn.fetch("""
                SELECT DISTINCT pl.phone, pl.link, vr.full_name, vr.birth_date
                FROM phone_links pl
                JOIN vk_results vr ON pl.link = vr.link
                WHERE pl.phone = ANY($1::text[])
                ORDER BY pl.phone, pl.link
            """, phones)

            # Группируем по телефонам
            for row in rows:
                phone = row["phone"]
                if phone not in results:
                    results[phone] = []

                results[phone].append({
                    "link": row["link"],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                })

        return results

    async def check_both_duplicates(self, links: List[str], phones: List[str]) -> Dict[str, Any]:
        """
        Проверяет и ссылки и телефоны на дубликаты одновременно

        Returns:
            {
                "duplicate_links": Set[str],  # Ссылки, которые уже есть в БД
                "duplicate_phones": Set[str],  # Телефоны, которые уже есть в БД
                "all_duplicates": bool  # True если есть хоть один дубликат
            }
        """
        duplicate_links = set()
        duplicate_phones = set()

        async with self.acquire() as conn:
            # Проверяем ссылки
            if links:
                link_rows = await conn.fetch("""
                    SELECT link FROM vk_results WHERE link = ANY($1::text[])
                """, links)
                duplicate_links = {row["link"] for row in link_rows}

            # Проверяем телефоны
            if phones:
                phone_rows = await conn.fetch("""
                    SELECT DISTINCT phone FROM phone_links WHERE phone = ANY($1::text[])
                """, phones)
                duplicate_phones = {row["phone"] for row in phone_rows}

        return {
            "duplicate_links": duplicate_links,
            "duplicate_phones": duplicate_phones,
            "all_duplicates": bool(duplicate_links or duplicate_phones)
        }

    async def find_links_by_phone(self, phone: str) -> List[Dict[str, Any]]:
        """
        Находит все ссылки, связанные с телефоном

        Args:
            phone: Номер телефона для поиска

        Returns:
            List[Dict]: Список найденных записей
        """
        results = []

        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT vr.link, vr.phones, vr.full_name, vr.birth_date, vr.checked_at
                FROM phone_links pl
                JOIN vk_results vr ON pl.link = vr.link
                WHERE pl.phone = $1
                ORDER BY vr.checked_at DESC
            """, phone)

            for row in rows:
                # Обработка phones из JSONB
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                results.append({
                    "link": row["link"],
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "checked_at": row["checked_at"]
                })

        return results

    async def save_result(self, link: str, result_data: Dict[str, Any], user_id: int, source: str = "search"):
        """Сохраняет результат проверки ссылки"""
        phones = result_data.get("phones", [])
        full_name = result_data.get("full_name", "")
        birth_date = result_data.get("birth_date", "")
        found_data = bool(phones or full_name or birth_date)

        try:
            async with self.acquire() as conn:
                # Используем транзакцию
                async with conn.transaction():
                    # Сохраняем или обновляем результат
                    await conn.execute("""
                        INSERT INTO vk_results 
                        (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data, source)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (link) DO UPDATE SET
                            phones = EXCLUDED.phones,
                            full_name = EXCLUDED.full_name,
                            birth_date = EXCLUDED.birth_date,
                            checked_at = EXCLUDED.checked_at,
                            checked_by_user_id = EXCLUDED.checked_by_user_id,
                            found_data = EXCLUDED.found_data,
                            source = EXCLUDED.source
                    """, link, json.dumps(phones), full_name, birth_date,
                                       datetime.now(), user_id, found_data, source)

                    # Удаляем старые записи телефонов для этой ссылки
                    await conn.execute("DELETE FROM phone_links WHERE link = $1", link)

                    # Добавляем новые телефоны
                    if phones:
                        phone_data = [(phone, link) for phone in phones]
                        await conn.executemany(
                            "INSERT INTO phone_links (phone, link) VALUES ($1, $2)",
                            phone_data
                        )

                # Логируем действие
                await self.log_action(user_id, "save_result", f"link: {link}, found: {found_data}")

                logger.info(f"💾 Сохранен результат для {link}")
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результата для {link}: {e}")

    async def get_cached_results(self, links: List[str]) -> Dict[str, Dict]:
        """Получает закешированные результаты для списка ссылок"""
        results = {}

        if not links:
            return results

        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT link, phones, full_name, birth_date 
                FROM vk_results 
                WHERE link = ANY($1::text[]) AND found_data = TRUE
            """, links)

            for row in rows:
                # Обработка phones - PostgreSQL JSONB возвращает уже распарсенный список
                phones = row["phones"]
                if phones is None:
                    phones = []
                elif isinstance(phones, str):
                    # На всякий случай, если вернулась строка
                    try:
                        import json
                        phones = json.loads(phones)
                    except:
                        phones = []

                results[row["link"]] = {
                    "phones": phones,
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                }

        return results

    async def get_user_statistics(self, user_id: int) -> Dict[str, int]:
        """Получает статистику пользователя"""
        async with self.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_checked,
                    COUNT(*) FILTER (WHERE found_data = TRUE) as found_data_count,
                    COUNT(DISTINCT DATE(checked_at)) as days_active
                FROM vk_results 
                WHERE checked_by_user_id = $1
            """, user_id)

            return {
                "total_checked": row["total_checked"] or 0,
                "found_data_count": row["found_data_count"] or 0,
                "days_active": row["days_active"] or 0
            }

    async def get_database_statistics(self) -> Dict[str, int]:
        """Получает общую статистику базы данных"""
        async with self.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE found_data = TRUE) as with_data,
                    COUNT(*) FILTER (WHERE found_data = FALSE) as without_data
                FROM vk_results
            """)

            return {
                "total_records": row["total_records"] or 0,
                "with_data": row["with_data"] or 0,
                "without_data": row["without_data"] or 0
            }

    async def batch_save_results(self, results: List[Dict[str, Any]], user_id: int, source: str = "import") -> Dict[
        str, int]:
        """Массовое сохранение результатов (для импорта БД)"""
        stats = {"added": 0, "updated": 0, "errors": 0}

        async with self.acquire() as conn:
            for result in results:
                try:
                    link = result.get("link", "")
                    if not link:
                        stats["errors"] += 1
                        continue

                    # Проверяем, существует ли запись
                    existing = await conn.fetchval(
                        "SELECT 1 FROM vk_results WHERE link = $1", link
                    )

                    phones = result.get("phones", [])
                    full_name = result.get("full_name", "")
                    birth_date = result.get("birth_date", "")
                    found_data = bool(phones or full_name or birth_date)

                    async with conn.transaction():
                        # Сохраняем результат
                        await conn.execute("""
                            INSERT INTO vk_results 
                            (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data, source)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (link) DO UPDATE SET
                                phones = EXCLUDED.phones,
                                full_name = EXCLUDED.full_name,
                                birth_date = EXCLUDED.birth_date,
                                checked_at = EXCLUDED.checked_at,
                                checked_by_user_id = EXCLUDED.checked_by_user_id,
                                found_data = EXCLUDED.found_data,
                                source = EXCLUDED.source
                        """, link, json.dumps(phones), full_name, birth_date,
                                           datetime.now(), user_id, found_data, source)

                        # Обновляем телефоны
                        await conn.execute("DELETE FROM phone_links WHERE link = $1", link)

                        if phones:
                            phone_data = [(phone, link) for phone in phones]
                            await conn.executemany(
                                "INSERT INTO phone_links (phone, link) VALUES ($1, $2)",
                                phone_data
                            )

                    if existing:
                        stats["updated"] += 1
                    else:
                        stats["added"] += 1

                except Exception as e:
                    logger.error(f"Ошибка при сохранении {result.get('link', 'unknown')}: {e}")
                    stats["errors"] += 1

        # Логируем массовое действие
        await self.log_action(user_id, "batch_import", json.dumps(stats))

        return stats

    async def check_user_accepted_disclaimer(self, user_id: int) -> bool:
        """Проверка, принял ли пользователь условия использования"""
        async with self.acquire() as conn:
            accepted = await conn.fetchval(
                "SELECT accepted_disclaimer FROM users WHERE user_id = $1",
                user_id
            )
            return bool(accepted)

    async def set_user_accepted_disclaimer(self, user_id: int, user_data: Optional[Dict] = None):
        """Отметка о принятии условий использования"""
        username = user_data.get("username", "") if user_data else ""
        first_name = user_data.get("first_name", "") if user_data else ""
        last_name = user_data.get("last_name", "") if user_data else ""

        async with self.acquire() as conn:
            await conn.execute("""
                INSERT INTO users 
                (user_id, username, first_name, last_name, accepted_disclaimer, accepted_at)
                VALUES ($1, $2, $3, $4, TRUE, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    accepted_disclaimer = TRUE,
                    accepted_at = EXCLUDED.accepted_at,
                    last_activity = NOW()
            """, user_id, username, first_name, last_name, datetime.now())

            await self.log_action(user_id, "accept_disclaimer", "")

    async def update_user_activity(self, user_id: int):
        """Обновление времени последней активности пользователя"""
        async with self.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_activity = $1 WHERE user_id = $2",
                datetime.now(), user_id
            )

    async def log_action(self, user_id: int, action: str, details: str = ""):
        """Логирование действий пользователя"""
        try:
            async with self.acquire() as conn:
                await conn.execute("""
                    INSERT INTO action_logs (user_id, action, details)
                    VALUES ($1, $2, $3)
                """, user_id, action, details[:1000])  # Ограничиваем длину деталей
        except Exception as e:
            logger.error(f"Ошибка логирования действия: {e}")

    async def get_phone_statistics(self) -> Dict[str, Any]:
        """Получает статистику по телефонам в базе"""
        async with self.acquire() as conn:
            # Общее количество уникальных телефонов
            total_phones = await conn.fetchval(
                "SELECT COUNT(DISTINCT phone) FROM phone_links"
            ) or 0

            # Телефоны с несколькими ссылками
            duplicate_phones = await conn.fetchval("""
                SELECT COUNT(DISTINCT phone) 
                FROM (
                    SELECT phone, COUNT(link) as cnt 
                    FROM phone_links 
                    GROUP BY phone 
                    HAVING COUNT(link) > 1
                ) t
            """) or 0

            # Топ телефонов по количеству ссылок
            top_phones_rows = await conn.fetch("""
                SELECT phone, COUNT(link) as link_count
                FROM phone_links
                GROUP BY phone
                ORDER BY link_count DESC
                LIMIT 10
            """)

            return {
                "total_unique_phones": total_phones,
                "phones_with_multiple_links": duplicate_phones,
                "top_phones": [(row["phone"], row["link_count"]) for row in top_phones_rows]
            }

    async def export_to_dict(self, user_id: Optional[int] = None) -> List[Dict]:
        """Экспорт данных в формате словаря (для бэкапов)"""
        async with self.acquire() as conn:
            if user_id:
                rows = await conn.fetch("""
                    SELECT link, phones, full_name, birth_date
                    FROM vk_results
                    WHERE checked_by_user_id = $1 AND found_data = TRUE
                """, user_id)
            else:
                rows = await conn.fetch("""
                    SELECT link, phones, full_name, birth_date
                    FROM vk_results
                    WHERE found_data = TRUE
                """)

            results = []
            for row in rows:
                results.append({
                    "link": row["link"],
                    "phones": row["phones"] or [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                })

            return results