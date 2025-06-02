import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set, Any
from contextlib import contextmanager

logger = logging.getLogger("database")

# Путь к базе данных
DB_PATH = Path("data") / "vk_data.db"


class VKDatabase:
    """Класс для работы с базой данных результатов VK"""

    def __init__(self):
        # Убедимся, что папка data существует
        DB_PATH.parent.mkdir(exist_ok=True)
        self.db_path = DB_PATH
        self._init_db()

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для безопасной работы с БД"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка БД: {e}")
            raise
        finally:
            conn.close()

    def _init_db(self):
        """Инициализация структуры базы данных"""
        with self.get_connection() as conn:
            # Таблица результатов VK
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vk_results (
                    link TEXT PRIMARY KEY,
                    phones TEXT,  -- JSON массив телефонов
                    full_name TEXT,
                    birth_date TEXT,
                    checked_at TIMESTAMP,
                    checked_by_user_id INTEGER,
                    found_data BOOLEAN DEFAULT 0,
                    source TEXT DEFAULT 'search'  -- 'search' или 'import'
                )
            """)

            # Миграция: добавляем колонку source если её нет
            cursor = conn.execute("PRAGMA table_info(vk_results)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'source' not in columns:
                logger.info("🔄 Миграция БД: добавление колонки source")
                conn.execute("ALTER TABLE vk_results ADD COLUMN source TEXT DEFAULT 'search'")

            # Таблица пользователей и их согласий
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    accepted_disclaimer BOOLEAN DEFAULT 0,
                    accepted_at TIMESTAMP,
                    first_seen TIMESTAMP,
                    last_activity TIMESTAMP
                )
            """)

            # Таблица логов действий (для мониторинга)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS action_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Индексы для оптимизации
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_id 
                ON vk_results(checked_by_user_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_checked_at 
                ON vk_results(checked_at)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_found_data 
                ON vk_results(found_data)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_activity 
                ON users(last_activity)
            """)

            # Создаем таблицу для телефонов (для быстрого поиска дубликатов)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS phone_links (
                    phone TEXT,
                    link TEXT,
                    PRIMARY KEY (phone, link),
                    FOREIGN KEY (link) REFERENCES vk_results(link)
                )
            """)

            # Индекс для быстрого поиска по телефону
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_phone 
                ON phone_links(phone)
            """)

            logger.info("✅ База данных инициализирована")

    def migrate_database(self):
        """Выполняет миграции базы данных для обновления схемы"""
        with self.get_connection() as conn:
            # Получаем текущую схему таблицы vk_results
            cursor = conn.execute("PRAGMA table_info(vk_results)")
            columns = [column[1] for column in cursor.fetchall()]

            migrations_applied = []

            # Миграция 1: Добавление колонки source
            if 'source' not in columns:
                logger.info("🔄 Применение миграции: добавление колонки source")
                conn.execute("ALTER TABLE vk_results ADD COLUMN source TEXT DEFAULT 'search'")
                migrations_applied.append("source")

            # Миграция 2: Создание таблицы phone_links
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
            if not cursor.fetchone():
                logger.info("🔄 Создание таблицы phone_links для дубликатов телефонов")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS phone_links (
                        phone TEXT,
                        link TEXT,
                        PRIMARY KEY (phone, link),
                        FOREIGN KEY (link) REFERENCES vk_results(link)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_phone 
                    ON phone_links(phone)
                """)
                migrations_applied.append("phone_links")

                # Заполняем таблицу существующими данными
                logger.info("🔄 Индексация существующих телефонов...")
                cursor = conn.execute("SELECT link, phones FROM vk_results WHERE phones IS NOT NULL AND phones != '[]'")
                for row in cursor:
                    try:
                        phones = json.loads(row['phones'])
                        for phone in phones:
                            conn.execute("INSERT OR IGNORE INTO phone_links (phone, link) VALUES (?, ?)",
                                         (phone, row['link']))
                    except:
                        pass

            if migrations_applied:
                logger.info(f"✅ Применено миграций: {len(migrations_applied)} - {', '.join(migrations_applied)}")
            else:
                logger.info("✅ База данных актуальна, миграции не требуются")

    def check_phone_duplicates(self, phones: List[str]) -> Dict[str, List[Dict[str, str]]]:
        """
        Проверяет телефоны на наличие в базе

        Returns:
            Dict: {phone: [{"link": "...", "full_name": "...", "birth_date": "..."}, ...]}
        """
        if not phones:
            return {}

        results = {}
        with self.get_connection() as conn:
            # Проверяем наличие таблицы phone_links
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
            if not cursor.fetchone():
                logger.warning("Таблица phone_links не существует, пропускаем проверку дубликатов телефонов")
                return {}

            for phone in phones:
                cursor = conn.execute("""
                    SELECT DISTINCT pl.link, vr.full_name, vr.birth_date
                    FROM phone_links pl
                    JOIN vk_results vr ON pl.link = vr.link
                    WHERE pl.phone = ?
                """, (phone,))

                phone_results = []
                for row in cursor:
                    phone_results.append({
                        "link": row["link"],
                        "full_name": row["full_name"] or "",
                        "birth_date": row["birth_date"] or ""
                    })

                if phone_results:
                    results[phone] = phone_results

        return results

    def check_duplicates_extended(self, links: List[str]) -> Dict[str, Any]:
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

        with self.get_connection() as conn:
            # Получаем все существующие ссылки одним запросом
            placeholders = ','.join('?' * len(links))
            query = f"""
                SELECT link, phones, full_name, birth_date, found_data 
                FROM vk_results 
                WHERE link IN ({placeholders})
            """

            existing_links = {}
            for row in conn.execute(query, links):
                link_data = {
                    "link": row["link"],
                    "phones": json.loads(row["phones"]) if row["phones"] else [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "found_data": bool(row["found_data"])
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

    def check_phone_duplicates(self, phones: List[str]) -> Dict[str, List[Dict[str, str]]]:
        """
        Проверяет телефоны на наличие в базе

        Returns:
            Dict: {phone: [{"link": "...", "full_name": "...", "birth_date": "..."}, ...]}
        """
        if not phones:
            return {}

        results = {}
        with self.get_connection() as conn:
            for phone in phones:
                cursor = conn.execute("""
                    SELECT DISTINCT pl.link, vr.full_name, vr.birth_date
                    FROM phone_links pl
                    JOIN vk_results vr ON pl.link = vr.link
                    WHERE pl.phone = ?
                """, (phone,))

                phone_results = []
                for row in cursor:
                    phone_results.append({
                        "link": row["link"],
                        "full_name": row["full_name"] or "",
                        "birth_date": row["birth_date"] or ""
                    })

                if phone_results:
                    results[phone] = phone_results

        return results

    def find_links_by_phone(self, phone: str) -> List[Dict[str, Any]]:
        """
        Находит все ссылки, связанные с телефоном

        Args:
            phone: Номер телефона для поиска

        Returns:
            List[Dict]: Список найденных записей
        """
        results = []

        with self.get_connection() as conn:
            # Проверяем наличие таблицы phone_links
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
            if not cursor.fetchone():
                logger.warning("Таблица phone_links не существует")
                return []

            cursor = conn.execute("""
                SELECT vr.link, vr.phones, vr.full_name, vr.birth_date, vr.checked_at
                FROM phone_links pl
                JOIN vk_results vr ON pl.link = vr.link
                WHERE pl.phone = ?
                ORDER BY vr.checked_at DESC
            """, (phone,))

            for row in cursor:
                results.append({
                    "link": row["link"],
                    "phones": json.loads(row["phones"]) if row["phones"] else [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or "",
                    "checked_at": row["checked_at"]
                })

        return results

    def get_phone_statistics(self) -> Dict[str, Any]:
        """Получает статистику по телефонам в базе"""
        with self.get_connection() as conn:
            # Проверяем наличие таблицы phone_links
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
            if not cursor.fetchone():
                return {
                    "total_unique_phones": 0,
                    "phones_with_multiple_links": 0,
                    "top_phones": []
                }

            # Общее количество уникальных телефонов
            total_phones = conn.execute("SELECT COUNT(DISTINCT phone) FROM phone_links").fetchone()[0]

            # Телефоны с несколькими ссылками
            duplicate_phones = conn.execute("""
                SELECT COUNT(DISTINCT phone) 
                FROM (
                    SELECT phone, COUNT(link) as cnt 
                    FROM phone_links 
                    GROUP BY phone 
                    HAVING cnt > 1
                )
            """).fetchone()[0]

            # Топ телефонов по количеству ссылок
            top_phones = conn.execute("""
                SELECT phone, COUNT(link) as link_count
                FROM phone_links
                GROUP BY phone
                ORDER BY link_count DESC
                LIMIT 10
            """).fetchall()

            return {
                "total_unique_phones": total_phones,
                "phones_with_multiple_links": duplicate_phones,
                "top_phones": [(row[0], row[1]) for row in top_phones]
            }

    def save_result(self, link: str, result_data: Dict[str, Any], user_id: int, source: str = "search"):
        """Сохраняет результат проверки ссылки"""
        phones_json = json.dumps(result_data.get("phones", []))
        full_name = result_data.get("full_name", "")
        birth_date = result_data.get("birth_date", "")
        found_data = bool(result_data.get("phones") or full_name or birth_date)

        try:
            with self.get_connection() as conn:
                # Проверяем наличие колонки source
                cursor = conn.execute("PRAGMA table_info(vk_results)")
                columns = [column[1] for column in cursor.fetchall()]

                if 'source' in columns:
                    conn.execute("""
                        INSERT OR REPLACE INTO vk_results 
                        (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (link, phones_json, full_name, birth_date, datetime.now(), user_id, found_data, source))
                else:
                    # Fallback для старой схемы БД
                    conn.execute("""
                        INSERT OR REPLACE INTO vk_results 
                        (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (link, phones_json, full_name, birth_date, datetime.now(), user_id, found_data))

                # Сохраняем телефоны в таблицу phone_links
                phones = result_data.get("phones", [])
                if phones:
                    # Проверяем наличие таблицы phone_links
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
                    if cursor.fetchone():
                        # Удаляем старые записи для этой ссылки
                        conn.execute("DELETE FROM phone_links WHERE link = ?", (link,))
                        # Добавляем новые
                        for phone in phones:
                            conn.execute("INSERT OR IGNORE INTO phone_links (phone, link) VALUES (?, ?)",
                                         (phone, link))

                # Логируем действие
                self.log_action(user_id, "save_result", f"link: {link}, found: {found_data}")

                logger.info(f"💾 Сохранен результат для {link}")
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результата для {link}: {e}")

    def get_cached_results(self, links: List[str]) -> Dict[str, Dict]:
        """Получает закешированные результаты для списка ссылок"""
        results = {}

        if not links:
            return results

        with self.get_connection() as conn:
            placeholders = ','.join('?' * len(links))
            query = f"""
                SELECT link, phones, full_name, birth_date 
                FROM vk_results 
                WHERE link IN ({placeholders}) AND found_data = 1
            """

            for row in conn.execute(query, links):
                results[row["link"]] = {
                    "phones": json.loads(row["phones"]) if row["phones"] else [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                }

        return results

    def get_user_statistics(self, user_id: int) -> Dict[str, int]:
        """Получает статистику пользователя"""
        with self.get_connection() as conn:
            stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_checked,
                    SUM(CASE WHEN found_data = 1 THEN 1 ELSE 0 END) as found_data_count,
                    COUNT(DISTINCT DATE(checked_at)) as days_active
                FROM vk_results 
                WHERE checked_by_user_id = ?
            """, (user_id,)).fetchone()

            return {
                "total_checked": stats["total_checked"] or 0,
                "found_data_count": stats["found_data_count"] or 0,
                "days_active": stats["days_active"] or 0
            }

    def get_database_statistics(self) -> Dict[str, int]:
        """Получает общую статистику базы данных"""
        with self.get_connection() as conn:
            stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    SUM(CASE WHEN found_data = 1 THEN 1 ELSE 0 END) as with_data,
                    SUM(CASE WHEN found_data = 0 THEN 1 ELSE 0 END) as without_data
                FROM vk_results
            """).fetchone()

            return {
                "total_records": stats["total_records"] or 0,
                "with_data": stats["with_data"] or 0,
                "without_data": stats["without_data"] or 0
            }

    def batch_save_results(self, results: List[Dict[str, Any]], user_id: int, source: str = "import") -> Dict[str, int]:
        """Массовое сохранение результатов (для импорта БД)"""
        stats = {"added": 0, "updated": 0, "errors": 0}

        with self.get_connection() as conn:
            # Проверяем наличие колонки source
            cursor = conn.execute("PRAGMA table_info(vk_results)")
            columns = [column[1] for column in cursor.fetchall()]
            has_source_column = 'source' in columns

            for result in results:
                try:
                    link = result.get("link", "")
                    if not link:
                        stats["errors"] += 1
                        continue

                    # Проверяем, существует ли запись
                    existing = conn.execute(
                        "SELECT 1 FROM vk_results WHERE link = ?", (link,)
                    ).fetchone()

                    phones_json = json.dumps(result.get("phones", []))
                    full_name = result.get("full_name", "")
                    birth_date = result.get("birth_date", "")
                    found_data = bool(result.get("phones") or full_name or birth_date)

                    if has_source_column:
                        conn.execute("""
                            INSERT OR REPLACE INTO vk_results 
                            (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (link, phones_json, full_name, birth_date, datetime.now(), user_id, found_data, source))
                    else:
                        # Fallback для старой схемы БД
                        conn.execute("""
                            INSERT OR REPLACE INTO vk_results 
                            (link, phones, full_name, birth_date, checked_at, checked_by_user_id, found_data)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (link, phones_json, full_name, birth_date, datetime.now(), user_id, found_data))

                    if existing:
                        stats["updated"] += 1
                    else:
                        stats["added"] += 1

                    # Сохраняем телефоны в таблицу phone_links
                    phones = result.get("phones", [])
                    if phones and has_source_column:
                        # Проверяем наличие таблицы phone_links
                        cursor = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='phone_links'")
                        if cursor.fetchone():
                            # Удаляем старые записи для этой ссылки
                            conn.execute("DELETE FROM phone_links WHERE link = ?", (link,))
                            # Добавляем новые
                            for phone in phones:
                                conn.execute("INSERT OR IGNORE INTO phone_links (phone, link) VALUES (?, ?)",
                                             (phone, link))

                except Exception as e:
                    logger.error(f"Ошибка при сохранении {result.get('link', 'unknown')}: {e}")
                    stats["errors"] += 1

        # Логируем массовое действие
        self.log_action(user_id, "batch_import", json.dumps(stats))

        return stats

    def check_user_accepted_disclaimer(self, user_id: int) -> bool:
        """Проверка, принял ли пользователь условия использования"""
        with self.get_connection() as conn:
            user = conn.execute(
                "SELECT accepted_disclaimer FROM users WHERE user_id = ?",
                (user_id,)
            ).fetchone()

            return bool(user and user["accepted_disclaimer"])

    def set_user_accepted_disclaimer(self, user_id: int, user_data: Optional[Dict] = None):
        """Отметка о принятии условий использования"""
        with self.get_connection() as conn:
            username = user_data.get("username", "") if user_data else ""
            first_name = user_data.get("first_name", "") if user_data else ""
            last_name = user_data.get("last_name", "") if user_data else ""

            conn.execute("""
                INSERT OR REPLACE INTO users 
                (user_id, username, first_name, last_name, accepted_disclaimer, accepted_at, first_seen, last_activity)
                VALUES (?, ?, ?, ?, 1, ?, 
                    COALESCE((SELECT first_seen FROM users WHERE user_id = ?), ?),
                    ?)
            """, (user_id, username, first_name, last_name, datetime.now(), user_id, datetime.now(), datetime.now()))

            self.log_action(user_id, "accept_disclaimer", "")

    def update_user_activity(self, user_id: int):
        """Обновление времени последней активности пользователя"""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE users SET last_activity = ? WHERE user_id = ?",
                (datetime.now(), user_id)
            )

    def log_action(self, user_id: int, action: str, details: str = ""):
        """Логирование действий пользователя"""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO action_logs (user_id, action, details)
                    VALUES (?, ?, ?)
                """, (user_id, action, details[:1000]))  # Ограничиваем длину деталей
        except Exception as e:
            logger.error(f"Ошибка логирования действия: {e}")

    def get_recent_actions(self, user_id: Optional[int] = None, limit: int = 100) -> List[Dict]:
        """Получение последних действий (для мониторинга)"""
        with self.get_connection() as conn:
            if user_id:
                query = """
                    SELECT * FROM action_logs 
                    WHERE user_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """
                cursor = conn.execute(query, (user_id, limit))
            else:
                query = """
                    SELECT * FROM action_logs 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """
                cursor = conn.execute(query, (limit,))

            return [dict(row) for row in cursor]

    def clear_old_records(self, days: int = 30):
        """Удаляет старые записи (опционально)"""
        with self.get_connection() as conn:
            # Сначала удаляем связанные записи из phone_links
            conn.execute("""
                DELETE FROM phone_links 
                WHERE link IN (
                    SELECT link FROM vk_results 
                    WHERE checked_at < datetime('now', '-{} days')
                    AND found_data = 0
                )
            """.format(days))

            # Удаляем старые результаты без данных
            conn.execute("""
                DELETE FROM vk_results 
                WHERE checked_at < datetime('now', '-{} days')
                AND found_data = 0
            """.format(days))

            # Удаляем старые логи
            conn.execute("""
                DELETE FROM action_logs 
                WHERE timestamp < datetime('now', '-{} days')
            """.format(days * 2))  # Логи храним дольше

            logger.info(f"🗑️ Удалены старые записи")

    def export_to_dict(self, user_id: Optional[int] = None) -> List[Dict]:
        """Экспорт данных в формате словаря (для бэкапов)"""
        with self.get_connection() as conn:
            if user_id:
                query = """
                    SELECT link, phones, full_name, birth_date
                    FROM vk_results
                    WHERE checked_by_user_id = ? AND found_data = 1
                """
                cursor = conn.execute(query, (user_id,))
            else:
                query = """
                    SELECT link, phones, full_name, birth_date
                    FROM vk_results
                    WHERE found_data = 1
                """
                cursor = conn.execute(query)

            results = []
            for row in cursor:
                results.append({
                    "link": row["link"],
                    "phones": json.loads(row["phones"]) if row["phones"] else [],
                    "full_name": row["full_name"] or "",
                    "birth_date": row["birth_date"] or ""
                })

            return results