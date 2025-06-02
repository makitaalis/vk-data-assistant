#!/usr/bin/env python3
"""
Скрипт мониторинга для VK Data Assistant Bot
Показывает статистику использования и активность
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Путь к базе данных
DB_PATH = Path("data") / "vk_data.db"


def get_db_stats():
    """Получить общую статистику из базы данных"""
    if not DB_PATH.exists():
        print("❌ База данных не найдена!")
        return None

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        # Общая статистика
        total_stats = conn.execute("""
            SELECT 
                COUNT(DISTINCT link) as total_links,
                COUNT(DISTINCT checked_by_user_id) as total_users,
                SUM(CASE WHEN found_data = 1 THEN 1 ELSE 0 END) as with_data,
                SUM(CASE WHEN found_data = 0 THEN 1 ELSE 0 END) as without_data
            FROM vk_results
        """).fetchone()

        # Статистика по телефонам
        phone_stats = conn.execute("""
            SELECT 
                COUNT(DISTINCT phone) as unique_phones,
                COUNT(*) as total_phone_links,
                (SELECT COUNT(DISTINCT phone) FROM (
                    SELECT phone, COUNT(link) as cnt 
                    FROM phone_links 
                    GROUP BY phone 
                    HAVING cnt > 1
                )) as duplicate_phones
            FROM phone_links
        """).fetchone()

        # Статистика за последние 24 часа
        daily_stats = conn.execute("""
            SELECT 
                COUNT(*) as checks_24h,
                COUNT(DISTINCT checked_by_user_id) as active_users_24h
            FROM vk_results
            WHERE checked_at > datetime('now', '-1 day')
        """).fetchone()

        # Топ пользователей
        top_users = conn.execute("""
            SELECT 
                u.user_id,
                u.username,
                u.first_name,
                COUNT(r.link) as total_checks,
                SUM(CASE WHEN r.found_data = 1 THEN 1 ELSE 0 END) as found_data
            FROM users u
            LEFT JOIN vk_results r ON u.user_id = r.checked_by_user_id
            GROUP BY u.user_id
            ORDER BY total_checks DESC
            LIMIT 10
        """).fetchall()

        # Топ телефонов с наибольшим количеством профилей
        top_phones = conn.execute("""
            SELECT phone, COUNT(link) as profile_count
            FROM phone_links
            GROUP BY phone
            ORDER BY profile_count DESC
            LIMIT 5
        """).fetchall()

        # Последние действия
        recent_actions = conn.execute("""
            SELECT 
                user_id,
                action,
                details,
                timestamp
            FROM action_logs
            ORDER BY timestamp DESC
            LIMIT 20
        """).fetchall()

        return {
            "total": dict(total_stats),
            "phones": dict(phone_stats) if phone_stats else {"unique_phones": 0, "total_phone_links": 0,
                                                             "duplicate_phones": 0},
            "daily": dict(daily_stats),
            "top_users": [dict(user) for user in top_users],
            "top_phones": [dict(phone) for phone in top_phones] if top_phones else [],
            "recent_actions": [dict(action) for action in recent_actions]
        }

    finally:
        conn.close()


def print_stats(stats):
    """Красиво выводит статистику"""
    if not stats:
        return

    print("\n" + "=" * 60)
    print("📊 VK DATA ASSISTANT - СТАТИСТИКА")
    print("=" * 60)

    # Общая статистика
    print("\n📈 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего проверено ссылок: {stats['total']['total_links']:,}")
    print(f"   Ссылок с данными: {stats['total']['with_data']:,}")
    print(f"   Ссылок без данных: {stats['total']['without_data']:,}")
    print(f"   Всего пользователей: {stats['total']['total_users']:,}")

    # Статистика по телефонам
    print("\n📱 СТАТИСТИКА ТЕЛЕФОНОВ:")
    print(f"   Уникальных телефонов: {stats['phones']['unique_phones']:,}")
    print(f"   Телефонов с несколькими профилями: {stats['phones']['duplicate_phones']:,}")
    print(f"   Всего связей телефон-профиль: {stats['phones']['total_phone_links']:,}")

    # Статистика за 24 часа
    print("\n⏰ ЗА ПОСЛЕДНИЕ 24 ЧАСА:")
    print(f"   Проверок: {stats['daily']['checks_24h']:,}")
    print(f"   Активных пользователей: {stats['daily']['active_users_24h']}")

    # Топ телефонов
    if stats.get('top_phones'):
        print("\n📞 ТОП ТЕЛЕФОНОВ (по количеству профилей):")
        for phone_data in stats['top_phones']:
            print(f"   {phone_data['phone']} - {phone_data['profile_count']} профилей")

    # Топ пользователей
    print("\n🏆 ТОП ПОЛЬЗОВАТЕЛЕЙ:")
    print(f"{'ID':>10} | {'Имя':<20} | {'Проверок':>10} | {'Найдено':>10}")
    print("-" * 60)
    for user in stats['top_users']:
        name = user['first_name'] or user['username'] or "Unknown"
        print(f"{user['user_id']:>10} | {name:<20} | {user['total_checks']:>10} | {user['found_data']:>10}")

    # Последние действия
    print("\n📝 ПОСЛЕДНИЕ ДЕЙСТВИЯ:")
    for action in stats['recent_actions'][:10]:
        timestamp = datetime.fromisoformat(action['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
        details = action['details'][:50] + "..." if len(action['details']) > 50 else action['details']
        print(f"   [{timestamp}] User {action['user_id']}: {action['action']} - {details}")

    print("\n" + "=" * 60)
    print(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


def continuous_monitor(interval=30):
    """Непрерывный мониторинг с обновлением каждые N секунд"""
    import time
    import os

    print("🔄 Запущен непрерывный мониторинг...")
    print(f"Обновление каждые {interval} секунд. Нажмите Ctrl+C для выхода.\n")

    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            stats = get_db_stats()
            print_stats(stats)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\n👋 Мониторинг остановлен.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        continuous_monitor(interval)
    else:
        stats = get_db_stats()
        print_stats(stats)
        print("\n💡 Совет: используйте --continuous для непрерывного мониторинга")
        print("   Пример: python monitor.py --continuous 30")