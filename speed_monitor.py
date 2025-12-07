#!/usr/bin/env python3
"""
Монитор скорости работы VK бота в реальном времени
Показывает детальную статистику и помогает оптимизировать работу
"""

import asyncio
import time
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import statistics

from telethon import TelegramClient, events
from bot.config import API_ID, API_HASH, SESSION_NAME, ACCOUNT_PHONE, VK_BOT_USERNAME


class SpeedMonitor:
    def __init__(self):
        self.stats_file = Path("data/speed_stats.json")
        self.stats_file.parent.mkdir(exist_ok=True)

        self.sessions = defaultdict(dict)  # Статистика по сессиям
        self.message_times = {}  # Время отправки запросов
        self.edit_times = {}  # Время редактирования
        self.response_times = []  # Все времена ответов

    async def monitor_bot(self):
        """Основная функция мониторинга"""
        print("🚀 VK Bot Speed Monitor")
        print("=" * 60)

        # Создаем клиент
        client = TelegramClient(SESSION_NAME + "_monitor", API_ID, API_HASH)
        await client.start(phone=ACCOUNT_PHONE)

        # Получаем бота
        bot = await client.get_entity(VK_BOT_USERNAME)
        print(f"📡 Подключен к @{VK_BOT_USERNAME}")
        print("\n📊 Мониторинг активен. Отправьте боту несколько ссылок...")
        print("Нажмите Ctrl+C для завершения и показа статистики\n")

        # Обработчики событий
        @client.on(events.NewMessage(from_users=bot))
        async def on_new_message(event):
            msg_id = event.message.id
            text = event.message.text or ""

            # Если это сообщение "Идёт поиск"
            if any(phrase in text.lower() for phrase in ["идёт поиск", "идет поиск", "searching"]):
                self.message_times[msg_id] = time.time()
                print(f"🔍 [{datetime.now().strftime('%H:%M:%S')}] Начат поиск (ID: {msg_id})")

        @client.on(events.MessageEdited(from_users=bot))
        async def on_message_edited(event):
            msg_id = event.message.id

            # Если это редактирование сообщения "Идёт поиск"
            if msg_id in self.message_times:
                start_time = self.message_times[msg_id]
                response_time = time.time() - start_time

                self.edit_times[msg_id] = response_time
                self.response_times.append(response_time)

                # Определяем тип результата
                text = event.message.text or ""
                has_phone = bool("7" in text and len([c for c in text if c.isdigit()]) >= 11)
                has_name = any(phrase in text for phrase in ["Полное имя:", "Full name:"])

                result_type = "✅ Данные найдены" if (has_phone or has_name) else "❌ Нет данных"

                print(
                    f"📝 [{datetime.now().strftime('%H:%M:%S')}] {result_type} | Время: {response_time:.2f}с (ID: {msg_id})")

                # Показываем текущую статистику каждые 10 запросов
                if len(self.response_times) % 10 == 0:
                    self._show_current_stats()

        # Отправка своих сообщений боту
        @client.on(events.NewMessage(outgoing=True, chats=bot))
        async def on_outgoing_message(event):
            # Засекаем время отправки для расчета полного времени
            if event.message.text and "vk.com" in event.message.text:
                print(f"📤 [{datetime.now().strftime('%H:%M:%S')}] Отправлен запрос: {event.message.text[:50]}...")

        try:
            await client.run_until_disconnected()
        except KeyboardInterrupt:
            print("\n\n" + "=" * 60)
            self._show_final_stats()
            self._save_stats()

    def _show_current_stats(self):
        """Показывает текущую статистику"""
        if not self.response_times:
            return

        print("\n--- Текущая статистика ---")
        print(f"Запросов обработано: {len(self.response_times)}")
        print(f"Среднее время ответа: {statistics.mean(self.response_times):.2f}с")
        print(f"Скорость: {60 / statistics.mean(self.response_times):.1f} запросов/мин")
        print("-" * 26 + "\n")

    def _show_final_stats(self):
        """Показывает финальную статистику"""
        if not self.response_times:
            print("❌ Нет данных для анализа")
            return

        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 60)

        # Основные метрики
        total = len(self.response_times)
        avg_time = statistics.mean(self.response_times)
        median_time = statistics.median(self.response_times)
        min_time = min(self.response_times)
        max_time = max(self.response_times)

        print(f"\n📈 Обработано запросов: {total}")
        print(f"\n⏱ Время ответа бота:")
        print(f"  • Минимум: {min_time:.2f} сек")
        print(f"  • Среднее: {avg_time:.2f} сек")
        print(f"  • Медиана: {median_time:.2f} сек")
        print(f"  • Максимум: {max_time:.2f} сек")

        # Распределение по времени
        fast = sum(1 for t in self.response_times if t < 2)
        normal = sum(1 for t in self.response_times if 2 <= t < 4)
        slow = sum(1 for t in self.response_times if t >= 4)

        print(f"\n📊 Распределение:")
        print(f"  • Быстрые (<2с): {fast} ({fast / total * 100:.1f}%)")
        print(f"  • Обычные (2-4с): {normal} ({normal / total * 100:.1f}%)")
        print(f"  • Медленные (>4с): {slow} ({slow / total * 100:.1f}%)")

        # Производительность
        requests_per_minute = 60 / avg_time
        requests_per_hour = requests_per_minute * 60

        print(f"\n⚡ Производительность:")
        print(f"  • Скорость: {requests_per_minute:.1f} запросов/минуту")
        print(f"  • Прогноз: {requests_per_hour:.0f} запросов/час")

        # Время на файлы
        print(f"\n📁 Время обработки файлов:")
        print(f"  • 100 ссылок: {100 * avg_time / 60:.1f} минут")
        print(f"  • 1000 ссылок: {1000 * avg_time / 60:.1f} минут")
        print(f"  • 5000 ссылок: {5000 * avg_time / 60:.1f} минут")

        # Рекомендации
        print(f"\n💡 РЕКОМЕНДАЦИИ:")
        if avg_time < 2:
            print("  ✅ Отличная скорость! Бот работает оптимально.")
        elif avg_time < 3:
            print("  ✅ Хорошая скорость. Можно попробовать уменьшить INITIAL_DELAY до 0.3")
        elif avg_time < 4:
            print("  ⚠️ Средняя скорость. Проверьте нагрузку на бота.")
        else:
            print("  ❌ Низкая скорость. Возможно, бот перегружен.")

        print(f"\n🔧 Оптимальные настройки для вашего случая:")
        print(f"  • MESSAGE_TIMEOUT = {max(5.0, max_time + 1):.1f}")
        print(f"  • INITIAL_DELAY = {max(0.3, median_time * 0.2):.1f}")
        print(f"  • Интервал проверки = 0.2 сек")

    def _save_stats(self):
        """Сохраняет статистику в файл"""
        if not self.response_times:
            return

        stats = {
            "timestamp": datetime.now().isoformat(),
            "total_requests": len(self.response_times),
            "avg_response_time": statistics.mean(self.response_times),
            "median_response_time": statistics.median(self.response_times),
            "min_response_time": min(self.response_times),
            "max_response_time": max(self.response_times),
            "all_times": self.response_times[-100:]  # Последние 100 для истории
        }

        with open(self.stats_file, 'w') as f:
            json.dump(stats, f, indent=2)

        print(f"\n💾 Статистика сохранена в {self.stats_file}")


async def main():
    monitor = SpeedMonitor()
    await monitor.monitor_bot()


if __name__ == "__main__":
    print("🔧 VK Bot Speed Monitor v2.0")
    print("Этот инструмент поможет оптимизировать скорость работы бота\n")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✅ Мониторинг завершен")