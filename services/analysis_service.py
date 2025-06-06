"""Сервис для анализа Excel файлов"""

import logging
from typing import Dict, Any, List
from pathlib import Path

from bot.utils.messages import MESSAGES
from db_module import VKDatabase
from db_loader import DatabaseLoader

logger = logging.getLogger("analysis_service")


class FileAnalyzer:
    """Класс для анализа Excel файлов"""

    def __init__(self, db: VKDatabase):
        self.db = db
        self.loader = DatabaseLoader(db)

    async def analyze_file(self, file_path: Path) -> Dict[str, Any]:
        """Полный анализ файла"""

        # Базовый анализ структуры
        analysis = self.loader.analyze_excel_structure(file_path)

        # Полная обработка для получения данных
        records, stats = self.loader.process_excel_file(file_path)

        # Анализ связей
        network = self.loader.find_all_related_data(records)

        # Извлекаем VK ссылки и телефоны для проверки дубликатов
        all_vk_links = [r['link'] for r in records if not r['link'].startswith('phone:')]
        all_phones = set()
        for r in records:
            all_phones.update(r.get('phones', []))

        # Проверка дубликатов
        duplicate_vk = {}
        duplicate_phones = {}

        if self.db:
            duplicate_vk = await self.db.check_duplicates_extended(all_vk_links) if all_vk_links else {
                "new": [],
                "duplicates_with_data": {},
                "duplicates_no_data": []
            }
            duplicate_phones = await self.db.check_phone_duplicates(list(all_phones)) if all_phones else {}

        # Генерация рекомендаций
        recommendations = self.generate_recommendations(stats, network, duplicate_vk, duplicate_phones)

        return {
            "basic": analysis,
            "stats": stats,
            "network": network,
            "records": records,
            "duplicates": {
                "vk": duplicate_vk,
                "phones": duplicate_phones
            },
            "recommendations": recommendations
        }

    def generate_recommendations(
            self,
            stats: Dict,
            network: Dict,
            duplicate_vk: Dict,
            duplicate_phones: Dict
    ) -> List[str]:
        """Генерирует рекомендации на основе анализа"""
        recommendations = []

        # Рекомендации по дубликатам
        total_vk = len(duplicate_vk.get("new", [])) + len(duplicate_vk.get("duplicates_with_data", {})) + len(
            duplicate_vk.get("duplicates_no_data", []))
        if total_vk > 0:
            duplicate_percent = ((len(duplicate_vk.get("duplicates_with_data", {})) + len(
                duplicate_vk.get("duplicates_no_data", []))) / total_vk) * 100
            if duplicate_percent > 50:
                recommendations.append(f"🔄 {int(duplicate_percent)}% ссылок уже в базе - рекомендую удалить дубликаты")

        # Рекомендации по телефонам
        if network['stats']['phones_with_multiple_vk'] > 5:
            recommendations.append(
                f"📱 Найдено {network['stats']['phones_with_multiple_vk']} телефонов с несколькими VK - возможны связанные аккаунты")

        # Рекомендации по качеству данных
        if stats.get('unique_phones', 0) > stats.get('unique_vk_links', 0):
            recommendations.append("☎️ Телефонов больше чем VK ссылок - можно найти дополнительные профили")

        # Рекомендации по обработке
        if len(duplicate_phones) > 10:
            recommendations.append(f"🔍 {len(duplicate_phones)} телефонов уже в базе - проверьте связанные профили")

        if not recommendations:
            recommendations.append("✅ Файл готов к обработке")

        return recommendations

    async def format_analysis_message(self, analysis: Dict) -> str:
        """Форматирование результатов анализа для Telegram"""
        stats = analysis['stats']
        network = analysis['network']['stats']
        duplicates = analysis['duplicates']

        # Подсчет дубликатов
        duplicate_vk_count = len(duplicates['vk'].get('duplicates_with_data', {})) + len(
            duplicates['vk'].get('duplicates_no_data', []))
        duplicate_vk_with_data = len(duplicates['vk'].get('duplicates_with_data', {}))
        duplicate_phones_count = len(duplicates['phones'])

        # Форматирование рекомендаций
        recommendations_text = ""
        if analysis['recommendations']:
            recommendations_text = MESSAGES["recommendations"].format(
                items="\n".join(f"• {rec}" for rec in analysis['recommendations'])
            )

        return MESSAGES["analysis_complete"].format(
            filename=analysis['basic']['file_name'],
            vk_links=stats.get('unique_vk_links', 0),
            phones=stats.get('unique_phones', 0),
            data_rows=stats.get('rows_with_vk_links', 0) + stats.get('rows_with_phones', 0),
            phones_multiple_vk=network.get('phones_with_multiple_vk', 0),
            vk_multiple_phones=network.get('vk_with_multiple_phones', 0),
            duplicate_vk=duplicate_vk_count,
            duplicate_vk_with_data=duplicate_vk_with_data,
            duplicate_phones=duplicate_phones_count,
            recommendations=recommendations_text
        )

    async def format_analysis_details(self, analysis: Dict) -> str:
        """Форматирование детального анализа"""
        network = analysis['network']
        details = []

        # Показываем телефоны с несколькими VK
        if network['stats']['phones_with_multiple_vk'] > 0:
            details.append("<b>📱 Телефоны с несколькими VK профилями:</b>")
            count = 0
            for phone, data in network['phone_network'].items():
                if len(data['vk_links']) > 1:
                    details.append(f"\n☎️ <code>{phone}</code> ({len(data['vk_links'])} профилей)")
                    for vk in data['vk_links'][:3]:
                        details.append(f"  └ {vk}")
                    if len(data['vk_links']) > 3:
                        details.append(f"  └ ... и еще {len(data['vk_links']) - 3}")
                    count += 1
                    if count >= 5:
                        details.append("\n... и другие")
                        break
            details.append("")

        # Показываем VK с несколькими телефонами
        if network['stats']['vk_with_multiple_phones'] > 0:
            details.append("<b>🔗 VK профили с несколькими телефонами:</b>")
            count = 0
            for vk, data in network['vk_network'].items():
                if len(data['phones']) > 1 and not vk.startswith('phone:'):
                    details.append(f"\n👤 {vk}")
                    details.append(f"  📱 Телефонов: {len(data['phones'])}")
                    count += 1
                    if count >= 5:
                        details.append("\n... и другие")
                        break

        if not details:
            details.append("📊 Нет сложных связей между данными")

        return MESSAGES["analysis_details"].format(details="\n".join(details))