#!/usr/bin/env python3
"""
Экспорт последних результатов поиска из базы данных в Excel файл
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения к БД
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'vk_data'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'Paradigma1681')
}

async def export_latest_results():
    """Экспорт результатов за последние 24 часа"""
    
    # Подключаемся к БД
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # Получаем результаты за последние 24 часа
        query = """
            SELECT 
                link,
                phones,
                full_name,
                birth_date,
                checked_at,
                found_data,
                checked_by_user_id
            FROM vk_results
            WHERE checked_at > NOW() - INTERVAL '24 hours'
            ORDER BY checked_at DESC
        """
        
        rows = await conn.fetch(query)
        
        if not rows:
            print("Нет результатов за последние 24 часа")
            
            # Проверяем все результаты
            query_all = """
                SELECT 
                    link,
                    phones,
                    full_name,
                    birth_date,
                    checked_at,
                    found_data,
                    checked_by_user_id
                FROM vk_results
                ORDER BY checked_at DESC
                LIMIT 100
            """
            rows = await conn.fetch(query_all)
            
            if not rows:
                print("База данных пуста")
                return None
        
        # Преобразуем в DataFrame
        data = []
        for row in rows:
            phones_str = ', '.join(row['phones']) if row['phones'] else ''
            data.append({
                'VK Ссылка': row['link'],
                'Телефоны': phones_str,
                'Имя': row['full_name'] or '',
                'Дата рождения': row['birth_date'] or '',
                'Дата проверки': row['checked_at'].strftime('%d.%m.%Y %H:%M'),
                'Найдены данные': 'Да' if row['found_data'] else 'Нет',
                'ID пользователя': row['checked_by_user_id']
            })
        
        df = pd.DataFrame(data)
        
        # Сохраняем в Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'vk_results_{timestamp}.xlsx'
        filepath = os.path.join('/home/vkbot/vk-data-assistant', filename)
        
        # Создаем Excel writer
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Результаты', index=False)
            
            # Настраиваем ширину колонок
            worksheet = writer.sheets['Результаты']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"✅ Результаты сохранены в файл: {filepath}")
        print(f"📊 Всего записей: {len(df)}")
        print(f"✅ С данными: {len(df[df['Найдены данные'] == 'Да'])}")
        print(f"❌ Без данных: {len(df[df['Найдены данные'] == 'Нет'])}")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None
    finally:
        await conn.close()

async def get_statistics():
    """Получение общей статистики"""
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE found_data = TRUE) as with_data,
                COUNT(*) FILTER (WHERE found_data = FALSE) as without_data,
                COUNT(DISTINCT checked_by_user_id) as unique_users
            FROM vk_results
        """)
        
        print("\n📈 Общая статистика базы данных:")
        print(f"  Всего проверок: {stats['total']}")
        print(f"  С найденными данными: {stats['with_data']}")
        print(f"  Без данных: {stats['without_data']}")
        print(f"  Уникальных пользователей: {stats['unique_users']}")
        
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
    finally:
        await conn.close()

async def main():
    """Главная функция"""
    print("🔍 Экспорт результатов поиска VK Data Assistant")
    print("-" * 50)
    
    # Экспортируем результаты
    filepath = await export_latest_results()
    
    # Показываем статистику
    await get_statistics()
    
    if filepath:
        print(f"\n📁 Файл готов для скачивания:")
        print(f"   {filepath}")
        print(f"\n💡 Для скачивания используйте:")
        print(f"   scp vkbot@{os.getenv('POSTGRES_HOST', 'localhost')}:{filepath} .")

if __name__ == "__main__":
    asyncio.run(main())