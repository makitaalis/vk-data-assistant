# update_db_imports.py
import os
import re


def update_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Заменяем импорты
    updated = content.replace('from db_module import VKDatabase', 'from db_module import VKDatabase')
    updated = updated.replace('from db_module.connection import', 'from db_module.connection import')
    updated = updated.replace('import db_module.', 'import db_module.')

    if updated != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(updated)
        print(f"✅ Updated: {filepath}")


# Обновляем все Python файлы
for root, dirs, files in os.walk('.'):
    if any(skip in root for skip in ['.venv', '__pycache__', '.git']):
        continue
    for file in files:
        if file.endswith('.py'):
            update_file(os.path.join(root, file))

print("✅ Все импорты обновлены")