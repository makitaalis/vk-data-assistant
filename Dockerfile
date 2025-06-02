FROM python:3.10-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /app

# Копирование requirements и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY *.py ./

# Создание необходимых директорий
RUN mkdir -p data debug

# Создание непривилегированного пользователя
RUN useradd -m -u 1000 botuser && \
    chown -R botuser:botuser /app

# Переключение на непривилегированного пользователя
USER botuser

# Запуск бота
CMD ["python", "bot_main.py"]