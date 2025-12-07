#!/bin/bash

# Проверка работы бота
BOT_PID=$(systemctl show -p MainPID --value vkbot)

if [ "$BOT_PID" -eq 0 ]; then
    echo "? Bot is not running!"
    systemctl start vkbot
    exit 1
fi

# Проверка PostgreSQL
if ! pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    echo "? PostgreSQL is not responding!"
    exit 1
fi

# Проверка Redis
if ! redis-cli ping > /dev/null 2>&1; then
    echo "? Redis is not responding!"
    exit 1
fi

# Проверка использования памяти
MEMORY_USAGE=$(ps -p $BOT_PID -o %mem --no-headers | tr -d ' ')
if (( $(echo "$MEMORY_USAGE > 80.0" | bc -l) )); then
    echo "?? High memory usage: $MEMORY_USAGE%"
    systemctl restart vkbot
fi

echo "? All systems operational"