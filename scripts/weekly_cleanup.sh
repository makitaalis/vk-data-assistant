#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[weekly-cleanup] Starting at $(date '+%Y-%m-%d %H:%M:%S')"

# 1. Truncate large log files (create if missing).
truncate -s 0 live_logs.txt
truncate -s 0 bot_stdout.log
truncate -s 0 logs/run_stdout.log
truncate -s 0 logs/error.log
truncate -s 0 logs/bot_restart.log

# 2. Remove temporary export/upload folders.
if [ -d data/temp ]; then
  find data/temp -mindepth 1 -maxdepth 1 -print0 | xargs -0r rm -rf
fi

# 3. Remove old exports and debug artifacts.
if [ -d exports ]; then
  find exports -type f -mtime +14 -print0 | xargs -0r rm -f
fi

if [ -d debug ]; then
  find debug -type f -mtime +30 -print0 | xargs -0r rm -f
fi

# 4. Keep only newest SQLite backups (leave latest file untouched).
if ls data/vk_data*.db* >/dev/null 2>&1; then
  mapfile -t backups < <(ls -1t data/vk_data*.db* | tail -n +3)
  if [ "${#backups[@]}" -gt 0 ]; then
    rm -f "${backups[@]}"
  fi
fi

echo "[weekly-cleanup] Finished at $(date '+%Y-%m-%d %H:%M:%S')"
