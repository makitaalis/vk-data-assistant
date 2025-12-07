"""Сервис для долговременного учета поисковых метрик."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("search_stats_service")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _today_key() -> str:
    return _utc_now().strftime("%Y-%m-%d")


def _safe_session_name(value: Optional[str]) -> str:
    return value or "default"


def _safe_bot_name(value: Optional[str]) -> str:
    if not value:
        return "unknown"
    return value.lstrip("@") or value


@dataclass
class SearchStatsData:
    totals: Dict[str, int] = field(default_factory=lambda: {"links_sent": 0, "bot_requests": 0})
    sessions: Dict[str, Dict[str, int]] = field(default_factory=dict)
    bots: Dict[str, Dict[str, int]] = field(default_factory=dict)
    per_day: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    updated_at: str = field(default_factory=lambda: _utc_now().isoformat())

    def as_dict(self) -> Dict[str, Any]:
        return {
            "totals": self.totals,
            "sessions": self.sessions,
            "bots": self.bots,
            "per_day": self.per_day,
            "updated_at": self.updated_at,
        }


class SearchStatsManager:
    """Хранит статистику по отправленным ссылкам и обращениям к Telegram-ботам."""

    HISTORY_DAYS = 14

    def __init__(self, storage_path: Path):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._data = self._load()

    def _load(self) -> SearchStatsData:
        if not self.storage_path.exists():
            return SearchStatsData()
        try:
            with self.storage_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            data = SearchStatsData(
                totals=payload.get("totals", {"links_sent": 0, "bot_requests": 0}),
                sessions=payload.get("sessions", {}),
                bots=payload.get("bots", {}),
                per_day=payload.get("per_day", {}),
                updated_at=payload.get("updated_at", _utc_now().isoformat()),
            )
            return data
        except Exception as exc:
            logger.error("Не удалось загрузить статистику (%s), начинаем с нуля", exc)
            backup_path = self.storage_path.with_suffix(".broken")
            try:
                self.storage_path.replace(backup_path)
            except Exception:
                pass
            return SearchStatsData()

    def _persist(self) -> None:
        tmp_path = self.storage_path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(self._data.as_dict(), f, ensure_ascii=False, indent=2)
        tmp_path.replace(self.storage_path)

    def _touch_day_bucket(self, date_key: str) -> Dict[str, Any]:
        day_bucket = self._data.per_day.setdefault(
            date_key,
            {"links_sent": 0, "bot_requests": 0, "sessions": {}, "bots": {}},
        )
        return day_bucket

    def _increment_bucket(self, bucket: Dict[str, int], key: str, metric: str, value: int = 1):
        stats = bucket.setdefault(key, {"links_sent": 0, "bot_requests": 0})
        stats[metric] = stats.get(metric, 0) + value

    def _prune_history(self):
        per_day = self._data.per_day
        if len(per_day) <= self.HISTORY_DAYS:
            return
        for day_key in sorted(per_day.keys())[:-self.HISTORY_DAYS]:
            per_day.pop(day_key, None)

    async def record_link_sent(self, session_name: Optional[str], bot_username: Optional[str]) -> None:
        async with self._lock:
            session_key = _safe_session_name(session_name)
            bot_key = _safe_bot_name(bot_username)
            self._data.totals["links_sent"] = self._data.totals.get("links_sent", 0) + 1
            self._increment_bucket(self._data.sessions, session_key, "links_sent")
            self._increment_bucket(self._data.bots, bot_key, "links_sent")

            day_bucket = self._touch_day_bucket(_today_key())
            day_bucket["links_sent"] = day_bucket.get("links_sent", 0) + 1
            self._increment_bucket(day_bucket["sessions"], session_key, "links_sent")
            self._increment_bucket(day_bucket["bots"], bot_key, "links_sent")

            self._data.updated_at = _utc_now().isoformat()
            self._prune_history()
            self._persist()

    async def record_bot_request(self, session_name: Optional[str], bot_username: Optional[str]) -> None:
        async with self._lock:
            session_key = _safe_session_name(session_name)
            bot_key = _safe_bot_name(bot_username)
            self._data.totals["bot_requests"] = self._data.totals.get("bot_requests", 0) + 1
            self._increment_bucket(self._data.sessions, session_key, "bot_requests")
            self._increment_bucket(self._data.bots, bot_key, "bot_requests")

            day_bucket = self._touch_day_bucket(_today_key())
            day_bucket["bot_requests"] = day_bucket.get("bot_requests", 0) + 1
            self._increment_bucket(day_bucket["sessions"], session_key, "bot_requests")
            self._increment_bucket(day_bucket["bots"], bot_key, "bot_requests")

            self._data.updated_at = _utc_now().isoformat()
            self._prune_history()
            self._persist()

    async def get_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            payload = json.loads(json.dumps(self._data.as_dict()))
            return payload
