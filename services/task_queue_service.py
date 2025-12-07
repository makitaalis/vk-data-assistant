import asyncio
from typing import List, Dict, Optional, Any
from db_module import VKDatabase


class TaskQueueService:
    """Обёртка над VKDatabase для работы с очередью поиска (persist)."""

    def __init__(self, db: VKDatabase, *, batch_size: int = 20, stale_minutes: int = 60):
        self.db = db
        self.batch_size = batch_size
        self.stale_minutes = stale_minutes
        self._lock = asyncio.Lock()

    async def enqueue_links(self, user_id: int, links: List[str], session_name: Optional[str] = None) -> List[int]:
        """Добавляет ссылки в очередь. Возвращает id задач."""
        if not links:
            return []
        return await self.db.add_search_tasks(user_id, links, session_name=session_name)

    async def fetch_batch(self) -> List[Dict[str, Any]]:
        """Забирает пачку pending задач и помечает их processing."""
        async with self._lock:
            await self.db.reset_stale_tasks(self.stale_minutes)
            return await self.db.fetch_next_tasks(self.batch_size)

    async def complete(self, task_id: int, result: Dict[str, Any]):
        await self.db.complete_task(task_id, result)

    async def fail(self, task_id: int, error: str):
        await self.db.fail_task(task_id, error)

    async def stats(self) -> Dict[str, int]:
        return await self.db.get_queue_stats()

    async def user_stats(self, user_id: int) -> Dict[str, int]:
        return await self.db.get_user_task_stats(user_id)

    async def cancel_user_tasks(self, user_id: int) -> int:
        return await self.db.cancel_user_tasks(user_id)

    async def recent_results(self, user_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        return await self.db.get_recent_results(user_id, limit)

    async def user_results(self, user_id: int) -> Dict[str, Any]:
        return await self.db.get_user_results(user_id)

    async def set_user_cancel_flag(self, user_id: int):
        """Устанавливает флаг отмены пользователя (простая запись в память)."""
        # Храним в памяти (процессовая очередь). Для многопроцессного варианта нужен Redis.
        setattr(self, "_cancel_flags", getattr(self, "_cancel_flags", {}))
        self._cancel_flags[user_id] = True

    def is_user_cancelled(self, user_id: int) -> bool:
        flags = getattr(self, "_cancel_flags", {})
        return bool(flags.get(user_id))

    async def failed_summary(self, limit: int = 3, window_hours: int = 6) -> List[Dict[str, Any]]:
        return await self.db.get_failed_summary(limit=limit, window_hours=window_hours)
