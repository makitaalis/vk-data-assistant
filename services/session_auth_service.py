"""
Сервис для интерактивной авторизации Telegram-сессий через бота.
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Any

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError

logger = logging.getLogger("session_auth_service")


@dataclass
class SessionAuthJob:
    job_id: str
    admin_id: int
    phone: str
    session_name: str
    slot: Optional[str]
    client: TelegramClient
    status: str = "waiting_code"
    phone_code_hash: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())
    password_required: bool = False
    last_error: Optional[str] = None
    profile_info: Dict[str, Any] = field(default_factory=dict)


class SessionAuthManager:
    """Управляет процессом авторизации Telegram-сессий через Telethon."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_base_dir: Path,
        client_factory: Optional[Any] = None,
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_base_dir = Path(session_base_dir)
        self.jobs: Dict[int, SessionAuthJob] = {}
        self.lock = asyncio.Lock()
        self.client_factory = client_factory

    async def _create_client(self) -> TelegramClient:
        if self.client_factory:
            maybe_client = self.client_factory()
            if asyncio.iscoroutine(maybe_client):
                client = await maybe_client
            else:
                client = maybe_client
            return client

        session = StringSession()
        client = TelegramClient(session, self.api_id, self.api_hash)
        await client.connect()
        return client

    async def start_job(self, admin_id: int, session_name: str, phone: str, slot: Optional[str]) -> SessionAuthJob:
        """Отправляет код на телефон и создает новую задачу авторизации."""
        async with self.lock:
            await self.cancel_job(admin_id)

            try:
                client = await self._create_client()
            except Exception as exc:
                logger.error("Не удалось создать Telethon client: %s", exc)
                raise RuntimeError("Не удалось подключиться к Telegram. Попробуйте позже.") from exc

            try:
                sent = await client.send_code_request(phone)
            except PhoneNumberInvalidError as exc:
                await client.disconnect()
                raise ValueError("Неверный номер телефона") from exc
            except Exception as exc:
                await client.disconnect()
                logger.error("Ошибка отправки кода: %s", exc)
                raise RuntimeError("Не удалось отправить код. Попробуйте позже.") from exc

            job = SessionAuthJob(
                job_id=str(uuid.uuid4()),
                admin_id=admin_id,
                phone=phone,
                session_name=session_name,
                slot=slot,
                client=client,
                phone_code_hash=getattr(sent, "phone_code_hash", None),
            )
            self.jobs[admin_id] = job
            logger.info("Создана задача авторизации %s для %s", job.job_id, session_name)
            return job

    async def submit_code(self, admin_id: int, code: str) -> Dict[str, Any]:
        """Принимает код из SMS и пытается завершить авторизацию."""
        job = self.jobs.get(admin_id)
        if not job:
            raise RuntimeError("Нет активной авторизации. Начните заново.")

        try:
            await job.client.sign_in(
                phone=job.phone,
                code=code,
                phone_code_hash=job.phone_code_hash,
            )
            return await self._finalize_job(admin_id)
        except SessionPasswordNeededError:
            job.status = "waiting_password"
            job.password_required = True
            logger.info("Для %s требуется 2FA пароль", job.session_name)
            return {"status": "password_required"}
        except Exception as exc:
            logger.error("Ошибка подтверждения кода: %s", exc)
            raise RuntimeError("Не удалось подтвердить код. Попробуйте заново.") from exc

    async def submit_password(self, admin_id: int, password: str) -> Dict[str, Any]:
        """Принимает пароль 2FA."""
        job = self.jobs.get(admin_id)
        if not job or job.status != "waiting_password":
            raise RuntimeError("Нет задачи, ожидающей пароль.")

        try:
            await job.client.sign_in(password=password)
            return await self._finalize_job(admin_id)
        except Exception as exc:
            logger.error("Ошибка ввода пароля: %s", exc)
            raise RuntimeError("Неверный пароль или ошибка Telegram. Попробуйте снова.") from exc

    async def cancel_job(self, admin_id: int):
        """Отменяет активную задачу."""
        job = self.jobs.pop(admin_id, None)
        if job and job.client:
            try:
                await job.client.disconnect()
            except Exception:
                pass
            logger.info("Задача авторизации %s отменена", job.job_id)

    async def get_job_status(self, admin_id: int) -> Optional[Dict[str, Any]]:
        job = self.jobs.get(admin_id)
        if not job:
            return None
        return {
            "session_name": job.session_name,
            "phone": job.phone,
            "status": job.status,
            "slot": job.slot,
            "created_at": job.created_at,
            "password_required": job.password_required,
            "last_error": job.last_error,
        }

    async def _finalize_job(self, admin_id: int) -> Dict[str, Any]:
        job = self.jobs.get(admin_id)
        if not job:
            raise RuntimeError("Задача авторизации не найдена.")

        try:
            me = await job.client.get_me()
            info = {
                "first_name": me.first_name,
                "last_name": me.last_name or "",
                "phone": me.phone or job.phone,
                "user_id": me.id,
                "username": me.username,
            }
            session_string = job.client.session.save()
            self._persist_session(job.session_name, session_string)
            logger.info("Сессия %s успешно сохранена", job.session_name)
            await job.client.disconnect()
            self.jobs.pop(admin_id, None)
            return {"status": "completed", "profile": info, "session_name": job.session_name, "slot": job.slot}
        except Exception as exc:
            logger.error("Ошибка финализации сессии %s: %s", job.session_name, exc)
            job.last_error = str(exc)
            raise RuntimeError("Не удалось сохранить сессию. Попробуйте заново.") from exc

    def _persist_session(self, session_name: str, session_string: str):
        """Сохраняет StringSession в файловую систему."""
        session_dir = self.session_base_dir / session_name
        session_dir.mkdir(parents=True, exist_ok=True)
        session_file = session_dir / f"{session_name}.session_string"
        backup_file = session_dir / f"{session_name}.session_string.bak"

        if session_file.exists():
            session_file.replace(backup_file)

        session_file.write_text(session_string, encoding="utf-8")
        logger.info("Session string сохранен в %s", session_file)
