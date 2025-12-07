#!/usr/bin/env python3
"""Тестирование SessionAuthManager без реального Telegram."""

import asyncio
import shutil
from pathlib import Path
from dataclasses import dataclass

from telethon.errors import PhoneNumberInvalidError, SessionPasswordNeededError

from services.session_auth_service import SessionAuthManager


@dataclass
class FakeSession:
    value: str = "FAKE_SESSION_STRING"

    def save(self) -> str:
        return self.value


class FakeClient:
    def __init__(self, scenario: dict):
        self.scenario = scenario
        self.session = FakeSession()
        self.sent = False
        self.disconnected = False
        self.password_used = False

    async def connect(self):
        return True

    async def send_code_request(self, phone):
        if self.scenario.get("invalid_phone"):
            raise PhoneNumberInvalidError(request=None)
        self.sent = True

        class Sent:
            phone_code_hash = "hash"

        return Sent()

    async def sign_in(self, phone=None, code=None, phone_code_hash=None, password=None):
        if self.scenario.get("require_password") and not password:
            raise SessionPasswordNeededError(request=None)

        if password:
            self.password_used = True
            return True

        if self.scenario.get("bad_code"):
            raise ValueError("bad code")

        return True

    async def get_me(self):
        class Me:
            first_name = "Test"
            last_name = "User"
            phone = "+10000000000"
            id = 777
            username = "tester"

        return Me()

    async def disconnect(self):
        self.disconnected = True


async def fake_client_factory(scenario):
    return FakeClient(scenario)


async def test_success(tmp_dir: Path):
    factory = lambda: FakeClient({"require_password": False})
    manager = SessionAuthManager(1, "hash", tmp_dir, client_factory=factory)
    job = await manager.start_job(1, "session_a", "+1000000000", "slot_a")
    assert job.phone == "+1000000000"
    result = await manager.submit_code(1, "12345")
    assert result["status"] == "completed"
    saved = tmp_dir / "session_a" / "session_a.session_string"
    assert saved.exists()
    print("✅ Успешная авторизация прошла")


async def test_invalid_number(tmp_dir: Path):
    factory = lambda: FakeClient({"invalid_phone": True})
    manager = SessionAuthManager(1, "hash", tmp_dir, client_factory=factory)
    try:
        await manager.start_job(2, "session_b", "+1999", None)
    except ValueError:
        print("✅ Некорректный номер распознан")
    else:
        raise AssertionError("Ожидалась ошибка для неправильного номера")


async def test_password_flow(tmp_dir: Path):
    scenario = {"require_password": True}
    factory = lambda: FakeClient(scenario)
    manager = SessionAuthManager(1, "hash", tmp_dir, client_factory=factory)
    await manager.start_job(3, "session_c", "+12223334444", "slot_b")
    intermediate = await manager.submit_code(3, "12345")
    assert intermediate["status"] == "password_required"
    result = await manager.submit_password(3, "pass123")
    assert result["status"] == "completed"
    saved = tmp_dir / "session_c" / "session_c.session_string"
    assert saved.exists()
    print("✅ Сценарий с 2FA завершен успешно")


async def main():
    tmp_dir = Path("/tmp/session_auth_test")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    await test_success(tmp_dir)
    await test_invalid_number(tmp_dir)
    await test_password_flow(tmp_dir)

    print("🎉 Все сценарии SessionAuthManager пройдены")


if __name__ == "__main__":
    asyncio.run(main())
