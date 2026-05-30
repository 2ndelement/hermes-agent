from types import SimpleNamespace

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.qqbot.adapter import QQAdapter


@pytest.mark.asyncio
async def test_listen_loop_retries_after_reconnect_failure_on_closed_websocket(monkeypatch):
    adapter = QQAdapter(
        PlatformConfig(
            enabled=True,
            extra={"app_id": "app", "client_secret": "secret"},
        )
    )
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=True)

    attempts = 0

    async def fake_reconnect(backoff_idx):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            assert adapter._running is True
            assert not adapter.is_connected
        if attempts >= 2:
            adapter._ws = SimpleNamespace(closed=False)
            adapter._mark_connected()
            return True
        return False

    reads = 0

    async def fake_read_events():
        nonlocal reads
        reads += 1
        adapter._running = False
        return None

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(adapter, "_read_events", fake_read_events)
    monkeypatch.setattr(adapter, "_reconnect", fake_reconnect)
    monkeypatch.setattr("gateway.platforms.qqbot.adapter.asyncio.sleep", fake_sleep)

    await adapter._listen_loop()

    assert attempts == 2
    assert reads == 1
