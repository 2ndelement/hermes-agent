from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from gateway.config import Platform
from gateway.platform_registry import PlatformEntry, platform_registry


def _load_adapter():
    module_name = "plugin_adapter_qqbot_plus_local_test"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached
    adapter_path = Path(__file__).with_name("adapter.py")
    spec = importlib.util.spec_from_file_location(module_name, adapter_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {adapter_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_qqbot_plus = _load_adapter()
QQBotPlusAdapter = _qqbot_plus.QQBotPlusAdapter
register = _qqbot_plus.register


class _PluginContext:
    def register_platform(self, **kwargs):
        entry = PlatformEntry(source="plugin", **kwargs)
        platform_registry.register(entry)


@pytest.fixture
def clean_platform_registry():
    original = dict(platform_registry._entries)
    platform_registry._entries.clear()
    try:
        yield
    finally:
        platform_registry._entries.clear()
        platform_registry._entries.update(original)


def _config() -> SimpleNamespace:
    return SimpleNamespace(extra={}, enabled=True, home_channel=None, reply_to_mode="first")


def test_register_adds_qqbot_plus_platform(clean_platform_registry):
    register(_PluginContext())

    entry = platform_registry.get("qqbot-plus")

    assert entry is not None
    assert entry.name == "qqbot-plus"
    assert entry.label == "QQBot Plus"
    assert Platform("qqbot-plus").value == "qqbot-plus"


def test_adapter_advertises_append_streaming_capabilities():
    adapter = QQBotPlusAdapter(_config())

    assert adapter.SUPPORTS_MESSAGE_EDITING is True
    assert adapter.APPENDS_STREAMING_MESSAGE_UPDATES is True
    assert adapter.REQUIRES_EDIT_FINALIZE is True


@pytest.mark.asyncio
async def test_identify_subscribes_to_interaction_events_for_buttons():
    adapter = QQBotPlusAdapter(_config())
    adapter._ensure_token = AsyncMock(return_value="token-1")
    adapter._ws = SimpleNamespace(closed=False, send_json=AsyncMock())

    await adapter._send_identify()

    payload = adapter._ws.send_json.await_args.args[0]
    assert payload["op"] == 2
    assert payload["d"]["intents"] & adapter.INTENT_INTERACTION_CREATE


@pytest.mark.asyncio
async def test_group_interaction_from_disallowed_user_is_acked_but_not_dispatched():
    adapter = QQBotPlusAdapter(
        SimpleNamespace(
            extra={"group_policy": "allowlist", "group_allow_from": ["group-1"]},
            enabled=True,
            home_channel=None,
            reply_to_mode="first",
        )
    )
    ack_calls = []

    async def fake_ack(interaction_id, code=0):
        ack_calls.append((interaction_id, code))

    adapter._acknowledge_interaction = fake_ack
    callback = AsyncMock()
    adapter.set_interaction_callback(callback)

    await adapter._on_interaction({
        "id": "interaction-1",
        "chat_type": 1,
        "group_openid": "group-2",
        "group_member_openid": "member-1",
        "data": {"type": 11, "resolved": {"button_data": "approve:s:deny"}},
    })

    assert ack_calls == [("interaction-1", 0)]
    callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_c2c_interaction_from_disallowed_user_is_acked_but_not_dispatched():
    adapter = QQBotPlusAdapter(
        SimpleNamespace(
            extra={"dm_policy": "allowlist", "allow_from": ["user-1"]},
            enabled=True,
            home_channel=None,
            reply_to_mode="first",
        )
    )
    ack_calls = []

    async def fake_ack(interaction_id, code=0):
        ack_calls.append((interaction_id, code))

    adapter._acknowledge_interaction = fake_ack
    callback = AsyncMock()
    adapter.set_interaction_callback(callback)

    await adapter._on_interaction({
        "id": "interaction-1",
        "chat_type": 2,
        "user_openid": "user-2",
        "data": {"type": 11, "resolved": {"button_data": "approve:s:deny"}},
    })

    assert ack_calls == [("interaction-1", 0)]
    callback.assert_not_awaited()


def test_env_enablement_uses_plus_specific_credentials():
    with patch.dict(
        "os.environ",
        {
            "QQBOT_PLUS_APP_ID": "plus-app",
            "QQBOT_PLUS_CLIENT_SECRET": "plus-secret",
            "QQBOT_PLUS_HOME_CHANNEL": "user-1",
        },
        clear=True,
    ):
        seed = _qqbot_plus._env_enablement()

    assert seed == {
        "app_id": "plus-app",
        "client_secret": "plus-secret",
        "markdown_support": True,
        "home_channel": {"chat_id": "user-1", "name": "user-1"},
    }


def test_env_enablement_ignores_plain_qqbot_credentials():
    with patch.dict(
        "os.environ",
        {"QQ_APP_ID": "qq-app", "QQ_CLIENT_SECRET": "qq-secret"},
        clear=True,
    ):
        assert _qqbot_plus._env_enablement() is None


@pytest.mark.asyncio
async def test_initial_send_uses_stream_protocol_for_text():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})

    result = await adapter.send("user-1", "hello")

    assert result.success is True
    assert result.message_id == "stream-1"
    adapter._api_request.assert_awaited_once()
    method, path, body = adapter._api_request.await_args.args
    assert method == "POST"
    assert path == "/v2/users/user-1/messages"
    assert body["stream"] == {"state": 1, "index": 0, "reset": False}
    assert "id" not in body["stream"]
    assert isinstance(body["msg_seq"], int)
    assert adapter._stream_sent_text["stream-1"] == "hello"
    assert adapter._stream_indices["stream-1"] == 1


@pytest.mark.asyncio
async def test_send_with_harmless_metadata_still_uses_stream_protocol():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})

    result = await adapter.send("user-1", "hello", metadata={"source": "gateway"})

    assert result.success is True
    assert result.message_id == "stream-1"
    adapter._api_request.assert_awaited_once()
    method, path, body = adapter._api_request.await_args.args
    assert method == "POST"
    assert path == "/v2/users/user-1/messages"
    assert body["stream"]["state"] == 1
    assert body["stream"]["index"] == 0


@pytest.mark.asyncio
async def test_send_with_streaming_false_uses_qq_text_send():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    metadata = {"streaming": False}

    with patch.object(_qqbot_plus.QQAdapter, "send", new=AsyncMock(return_value=SimpleNamespace(success=True, message_id="text-1"))) as qq_send:
        result = await adapter.send("user-1", "hello", metadata=metadata)

    assert result.success is True
    assert result.message_id == "text-1"
    qq_send.assert_awaited_once_with("user-1", "hello", reply_to=None, metadata=metadata)


def test_adapter_inherits_qq_media_and_keyboard_methods():
    adapter = QQBotPlusAdapter(_config())

    for method_name in (
        "send_with_keyboard",
        "send_exec_approval",
        "send_update_prompt",
        "send_image",
        "send_image_file",
        "send_voice",
        "send_video",
        "send_document",
    ):
        assert callable(getattr(adapter, method_name))


@pytest.mark.asyncio
async def test_media_methods_reuse_qq_rich_media_upload_with_tool_metadata():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True

    with patch.object(QQBotPlusAdapter, "_send_media", new=AsyncMock(return_value=SimpleNamespace(success=True, message_id="media-1"))) as send_media:
        result = await adapter.send_image_file(
            chat_id="user-1",
            image_path="/tmp/example.png",
            metadata={"streaming": False},
        )

    assert result.success is True
    assert result.message_id == "media-1"
    send_media.assert_awaited_once()
    args = send_media.await_args.args
    assert args[0] == "user-1"
    assert args[1] == "/tmp/example.png"
    assert args[3] == "image"


def test_stream_body_only_adds_newline_when_finalizing():
    adapter = QQBotPlusAdapter(_config())

    middle = adapter._build_stream_body("hello", "stream-1", 1, finalize=False)
    final = adapter._build_stream_body("world", "stream-1", 2, finalize=True)

    assert middle["markdown"]["content"] == "hello"
    assert final["markdown"]["content"] == "world\n"


def test_stream_body_uses_generated_msg_seq():
    adapter = QQBotPlusAdapter(_config())
    adapter._next_msg_seq = Mock(side_effect=[101, 102])

    first = adapter._build_stream_body("hello", None, 0, finalize=False)
    second = adapter._build_stream_body("world", "stream-1", 1, finalize=True)

    assert first["msg_seq"] == 101
    assert second["msg_seq"] == 102
    assert adapter._next_msg_seq.call_args_list[0].args == ("qqbot-plus-stream",)
    assert adapter._next_msg_seq.call_args_list[1].args == ("stream-1",)


def test_stream_path_url_encodes_chat_id():
    adapter = QQBotPlusAdapter(_config())

    assert adapter._stream_path_for_chat("user/../x") == "/v2/users/user%2F..%2Fx/messages"


@pytest.mark.asyncio
async def test_send_forwards_metadata_when_falling_back_to_qq_send():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    adapter._stream_path_for_chat = Mock(return_value=None)
    metadata = {"attachments": [{"id": "att-1"}]}

    with patch.object(_qqbot_plus.QQAdapter, "send", new=AsyncMock(return_value=SimpleNamespace(success=True, message_id="msg-1"))) as qq_send:
        result = await adapter.send("guild-1", "hello", metadata=metadata)

    assert result.success is True
    qq_send.assert_awaited_once_with("guild-1", "hello", reply_to=None, metadata=metadata)


@pytest.mark.asyncio
async def test_streaming_diff_ignores_gateway_cursor_suffix():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    adapter._api_request = AsyncMock(
        side_effect=[{"id": "stream-1"}, {"id": "stream-1"}, {"id": "stream-1"}],
    )

    await adapter.send("user-1", "你 ▉")
    await adapter.edit_message("user-1", "stream-1", "你好 ▉")
    await adapter.edit_message("user-1", "stream-1", "你好", finalize=True)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    chunks = [body["markdown"]["content"] for body in bodies]

    assert chunks == ["你", "好", "\n"]


@pytest.mark.asyncio
async def test_streaming_sends_each_increment_without_background_coalescing():
    adapter = QQBotPlusAdapter(_config())
    adapter._running = True
    adapter._api_request = AsyncMock(
        side_effect=[
            {"id": "stream-1"},
            {"id": "stream-1"},
            {"id": "stream-1"},
            {"id": "stream-1"},
        ],
    )

    await adapter.send("user-1", "你")
    await adapter.edit_message("user-1", "stream-1", "你好")
    await adapter.edit_message("user-1", "stream-1", "你好啊")
    await adapter.edit_message("user-1", "stream-1", "你好啊", finalize=True)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    chunks = [body["markdown"]["content"] for body in bodies]
    states = [body["stream"]["state"] for body in bodies]

    assert chunks == ["你", "好", "啊", "\n"]
    assert states == [1, 1, 1, 10]


@pytest.mark.asyncio
async def test_duplicate_nonfinal_update_does_not_send_empty_chunk():
    adapter = QQBotPlusAdapter(_config())
    adapter._stream_sent_text["stream-1"] = "hello"
    adapter._stream_indices["stream-1"] = 1
    adapter._stream_touched_at["stream-1"] = time.monotonic()
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})

    result = await adapter.edit_message("user-1", "stream-1", "hello")

    assert result.success is True
    adapter._api_request.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_finalize_for_same_content_is_idempotent():
    adapter = QQBotPlusAdapter(_config())
    adapter._stream_sent_text["stream-1"] = "hello"
    adapter._stream_indices["stream-1"] = 1
    adapter._stream_touched_at["stream-1"] = time.monotonic()
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})

    first = await adapter.edit_message("user-1", "stream-1", "hello", finalize=True)
    second = await adapter.edit_message("user-1", "stream-1", "hello", finalize=True)

    assert first.success is True
    assert second.success is True
    assert adapter._api_request.await_count == 1


@pytest.mark.asyncio
async def test_failed_finalize_is_not_cached_as_idempotent():
    adapter = QQBotPlusAdapter(_config())
    adapter._stream_sent_text["stream-1"] = "hello"
    adapter._stream_indices["stream-1"] = 1
    adapter._stream_touched_at["stream-1"] = time.monotonic()
    adapter._api_request = AsyncMock(side_effect=[RuntimeError("boom"), {"id": "stream-1"}])

    with pytest.raises(RuntimeError):
        await adapter.edit_message("user-1", "stream-1", "hello", finalize=True)

    result = await adapter.edit_message("user-1", "stream-1", "hello", finalize=True)

    assert result.success is True
    assert adapter._api_request.await_count == 2
