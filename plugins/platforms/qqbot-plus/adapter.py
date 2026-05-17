from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import SendResult
from gateway.platforms.qqbot import QQAdapter, check_qq_requirements
from gateway.platforms.qqbot.keyboards import parse_interaction_event

logger = logging.getLogger(__name__)

QQBOT_PLUS_PLATFORM = "qqbot-plus"


class QQBotPlusAdapter(QQAdapter):
    SUPPORTS_MESSAGE_EDITING = True
    APPENDS_STREAMING_MESSAGE_UPDATES = True
    REQUIRES_EDIT_FINALIZE = True
    INTENT_DIRECT_MESSAGE = 1 << 12
    INTENT_C2C_GROUP_AT_MESSAGES = 1 << 25
    INTENT_INTERACTION_CREATE = 1 << 26
    INTENT_PUBLIC_GUILD_MESSAGES = 1 << 30
    _STREAM_STATE_TTL_SECONDS = 600.0

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config)
        platform = Platform(QQBOT_PLUS_PLATFORM)
        self.platform = platform
        self.PLATFORM = platform
        self._stream_indices: dict[str, int] = {}
        self._stream_sent_text: dict[str, str] = {}
        self._stream_touched_at: dict[str, float] = {}
        self._stream_finalized: dict[str, str] = {}

    async def _send_identify(self) -> None:
        token = await self._ensure_token()
        identify_payload = {
            "op": 2,
            "d": {
                "token": f"QQBot {token}",
                "intents": (
                    self.INTENT_DIRECT_MESSAGE
                    | self.INTENT_C2C_GROUP_AT_MESSAGES
                    | self.INTENT_INTERACTION_CREATE
                    | self.INTENT_PUBLIC_GUILD_MESSAGES
                ),
                "shard": [0, 1],
                "properties": {
                    "$os": "macOS",
                    "$browser": "hermes-agent",
                    "$device": "hermes-agent",
                },
            },
        }
        try:
            if self._ws and not self._ws.closed:
                await self._ws.send_json(identify_payload)
                logger.info("[%s] Identify sent", self._log_tag)
            else:
                logger.warning(
                    "[%s] Cannot send Identify: WebSocket not connected", self._log_tag
                )
        except Exception as exc:
            logger.error("[%s] Failed to send Identify: %s", self._log_tag, exc)

    async def _on_interaction(self, d: Any) -> None:
        if not isinstance(d, dict):
            return
        try:
            event = parse_interaction_event(d)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to parse INTERACTION_CREATE: %s", self._log_tag, exc
            )
            return

        if not event.id:
            logger.warning(
                "[%s] INTERACTION_CREATE missing id, skipping ACK", self._log_tag
            )
            return

        try:
            await self._acknowledge_interaction(event.id)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to ACK interaction %s: %s",
                self._log_tag,
                event.id,
                exc,
            )

        if event.scene == "c2c" and not self._is_dm_allowed(event.user_openid):
            logger.info(
                "[%s] Dropping disallowed c2c interaction from %s",
                self._log_tag,
                event.user_openid,
            )
            return
        if event.scene == "group" and not self._is_group_allowed(
            event.group_openid,
            event.operator_openid,
        ):
            logger.info(
                "[%s] Dropping disallowed group interaction from group=%s operator=%s",
                self._log_tag,
                event.group_openid,
                event.operator_openid,
            )
            return

        logger.info(
            "[%s] Interaction: scene=%s button_data=%r operator=%s",
            self._log_tag,
            event.scene,
            event.button_data,
            event.operator_openid,
        )

        callback = self._interaction_callback
        if callback is None:
            logger.debug(
                "[%s] No interaction callback registered; dropping button click %r",
                self._log_tag,
                event.button_data,
            )
            return
        try:
            await callback(event)
        except Exception as exc:
            logger.error(
                "[%s] Interaction callback raised: %s",
                self._log_tag,
                exc,
                exc_info=True,
            )

    def _prune_stream_state(self) -> None:
        cutoff = time.monotonic() - self._STREAM_STATE_TTL_SECONDS
        stale_keys = [
            key
            for key, touched_at in self._stream_touched_at.items()
            if touched_at < cutoff
        ]
        for key in stale_keys:
            self._clear_stream_state(key)

    def _clear_stream_state(self, message_id: str) -> None:
        self._stream_indices.pop(message_id, None)
        self._stream_sent_text.pop(message_id, None)
        self._stream_touched_at.pop(message_id, None)

    async def _clear_all_stream_state(self) -> None:
        self._stream_indices.clear()
        self._stream_sent_text.clear()
        self._stream_touched_at.clear()
        self._stream_finalized.clear()

    @staticmethod
    def _clean_stream_content(content: str) -> str:
        if content.endswith(" ▉"):
            return content[:-2]
        if content.endswith("▉"):
            return content[:-1].rstrip()
        return content

    async def disconnect(self) -> None:
        await self._clear_all_stream_state()
        await super().disconnect()

    def _stream_path_for_chat(self, chat_id: str) -> Optional[str]:
        chat_type = self._guess_chat_type(chat_id)
        encoded_chat_id = quote(chat_id, safe="")
        if chat_type == "c2c":
            return f"/v2/users/{encoded_chat_id}/messages"
        if chat_type == "group":
            return f"/v2/groups/{encoded_chat_id}/messages"
        return None

    def _build_stream_body(
        self,
        chunk: str,
        message_id: str | None,
        index: int,
        *,
        finalize: bool,
    ) -> Dict[str, Any]:
        if self._markdown_support and not chunk.endswith("\n"):
            chunk += "\n"
        body = self._build_text_body(chunk, message_id or "qqbot-plus-stream")
        body.pop("message_reference", None)
        body["stream"] = {
            "state": 10 if finalize else 1,
            "id": message_id,
            "index": index,
            "reset": False,
        }
        return body

    def _log_stream_send_failure(self, exc: BaseException, body: Dict[str, Any]) -> None:
        stream = body.get("stream") or {}
        content = ""
        markdown = body.get("markdown")
        if isinstance(markdown, dict):
            content = str(markdown.get("content", ""))
        else:
            content = str(body.get("content", ""))
        logger.error(
            "QQBot Plus stream send failed: %s state=%s index=%s has_stream_id=%s has_msg_seq=%s content_len=%d",
            exc,
            stream.get("state"),
            stream.get("index"),
            bool(stream.get("id")),
            bool(body.get("msg_seq")),
            len(content),
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if metadata and metadata.get("streaming") is False:
            return await super().send(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=metadata,
            )

        if not self.is_connected:
            if not await self._wait_for_reconnection():
                return SendResult(success=False, error="Not connected", retryable=True)

        if not content or not content.strip():
            return SendResult(success=True)

        path = self._stream_path_for_chat(chat_id)
        if path is None:
            return await super().send(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=metadata,
            )

        content = self._clean_stream_content(content)
        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, self.MAX_MESSAGE_LENGTH)
        if len(chunks) != 1:
            return await super().send(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=metadata,
            )

        body = self._build_stream_body(
            chunks[0],
            None,
            0,
            finalize=False,
        )
        body["stream"].pop("id", None)
        if reply_to:
            body["msg_id"] = reply_to
        try:
            data = await self._api_request("POST", path, body)
        except Exception as exc:
            self._log_stream_send_failure(exc, body)
            raise
        response_id = str(data.get("id") or "")
        if response_id:
            self._stream_indices[response_id] = 1
            self._stream_sent_text[response_id] = chunks[0]
            self._stream_touched_at[response_id] = time.monotonic()
        return SendResult(success=True, message_id=response_id or None, raw_response=data)

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del session_key
        non_streaming_metadata = dict(metadata or {})
        non_streaming_metadata["streaming"] = False
        text = f"❓ {question}"
        if choices:
            from gateway.platforms.qqbot.keyboards import build_clarify_keyboard

            result = await self.send_with_keyboard(
                chat_id,
                text,
                build_clarify_keyboard(clarify_id, [str(choice) for choice in choices]),
            )
            if result.success:
                return result
            try:
                from tools.clarify_gateway import mark_awaiting_text
                mark_awaiting_text(clarify_id)
            except Exception:
                pass
            lines = [text, ""]
            for idx, choice in enumerate(choices, start=1):
                lines.append(f"{idx}. {choice}")
            lines.extend(["", "请回复编号、选项文本，或直接输入其他答案。"])
            return await QQAdapter.send(
                self,
                chat_id,
                "\n".join(lines),
                metadata=non_streaming_metadata,
            )
        return await QQAdapter.send(
            self,
            chat_id,
            text,
            metadata=non_streaming_metadata,
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        path = self._stream_path_for_chat(chat_id)
        if path is None:
            return SendResult(
                success=False,
                error="QQBot Plus streaming is only supported for C2C and group chats",
            )

        self._prune_stream_state()
        if finalize and message_id and self._stream_finalized.get(message_id) == content:
            return SendResult(success=True, message_id=message_id, raw_response={})
        content = self._clean_stream_content(content)
        sent_text = self._stream_sent_text.get(message_id, "") if message_id else ""
        chunk = content
        if sent_text and content.startswith(sent_text):
            chunk = content[len(sent_text):]
        if not chunk:
            self._stream_sent_text[message_id] = content
            self._stream_touched_at[message_id] = time.monotonic()
            if finalize and message_id:
                self._stream_finalized[message_id] = content
                self._clear_stream_state(message_id)
            return SendResult(success=True, message_id=message_id, raw_response={})
        if finalize and not chunk.endswith("\n"):
            chunk += "\n"
        index = self._stream_indices.get(message_id, 0) if message_id else 0
        body = self._build_stream_body(
            chunk,
            message_id or None,
            index,
            finalize=finalize,
        )
        try:
            data = await self._api_request("POST", path, body)
        except Exception as exc:
            self._log_stream_send_failure(exc, body)
            if finalize and message_id:
                self._clear_stream_state(message_id)
            raise
        response_id = str(data.get("id") or message_id or "")
        if response_id:
            self._stream_indices[response_id] = index + 1
            self._stream_sent_text[response_id] = content
            self._stream_touched_at[response_id] = time.monotonic()
        if finalize and response_id:
            self._stream_finalized[response_id] = content
            self._clear_stream_state(response_id)
        return SendResult(success=True, message_id=response_id or None, raw_response=data)


check_qqbot_plus_requirements = check_qq_requirements


def _env_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _env_enablement() -> dict[str, Any] | None:
    app_id = os.getenv("QQBOT_PLUS_APP_ID", "").strip()
    client_secret = os.getenv("QQBOT_PLUS_CLIENT_SECRET", "").strip()
    if not (app_id and client_secret):
        return None
    if os.getenv("QQBOT_PLUS_ENABLED") and not _env_enabled("QQBOT_PLUS_ENABLED"):
        return None
    seed: dict[str, Any] = {
        "app_id": app_id,
        "client_secret": client_secret,
        "markdown_support": True,
    }
    home = os.getenv("QQBOT_PLUS_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("QQBOT_PLUS_HOME_CHANNEL_NAME", home),
        }
    return seed


def validate_config(config: PlatformConfig) -> bool:
    extra = getattr(config, "extra", {}) or {}
    app_id = str(extra.get("app_id") or os.getenv("QQBOT_PLUS_APP_ID", "")).strip()
    client_secret = str(
        extra.get("client_secret") or os.getenv("QQBOT_PLUS_CLIENT_SECRET", "")
    ).strip()
    return bool(app_id and client_secret)


def is_connected(config: PlatformConfig) -> bool:
    return validate_config(config)


def register(ctx: Any) -> None:
    ctx.register_platform(
        name=QQBOT_PLUS_PLATFORM,
        label="QQBot Plus",
        adapter_factory=lambda cfg: QQBotPlusAdapter(cfg),
        check_fn=check_qqbot_plus_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["QQBOT_PLUS_APP_ID", "QQBOT_PLUS_CLIENT_SECRET"],
        env_enablement_fn=_env_enablement,
        allowed_users_env="QQ_ALLOWED_USERS",
        allow_all_env="QQ_ALLOW_ALL_USERS",
        cron_deliver_env_var="QQBOT_PLUS_HOME_CHANNEL",
        max_message_length=QQAdapter.MAX_MESSAGE_LENGTH,
        emoji="QQ",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via QQ Bot Plus. Native QQ streaming is supported "
            "for C2C and group chats using append-style message updates."
        ),
    )
