from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import logging
import os
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import aiohttp


logger = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")
LOCK_NAME = "pcrdb:arena-alert"


class ArenaAlertError(RuntimeError):
    """Base error for arena alert checks."""


class DingTalkWebhookError(ArenaAlertError):
    """The DingTalk webhook rejected or failed to receive a notification."""


@dataclass(frozen=True)
class ArenaAlertConfig:
    target_viewer_id: int
    webhook_url: str
    webhook_secret: str = ""
    account_uid: str = ""
    poll_seconds: int = 30

    @classmethod
    def from_env(cls) -> "ArenaAlertConfig":
        target = os.getenv("ARENA_ALERT_TARGET_VIEWER_ID", "").strip()
        if not target.isdigit() or int(target) <= 0:
            raise ValueError("ARENA_ALERT_TARGET_VIEWER_ID must be a positive integer")

        webhook_url = os.getenv("ARENA_ALERT_DINGTALK_WEBHOOK", "").strip()
        parsed_url = urlsplit(webhook_url)
        if parsed_url.scheme != "https" or not parsed_url.netloc:
            raise ValueError("ARENA_ALERT_DINGTALK_WEBHOOK must be a valid HTTPS URL")

        poll_value = os.getenv("ARENA_ALERT_POLL_SECONDS", "30").strip()
        try:
            poll_seconds = int(poll_value)
        except ValueError as exc:
            raise ValueError("ARENA_ALERT_POLL_SECONDS must be an integer") from exc
        if not 10 <= poll_seconds <= 3600:
            raise ValueError("ARENA_ALERT_POLL_SECONDS must be between 10 and 3600")

        return cls(
            target_viewer_id=int(target),
            webhook_url=webhook_url,
            webhook_secret=os.getenv("ARENA_ALERT_DINGTALK_SECRET", "").strip(),
            account_uid=os.getenv("ARENA_ALERT_ACCOUNT_UID", "").strip(),
            poll_seconds=poll_seconds,
        )


@dataclass(frozen=True)
class ArenaRanks:
    viewer_id: int
    user_name: str
    arena_rank: int
    grand_arena_rank: int


@dataclass(frozen=True)
class RankDrop:
    label: str
    previous: int
    current: int

    @property
    def amount(self) -> int:
        return self.current - self.previous


def parse_profile(profile: dict[str, Any], target_viewer_id: int) -> ArenaRanks:
    user = profile.get("user_info")
    if not isinstance(user, dict):
        raise ArenaAlertError("profile response does not contain user_info")

    viewer_id = user.get("viewer_id")
    if viewer_id != target_viewer_id:
        raise ArenaAlertError("profile response viewer_id does not match the configured target")

    arena_rank = user.get("arena_rank")
    grand_arena_rank = user.get("grand_arena_rank")
    for field_name, value in (
        ("arena_rank", arena_rank),
        ("grand_arena_rank", grand_arena_rank),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ArenaAlertError(f"profile response has an invalid {field_name}")

    user_name = user.get("user_name")
    return ArenaRanks(
        viewer_id=viewer_id,
        user_name=user_name if isinstance(user_name, str) else "",
        arena_rank=arena_rank,
        grand_arena_rank=grand_arena_rank,
    )


def detect_rank_drops(previous: ArenaRanks, current: ArenaRanks) -> tuple[RankDrop, ...]:
    drops = []
    for label, old_rank, new_rank in (
        ("竞技场", previous.arena_rank, current.arena_rank),
        ("公主竞技场", previous.grand_arena_rank, current.grand_arena_rank),
    ):
        if old_rank > 0 and new_rank > old_rank:
            drops.append(RankDrop(label=label, previous=old_rank, current=new_rank))
    return tuple(drops)


def format_notification(
    current: ArenaRanks,
    drops: tuple[RankDrop, ...],
    observed_at: datetime,
) -> str:
    display_name = current.user_name or "玩家"
    lines = [
        "【PCR竞技场提醒】",
        f"{display_name}（{current.viewer_id}）",
    ]
    lines.extend(
        f"{drop.label}：{drop.previous} -> {drop.current}（下降 {drop.amount} 名）"
        for drop in drops
    )
    lines.append(f"检查时间：{observed_at.astimezone(BEIJING):%Y-%m-%d %H:%M:%S}")
    return "\n".join(lines)


def build_signed_webhook_url(
    webhook_url: str,
    secret: str,
    timestamp_ms: int | None = None,
) -> str:
    if not secret:
        return webhook_url

    timestamp_ms = timestamp_ms or int(datetime.now().timestamp() * 1000)
    string_to_sign = f"{timestamp_ms}\n{secret}".encode()
    signature = base64.b64encode(
        hmac.new(secret.encode(), string_to_sign, hashlib.sha256).digest()
    ).decode()

    parsed = urlsplit(webhook_url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key not in {"timestamp", "sign"}
    ]
    query.extend((("timestamp", str(timestamp_ms)), ("sign", signature)))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )


class DingTalkNotifier:
    def __init__(self, webhook_url: str, secret: str = "", timeout_seconds: float = 10):
        self.webhook_url = webhook_url
        self.secret = secret
        self.timeout_seconds = timeout_seconds

    async def send_text(self, content: str) -> None:
        url = build_signed_webhook_url(self.webhook_url, self.secret)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    url,
                    json={"msgtype": "text", "text": {"content": content}},
                ) as response:
                    status = response.status
                    try:
                        result = await response.json(content_type=None)
                    except (ValueError, aiohttp.ContentTypeError):
                        result = None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise DingTalkWebhookError(
                f"DingTalk webhook request failed ({type(exc).__name__})"
            ) from None

        if not 200 <= status < 300:
            raise DingTalkWebhookError(f"DingTalk webhook returned HTTP {status}")
        if not isinstance(result, dict) or result.get("errcode") != 0:
            error_code = result.get("errcode") if isinstance(result, dict) else "invalid_response"
            raise DingTalkWebhookError(f"DingTalk rejected the message ({error_code})")


def _default_connection_factory(**kwargs):
    from pcrdb.db.connection import create_connection

    return create_connection(**kwargs)


async def _default_client_factory(account: dict[str, Any]):
    from pcrdb.api.endpoints import create_client

    return await create_client(account)


class ArenaAlertMonitor:
    def __init__(
        self,
        config: ArenaAlertConfig,
        notifier: DingTalkNotifier | None = None,
        connection_factory: Callable[..., Any] = _default_connection_factory,
        client_factory: Callable[[dict[str, Any]], Awaitable[Any]] = _default_client_factory,
    ):
        self.config = config
        self.notifier = notifier or DingTalkNotifier(
            config.webhook_url,
            config.webhook_secret,
        )
        self.connection_factory = connection_factory
        self.client_factory = client_factory
        self._client = None

    def run(self) -> str:
        return asyncio.run(self.run_once())

    async def run_once(self) -> str:
        connection = self.connection_factory(application_name="pcrdb-arena-alert")
        connection.autocommit = True
        locked = False
        try:
            locked = self._try_lock(connection)
            if not locked:
                logger.info("Arena alert check skipped because another worker holds the lock")
                return "locked"

            previous = self._load_state(connection)
            current = await self._query_current_ranks(connection)
            observed_at = datetime.now(BEIJING)

            if previous is None:
                self._save_state(connection, current, observed_at, notified=False)
                logger.info(
                    "Arena alert baseline initialized for viewer_id=%s",
                    self.config.target_viewer_id,
                )
                return "initialized"

            drops = detect_rank_drops(previous, current)
            if drops:
                await self.notifier.send_text(
                    format_notification(current, drops, observed_at)
                )

            self._save_state(connection, current, observed_at, notified=bool(drops))
            if drops:
                logger.info(
                    "Arena alert sent for viewer_id=%s changes=%s",
                    self.config.target_viewer_id,
                    len(drops),
                )
                return "notified"
            return "unchanged"
        finally:
            if locked:
                try:
                    self._unlock(connection)
                except Exception:
                    logger.warning("Failed to release the arena alert database lock")
            connection.close()

    def _try_lock(self, connection) -> bool:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (LOCK_NAME,))
            return bool(cursor.fetchone()[0])

    def _unlock(self, connection) -> None:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (LOCK_NAME,))

    def _load_state(self, connection) -> ArenaRanks | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT viewer_id, user_name, arena_rank, grand_arena_rank
                FROM arena_alert_state
                WHERE viewer_id = %s
                """,
                (self.config.target_viewer_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return ArenaRanks(
            viewer_id=row[0],
            user_name=row[1] or "",
            arena_rank=row[2],
            grand_arena_rank=row[3],
        )

    def _save_state(
        self,
        connection,
        current: ArenaRanks,
        observed_at: datetime,
        notified: bool,
    ) -> None:
        notified_at = observed_at if notified else None
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO arena_alert_state (
                    viewer_id,
                    user_name,
                    arena_rank,
                    grand_arena_rank,
                    last_checked_at,
                    last_notified_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (viewer_id) DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    arena_rank = EXCLUDED.arena_rank,
                    grand_arena_rank = EXCLUDED.grand_arena_rank,
                    last_checked_at = EXCLUDED.last_checked_at,
                    last_notified_at = COALESCE(
                        EXCLUDED.last_notified_at,
                        arena_alert_state.last_notified_at
                    ),
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    current.viewer_id,
                    current.user_name,
                    current.arena_rank,
                    current.grand_arena_rank,
                    observed_at,
                    notified_at,
                    observed_at,
                ),
            )

    async def _query_current_ranks(self, connection) -> ArenaRanks:
        if self._client is None:
            account = self._select_account(connection)
            self._client = await self.client_factory(account)

        try:
            profile = await self._client.query_profile(self.config.target_viewer_id)
        except Exception:
            self._client = None
            raise
        return parse_profile(profile, self.config.target_viewer_id)

    def _select_account(self, connection) -> dict[str, Any]:
        query = """
            SELECT viewer_id, uid, access_key
            FROM accounts
            WHERE is_active = TRUE
        """
        params: tuple[Any, ...] = ()
        if self.config.account_uid:
            query += " AND uid = %s"
            params = (self.config.account_uid,)
        query += " ORDER BY id LIMIT 1"

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        if row is None:
            if self.config.account_uid:
                raise ArenaAlertError("configured arena alert account is not active")
            raise ArenaAlertError("no active PCRDB account is available for arena alerts")
        return {
            "vid": row[0] or 0,
            "uid": str(row[1]),
            "access_key": row[2],
        }
