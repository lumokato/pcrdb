import base64
from datetime import datetime
import hashlib
import hmac
import os
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from pcrdb.tasks.arena_alert import (
    ArenaAlertConfig,
    ArenaAlertError,
    ArenaAlertMonitor,
    ArenaRanks,
    DingTalkWebhookError,
    build_signed_webhook_url,
    detect_rank_drops,
    format_notification,
    parse_profile,
)


TARGET_VIEWER_ID = 1234567890123


class ProfileTests(TestCase):
    def test_parses_target_profile(self):
        result = parse_profile(
            {
                "user_info": {
                    "viewer_id": TARGET_VIEWER_ID,
                    "user_name": "Kanon",
                    "arena_rank": 12,
                    "grand_arena_rank": 34,
                }
            },
            TARGET_VIEWER_ID,
        )

        self.assertEqual(
            result,
            ArenaRanks(TARGET_VIEWER_ID, "Kanon", 12, 34),
        )

    def test_rejects_a_different_viewer(self):
        with self.assertRaisesRegex(ArenaAlertError, "does not match"):
            parse_profile(
                {
                    "user_info": {
                        "viewer_id": TARGET_VIEWER_ID + 1,
                        "arena_rank": 12,
                        "grand_arena_rank": 34,
                    }
                },
                TARGET_VIEWER_ID,
            )


class RankDecisionTests(TestCase):
    def test_only_reports_worse_ranks(self):
        previous = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 10, 50)
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 17, 40)

        drops = detect_rank_drops(previous, current)

        self.assertEqual(len(drops), 1)
        self.assertEqual(drops[0].label, "竞技场")
        self.assertEqual(drops[0].amount, 7)

    def test_reports_both_arena_types_in_one_message(self):
        previous = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 10, 20)
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 14, 29)
        observed_at = datetime(2026, 7, 24, 15, 1, 2, tzinfo=ZoneInfo("Asia/Shanghai"))

        message = format_notification(
            current,
            detect_rank_drops(previous, current),
            observed_at,
        )

        self.assertIn("pcrjjc", message)
        self.assertIn("竞技场：10 -> 14（下降 4 名）", message)
        self.assertIn("公主竞技场：20 -> 29（下降 9 名）", message)
        self.assertIn(str(TARGET_VIEWER_ID), message)
        self.assertIn("2026-07-24 15:01:02", message)

    def test_unranked_to_ranked_does_not_create_a_false_drop(self):
        previous = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 0, 0)
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 100, 200)

        self.assertEqual(detect_rank_drops(previous, current), ())


class DingTalkSigningTests(TestCase):
    def test_adds_dingtalk_signature_without_losing_access_token(self):
        secret = "SEC-test"
        timestamp = 1721800000123
        url = build_signed_webhook_url(
            "https://oapi.dingtalk.com/robot/send?access_token=token-value",
            secret,
            timestamp,
        )
        query = parse_qs(urlsplit(url).query)
        expected = base64.b64encode(
            hmac.new(
                secret.encode(),
                f"{timestamp}\n{secret}".encode(),
                hashlib.sha256,
            ).digest()
        ).decode()

        self.assertEqual(query["access_token"], ["token-value"])
        self.assertEqual(query["timestamp"], [str(timestamp)])
        self.assertEqual(query["sign"], [expected])


class ConfigTests(TestCase):
    def test_reads_single_target_configuration(self):
        env = {
            "ARENA_ALERT_TARGET_VIEWER_ID": str(TARGET_VIEWER_ID),
            "ARENA_ALERT_DINGTALK_WEBHOOK": (
                "https://oapi.dingtalk.com/robot/send?access_token=test"
            ),
            "ARENA_ALERT_POLL_SECONDS": "30",
        }
        with patch.dict(os.environ, env, clear=True):
            config = ArenaAlertConfig.from_env()

        self.assertEqual(config.target_viewer_id, TARGET_VIEWER_ID)
        self.assertEqual(config.poll_seconds, 30)

    def test_rejects_an_insecure_webhook(self):
        env = {
            "ARENA_ALERT_TARGET_VIEWER_ID": str(TARGET_VIEWER_ID),
            "ARENA_ALERT_DINGTALK_WEBHOOK": "http://example.com/webhook",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, "HTTPS"):
                ArenaAlertConfig.from_env()


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params=()):
        normalized = " ".join(query.split())
        if "pg_try_advisory_lock" in normalized:
            self.row = (True,)
        elif "pg_advisory_unlock" in normalized:
            self.row = (True,)
        elif "FROM arena_alert_state" in normalized:
            state = self.connection.state
            self.row = None if state is None else (
                state.viewer_id,
                state.user_name,
                state.arena_rank,
                state.grand_arena_rank,
            )
        elif "FROM accounts" in normalized:
            self.row = (1111111111111, "login-uid", "access-key")
        elif normalized.startswith("INSERT INTO arena_alert_state"):
            self.connection.state = ArenaRanks(
                viewer_id=params[0],
                user_name=params[1],
                arena_rank=params[2],
                grand_arena_rank=params[3],
            )
        else:
            raise AssertionError(f"Unexpected query: {normalized}")

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, state=None):
        self.state = state
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class MonitorTests(IsolatedAsyncioTestCase):
    def make_monitor(self, connection, current, notifier):
        client = AsyncMock()
        client.query_profile.return_value = {
            "user_info": {
                "viewer_id": current.viewer_id,
                "user_name": current.user_name,
                "arena_rank": current.arena_rank,
                "grand_arena_rank": current.grand_arena_rank,
            }
        }
        client_factory = AsyncMock(return_value=client)
        config = ArenaAlertConfig(
            target_viewer_id=TARGET_VIEWER_ID,
            webhook_url="https://oapi.dingtalk.com/robot/send?access_token=test",
        )
        monitor = ArenaAlertMonitor(
            config,
            notifier=notifier,
            connection_factory=lambda **kwargs: connection,
            client_factory=client_factory,
        )
        return monitor

    async def test_first_check_only_initializes_the_baseline(self):
        connection = FakeConnection()
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 10, 20)
        notifier = AsyncMock()
        monitor = self.make_monitor(connection, current, notifier)

        result = await monitor.run_once()

        self.assertEqual(result, "initialized")
        self.assertEqual(connection.state, current)
        notifier.send_text.assert_not_awaited()
        self.assertTrue(connection.closed)

    async def test_successful_notification_advances_the_baseline(self):
        previous = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 10, 20)
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 15, 28)
        connection = FakeConnection(previous)
        notifier = AsyncMock()
        monitor = self.make_monitor(connection, current, notifier)

        result = await monitor.run_once()

        self.assertEqual(result, "notified")
        self.assertEqual(connection.state, current)
        notifier.send_text.assert_awaited_once()

    async def test_failed_notification_keeps_the_previous_baseline(self):
        previous = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 10, 20)
        current = ArenaRanks(TARGET_VIEWER_ID, "Kanon", 15, 28)
        connection = FakeConnection(previous)
        notifier = AsyncMock()
        notifier.send_text.side_effect = DingTalkWebhookError("delivery failed")
        monitor = self.make_monitor(connection, current, notifier)

        with self.assertRaises(DingTalkWebhookError):
            await monitor.run_once()

        self.assertEqual(connection.state, previous)
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    import unittest

    unittest.main()
