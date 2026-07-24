from unittest import TestCase
from unittest.mock import patch

from pcrdb.account_pool import (
    ACCOUNT_LOCK_NAMESPACE,
    AccountLease,
    AccountPoolUnavailable,
    lease_account,
    lease_accounts,
)
from pcrdb.db.connection import Account


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
        self.connection.queries.append((normalized, params))
        if "pg_try_advisory_lock" in normalized:
            self.row = (self.connection.locked,)
        elif "pg_advisory_unlock" in normalized:
            self.row = (True,)

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, locked=True):
        self.locked = locked
        self.autocommit = False
        self.closed = False
        self.queries = []

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def candidate(account_id):
    return {
        "id": account_id,
        "uid": f"uid-{account_id}",
        "access_key": "key",
        "viewer_id": 1000 + account_id,
        "name": "farm",
        "arena_group": 1,
        "grand_arena_group": 2,
        "is_active": True,
        "note": None,
        "pool_enabled": True,
    }


class LeaseSelectionTests(TestCase):
    def test_skips_a_locked_account_and_leases_the_next_candidate(self):
        busy = FakeConnection(locked=False)
        available = FakeConnection(locked=True)
        with (
            patch(
                "pcrdb.account_pool._candidate_accounts",
                return_value=[candidate(1), candidate(2)],
            ),
            patch(
                "pcrdb.account_pool.create_connection",
                side_effect=[busy, available],
            ),
        ):
            leases = lease_accounts(1, "test")

        self.assertEqual([lease.account.id for lease in leases], [2])
        self.assertTrue(busy.closed)
        self.assertFalse(available.closed)
        leases[0].release(True)
        self.assertTrue(available.closed)
        self.assertTrue(
            any(
                "pg_advisory_unlock" in query
                and params == (ACCOUNT_LOCK_NAMESPACE, 2)
                for query, params in available.queries
            )
        )

    def test_raises_when_no_pool_account_is_available(self):
        with patch("pcrdb.account_pool.lease_accounts", return_value=[]):
            with self.assertRaises(AccountPoolUnavailable):
                lease_account("test")


class LeaseStateTests(TestCase):
    def test_failed_release_records_cooldown_and_error_type(self):
        connection = FakeConnection()
        lease = AccountLease(
            account=Account(id=7, uid="uid", access_key="key"),
            purpose="test",
            connection=connection,
        )

        with patch.dict(
            "os.environ",
            {"PCRDB_ACCOUNT_FAILURE_COOLDOWN_SECONDS": "120"},
            clear=False,
        ):
            lease.release(False, "LoginError")

        failure_updates = [
            params
            for query, params in connection.queries
            if "failure_count = failure_count + 1" in query
        ]
        self.assertEqual(failure_updates, [(120, "LoginError", 7)])
        self.assertTrue(connection.closed)

    def test_invalid_cooldown_configuration_still_releases_the_lock(self):
        connection = FakeConnection()
        lease = AccountLease(
            account=Account(id=8, uid="uid", access_key="key"),
            purpose="test",
            connection=connection,
        )

        with patch.dict(
            "os.environ",
            {"PCRDB_ACCOUNT_FAILURE_COOLDOWN_SECONDS": "invalid"},
            clear=False,
        ):
            lease.release(False, "LoginError")

        failure_updates = [
            params
            for query, params in connection.queries
            if "failure_count = failure_count + 1" in query
        ]
        self.assertEqual(failure_updates, [(300, "LoginError", 8)])
        self.assertTrue(connection.closed)
