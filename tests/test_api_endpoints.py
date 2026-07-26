import unittest
from unittest.mock import AsyncMock

from pcrdb.api.client import PCRClientError, PCRProtocolError, PCRServerError
from pcrdb.api.endpoints import PCRApi


class ClanBattleEndpointTests(unittest.IsolatedAsyncioTestCase):
    def make_api(self, clan_id=54):
        api = PCRApi(123, "uid", "access-key")
        api.home = {"user_clan": {"clan_id": clan_id}}
        api.client.login = AsyncMock(return_value=({}, api.home))
        return api

    async def test_uses_the_logged_in_accounts_clan_id(self):
        api = self.make_api(54)
        api.client.call_api = AsyncMock(
            return_value={"period_ranking": [{"rank": 1}]}
        )

        result = await api.query_clan_battle_ranking(3)

        self.assertEqual(result, [{"rank": 1}])
        api.client.call_api.assert_awaited_once_with(
            "/clan_battle/period_ranking",
            {
                "clan_id": 54,
                "clan_battle_id": -1,
                "period": -1,
                "month": 0,
                "page": 3,
                "is_my_clan": 0,
                "is_first": 1,
            },
        )

    async def test_rejects_an_account_without_clan_membership(self):
        api = self.make_api(0)
        api.client.call_api = AsyncMock()

        with self.assertRaisesRegex(PCRClientError, "does not belong"):
            await api.query_clan_battle_ranking(0)

        api.client.call_api.assert_not_awaited()

    async def test_server_error_is_not_treated_as_an_empty_ranking(self):
        api = self.make_api()
        api.client.call_api = AsyncMock(
            return_value={
                "server_error": {
                    "status": 3,
                    "message": "account is not in the clan",
                }
            }
        )

        with self.assertRaises(PCRServerError):
            await api.query_clan_battle_ranking(0)

        self.assertEqual(api.client.call_api.await_count, 2)
        api.client.login.assert_awaited_once()

    async def test_missing_ranking_field_is_a_protocol_error(self):
        api = self.make_api()
        api.client.call_api = AsyncMock(return_value={"period": 1})

        with self.assertRaisesRegex(PCRProtocolError, "ranking list"):
            await api.query_clan_battle_ranking(0)


if __name__ == "__main__":
    unittest.main()
