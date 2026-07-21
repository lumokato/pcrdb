import unittest
from unittest.mock import AsyncMock

from pcrdb.api.client import PCRClient, PCRProtocolError


class LoginBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_refreshes_app_version_after_bootstrap_rejection(self):
        client = PCRClient(123)
        client.call_api = AsyncMock(
            side_effect=[
                {"server_error": {"status": 3}},
                {"required_manifest_ver": "202607141823"},
                {},
                {},
                {"load": True},
                {"home": True},
            ]
        )
        client._refresh_app_version = AsyncMock(return_value="11.4.0")

        load, home = await client.login("uid", "access-key")

        client._refresh_app_version.assert_awaited_once()
        self.assertEqual(load, {"load": True})
        self.assertEqual(home, {"home": True})

    async def test_rejects_unrecognized_bootstrap_response(self):
        client = PCRClient(123)
        client.call_api = AsyncMock(return_value={"unexpected": True})

        with self.assertRaisesRegex(PCRProtocolError, "required_manifest_ver"):
            await client.login("uid", "access-key")


if __name__ == "__main__":
    unittest.main()
