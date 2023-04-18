from pcrclient import PCRClient
import json

with open('json/account-farm.json') as f:
    account_data = json.load(f)


async def create_client(index):
    qd_client = PCRApi(account_data['accounts'][index]['vid'], account_data['accounts'][index]['uid'], account_data["access_key"])
    await qd_client.login()
    return qd_client


class PCRApi:
    def __init__(self, vid, uid, ac_key):
        self.home = None
        self.load = None
        self.uid = uid
        self.access_key = ac_key
        self.viewer_id = vid
        self.client = PCRClient(self.viewer_id)

    async def login(self):
        print('login'+str(self.viewer_id))
        self.load, self.home = await self.client.login(self.uid, self.access_key)

    async def query_id(self, viewer_id: int):
        try:
            res = await self.client.callapi('/profile/get_profile', {'target_viewer_id': viewer_id})
        except Exception:
            await self.client.login(self.uid, self.access_key)
            res = await self.client.callapi('/profile/get_profile', {'target_viewer_id': viewer_id})
        return res

    async def query_clan(self, clan_id: int):
        try:
            res = await self.client.callapi('/clan/others_info', {'clan_id': clan_id})
        except Exception:
            await self.client.login(self.uid, self.access_key)
            res = await self.client.callapi('/clan/others_info', {'clan_id': clan_id})
        return res

    async def query_grand(self, page:int):
        try:
            res = await self.client.callapi('/grand_arena/ranking', {'limit': 20, 'page': page})
        except Exception:
            await self.client.login(self.uid, self.access_key)
            res = await self.client.callapi('/grand_arena/ranking', {'limit': 20, 'page': page})
        return res

    async def query_arena(self, page:int):
        try:
            res = await self.client.callapi('/arena/ranking', {'limit': 20, 'page': page})
            res['ranking']
        except Exception:
            await self.client.login(self.uid, self.access_key)
            res = await self.client.callapi('/arena/ranking', {'limit': 20, 'page': page})
        return res

    # 查询会战排名页数
    async def get_page_status(self, page: int):
        res = await self.client.callapi('clan_battle/period_ranking', {
                                   'clan_id': self.clan_id, 'clan_battle_id': -1, 'period': -1, 'month': 0, 'page': page, 'is_my_clan': 0, 'is_first': 1})
        if 'period_ranking' not in res:
            await self.client.login(self.uid, self.access_key)
            res = await self.client.callapi('clan_battle/period_ranking', {
                                       'clan_id': self.clan_id, 'clan_battle_id': -1, 'period': -1, 'month': 0, 'page': page, 'is_my_clan': 0, 'is_first': 1})
        return res['period_ranking']
