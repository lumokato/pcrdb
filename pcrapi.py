from pcrclient import PCRClient
import time


class PCRApi:
    def __init__(self, viewer_id, uid, access_key):
        self.uid = uid
        self.access_key = access_key
        self.client = PCRClient(viewer_id)
        self.load, self.home = self.client.login(uid, access_key)
        self.clan_id = self.home['user_clan']['clan_id']

    def query_id(self, viewer_id: int):
        try:
            res = self.client.callapi('/profile/get_profile', {'target_viewer_id': viewer_id})
            res['user_info']
        except Exception:
            time.sleep(5)
            self.client.login(self.uid, self.access_key)
            res = self.client.callapi('/profile/get_profile', {'target_viewer_id': viewer_id})            
        return res

    def query_clan(self, clan_id: int):
        try:
            res = self.client.callapi('/clan/others_info', {'clan_id': clan_id})
            res['clan']
        except Exception:
            time.sleep(5)
            self.client.login(self.uid, self.access_key)
            res = self.client.callapi('/clan/others_info', {'clan_id': clan_id})
        return res

    # 查询会战排名页数
    def get_page_status(self, page: int):
        temp = self.client.callapi('clan_battle/period_ranking', {
                                   'clan_id': self.clan_id, 'clan_battle_id': -1, 'period': -1, 'month': 0, 'page': page, 'is_my_clan': 0, 'is_first': 1})
        if 'period_ranking' not in temp:
            self.client.login(self.uid, self.access_key)
            temp = self.client.callapi('clan_battle/period_ranking', {
                                       'clan_id': self.clan_id, 'clan_battle_id': -1, 'period': -1, 'month': 0, 'page': page, 'is_my_clan': 0, 'is_first': 1})
        return temp['period_ranking']
