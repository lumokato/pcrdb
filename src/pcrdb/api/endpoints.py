"""
游戏 API 端点封装
提供高层次的游戏数据查询接口
"""
from typing import Optional, Dict, Any
from .client import PCRClient


class PCRApi:
    """公主连结游戏 API 封装"""
    
    def __init__(self, viewer_id: int, uid: str, access_key: str):
        """
        初始化 API 客户端
        
        Args:
            viewer_id: 玩家 viewer_id
            uid: 账号 UID
            access_key: 访问密钥
        """
        self.viewer_id = viewer_id
        self.uid = uid
        self.access_key = access_key
        self.client = PCRClient(viewer_id)
        self.load = None
        self.home = None
    
    async def login(self):
        """登录游戏"""
        # print(f'登录账号 {self.viewer_id}')
        self.load, self.home = await self.client.login(self.uid, self.access_key)
    
    async def _safe_call(self, endpoint: str, request: dict) -> dict:
        """安全调用 API，失败时自动重试"""
        try:
            return await self.client.call_api(endpoint, request)
        except Exception:
            await self.client.login(self.uid, self.access_key)
            return await self.client.call_api(endpoint, request)
    
    async def query_profile(self, target_viewer_id: int) -> dict:
        """
        查询玩家档案
        
        Args:
            target_viewer_id: 目标玩家的 viewer_id
            
        Returns:
            玩家档案信息
        """
        return await self._safe_call('/profile/get_profile', {
            'target_viewer_id': target_viewer_id
        })
    
    async def query_clan(self, clan_id: int) -> dict:
        """
        查询公会信息
        
        Args:
            clan_id: 公会 ID
            
        Returns:
            公会详细信息，包含成员列表
        """
        return await self._safe_call('/clan/others_info', {
            'clan_id': clan_id
        })
    
    async def query_arena_ranking(self, page: int) -> dict:
        """
        查询 JJC 排名
        
        Args:
            page: 页码（每页 20 人）
            
        Returns:
            排名列表
        """
        return await self._safe_call('/arena/ranking', {
            'limit': 20, 
            'page': page
        })
    
    async def query_grand_arena_ranking(self, page: int) -> dict:
        """
        查询 PJJC 排名
        
        Args:
            page: 页码（每页 20 人）
            
        Returns:
            排名列表
        """
        return await self._safe_call('/grand_arena/ranking', {
            'limit': 20, 
            'page': page
        })

    async def query_arena_info(self) -> dict:
        """
        查询 JJC 信息 (用于激活/刷新)
        """
        return await self._safe_call('/arena/info', {})

    async def query_grand_arena_info(self) -> dict:
        """
        查询 PJJC 信息 (用于激活/刷新)
        """
        return await self._safe_call('/grand_arena/info', {})
    
    async def query_clan_battle_ranking(self, page: int, clan_id: int = 0) -> dict:
        """
        查询会战排名
        
        Args:
            page: 页码
            clan_id: 公会 ID（可选）
            
        Returns:
            会战排名列表
        """
        result = await self._safe_call('clan_battle/period_ranking', {
            'clan_id': clan_id, 
            'clan_battle_id': -1, 
            'period': -1, 
            'month': 0, 
            'page': page, 
            'is_my_clan': 0, 
            'is_first': 1
        })
        return result.get('period_ranking', [])


async def create_client(account: dict) -> PCRApi:
    """
    创建并登录客户端
    
    Args:
        account: 账号信息字典，包含 vid, uid, access_key
        
    Returns:
        已登录的 PCRApi 实例
    """
    client = PCRApi(
        account['vid'], 
        account['uid'], 
        account['access_key']
    )
    await client.login()
    return client
