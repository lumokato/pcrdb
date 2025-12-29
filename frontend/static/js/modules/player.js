/**
 * 玩家查询模块
 */
import { LOCAL_API } from './utils.js';

/**
 * 创建玩家查询模块
 */
export function usePlayer(authFetch, logout) {
    const { reactive } = Vue;

    const player = reactive({
        mode: 'clan_history',
        loading: false,
        // 公会历史查询
        viewerId: '',
        result: null,
        error: '',
        // 玩家搜索
        searchName: '',
        searchPeriod: '',
        searchResults: [],
        periodOptions: []
    });

    // 加载可用月份
    const loadPeriodOptions = async () => {
        try {
            // 使用新的接口获取有玩家数据的月份
            const res = await authFetch(`${LOCAL_API}/api/player/periods`);
            if (res.status === 401) {
                logout();
                return;
            }
            const data = await res.json();
            if (Array.isArray(data)) {
                player.periodOptions = data;
                if (data.length > 0 && !player.searchPeriod) {
                    player.searchPeriod = data[0];
                }
            }
        } catch (e) {
            console.error('加载月份失败', e);
        }
    };

    // 搜索玩家公会历史
    const searchPlayerHistory = async () => {
        if (!player.viewerId) return;

        player.loading = true;
        player.error = '';
        player.result = null;

        try {
            const res = await authFetch(`${LOCAL_API}/api/player/history?viewer_id=${player.viewerId}`);
            if (res.status === 401) {
                player.error = '认证已过期，请重新登录';
                logout();
                return;
            }
            const data = await res.json();

            if (data.error) {
                player.error = data.error;
            } else {
                player.result = data;
            }
        } catch (e) {
            player.error = '查询失败，请确保后端服务已启动';
        } finally {
            player.loading = false;
        }
    };

    // 搜索玩家
    const searchPlayers = async () => {
        if (!player.searchName) {
            player.error = '请输入玩家名';
            return;
        }

        player.loading = true;
        player.error = '';
        player.searchResults = [];

        try {
            let url = `${LOCAL_API}/api/player/search?name=${encodeURIComponent(player.searchName)}&limit=100`;
            if (player.searchPeriod) {
                url += `&period=${player.searchPeriod}`;
            }
            const res = await authFetch(url);
            if (res.status === 401) {
                player.error = '认证已过期，请重新登录';
                logout();
                return;
            }
            const data = await res.json();

            if (data.error) {
                player.error = data.error;
            } else if (Array.isArray(data)) {
                player.searchResults = data;
            }
        } catch (e) {
            player.error = '查询失败，请确保后端服务已启动';
        } finally {
            player.loading = false;
        }
    };

    return {
        player,
        loadPeriodOptions,
        searchPlayerHistory,
        searchPlayers
    };
}
