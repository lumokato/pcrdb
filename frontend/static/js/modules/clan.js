/**
 * 公会查询模块
 */
import { LOCAL_API } from './utils.js';

/**
 * 创建公会查询模块
 */
export function useClan(authFetch, logout) {
    const { reactive } = Vue;

    const clan = reactive({
        mode: 'history',
        loading: false,
        searchName: '',
        searchId: '',
        result: null,
        error: ''
    });

    // 搜索公会历史
    const searchClanHistory = async () => {
        if (!clan.searchId && !clan.searchName) {
            clan.error = '请输入公会名或公会ID';
            return;
        }

        clan.loading = true;
        clan.error = '';
        clan.result = null;

        try {
            let url = `${LOCAL_API}/api/clan/history?`;
            if (clan.searchId) {
                url += `clan_id=${encodeURIComponent(clan.searchId)}`;
            } else {
                url += `clan_name=${encodeURIComponent(clan.searchName)}`;
            }

            const res = await authFetch(url);
            if (res.status === 401) {
                clan.error = '认证已过期，请重新登录';
                logout();
                return;
            }
            const data = await res.json();

            if (data.error) {
                clan.error = data.error;
            } else {
                clan.result = data;
            }
        } catch (e) {
            clan.error = '查询失败，请确保后端服务已启动';
        } finally {
            clan.loading = false;
        }
    };

    return {
        clan,
        searchClanHistory
    };
}
