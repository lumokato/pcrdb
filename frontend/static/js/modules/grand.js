/**
 * 双场查询模块
 */
import { LOCAL_API } from './utils.js';

/**
 * 创建双场查询模块
 */
export function useGrand(authFetch, logout) {
    const { reactive } = Vue;

    const grand = reactive({
        mode: 'param_ranking',
        loading: false,
        group: 0,
        limit: 100,
        results: []
    });

    // 搜索胜场排名
    const searchGrandWinning = async () => {
        grand.loading = true;

        try {
            const res = await authFetch(`${LOCAL_API}/api/grand/winning?group=${grand.group}&limit=${grand.limit}`);
            if (res.status === 401) {
                logout();
                return;
            }
            grand.results = await res.json();
        } catch (e) {
            console.error('双场查询失败:', e);
            grand.results = [];
        } finally {
            grand.loading = false;
        }
    };

    return {
        grand,
        searchGrandWinning
    };
}
