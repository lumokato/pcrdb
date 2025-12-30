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
        mode: 'history', // history, details, profiles
        loading: false,
        searchName: '',
        searchId: '',
        result: null,
        error: '',
        // Details mode state
        searchPeriod: '',
        periodOptions: [],
        detailsResult: null,
        // Profiles mode state
        profilesResult: null,
        sortColumn: 'total_power',
        sortAsc: false,  // 默认降序排列
        topClans: [],
        selectedClanId: '',
        searchDate: '',
        dateOptions: []
    });

    // 加载可用月份
    const loadPeriodOptions = async () => {
        try {
            const res = await authFetch(`${LOCAL_API}/api/player/periods`);
            if (res.status === 401) return;
            const data = await res.json();
            if (Array.isArray(data)) {
                clan.periodOptions = data;
                if (data.length > 0 && !clan.searchPeriod) {
                    clan.searchPeriod = data[0];
                }
            }
        } catch (e) {
            console.error('加载月份失败', e);
        }
    };

    // 加载可用日期 (YYYY-MM-DD)
    const loadDateOptions = async () => {
        try {
            const res = await authFetch(`${LOCAL_API}/api/clan/profile_dates`);
            if (res.status === 401) return;
            const data = await res.json();
            if (Array.isArray(data)) {
                clan.dateOptions = data;
                if (data.length > 0 && !clan.searchDate) {
                    clan.searchDate = data[0];
                    loadTopClans();
                }
            }
        } catch (e) {
            console.error('加载日期失败', e);
        }
    };

    // 加载前30公会列表
    const loadTopClans = async () => {
        if (!clan.searchDate) return;
        // Extract month from date for top clans API
        const month = clan.searchDate.substring(0, 7);
        try {
            const res = await authFetch(`${LOCAL_API}/api/clan/top_clans?period=${month}`);
            if (res.status === 401) return;
            const data = await res.json();
            if (data.clans) {
                clan.topClans = data.clans;
                clan.selectedClanId = '';
            }
        } catch (e) {
            console.error('加载公会列表失败', e);
        }
    };

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

    // 搜索公会成员详情
    const searchClanDetails = async () => {
        if (!clan.searchId && !clan.searchName) {
            clan.error = '请输入公会名或公会ID';
            return;
        }

        clan.loading = true;
        clan.error = '';
        clan.detailsResult = null;

        try {
            let url = `${LOCAL_API}/api/clan/members?`;
            if (clan.searchId) {
                url += `clan_id=${encodeURIComponent(clan.searchId)}`;
            } else {
                url += `clan_name=${encodeURIComponent(clan.searchName)}`;
            }
            if (clan.searchPeriod) {
                url += `&period=${clan.searchPeriod}`;
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
                clan.detailsResult = data;
            }
        } catch (e) {
            clan.error = '查询失败，请确保后端服务已启动';
        } finally {
            clan.loading = false;
        }
    };

    // 搜索前排公会成员资料
    const searchClanProfiles = async () => {
        clan.loading = true;
        clan.error = '';
        clan.profilesResult = null;

        try {
            let url = `${LOCAL_API}/api/clan/profiles?`;
            const params = [];
            if (clan.searchDate) {
                params.push(`date=${clan.searchDate}`);
            }
            if (clan.selectedClanId) {
                params.push(`clan_id=${clan.selectedClanId}`);
            }
            url += params.join('&');

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
                clan.profilesResult = data;
                // Apply default sort (force descending, don't toggle)
                sortProfiles('total_power', true);
            }
        } catch (e) {
            clan.error = '查询失败，请确保后端服务已启动';
        } finally {
            clan.loading = false;
        }
    };

    // 排序前排成员表格
    // forceDesc: 如果为 true，强制降序排列，不切换方向
    const sortProfiles = (column, forceDesc = false) => {
        if (!clan.profilesResult || !clan.profilesResult.players) return;

        if (forceDesc) {
            // 强制降序，不切换
            clan.sortColumn = column;
            clan.sortAsc = false;
        } else if (clan.sortColumn === column) {
            // Toggle direction if same column
            clan.sortAsc = !clan.sortAsc;
        } else {
            clan.sortColumn = column;
            // Default descending for numeric columns
            clan.sortAsc = false;
        }

        // Create a shallow copy to sort (avoids in-place mutation issues and forces update)
        const players = [...clan.profilesResult.players];

        // Debug Information
        const count = players.length;
        console.log(`[Debug] Sorting ${count} items by ${column}`);

        // Check for duplicates
        const viewerIds = new Set();
        let dupCount = 0;
        players.forEach(p => {
            if (viewerIds.has(p.viewer_id)) dupCount++;
            viewerIds.add(p.viewer_id);
        });
        if (dupCount > 0) {
            console.error(`[Debug] Found ${dupCount} duplicate viewer_ids! This causes Vue rendering issues.`);
        }

        players.sort((a, b) => {
            let valA = a[column];
            let valB = b[column];

            // String columns sorting (User Name, Clan Name)
            if (column === 'join_clan_name' || column === 'user_name') {
                if (valA === valB) return 0;
                // Handle null/undefined
                if (!valA) return 1;
                if (!valB) return -1;

                const cmp = String(valA).localeCompare(String(valB), 'zh-CN');
                return clan.sortAsc ? cmp : -cmp;
            }

            // Numeric sorting (with special handling for strings like "100+")
            if (typeof valA === 'string') {
                valA = valA.replace('+', '999');
                valA = parseInt(valA) || 0;
            }
            if (typeof valB === 'string') {
                valB = valB.replace('+', '999');
                valB = parseInt(valB) || 0;
            }

            if (clan.sortAsc) {
                return valA - valB;
            } else {
                return valB - valA;
            }
        });

        // Reassign the sorted array to trigger reactivity update
        clan.profilesResult.players = players;
        console.log(`[Debug] Loop sort complete. New first item: ${players[0].user_name}`);
    };

    return {
        clan,
        searchClanHistory,
        searchClanDetails,
        searchClanProfiles,
        sortProfiles,
        loadPeriodOptions,
        loadTopClans,
        loadDateOptions
    };
}
