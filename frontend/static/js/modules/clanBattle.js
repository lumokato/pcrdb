/**
 * 会战查询模块
 */
import { CLAN_BATTLE_API } from './utils.js';

/**
 * 创建会战查询模块
 */
export function useClanBattle() {
    const { reactive } = Vue;

    const clanBattle = reactive({
        mode: 'current',
        loading: false,
        timeData: {},
        historyData: [],
        selectedDate: '',
        selectedTime: '',
        selectedHistory: '',
        searchText: '',
        results: [],
        page: 0,
        maxPage: 0,
        limit: 10,
        errorOccured: false,
        errorMsg: ''
    });

    // 加载当期时间
    const loadClanBattleTime = async () => {
        try {
            const res = await fetch(`${CLAN_BATTLE_API}/current/getalltime/qd`);
            const data = await res.json();
            clanBattle.timeData = data.data?.['1'] || {};

            const dates = Object.keys(clanBattle.timeData);
            if (dates.length > 0) {
                clanBattle.selectedDate = dates[dates.length - 1];
                const times = clanBattle.timeData[clanBattle.selectedDate];
                clanBattle.selectedTime = times[times.length - 1];
            }
        } catch (e) {
            console.error('加载时间数据失败:', e);
        }
    };

    // 加载历史月份
    const loadClanBattleHistory = async () => {
        try {
            const res = await fetch(`${CLAN_BATTLE_API}/history/getalltime/qd`);
            const data = await res.json();
            clanBattle.historyData = data.data?.['1'] || [];
            if (clanBattle.historyData.length > 0) {
                clanBattle.selectedHistory = clanBattle.historyData[clanBattle.historyData.length - 1];
            }
        } catch (e) {
            console.error('加载历史数据失败:', e);
        }
    };

    // 更新时间选项
    const updateTimeOptions = () => {
        const times = clanBattle.timeData[clanBattle.selectedDate];
        if (times && times.length > 0) {
            clanBattle.selectedTime = times[times.length - 1];
        }
    };

    // 搜索会战
    const searchClanBattle = async (page = 0) => {
        clanBattle.loading = true;
        clanBattle.page = page;

        let filename = '';
        if (clanBattle.mode === 'current') {
            filename = `qd/1/${clanBattle.selectedDate}${clanBattle.selectedTime}`;
        } else {
            filename = `qd/history/1/${clanBattle.selectedHistory}`;
        }

        try {
            const res = await fetch(`${CLAN_BATTLE_API}/search`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    filename,
                    search: clanBattle.searchText,
                    page: page,
                    page_limit: clanBattle.limit
                })
            });
            const data = await res.json();

            if (data.state === 'success') {
                clanBattle.results = data.data;
                clanBattle.maxPage = Math.ceil(data.total / clanBattle.limit);
            } else {
                showError(data.error_message || '查询失败');
            }
        } catch (e) {
            console.error('查询失败:', e);
        } finally {
            clanBattle.loading = false;
        }
    };

    // 搜索档线
    const searchScoreLine = async () => {
        clanBattle.loading = true;

        let filename = '';
        if (clanBattle.mode === 'current') {
            filename = `qd/1/${clanBattle.selectedDate}${clanBattle.selectedTime}`;
        } else {
            filename = `qd/history/1/${clanBattle.selectedHistory}`;
        }

        try {
            const res = await fetch(`${CLAN_BATTLE_API}/search/scoreline`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    filename,
                    search: clanBattle.searchText
                })
            });
            const data = await res.json();

            if (data.state === 'success') {
                clanBattle.results = data.data;
                clanBattle.maxPage = 1;
                clanBattle.page = 0;
            } else {
                showError(data.error_message || '查询失败');
            }
        } catch (e) {
            console.error('查询失败:', e);
        } finally {
            clanBattle.loading = false;
        }
    };

    // 翻页
    const clanBattlePage = (delta) => {
        const newPage = clanBattle.page + delta;
        if (newPage >= 0 && newPage < clanBattle.maxPage) {
            searchClanBattle(newPage);
        }
    };

    // 显示错误
    const showError = (msg) => {
        clanBattle.errorOccured = true;
        clanBattle.errorMsg = msg;
        setTimeout(() => { clanBattle.errorOccured = false; }, 3000);
    };

    return {
        clanBattle,
        loadClanBattleTime,
        loadClanBattleHistory,
        updateTimeOptions,
        searchClanBattle,
        searchScoreLine,
        clanBattlePage
    };
}
