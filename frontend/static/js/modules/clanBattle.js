/**
 * 会战查询模块
 */
import { CLAN_BATTLE_API } from './utils.js';


const compactDateTime = (value) => {
    const date = new Date(value);
    const parts = Object.fromEntries(
        new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Shanghai',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            hourCycle: 'h23'
        }).formatToParts(date).map(part => [part.type, part.value])
    );
    return {
        date: `${parts.year}${parts.month}${parts.day}`,
        time: `${parts.hour}${parts.minute}`
    };
};


export function useClanBattle() {
    const { reactive } = Vue;

    const clanBattle = reactive({
        mode: 'current',
        loading: false,
        timeData: {},
        snapshotMap: {},
        historyData: [],
        currentPeriod: '',
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

    const getJson = async (url) => {
        const response = await fetch(url);
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || '请求失败');
        }
        return data;
    };

    const loadHistoryPeriods = async () => {
        const data = await getJson(`${CLAN_BATTLE_API}/periods?final_only=true`);
        clanBattle.historyData = data.items.map(item => item.period.slice(0, 7));
        if (clanBattle.historyData.length > 0) {
            if (!clanBattle.historyData.includes(clanBattle.selectedHistory)) {
                clanBattle.selectedHistory = clanBattle.historyData[0];
            }
        } else {
            clanBattle.selectedHistory = '';
        }
    };

    const loadCurrentPeriod = async () => {
        const data = await getJson(`${CLAN_BATTLE_API}/periods?limit=1`);
        clanBattle.currentPeriod = data.items.length > 0
            ? data.items[0].period.slice(0, 7)
            : '';
    };

    const loadSnapshots = async (period) => {
        const data = await getJson(`${CLAN_BATTLE_API}/snapshots?period=${encodeURIComponent(period)}`);
        const timeData = {};
        const snapshotMap = {};

        for (const snapshot of data.items) {
            if (!snapshot.captured_at) continue;
            const parts = compactDateTime(snapshot.captured_at);
            if (!timeData[parts.date]) timeData[parts.date] = [];
            if (!timeData[parts.date].includes(parts.time)) timeData[parts.date].push(parts.time);
            snapshotMap[`${parts.date}${parts.time}`] = snapshot.snapshot_id;
        }

        for (const times of Object.values(timeData)) times.sort();
        clanBattle.timeData = timeData;
        clanBattle.snapshotMap = snapshotMap;

        const dates = Object.keys(timeData).sort();
        if (dates.length > 0) {
            clanBattle.selectedDate = dates[dates.length - 1];
            const times = timeData[clanBattle.selectedDate];
            clanBattle.selectedTime = times[times.length - 1];
        }
    };

    const loadClanBattleTime = async () => {
        clanBattle.loading = true;
        try {
            await loadCurrentPeriod();
            if (clanBattle.currentPeriod) await loadSnapshots(clanBattle.currentPeriod);
        } catch (error) {
            showError(error.message);
        } finally {
            clanBattle.loading = false;
        }
    };

    const loadClanBattleHistory = async () => {
        clanBattle.loading = true;
        try {
            await loadHistoryPeriods();
        } catch (error) {
            showError(error.message);
        } finally {
            clanBattle.loading = false;
        }
    };

    const updateTimeOptions = () => {
        const times = clanBattle.timeData[clanBattle.selectedDate];
        if (times && times.length > 0) {
            clanBattle.selectedTime = times[times.length - 1];
        }
    };

    const selectionParams = () => {
        if (clanBattle.mode === 'current') {
            const snapshotId = clanBattle.snapshotMap[`${clanBattle.selectedDate}${clanBattle.selectedTime}`];
            if (!snapshotId) throw new Error('当前没有可查询的快照');
            return `snapshot_id=${snapshotId}`;
        }
        if (!clanBattle.selectedHistory) throw new Error('请选择历史月份');
        return `period=${encodeURIComponent(clanBattle.selectedHistory)}`;
    };

    const searchClanBattle = async (page = 0) => {
        clanBattle.loading = true;
        clanBattle.page = page;
        try {
            const params = selectionParams();
            const data = await getJson(
                `${CLAN_BATTLE_API}/rankings?${params}`
                + `&search=${encodeURIComponent(clanBattle.searchText)}`
                + `&page=${page}&limit=${clanBattle.limit}`
            );
            clanBattle.results = data.items;
            clanBattle.maxPage = Math.ceil(data.total / clanBattle.limit);
        } catch (error) {
            showError(error.message);
        } finally {
            clanBattle.loading = false;
        }
    };

    const searchScoreLine = async () => {
        clanBattle.loading = true;
        try {
            const params = selectionParams();
            const rank = /^\d+$/.test(clanBattle.searchText.trim())
                ? `&rank=${clanBattle.searchText.trim()}`
                : '';
            const data = await getJson(`${CLAN_BATTLE_API}/scorelines?${params}${rank}`);
            clanBattle.results = data.items;
            clanBattle.maxPage = data.items.length > 0 ? 1 : 0;
            clanBattle.page = 0;
        } catch (error) {
            showError(error.message);
        } finally {
            clanBattle.loading = false;
        }
    };

    const clanBattlePage = (delta) => {
        const newPage = clanBattle.page + delta;
        if (newPage >= 0 && newPage < clanBattle.maxPage) {
            searchClanBattle(newPage);
        }
    };

    const showError = (message) => {
        clanBattle.errorOccured = true;
        clanBattle.errorMsg = message;
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
