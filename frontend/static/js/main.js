/**
 * PCR 数据工具箱 - 前端 Vue 应用
 * 入口文件 - 组合各功能模块
 */
import { formatDate, formatTime, formatDateTime } from './modules/utils.js';
import { useAuth } from './modules/auth.js';
import { useClanBattle } from './modules/clanBattle.js';
import { useClan } from './modules/clan.js';
import { useGrand } from './modules/grand.js';
import { usePlayer } from './modules/player.js';

const { createApp, ref, onMounted } = Vue;

createApp({
    setup() {
        // 当前 Tab
        const currentTab = ref('clan_battle');

        // 主题
        const isDarkMode = ref(false);

        // === 初始化各模块 ===
        const {
            auth, admin, authFetch,
            initAuth, login, register, logout, checkStatus,
            adminGetUsers, adminApproveUser, loadApiStats, showApiDetails, showUserInfo
        } = useAuth(currentTab);

        const {
            clanBattle,
            loadClanBattleTime, loadClanBattleHistory,
            updateTimeOptions, searchClanBattle, searchScoreLine, clanBattlePage
        } = useClanBattle();

        const { clan, searchClanHistory } = useClan(authFetch, logout);
        const { grand, searchGrandWinning } = useGrand(authFetch, logout);
        const { player, loadPeriodOptions, searchPlayerHistory, searchPlayers } = usePlayer(authFetch, logout);

        // === 主题切换 ===
        const toggleTheme = () => {
            isDarkMode.value = !isDarkMode.value;
            document.body.classList.toggle('dark-mode', isDarkMode.value);
            localStorage.setItem('theme', isDarkMode.value ? 'dark' : 'light');
        };

        const loadTheme = () => {
            const saved = localStorage.getItem('theme');
            if (saved === 'dark') {
                isDarkMode.value = true;
                document.body.classList.add('dark-mode');
            }
        };

        // 辅助函数：获取用户调用次数
        const getApiCallCount = (userId) => {
            const stat = admin.apiStats.find(s => s.user_id === userId);
            return stat ? stat.total_calls : 0;
        };

        // 辅助函数：获取用户最后调用时间
        const getLastCallTime = (userId) => {
            const stat = admin.apiStats.find(s => s.user_id === userId);
            return stat && stat.last_call_at ? formatDateTime(stat.last_call_at) : '-';
        };

        // 初始化
        onMounted(() => {
            loadTheme();
            initAuth();
            loadClanBattleTime();
        });

        return {
            currentTab,
            isDarkMode,
            toggleTheme,

            // 认证
            auth,
            login,
            register,
            logout,

            // 会战
            clanBattle,
            loadClanBattleTime,
            loadClanBattleHistory,
            updateTimeOptions,
            searchClanBattle,
            searchScoreLine,
            clanBattlePage,
            formatDate,
            formatTime,

            // 公会
            clan,
            searchClanHistory,

            // 双场
            grand,
            searchGrandWinning,

            // 玩家
            player,
            loadPeriodOptions,
            searchPlayerHistory,
            searchPlayers,

            // 管理员 & 工具
            admin,
            checkStatus,
            adminGetUsers,
            adminApproveUser,
            loadApiStats,
            showApiDetails,
            showUserInfo,
            formatDateTime,
            getApiCallCount,
            getLastCallTime
        };
    }
}).mount('#app');
