/**
 * 工具函数模块
 * 提供格式化、API 请求等公共函数
 */

// API 配置
export const LOCAL_API = 'http://localhost:8000';
export const CLAN_BATTLE_API = 'http://localhost:8000/proxy';

/**
 * 格式化日期
 */
export function formatDate(str) {
    if (!str) return '';
    // ISO 时间格式 (2025-12-29T15:00:00)
    if (str.includes('-')) {
        try {
            const d = new Date(str);
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            const hour = String(d.getHours()).padStart(2, '0');
            const minute = String(d.getMinutes()).padStart(2, '0');
            return `${year}/${month}/${day} ${hour}:${minute}`;
        } catch (e) {
            return str;
        }
    }
    // 紧凑格式 (20251229)
    if (str.length === 8) {
        return `${str.slice(0, 4)}/${str.slice(4, 6)}/${str.slice(6, 8)}`;
    }
    return str;
}

/**
 * 格式化时间
 */
export function formatTime(str) {
    if (!str || str.length < 4) return str;
    return `${str.slice(0, 2)}:${str.slice(2, 4)}`;
}

/**
 * 格式化日期时间（用于 API 调用记录）
 */
export function formatDateTime(str) {
    if (!str) return '';
    try {
        const d = new Date(str);
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        const hour = String(d.getHours()).padStart(2, '0');
        const minute = String(d.getMinutes()).padStart(2, '0');
        return `${month}/${day} ${hour}:${minute}`;
    } catch (e) {
        return str;
    }
}

/**
 * 带认证的 fetch 封装
 */
export function createAuthFetch(getToken) {
    return async (url, options = {}) => {
        const headers = {
            ...options.headers,
            'Authorization': `Bearer ${getToken()}`
        };
        return fetch(url, { ...options, headers });
    };
}
