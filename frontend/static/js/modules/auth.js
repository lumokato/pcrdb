/**
 * 认证模块
 * 处理登录、注册、登出、管理员功能
 */
import { LOCAL_API, createAuthFetch } from './utils.js';

/**
 * 创建认证模块
 */
export function useAuth(currentTab) {
    const { reactive, watch } = Vue;

    // 认证状态
    const auth = reactive({
        isLoggedIn: false,
        username: '',
        token: '',
        role: 'user',
        status: 'pending',
        loginId: '',
        loginPassword: '',
        registerUsername: '',
        registerPassword: '',
        registerQQ: '',
        error: '',
        loading: false,
        isRegisterMode: false
    });

    // 管理员状态
    const admin = reactive({
        loading: false,
        users: []
    });

    // 带认证的 fetch
    const authFetch = createAuthFetch(() => auth.token);

    // 初始化认证
    const initAuth = () => {
        const token = localStorage.getItem('pcrdb_token');
        const username = localStorage.getItem('pcrdb_username');
        const role = localStorage.getItem('pcrdb_role');
        const status = localStorage.getItem('pcrdb_status');

        if (token && username) {
            auth.isLoggedIn = true;
            auth.token = token;
            auth.username = username;
            auth.role = role || 'user';
            auth.status = status || 'active';
            checkStatus();
        }
    };

    // 登录
    const login = async () => {
        auth.loading = true;
        auth.error = '';
        try {
            const res = await fetch(`${LOCAL_API}/api/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    login_id: auth.loginId,
                    password: auth.loginPassword
                })
            });
            const data = await res.json();
            if (data.success) {
                auth.isLoggedIn = true;
                auth.token = data.token;
                auth.username = data.username;
                auth.role = data.role;
                auth.status = data.status;

                localStorage.setItem('pcrdb_token', data.token);
                localStorage.setItem('pcrdb_username', data.username);
                localStorage.setItem('pcrdb_role', data.role);
                localStorage.setItem('pcrdb_status', data.status);

                auth.loginId = '';
                auth.loginPassword = '';
                currentTab.value = 'clan';
            } else {
                auth.error = data.error || '登录失败';
            }
        } catch (e) {
            auth.error = '网络错误，请确保后端服务已启动';
        } finally {
            auth.loading = false;
        }
    };

    // 注册
    const register = async () => {
        auth.loading = true;
        auth.error = '';
        try {
            const res = await fetch(`${LOCAL_API}/api/auth/register`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    username: auth.registerUsername,
                    password: auth.registerPassword,
                    qq_number: auth.registerQQ
                })
            });
            const data = await res.json();
            if (data.success) {
                auth.isLoggedIn = true;
                auth.token = data.token;
                auth.username = data.username;
                auth.role = data.role;
                auth.status = data.status;

                localStorage.setItem('pcrdb_token', data.token);
                localStorage.setItem('pcrdb_username', data.username);
                localStorage.setItem('pcrdb_role', data.role);
                localStorage.setItem('pcrdb_status', data.status);

                auth.registerUsername = '';
                auth.registerPassword = '';
                auth.registerQQ = '';
                currentTab.value = 'clan';
            } else {
                auth.error = data.error || '注册失败';
            }
        } catch (e) {
            auth.error = '网络错误，请确保后端服务已启动';
        } finally {
            auth.loading = false;
        }
    };

    // 登出
    const logout = () => {
        auth.isLoggedIn = false;
        auth.token = '';
        auth.username = '';
        localStorage.removeItem('pcrdb_token');
        localStorage.removeItem('pcrdb_username');
        localStorage.removeItem('pcrdb_role');
        localStorage.removeItem('pcrdb_status');
        currentTab.value = 'login';
    };

    // 检查状态
    const checkStatus = async () => {
        if (!auth.token) return;
        try {
            const res = await authFetch(`${LOCAL_API}/api/auth/me`);
            const data = await res.json();
            if (data.username) {
                auth.role = data.role;
                auth.status = data.status;
                localStorage.setItem('pcrdb_role', data.role);
                localStorage.setItem('pcrdb_status', data.status);
            }
        } catch (e) {
            console.error('刷新状态失败', e);
        }
    };

    // 管理员：获取用户列表
    const adminGetUsers = async () => {
        if (auth.role !== 'admin') return;
        admin.loading = true;
        try {
            const res = await authFetch(`${LOCAL_API}/api/admin/users`);
            const data = await res.json();
            if (data.users) {
                admin.users = data.users;
            }
        } catch (e) {
            console.error(e);
        } finally {
            admin.loading = false;
        }
    };

    // 管理员：批准用户
    const adminApproveUser = async (userId) => {
        if (auth.role !== 'admin') return;
        if (!confirm('确定通过该用户吗？')) return;
        admin.loading = true;
        try {
            const res = await authFetch(`${LOCAL_API}/api/admin/approve/${userId}`, {
                method: 'POST'
            });
            const data = await res.json();
            if (data.success) {
                adminGetUsers();
            } else {
                alert(data.error || '操作失败');
            }
        } catch (e) {
            alert('操作失败');
        } finally {
            admin.loading = false;
        }
    };

    // 监听 Tab 切换加载管理员数据
    watch(currentTab, (newTab) => {
        if (newTab === 'admin' && auth.role === 'admin') {
            adminGetUsers();
        }
    });

    return {
        auth,
        admin,
        authFetch,
        initAuth,
        login,
        register,
        logout,
        checkStatus,
        adminGetUsers,
        adminApproveUser
    };
}
