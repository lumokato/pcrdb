# 添加新功能工作流

为 pcrdb 项目添加新功能时，提供给 LLM 的最小上下文参考。

// turbo-all

---

## 核心文件清单（含行号）

### 1. API 文档
- `docs/ANALYSIS_API.md` - 完整读取，作为规范模板

### 2. 后端
| 文件 | 关键行范围 | 说明 |
|------|-----------|------|
| `src/pcrdb/analysis/{module}.py` | 全文 | 相关分析模块 |
| `src/pcrdb/server.py` | 1-30（导入）, 195-230（路由） | 添加路由 |
| `src/pcrdb/db/schema.sql` | 30-80 | 主要表结构 |

### 3. 前端
| 文件 | 关键行范围 | 说明 |
|------|-----------|------|
| `frontend/static/js/modules/{module}.js` | 全文 | 相关前端模块 |
| `frontend/static/js/main.js` | 1-40, 85-105 | 导入和导出 |
| `frontend/index.html` | **见下方 Tab 位置表** | UI 模板 |

### index.html Tab 位置参考（约500行）
| Tab 名称 | 起始行 | 结束行 |
|---------|--------|--------|
| 会战查询 | 60 | 160 |
| 登录 | 160 | 210 |
| 管理面板 | 220 | 265 |
| 公会查询 | 265 | 330 |
| 双场排名 | 330 | 385 |
| 玩家查询 | 385 | 490 |

**添加新功能时，只读取对应 Tab 的行范围即可。**

---

## index.html 过长问题

### 现状
- 当前约 500 行
- 预计会增长到 1000+ 行

### 解决方案：Vue 组件化

将每个 Tab 拆分为独立的 `.vue` 组件或 `.js` 模板组件：

```
frontend/static/js/components/
├── ClanBattleTab.js
├── ClanTab.js
├── GrandTab.js
├── PlayerTab.js
└── AdminTab.js
```

**示例组件结构：**
```javascript
// PlayerTab.js
export const PlayerTab = {
    template: `
        <div class="card">
            <!-- 玩家查询 UI -->
        </div>
    `,
    props: ['player'],
    emits: ['search-history', 'search-players']
};
```

**主文件引用：**
```javascript
// main.js
import { PlayerTab } from './components/PlayerTab.js';

createApp({
    components: { PlayerTab },
    // ...
});
```

```html
<!-- index.html -->
<player-tab 
    :player="player"
    @search-history="searchPlayerHistory"
    @search-players="searchPlayers">
</player-tab>
```

> **建议**：当 index.html 超过 800 行时考虑组件化重构。

---

## 添加功能步骤

### Step 1: 明确需求
- 功能名称、输入参数、返回格式、UI 交互

### Step 2: 更新文档
- `docs/ANALYSIS_API.md` - 添加 API 规范

### Step 3: 后端实现
- `src/pcrdb/analysis/{module}.py` - 添加函数
- `src/pcrdb/server.py` - 添加路由 + 更新 import

### Step 4: 前端实现
- `frontend/static/js/modules/{module}.js` - 状态和函数
- `frontend/static/js/main.js` - 更新导出
- `frontend/index.html` - 添加 UI

---

## 减少 Token 消耗

1. **使用行范围读取**：`view_file` 指定 StartLine/EndLine
2. **跳过探索**：按本文档直接编写
3. **用 outline**：只看函数签名
4. **避免 grep**：schema.sql 和 API.md 已足够

---

## 常见问题预防

| 问题 | 预防措施 |
|------|----------|
| 结果互相干扰 | 显示用 `v-if="mode === 'xxx'"` |
| 下拉无数据 | 需独立的选项列表 API |
| 401 错误 | 处理 authFetch 的 401 状态 |
