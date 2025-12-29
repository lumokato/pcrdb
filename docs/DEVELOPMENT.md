# 开发指南 (Development Guide)

本文档旨在帮助开发者快速理解项目结构并进行功能扩展。

## 核心架构

- **后端**: FastAPI (`src/pcrdb/server.py`) + PostgreSQL (`src/pcrdb/db/`) + 业务逻辑 (`src/pcrdb/analysis/`)
- **前端**: Vue 3 (ES Modules) + CSS Glassmorphism

## 启动服务

```powershell
# 前端（必须使用 serve.py，支持 ES Module MIME 类型）
cd frontend
python serve.py          # http://localhost:8433

# 后端
cd ..
python -m uvicorn src.pcrdb.server:app --reload --host 127.0.0.1 --port 8000
```

> ⚠️ 不能使用 `python -m http.server`，它不支持 ES Module 的正确 MIME 类型。

## 前端 JS 模块结构

```
frontend/static/js/
├── main.js              # 入口文件
└── modules/
    ├── auth.js          # 认证 + 管理员
    ├── clanBattle.js    # 会战查询
    ├── clan.js          # 公会查询
    ├── grand.js         # 双场查询
    ├── player.js        # 玩家查询
    └── utils.js         # 工具函数
```

---

## 如何添加新功能

如果需要在现有的查询面板（如公会查询）中添加新功能，遵循以下流程：

### 1. 后端 (Backend)

在 `server.py` 中添加新的 API 接口。

```python
# src/pcrdb/server.py

@app.get("/api/clan/new_feature")
async def api_clan_new_feature(clan_id: int):
    # 调用 analysis 模块的具体逻辑
    return get_clan_feature_data(clan_id)
```

### 2. 前端界面 (HTML)

在 `index.html` 对应的 Tab 中添加控制按钮和显示区域。通过 `v-if` 控制显示。

```html
<!-- frontend/index.html -->

<!-- 1. 在 .tab-container 中添加按钮 -->
<div class="tab-container">
    <button class="tab-btn" :class="{ active: clan.mode === 'history' }" ...>公会历史</button>
    <!-- 新按钮 -->
    <button class="tab-btn" :class="{ active: clan.mode === 'new_feature' }"
        @click="clan.mode = 'new_feature'">新功能</button>
</div>

<!-- 2. 添加显示区域，使用 v-if 控制 -->
<div v-if="clan.mode === 'new_feature'">
    <div class="card">
        <!-- 你的内容 -->
    </div>
</div>
```

### 3. 前端逻辑 (JS)

在 `main.js` 中添加对应的状态和请求函数。

```javascript
// frontend/static/js/main.js

// 1. 在 reactive 对象中添加状态
const clan = reactive({
    mode: 'history', // 'history' | 'new_feature'
    // ...
});

// 2. 添加请求函数
const searchNewFeature = async () => {
    const res = await authFetch(`${LOCAL_API}/api/clan/new_feature?clan_id=...`);
    const data = await res.json();
    // 处理数据
}
```

---

## 文档索引

- [API 规范](API.md)
- [功能列表](FEATURES.md)
