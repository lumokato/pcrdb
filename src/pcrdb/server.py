"""
pcrdb Web API 服务
提供公会、玩家、PJJC 数据查询接口
"""
from fastapi import FastAPI, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / '.env')

from src.pcrdb.analysis.clan import get_clan_history, get_clan_power_ranking
from src.pcrdb.analysis.grand import get_winning_ranking
from src.pcrdb.analysis.player import get_player_clan_history, search_players_by_name, get_available_periods
from src.pcrdb.auth import (
    authenticate_user, create_user, create_access_token,
    get_current_user, get_user_by_username, get_user_by_qq,
    log_api_call, verify_password, update_password,
    get_current_active_user, get_current_admin_user, get_all_users, approve_user_status,
    get_user_api_stats, get_user_api_details
)

app = FastAPI(
    title="pcrdb API",
    description="公主连结渠道服数据查询 API",
    version="1.0.1"
)

# CORS 配置 - 允许本地前端访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8433",
        "http://127.0.0.1:8433",
        "http://localhost:5500",  # VS Code Live Server
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """版本信息"""
    return {"message": "pcrdb API", "version": "1.0.1"}


# === 认证 API ===
class RegisterRequest(BaseModel):
    username: str
    password: str
    qq_number: str


class LoginRequest(BaseModel):
    login_id: str  # 用户名或 QQ 号
    password: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    """用户注册"""
    if len(req.username) < 2:
        return {"error": "用户名至少 2 个字符"}
    
    if len(req.password) < 6:
        return {"error": "密码至少 6 位"}
    
    if not req.qq_number.isdigit() or len(req.qq_number) < 5:
        return {"error": "请输入有效的 QQ 号"}
    
    if get_user_by_username(req.username):
        return {"error": "用户名已存在"}
    
    if get_user_by_qq(req.qq_number):
        return {"error": "该 QQ 号已注册"}
    
    user = create_user(req.username, req.password, req.qq_number)
    token = create_access_token({"sub": user["username"]})
    return {
        "success": True, 
        "token": token, 
        "username": user["username"],
        "status": user["status"],
        "role": user["role"]
    }


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    """用户登录（支持用户名或 QQ 号）"""
    user = authenticate_user(req.login_id, req.password)
    if not user:
        return {"error": "用户名/QQ号或密码错误"}
    
    token = create_access_token({"sub": user["username"]})
    return {
        "success": True, 
        "token": token, 
        "username": user["username"], 
        "role": user.get("role", "user"),
        "status": user.get("status", "pending")
    }


@app.post("/api/auth/change_password")
async def change_password(req: ChangePasswordRequest, user: dict = Depends(get_current_user)):
    """修改密码（需要登录）"""
    # 验证旧密码
    if not verify_password(req.old_password, user["password_hash"]):
        return {"error": "原密码错误"}
    
    if len(req.new_password) < 6:
        return {"error": "新密码至少 6 位"}
    
    if update_password(user["id"], req.new_password):
        return {"success": True, "message": "密码修改成功"}
    else:
        return {"error": "密码修改失败"}


@app.get("/api/auth/me")
async def get_me(user: dict = Depends(get_current_user)):
    """获取当前用户信息"""
    return {
        "username": user["username"], 
        "id": user["id"], 
        "qq_number": user.get("qq_number"),
        "role": user.get("role", "user"),
        "status": user.get("status", "pending")
    }


# === 管理 API ===

@app.get("/api/admin/users")
async def admin_get_users(user: dict = Depends(get_current_admin_user)):
    """获取所有用户列表"""
    return {"users": get_all_users()}


@app.post("/api/admin/approve/{user_id}")
async def admin_approve_user(user_id: int, user: dict = Depends(get_current_admin_user)):
    """批准用户"""
    if approve_user_status(user_id):
        return {"success": True}
    return {"error": "操作失败"}


@app.get("/api/admin/api_stats")
async def admin_api_stats(user: dict = Depends(get_current_admin_user)):
    """获取所有用户 API 调用统计"""
    stats = get_user_api_stats()
    return {
        "stats": [
            {
                "user_id": s["id"],
                "username": s["username"],
                "qq_number": s["qq_number"],
                "total_calls": s["total_calls"],
                "last_call_at": s["last_call_at"].isoformat() if s["last_call_at"] else None
            }
            for s in stats
        ]
    }


@app.get("/api/admin/api_stats/{user_id}")
async def admin_api_details(
    user_id: int,
    limit: int = Query(50, description="返回数量"),
    user: dict = Depends(get_current_admin_user)
):
    """获取指定用户 API 调用详情"""
    details = get_user_api_details(user_id, limit)
    return {
        "details": [
            {
                "endpoint": d["endpoint"],
                "query_params": d["query_params"],
                "created_at": d["created_at"].isoformat() if d["created_at"] else None
            }
            for d in details
        ]
    }


@app.get("/api/clan/history")
async def api_clan_history(
    clan_id: Optional[int] = Query(None, description="公会 ID"),
    clan_name: Optional[str] = Query(None, description="公会名"),
    limit: int = 10,
    user: dict = Depends(get_current_active_user)
):
    """获取公会历史排名（需要激活账号）"""
    log_api_call(user["id"], "clan_history", {"clan_id": clan_id, "clan_name": clan_name})
    if not clan_id and not clan_name:
        return {"error": "请提供 clan_id 或 clan_name"}
    
    return get_clan_history(clan_id=clan_id, clan_name=clan_name, limit=limit)


@app.get("/api/clan/power_ranking")
async def api_clan_power_ranking(
    user: dict = Depends(get_current_active_user)
):
    """获取公会战力/人数排名（需要激活账号）"""
    log_api_call(user["id"], "clan_power", {})
    return get_clan_power_ranking()


@app.get("/api/grand/winning")
async def api_grand_winning(
    group: int = Query(0, description="分场 (1-10)"),
    limit: int = Query(50, description="返回数量"),
    user: dict = Depends(get_current_active_user)
):
    """获取 PJJC 胜场排名（需要激活账号）"""
    log_api_call(user["id"], "grand_winning", {"group": group, "limit": limit})
    return get_winning_ranking(group=group, limit=limit)


@app.get("/api/player/history")
async def api_player_history(
    viewer_id: int = Query(..., description="玩家 ViewerId"),
    user: dict = Depends(get_current_active_user)
):
    """获取玩家公会历史（需要激活账号）"""
    log_api_call(user["id"], "player_history", {"viewer_id": viewer_id})
    return get_player_clan_history(viewer_id=viewer_id)


@app.get("/api/player/search")
async def api_player_search(
    name: str = Query(..., description="玩家名（模糊匹配）"),
    period: Optional[str] = Query(None, description="月份 YYYY-MM"),
    limit: int = Query(50, description="返回数量"),
    user: dict = Depends(get_current_active_user)
):
    """搜索玩家（需要激活账号）"""
    log_api_call(user["id"], "player_search", {"name": name, "period": period, "limit": limit})
    return search_players_by_name(name_pattern=name, period=period, limit=limit)


@app.get("/api/player/periods")
async def api_player_periods(
    user: dict = Depends(get_current_active_user)
):
    """获取有玩家数据的月份列表（需要激活账号）"""
    return get_available_periods()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )


# === 会战 API 代理（解决跨域问题）===
import httpx
from fastapi import Request, Response

CLAN_BATTLE_API = "https://clan.120224.xyz"


@app.get("/proxy/current/getalltime/qd")
async def proxy_current_time():
    """代理：获取当期会战时间点"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{CLAN_BATTLE_API}/current/getalltime/qd")
        return resp.json()


@app.get("/proxy/history/getalltime/qd")
async def proxy_history_time():
    """代理：获取历史会战月份"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{CLAN_BATTLE_API}/history/getalltime/qd")
        return resp.json()


@app.post("/proxy/search")
async def proxy_search(request: Request):
    """代理：搜索公会排名"""
    body = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{CLAN_BATTLE_API}/search", json=body)
        return resp.json()


@app.post("/proxy/search/scoreline")
async def proxy_scoreline(request: Request):
    """代理：查档线"""
    body = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{CLAN_BATTLE_API}/search/scoreline", json=body)
        return resp.json()

