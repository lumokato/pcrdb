"""
PCRDB Analysis Server
"""
import sys
from pathlib import Path
from typing import Optional

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn

from pcrdb.analysis import clan, player, grand

app = FastAPI(title="PCRDB Analysis")

# 挂载静态文件
static_dir = Path(__file__).parent / "static"
if not static_dir.exists():
    static_dir.mkdir(parents=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def read_root():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return index_file.read_text(encoding="utf-8")
    return "<h1>PCRDB Analysis Server</h1><p>Please create index.html in static folder.</p>"


@app.get("/api/clan/history")
async def api_clan_history(
    name: Optional[str] = None, 
    id: Optional[int] = None
):
    """查询公会历史"""
    if not name and not id:
        raise HTTPException(status_code=400, detail="Missing name or id")
    
    result = clan.get_clan_history(clan_id=id, clan_name=name)
    if "error" in result:
        # 如果是找不到公会，返回空历史而不是 404，或者根据前端需求调整
        # 这里为了前端方便判断，保留错误信息但状态码设为 200 或 404
        # 现在的实现是返回 dict 带 error key
        return result
    return result


@app.get("/api/player/history")
async def api_player_history(vid: int):
    """查询玩家历史"""
    result = player.get_player_clan_history(viewer_id=vid)
    return result


@app.get("/api/clan/power-rank")
async def api_clan_power_rank(limit: int = 50):
    """查询战力排行"""
    result = clan.get_clan_power_ranking(limit=limit)
    return result


@app.get("/api/grand/winning")
async def api_grand_winning(group: int = 0, limit: int = 100):
    """查询 PJJC 胜场排行"""
    result = grand.get_winning_ranking(group=group, limit=limit)
    return result


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
