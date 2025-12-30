"""
PCRDB Unified Runner
Starts Backend (API), Frontend (Static + Proxy), and Scheduler.
"""
import os
import sys
import threading
import time
import httpx
import uvicorn
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# === FIX for Windows MIME Types ===
import mimetypes
# Force initialize standard types first
mimetypes.init()
# Explicitly override .js to application/javascript
if '.js' in mimetypes.types_map:
    del mimetypes.types_map['.js']
mimetypes.add_type("application/javascript", ".js")
# Also patch common_types just in case
mimetypes.common_types['.js'] = 'application/javascript'

# Load config
load_dotenv()

BACKEND_HOST = os.environ["BACKEND_HOST"]
BACKEND_PORT = int(os.environ["BACKEND_PORT"])
FRONTEND_HOST = os.environ["FRONTEND_HOST"]
FRONTEND_PORT = int(os.environ["FRONTEND_PORT"])
BACKEND_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}"

# === Backend Service ===
def run_backend():
    """Runs the Backend API Server"""
    print(f"[Backend] Starting on {BACKEND_HOST}:{BACKEND_PORT}...")
    # Import here to avoid early dependency loading
    uvicorn.run("pcrdb.server:app", host=BACKEND_HOST, port=BACKEND_PORT, log_level="info")

# === Scheduler Service ===
def run_scheduler():
    """Runs the Task Scheduler"""
    print("[Scheduler] Starting...")
    try:
        from scheduler import load_schedule_config, setup_schedules
        import schedule
        
        config = load_schedule_config()
        if config:
            setup_schedules(config)
            while True:
                schedule.run_pending()
                time.sleep(10)
    except Exception as e:
        print(f"[Scheduler] Error: {e}")

# === Frontend & Proxy Service ===
frontend_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# Custom StaticFiles to force correct MIME types on Windows
class CustomStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs):
        resp = super().file_response(*args, **kwargs)
        # Check by extension case-insensitively
        if hasattr(resp, 'path'):
            filename = str(resp.path).lower()
            if filename.endswith('.js'):
                 # Force header override
                 print(f"[Static] Serving JS: {resp.path} forced to application/javascript")
                 resp.media_type = "application/javascript"
                 resp.headers["content-type"] = "application/javascript"
        return resp

async def proxy_request(request: Request):
    """Forwards requests to the backend"""
    path = request.path_params.get("path", "")
    client = httpx.AsyncClient(base_url=BACKEND_URL)
    
    url = f"/{path}" # Reconstruct the path relative to backend root but ensuring we don't double slash if not needed, or just use request.url.path logic
    # Actually, the logic in previous code was: url = request.url.path which is full path like /api/foo/bar
    # If backend expects /api/foo/bar it is fine.
    # But wait, if I use /api/{path:path}, the backend likely expects /api/... as well if it has the same routes?
    # server.py has @app.post("/api/auth/register")
    # So if frontend requests /api/auth/register, proxy receives /api/auth/register
    # path param "path" would be "auth/register"
    # request.url.path would be "/api/auth/register".
    
    # Let's keep using request.url.path as it was in the original code, 
    # but we just need to fix the function signature.
    
    client = httpx.AsyncClient(base_url=BACKEND_URL)
    
    # We will use the full path from the request, so we don't technically need the 'path' variable 
    # for the URL construction if we just proxy exact path.
    # However, 'path' was incorrectly requested as an arg.
    
    url = request.url.path
    if request.url.query:
        url += f"?{request.url.query}"
    
    # Exclude headers that might cause issues
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None) # Let httpx handle this
    
    try:
        content = await request.body()
        
        rp_req = client.build_request(
            request.method,
            url,
            headers=headers,
            content=content
        )
        rp_resp = await client.send(rp_req, stream=True)
        
        return Response(
            content=await rp_resp.aread(),
            status_code=rp_resp.status_code,
            headers=dict(rp_resp.headers),
            background=BackgroundTask(rp_resp.aclose)
        )
    except Exception as e:
        return Response(content=f"Proxy Error: {str(e)}", status_code=502)
    finally:
        await client.aclose()

# Register Proxy Routes
# We need to catch /api/* and /proxy/*
# Note: "path" in {path:path} captures the rest of the URL
frontend_app.add_route("/api/{path:path}", proxy_request, methods=["GET", "POST", "PUT", "DELETE"])
frontend_app.add_route("/proxy/{path:path}", proxy_request, methods=["GET", "POST", "PUT", "DELETE"])

# Mount Static Files (Must be last to avoid catching api routes if folders match)
if Path("frontend").exists():
    frontend_app.mount("/", CustomStaticFiles(directory="frontend", html=True), name="static")

def run_frontend():
    """Runs the Frontend Server with Proxy"""
    print(f"[Frontend] Starting on {FRONTEND_HOST}:{FRONTEND_PORT}...")
    uvicorn.run(frontend_app, host=FRONTEND_HOST, port=FRONTEND_PORT, log_level="error")

if __name__ == "__main__":
    print("=== PCRDB Unified Server Starting ===")
    
    # Create threads
    t_backend = threading.Thread(target=run_backend, daemon=True)
    t_frontend = threading.Thread(target=run_frontend, daemon=True)
    t_scheduler = threading.Thread(target=run_scheduler, daemon=True)
    
    # Start threads
    t_backend.start()
    time.sleep(2) # Give backend a moment
    t_scheduler.start()
    t_frontend.start()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
