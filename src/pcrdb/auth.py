"""
pcrdb 认证模块
JWT Token 生成与验证、用户管理
"""
import os
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import psycopg2
from psycopg2.extras import RealDictCursor
from src.pcrdb.db.connection import create_connection

# 配置
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "pcrdb_secret_key_change_in_production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30

# Bearer token 提取
security = HTTPBearer(auto_error=False)


def get_auth_db():
    """获取认证数据库连接"""
    return create_connection(cursor_factory=RealDictCursor)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def hash_password(password: str) -> str:
    """密码哈希"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """创建 JWT token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_user_by_username(username: str) -> Optional[dict]:
    """通过用户名查询用户"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM auth.users WHERE username = %s", (username,))
        return cursor.fetchone()
    finally:
        conn.close()


def get_user_by_qq(qq_number: str) -> Optional[dict]:
    """通过 QQ 号查询用户"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM auth.users WHERE qq_number = %s", (qq_number,))
        return cursor.fetchone()
    finally:
        conn.close()


def create_user(username: str, password: str, qq_number: str) -> dict:
    """创建新用户"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)
        
        # 检查是否是第一个用户
        cursor.execute("SELECT COUNT(*) as count FROM auth.users")
        count = cursor.fetchone()["count"]
        
        # 第一个用户自动激活并设为管理员
        if count == 0:
            role = "admin"
            status = "active"
        else:
            role = "user"
            status = "pending"
            
        cursor.execute(
            "INSERT INTO auth.users (username, password_hash, qq_number, role, status) VALUES (%s, %s, %s, %s, %s) RETURNING id, username, qq_number, role, status, created_at",
            (username, password_hash, qq_number, role, status)
        )
        conn.commit()
        return cursor.fetchone()
    finally:
        conn.close()


def authenticate_user(login_id: str, password: str) -> Optional[dict]:
    """验证用户登录（支持用户名或 QQ 号）"""
    # 先尝试用户名
    user = get_user_by_username(login_id)
    # 再尝试 QQ 号
    if not user:
        user = get_user_by_qq(login_id)
    
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return user


def update_password(user_id: int, new_password: str) -> bool:
    """更新用户密码"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        password_hash = hash_password(new_password)
        cursor.execute(
            "UPDATE auth.users SET password_hash = %s WHERE id = %s",
            (password_hash, user_id)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """从 token 获取当前用户（受保护路由使用）"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无效的认证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    if credentials is None:
        raise credentials_exception
    
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = get_user_by_username(username)
    if user is None:
        raise credentials_exception
    
    return user


async def get_current_active_user(current_user: dict = Depends(get_current_user)) -> dict:
    """获取当前已激活的用户"""
    # 检查状态字段（如果数据库还没更新完，可能会没有这个字段，这里假设数据库已更新）
    user_status = current_user.get("status", "pending")
    if user_status != "active":
        raise HTTPException(status_code=403, detail="账号待审核")
    return current_user


async def get_current_admin_user(current_user: dict = Depends(get_current_active_user)) -> dict:
    """获取当前管理员用户"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return current_user


def get_all_users() -> list:
    """获取所有用户列表（管理员用）"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, qq_number, role, status, created_at FROM auth.users ORDER BY created_at DESC")
        return cursor.fetchall()
    finally:
        conn.close()


def approve_user_status(user_id: int) -> bool:
    """批准用户（设为 active）"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE auth.users SET status = 'active' WHERE id = %s", (user_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def log_api_call(user_id: int, endpoint: str, query_params: dict = None):
    """记录 API 调用日志"""
    import json
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO auth.api_logs (user_id, endpoint, query_params) VALUES (%s, %s, %s)",
            (user_id, endpoint, json.dumps(query_params) if query_params else None)
        )
        conn.commit()
    finally:
        conn.close()


def get_user_api_stats() -> list:
    """获取所有用户的 API 调用统计"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                u.id,
                u.username,
                u.qq_number,
                COUNT(l.id) as total_calls,
                MAX(l.created_at) as last_call_at
            FROM auth.users u
            LEFT JOIN auth.api_logs l ON u.id = l.user_id
            GROUP BY u.id, u.username, u.qq_number
            ORDER BY total_calls DESC
        """)
        return cursor.fetchall()
    finally:
        conn.close()


def get_user_api_details(user_id: int, limit: int = 50) -> list:
    """获取指定用户的 API 调用详情"""
    conn = get_auth_db()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT endpoint, query_params, created_at
            FROM auth.api_logs
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (user_id, limit))
        return cursor.fetchall()
    finally:
        conn.close()
