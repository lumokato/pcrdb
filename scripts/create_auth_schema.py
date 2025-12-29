"""创建 auth schema 和表"""
from src.pcrdb.db.connection import create_connection

conn = create_connection()
cur = conn.cursor()

# 创建 auth schema
cur.execute("CREATE SCHEMA IF NOT EXISTS auth")

# 创建 users 表
cur.execute("""
CREATE TABLE auth.users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    qq_number TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'user',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
)
""")

# 创建 api_logs 表
cur.execute("""
CREATE TABLE auth.api_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id),
    endpoint TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
)
""")

conn.commit()
print("✅ auth schema 和表创建成功！")

conn.close()
