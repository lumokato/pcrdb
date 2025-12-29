"""验证数据库结构"""
from src.pcrdb.db.connection import create_connection

conn = create_connection()
cur = conn.cursor()

# 查询所有表
cur.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema IN ('public', 'auth')
    ORDER BY table_schema, table_name
""")
print("=== Tables ===")
for schema, table in cur.fetchall():
    print(f"  {schema}.{table}")

# 查询 api_logs 表结构
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'auth' AND table_name = 'api_logs'
    ORDER BY ordinal_position
""")
print("\n=== auth.api_logs columns ===")
for col, dtype in cur.fetchall():
    print(f"  {col}: {dtype}")

conn.close()
