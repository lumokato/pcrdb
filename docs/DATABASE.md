# 数据库管理

本文档描述如何管理和维护 pcrdb 的 PostgreSQL 数据库。

## 数据库配置

数据库连接通过 `.env` 文件配置，参考 [.env.example](../.env.example)。

## 管理脚本

所有数据库管理脚本位于 `scripts/` 目录。

### 执行 Schema

将 `src/pcrdb/db/schema.sql` 应用到数据库。已存在的表/索引会自动跳过。

```bash
python scripts/apply_schema.py
```

**适用场景：**
- 初始化新数据库
- 添加新表后更新数据库结构

### 验证数据库结构

检查数据库结构与 `schema.sql` 的一致性，报告差异。

```bash
python scripts/verify_db.py
```

**输出示例：**
```
OK: Database structure matches schema.sql
```

或：
```
Found 2 differences:
  [MISSING COLUMN] auth.api_logs.query_params (jsonb)
  [TYPE MISMATCH] auth.users.created_at: timestamptz vs timestamp
```

### 数据库优化

执行 VACUUM FULL 压缩表空间。

```bash
python scripts/vacuum_db.py
```

## 常用修复命令

### 通过 Python 执行 SQL

无需 psql 客户端，直接用 Python 执行 SQL：

```python
python -c "
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv(Path('.env'))
conn = psycopg2.connect(
    host=os.getenv('PCRDB_HOST'),
    port=int(os.getenv('PCRDB_PORT')),
    database=os.getenv('PCRDB_DATABASE'),
    user=os.getenv('PCRDB_USER'),
    password=os.getenv('PCRDB_PASSWORD')
)
with conn.cursor() as cur:
    cur.execute('YOUR SQL HERE;')
conn.commit()
conn.close()
print('Done')
"
```

### 常见修复示例

```sql
-- 添加缺失的列
ALTER TABLE auth.api_logs ADD COLUMN IF NOT EXISTS query_params JSONB;

-- 修改列类型
ALTER TABLE auth.users ALTER COLUMN created_at TYPE TIMESTAMPTZ;

-- 删除列
ALTER TABLE arena_deck_snapshots DROP COLUMN IF EXISTS user_name;

-- 重命名列
ALTER TABLE auth.api_logs RENAME COLUMN access_time TO created_at;
```

## Docker 环境

如果 PostgreSQL 运行在 Docker 中：

```bash
# 通过 Docker 执行 SQL
docker exec -it <容器名> psql -U postgres -d pcrdb -c "YOUR SQL;"

# 执行 SQL 文件
docker cp src/pcrdb/db/schema.sql <容器名>:/tmp/schema.sql
docker exec -it <容器名> psql -U postgres -d pcrdb -f /tmp/schema.sql
```

## Schema 版本

当前 Schema 版本: **2.1** (2025-12-28)

表结构定义: `src/pcrdb/db/schema.sql`
