"""
验证数据库结构与 schema.sql 的一致性

功能：
- 检查 schema.sql 中定义的表是否都存在
- 检查每个表的列是否与 schema 定义一致
- 报告缺失的表和列
"""
import sys
import re
from pathlib import Path

# 添加项目根目录到路径
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root / 'src'))

from pcrdb.db.connection import create_connection


def parse_schema_file(schema_path: Path) -> dict:
    """
    解析 schema.sql，提取表结构定义
    
    Returns:
        {
            'public.table_name': {'col1': 'type', ...},
            'auth.users': {...},
            ...
        }
    """
    content = schema_path.read_text(encoding='utf-8')
    tables = {}
    
    # 匹配 CREATE TABLE，支持 schema.table 格式
    table_pattern = re.compile(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:\w+\.)?\w+)\s*\((.*?)\);',
        re.IGNORECASE | re.DOTALL
    )
    
    for match in table_pattern.finditer(content):
        table_name = match.group(1).lower()
        columns_block = match.group(2)
        
        # 确定 schema
        if '.' in table_name:
            full_name = table_name
        else:
            full_name = f'public.{table_name}'
        
        # 解析列
        columns = {}
        for line in columns_block.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            
            # 跳过纯约束行
            upper_line = line.upper()
            if upper_line.startswith(('UNIQUE', 'PRIMARY KEY', 'FOREIGN', 'CONSTRAINT', 'CHECK')):
                continue
            
            # 提取列名和类型
            # 匹配: col_name TYPE [其他修饰符]
            col_match = re.match(r'^(\w+)\s+(\w+)', line)
            if col_match:
                col_name = col_match.group(1).lower()
                col_type = col_match.group(2).lower()
                
                # 标准化类型名（映射到 PostgreSQL information_schema 返回的类型）
                type_map = {
                    'serial': 'integer',
                    'bigserial': 'bigint',
                    'timestamptz': 'timestamp with time zone',
                    'int': 'integer',
                    'int4': 'integer',
                    'int2': 'smallint',
                    'int8': 'bigint',
                    'bool': 'boolean',
                    'varchar': 'character varying',
                }
                col_type = type_map.get(col_type, col_type)
                columns[col_name] = col_type
        
        tables[full_name] = columns
    
    return tables


def get_db_structure(conn) -> dict:
    """从数据库获取实际表结构"""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            table_schema || '.' || table_name as full_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema IN ('public', 'auth')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    
    tables = {}
    for full_name, col_name, data_type in cur.fetchall():
        if full_name not in tables:
            tables[full_name] = {}
        tables[full_name][col_name.lower()] = data_type.lower()
    
    return tables


def types_compatible(expected: str, actual: str) -> bool:
    """检查类型是否兼容"""
    expected = expected.lower().strip()
    actual = actual.lower().strip()
    
    if expected == actual:
        return True
    
    # 兼容映射
    compat_groups = [
        ['integer', 'int', 'int4'],
        ['smallint', 'int2'],
        ['bigint', 'int8'],
        ['text', 'character varying', 'varchar'],
        ['boolean', 'bool'],
        ['timestamp with time zone', 'timestamptz'],
        ['timestamp without time zone', 'timestamp'],
    ]
    
    for group in compat_groups:
        if expected in group and actual in group:
            return True
    
    return False


def compare_structures(schema_tables: dict, db_tables: dict) -> list:
    """对比 schema 定义与数据库实际结构"""
    issues = []
    
    # 检查缺失的表
    for table in schema_tables:
        if table not in db_tables:
            issues.append(('missing_table', table, None, None))
    
    # 检查多余的表
    for table in db_tables:
        if table not in schema_tables:
            issues.append(('extra_table', table, None, None))
    
    # 检查每个表的列
    for table, schema_cols in schema_tables.items():
        if table not in db_tables:
            continue
        
        db_cols = db_tables[table]
        
        # 缺失的列
        for col, expected_type in schema_cols.items():
            if col not in db_cols:
                issues.append(('missing_col', table, col, expected_type))
            else:
                actual_type = db_cols[col]
                if not types_compatible(expected_type, actual_type):
                    issues.append(('type_mismatch', table, col, f'{expected_type} vs {actual_type}'))
        
        # 多余的列
        for col in db_cols:
            if col not in schema_cols:
                issues.append(('extra_col', table, col, db_cols[col]))
    
    return issues


def main():
    schema_path = project_root / 'src' / 'pcrdb' / 'db' / 'schema.sql'
    
    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        return 1
    
    print("Verifying database structure...")
    print(f"Schema file: {schema_path.relative_to(project_root)}")
    print()
    
    # 解析 schema
    schema_tables = parse_schema_file(schema_path)
    print(f"Schema defines {len(schema_tables)} tables:")
    for t in sorted(schema_tables.keys()):
        cols = schema_tables[t]
        print(f"  {t}: {len(cols)} columns")
        for c, ty in cols.items():
            print(f"      {c}: {ty}")
    print()
    
    # 获取数据库结构
    conn = create_connection()
    db_tables = get_db_structure(conn)
    conn.close()
    
    print(f"Database has {len(db_tables)} tables")
    print()
    
    # 对比
    issues = compare_structures(schema_tables, db_tables)
    
    if not issues:
        print("OK: Database structure matches schema.sql")
        return 0
    else:
        print(f"Found {len(issues)} differences:")
        for issue_type, table, col, detail in issues:
            if issue_type == 'missing_table':
                print(f"  [MISSING TABLE] {table}")
            elif issue_type == 'extra_table':
                print(f"  [EXTRA TABLE] {table} (not in schema)")
            elif issue_type == 'missing_col':
                print(f"  [MISSING COLUMN] {table}.{col} ({detail})")
            elif issue_type == 'extra_col':
                print(f"  [EXTRA COLUMN] {table}.{col} ({detail})")
            elif issue_type == 'type_mismatch':
                print(f"  [TYPE MISMATCH] {table}.{col}: {detail}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
