"""
PostgreSQL Connection Management
Provides connection pooling and helper functions for pcrdb
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# Module-level connection cache
_connection = None
_config = None


@dataclass
class Account:
    """Account data class"""
    id: int
    uid: str
    access_key: str
    viewer_id: Optional[int] = None
    name: Optional[str] = None
    arena_group: int = 0
    grand_arena_group: int = 0
    is_active: bool = True
    note: Optional[str] = None


def get_config() -> Dict[str, Any]:
    """
    Load database configuration from .env file in project root.
    Priority: OS Environment > .env > defaults
    """
    global _config
    if _config is not None:
        return _config
    
    # Load .env from project root
    project_root = Path(__file__).parent.parent.parent.parent
    env_file = project_root / '.env'
    load_dotenv(env_file)
    
    # Read config from environment
    host = os.getenv('PCRDB_HOST', 'localhost')
    port = int(os.getenv('PCRDB_PORT', '5432'))
    database = os.getenv('PCRDB_DATABASE', 'pcrdb')
    user = os.getenv('PCRDB_USER', 'postgres')
    password = os.getenv('PCRDB_PASSWORD', '')
    sync_num = int(os.getenv('PCRDB_SYNC_NUM', '10'))
    batch_size = int(os.getenv('PCRDB_BATCH_SIZE', '30'))
    access_key = os.getenv('PCRDB_ACCESS_KEY', '')

    _config = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password,
        'sync_num': sync_num,
        'batch_size': batch_size,
        'access_key': access_key
    }
    return _config



def create_connection(**kwargs):
    """
    Create a new PostgreSQL connection
    
    Args:
        **kwargs: Additional arguments passed to psycopg2.connect
    """
    config = get_config()
    # Merge default config with kwargs
    conn_args = {
        'host': config['host'],
        'port': config['port'],
        'database': config['database'],
        'user': config['user'],
        'password': config['password']
    }
    conn_args.update(kwargs)
    
    return psycopg2.connect(**conn_args)


def get_connection():
    """
    Get PostgreSQL connection (cached)
    """
    global _connection
    if _connection is not None and not _connection.closed:
        return _connection
    
    _connection = create_connection()
    return _connection


def get_cursor():
    """Get a cursor from the cached connection"""
    conn = get_connection()
    return conn.cursor()


def close_connection():
    """Close the cached connection"""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def get_accounts(active_only: bool = True) -> List[Account]:
    """
    Get all accounts from database
    
    Args:
        active_only: Only return active accounts
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if active_only:
        cursor.execute("SELECT * FROM accounts WHERE is_active = TRUE ORDER BY id")
    else:
        cursor.execute("SELECT * FROM accounts ORDER BY id")
    
    accounts = []
    for row in cursor.fetchall():
        accounts.append(Account(
            id=row[0],
            uid=row[1],
            access_key=row[2],
            viewer_id=row[3],
            name=row[4],
            arena_group=row[5] or 0,
            grand_arena_group=row[6] or 0,
            is_active=row[7],
            note=row[8]
        ))
    
    return accounts


def get_accounts_by_group(group_type: str = 'grand_arena') -> Dict[int, Account]:
    """
    Get one account per arena group
    
    Args:
        group_type: 'arena' or 'grand_arena'
        
    Returns:
        {group_id: account} - one account per group
    """
    accounts = get_accounts(active_only=True)
    result = {}
    
    for acc in accounts:
        if group_type == 'grand_arena':
            group_id = acc.grand_arena_group
        else:
            group_id = acc.arena_group
        
        if group_id > 0 and group_id not in result:
            result[group_id] = acc
    
    return result


def update_account(uid: int, **kwargs):
    """
    Update account fields
    
    Args:
        uid: Account UID
        **kwargs: Fields to update (viewer_id, name, arena_group, etc.)
    """
    if not kwargs:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    set_clauses = []
    values = []
    for key, value in kwargs.items():
        set_clauses.append(f"{key} = %s")
        values.append(value)
    
    set_clauses.append("updated_at = NOW()")
    values.append(uid)
    
    query = f"UPDATE accounts SET {', '.join(set_clauses)} WHERE uid = %s"
    cursor.execute(query, values)
    conn.commit()


def insert_snapshot(table: str, data: Dict[str, Any], collected_at: datetime = None):
    """
    Insert a snapshot record
    
    Args:
        table: Target table name
        data: Column values
        collected_at: Timestamp (default: NOW())
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if collected_at is None:
        collected_at = datetime.now()
    
    data['collected_at'] = collected_at
    
    columns = list(data.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_str = ', '.join(columns)
    
    # Get unique constraint columns for ON CONFLICT
    if table == 'clan_snapshots':
        conflict_cols = 'clan_id, collected_at'
    else:
        conflict_cols = 'viewer_id, collected_at'
    
    query = f"""
        INSERT INTO {table} ({column_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    
    cursor.execute(query, [data[col] for col in columns])
    conn.commit()


def insert_snapshots_batch(table: str, records: List[Dict[str, Any]], collected_at: datetime = None):
    """
    Batch insert snapshot records
    
    Args:
        table: Target table name
        records: List of column value dicts
        collected_at: Timestamp for all records (default: NOW())
    """
    if not records:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    if collected_at is None:
        collected_at = datetime.now()
    
    # Add collected_at to all records
    for record in records:
        record['collected_at'] = collected_at
    
    columns = list(records[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_str = ', '.join(columns)
    
    # Get unique constraint columns
    if table == 'clan_snapshots':
        conflict_cols = 'clan_id, collected_at'
    else:
        conflict_cols = 'viewer_id, collected_at'
    
    query = f"""
        INSERT INTO {table} ({column_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    
    values = [[record[col] for col in columns] for record in records]
    cursor.executemany(query, values)
    conn.commit()
