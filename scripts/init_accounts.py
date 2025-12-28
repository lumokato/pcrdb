"""
Initialize accounts table in PostgreSQL
Run once to create accounts table and optionally import from accounts.json
"""
import json
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from db.connection import get_connection


def create_accounts_table():
    """Create accounts table if not exists"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        DROP TABLE IF EXISTS accounts;
        CREATE TABLE accounts (
            id SERIAL PRIMARY KEY,
            uid TEXT NOT NULL UNIQUE,
            access_key TEXT NOT NULL,
            
            viewer_id BIGINT,
            name TEXT,
            
            arena_group SMALLINT DEFAULT 0,
            grand_arena_group SMALLINT DEFAULT 0,
            
            is_active BOOLEAN DEFAULT TRUE,
            note TEXT,
            
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    
    conn.commit()
    print("accounts table created")


def import_from_json():
    """Import accounts from config/accounts.json"""
    config_path = Path(__file__).parent.parent / 'config' / 'accounts.json'
    
    if not config_path.exists():
        print(f"accounts.json not found at {config_path}")
        return
    
    with open(config_path, encoding='utf-8') as f:
        data = json.load(f)
    
    access_key = data.get('access_key', '')
    accounts = data.get('accounts', [])
    
    conn = get_connection()
    cursor = conn.cursor()
    
    imported = 0
    for acc in accounts:
        uid = str(acc.get('uid', ''))  # uid is a string
        vid = acc.get('vid')  # viewer_id from JSON
        
        if not uid:
            continue
        
        try:
            cursor.execute("""
                INSERT INTO accounts (uid, access_key, viewer_id, is_active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (uid) DO NOTHING
            """, (uid, access_key, vid))
            imported += 1
        except Exception as e:
            print(f"Error importing {uid}: {e}")
            conn.rollback()
    
    conn.commit()
    print(f"Imported {imported} accounts from JSON")


def show_accounts():
    """Display current accounts"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, uid, viewer_id, name, arena_group, grand_arena_group, is_active FROM accounts ORDER BY id")
    rows = cursor.fetchall()
    
    print(f"\nTotal accounts: {len(rows)}")
    print("-" * 90)
    print(f"{'ID':>3} | {'UID':>25} | {'Viewer ID':>15} | {'Name':>10} | {'JJC':>3} | {'PJJC':>4} | Active")
    print("-" * 90)
    
    for row in rows:
        uid_short = str(row[1])[-10:] if row[1] else '-'  # Show last 10 chars
        print(f"{row[0]:>3} | ...{uid_short:>22} | {row[2] or '-':>15} | {row[3] or '-':>10} | {row[4]:>3} | {row[5]:>4} | {'Yes' if row[6] else 'No'}")


def main():
    create_accounts_table()
    import_from_json()
    show_accounts()


if __name__ == '__main__':
    main()

