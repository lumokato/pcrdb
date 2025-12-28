import psycopg2
import yaml
import os

def get_db_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database.yaml')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config['postgresql']

def vacuum_db():
    print("Starting VACUUM FULL to reclaim disk space...")
    try:
        db_config = get_db_config()
        # Connect with autocommit=True because VACUUM cannot run inside a transaction block
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        
        cursor = conn.cursor()
        
        tables = [
            'clan_snapshots',
            'player_clan_snapshots',
            'player_profile_snapshots',
            'grand_arena_snapshots',
            'arena_deck_snapshots'
        ]
        
        for table in tables:
            print(f"Vacuuming {table}...")
            cursor.execute(f"VACUUM FULL {table};")
            print(f"✓ {table} compacted.")
            
        cursor.close()
        conn.close()
        print("\n✅ Database optimization completed.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    vacuum_db()
