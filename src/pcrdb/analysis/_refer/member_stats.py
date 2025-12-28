"""
成员统计分析模块
提供天赋统计、公会平均值等功能
"""
import csv
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection

DATA_DIR = Path(__file__).parent.parent.parent.parent / 'data'

def query_talent_quest(levels: List[int]) -> Dict[str, Tuple[int, float]]:
    """
    统计天赋关卡通关情况 (基于最近 7 天活跃数据)
    
    Args:
        levels: 各属性关卡等级要求列表 [火, 水, 风, 光, 暗]
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    quest_names = ['火', '水', '风', '光', '暗']
    json_keys = ['clear_0', 'clear_1', 'clear_2', 'clear_3', 'clear_4']
    
    # 获取总人数 (有天赋数据的)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM player_profile_snapshots 
        WHERE talent_quest_clear IS NOT NULL
          AND collected_at > NOW() - INTERVAL '7 days'
    """)
    total = cursor.fetchone()[0]
    
    results = {}
    for i, (name, key, level) in enumerate(zip(quest_names, json_keys, levels)):
        if level > 0:
            # 查询 clear_count >= level 的人数
            query = f"""
                SELECT COUNT(*) 
                FROM player_profile_snapshots
                WHERE (talent_quest_clear->>'{key}')::int >= %s
                  AND collected_at > NOW() - INTERVAL '7 days'
            """
            cursor.execute(query, (level,))
            count = cursor.fetchone()[0]
            pct = count / total * 100 if total > 0 else 0
            results[name] = (count, pct)
            
    return results


def compute_clan_averages(output_csv: str = None) -> List[Dict]:
    """
    计算公会平均值 (基于最近快照)
    关联 player_profile_snapshots 和 player_clan_snapshots
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 复杂的聚合查询
    # 注意：需要关联 profile (详细数据) 和 clan (公会信息)
    # 并且只取最近的数据
    
    query = """
        WITH latest_profile AS (
            SELECT DISTINCT ON (viewer_id) *
            FROM player_profile_snapshots
            WHERE collected_at > NOW() - INTERVAL '7 days'
            ORDER BY viewer_id, collected_at DESC
        ),
        latest_clan AS (
            SELECT DISTINCT ON (viewer_id) *
            FROM player_clan_snapshots
            WHERE collected_at > NOW() - INTERVAL '7 days'
            ORDER BY viewer_id, collected_at DESC
        )
        SELECT 
            c.join_clan_id,
            c.join_clan_name,
            AVG(c.total_power) as avg_power,
            AVG(p.princess_knight_rank_total_exp) as avg_pk_exp,
            AVG(p.unit_num) as avg_unit_num,
            AVG((p.talent_quest_clear->>'clear_0')::int) as avg_tq0,
            AVG((p.talent_quest_clear->>'clear_1')::int) as avg_tq1,
            AVG((p.talent_quest_clear->>'clear_2')::int) as avg_tq2,
            AVG((p.talent_quest_clear->>'clear_3')::int) as avg_tq3,
            AVG((p.talent_quest_clear->>'clear_4')::int) as avg_tq4,
            COUNT(*) as member_count
        FROM latest_profile p
        JOIN latest_clan c ON p.viewer_id = c.viewer_id
        WHERE c.join_clan_id IS NOT NULL
        GROUP BY c.join_clan_id, c.join_clan_name
        ORDER BY avg_power DESC
    """
    
    cursor.execute(query)
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'clan_id': row[0],
            'clan_name': row[1],
            'avg_total_power': float(row[2] or 0),
            'avg_pk_exp': float(row[3] or 0),
            'avg_unit_num': float(row[4] or 0),
            'avg_tq': [
                float(row[5] or 0), float(row[6] or 0), float(row[7] or 0), 
                float(row[8] or 0), float(row[9] or 0)
            ],
            'member_count': row[10]
        })
    
    # 输出 CSV
    if output_csv:
        output_path = Path(output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                'clan_id', 'clan_name', 'member_count',
                'avg_pk_exp', 'avg_unit_num', 'avg_total_power',
                'avg_tq0', 'avg_tq1', 'avg_tq2', 'avg_tq3', 'avg_tq4'
            ])
            for r in results:
                writer.writerow([
                    r['clan_id'], r['clan_name'], r['member_count'],
                    f"{r['avg_pk_exp']:.2f}", f"{r['avg_unit_num']:.2f}", f"{r['avg_total_power']:.2f}",
                    *[f"{x:.2f}" for x in r['avg_tq']]
                ])
        print(f"已输出: {output_csv}")
    
    return results


# CLI 接口
def run_talent(levels: List[int] = None):
    """运行天赋统计"""
    if levels is None:
        levels = [50, 50, 50, 50, 50]
    
    results = query_talent_quest(levels)
    
    print("\n天赋关卡通关统计 (近7天)：")
    quest_names = ['火', '水', '风', '光', '暗']
    for name, (count, pct) in results.items():
        idx = quest_names.index(name)
        level = levels[idx]
        stage = (level - 1) // 10 + 1
        substage = level % 10 or 10
        print(f"  {name}{stage}-{substage}: {count} 人 ({pct:.1f}%)")


def run_averages():
    """运行公会平均值统计"""
    import time
    date_str = time.strftime("%y%m%d")
    output = DATA_DIR / 'reports' / 'member_stats' / f'clan_averages_{date_str}.csv'
    compute_clan_averages(str(output))
