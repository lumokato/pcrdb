"""
任务日志工具
记录定时任务的执行状态和统计信息
"""
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import json

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

from .connection import get_connection


# 任务对应的数据表映射
TASK_TABLES = {
    'clan_sync': ['clan_snapshots', 'player_clan_snapshots'],
    'player_profile_sync': ['player_profile_snapshots'],
    'player_profile_sync_monthly': ['player_profile_snapshots'],
    'grand_sync': ['grand_arena_snapshots'],
    'arena_deck_sync': ['arena_deck_snapshots'],
}


class TaskLogger:
    """
    任务日志记录器
    
    使用方式:
        logger = TaskLogger('clan_sync')
        logger.start(records_fetched=100, details={'mode': 'active'})
        try:
            # 执行任务...
            logger.finish_success()
        except Exception as e:
            logger.finish_failed(str(e))
    """
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.start_time: Optional[datetime] = None
        self.records_expected: int = 0  # 预计获取数
        self.records_fetched: int = 0   # 实际获取数（在finish时传入）
        self.details: Optional[Dict] = None
        self.initial_counts: Dict[str, int] = {}
    
    def _get_table_count(self, table_name: str) -> int:
        """获取表的当前记录数"""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def _snapshot_counts(self) -> Dict[str, int]:
        """获取任务相关表的记录数快照"""
        tables = TASK_TABLES.get(self.task_name, [])
        counts = {}
        for table in tables:
            try:
                counts[table] = self._get_table_count(table)
            except Exception:
                counts[table] = 0
        return counts
    
    def start(self, records_expected: int = 0, details: Optional[Dict] = None):
        """
        开始记录任务
        
        Args:
            records_expected: 预计获取的记录数
            details: 额外详情
        """
        self.start_time = datetime.now(BEIJING_TZ)
        self.records_expected = records_expected
        self.details = details
        self.initial_counts = self._snapshot_counts()
    
    def _calculate_saved(self) -> int:
        """计算实际保存的记录数（数据库增量）"""
        current_counts = self._snapshot_counts()
        total_saved = 0
        for table, initial in self.initial_counts.items():
            current = current_counts.get(table, initial)
            delta = current - initial
            if delta > 0:
                total_saved += delta
        return total_saved
    
    def _save_log(self, status: str, records_fetched: int = 0, error_message: Optional[str] = None):
        """保存日志到数据库"""
        if not self.start_time:
            return
            
        finished_at = datetime.now(BEIJING_TZ)
        duration = (finished_at - self.start_time).total_seconds()
        records_saved = self._calculate_saved()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        sql = """
            INSERT INTO task_logs 
            (task_name, started_at, finished_at, duration_seconds, status, 
             records_expected, records_fetched, records_saved, error_message, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            self.task_name,
            self.start_time,
            finished_at,
            round(duration, 2),
            status,
            self.records_expected,
            records_fetched,
            records_saved,
            error_message,
            json.dumps(self.details) if self.details else None
        ))
        
        conn.commit()
        cursor.close()
    
    def finish_success(self, records_fetched: int = 0):
        """
        标记任务成功完成
        
        Args:
            records_fetched: API实际返回的有效记录数
        """
        self._save_log('success', records_fetched)
    
    def finish_failed(self, error_message: str, records_fetched: int = 0):
        """标记任务失败"""
        self._save_log('failed', records_fetched, error_message)


def get_recent_logs(limit: int = 50, task_name: Optional[str] = None) -> List[Dict]:
    """
    获取最近的任务日志
    
    Args:
        limit: 返回数量
        task_name: 可选，筛选特定任务
    
    Returns:
        日志列表
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if task_name:
        sql = """
            SELECT id, task_name, started_at, finished_at, duration_seconds,
                   status, records_expected, records_fetched, records_saved, error_message, details
            FROM task_logs
            WHERE task_name = %s
            ORDER BY started_at DESC
            LIMIT %s
        """
        cursor.execute(sql, (task_name, limit))
    else:
        sql = """
            SELECT id, task_name, started_at, finished_at, duration_seconds,
                   status, records_expected, records_fetched, records_saved, error_message, details
            FROM task_logs
            ORDER BY started_at DESC
            LIMIT %s
        """
        cursor.execute(sql, (limit,))
    
    rows = cursor.fetchall()
    cursor.close()
    
    logs = []
    for row in rows:
        logs.append({
            'id': row[0],
            'task_name': row[1],
            'started_at': row[2].isoformat() if row[2] else None,
            'finished_at': row[3].isoformat() if row[3] else None,
            'duration_seconds': float(row[4]) if row[4] else 0,
            'status': row[5],
            'records_expected': row[6] or 0,
            'records_fetched': row[7] or 0,
            'records_saved': row[8] or 0,
            'error_message': row[9],
            'details': row[10]
        })
    
    return logs
