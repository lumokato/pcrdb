"""
任务调度器
读取 config/schedule.yaml 并按时执行各个同步任务
"""
import os
import yaml
import time
import schedule
from datetime import datetime
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_schedule_config():
    """加载调度配置"""
    config_path = Path(__file__).parent / 'config' / 'schedule.yaml'
    
    if not config_path.exists():
        logger.error(f"配置文件不存在: {config_path}")
        return None
    
    with open(config_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def run_task(task_name: str, task_config: dict):
    """运行指定任务"""
    logger.info(f"开始执行任务: {task_name}")
    start_time = time.time()
    
    try:
        # 导入对应的任务模块
        if task_name == 'clan_sync':
            from src.pcrdb.tasks.clan_sync import run
            run()
        
        elif task_name == 'player_profile_sync':
            from src.pcrdb.tasks.player_profile_sync import run
            mode = task_config.get('mode', 'top_clans')
            params = task_config.get('params', {})
            run(mode=mode, **params)
        
        elif task_name == 'player_profile_sync_monthly':
            from src.pcrdb.tasks.player_profile_sync import run
            run(mode='active_all')
        
        elif task_name == 'grand_sync':
            from src.pcrdb.tasks.grand_sync import run
            run()
        
        elif task_name == 'arena_deck_sync':
            from src.pcrdb.tasks.arena_deck_sync import run
            run()
        
        else:
            logger.warning(f"未知任务: {task_name}")
            return
        
        elapsed = time.time() - start_time
        logger.info(f"任务 {task_name} 完成，耗时 {elapsed:.2f} 秒")
    
    except Exception as e:
        logger.error(f"任务 {task_name} 执行失败: {e}", exc_info=True)


def setup_schedules(config: dict):
    """
    根据配置设置任务调度
    """
    tasks = config.get('tasks', {})
    
    for task_name, task_config in tasks.items():
        if not task_config.get('enabled', False):
            logger.info(f"任务 {task_name} 已禁用，跳过")
            continue
        
        cron_expr = task_config.get('schedule', '')
        description = task_config.get('description', '')
        
        if not cron_expr:
            logger.warning(f"任务 {task_name} 没有配置 schedule，跳过")
            continue
        
        # 解析 cron 表达式 "分 时 日 月 星期"
        parts = cron_expr.split()
        if len(parts) != 5:
            logger.error(f"任务 {task_name} 的 cron 表达式格式错误: {cron_expr}")
            continue
        
        minute, hour, day_of_month, month, day_of_week = parts
        
        # 构建调度job
        # schedule 库不直接支持完整cron，需要转换
        
        # 简化处理：支持常见模式
        if day_of_month == '*' and month == '*' and day_of_week == '*':
            # 每天执行
            time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
            schedule.every().day.at(time_str).do(
                run_task, task_name=task_name, task_config=task_config
            )
            logger.info(f"已调度任务: {task_name} 每天 {time_str} - {description}")
        
        elif day_of_month != '*' and month == '*' and day_of_week == '*':
            # 每月特定日期执行
            time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
            
            def monthly_job():
                """月度任务包装"""
                current_day = datetime.now().day
                if str(current_day) == day_of_month:
                    run_task(task_name, task_config)
            
            schedule.every().day.at(time_str).do(monthly_job)
            logger.info(f"已调度任务: {task_name} 每月{day_of_month}日 {time_str} - {description}")
        
        else:
            logger.warning(f"任务 {task_name} 使用了复杂的 cron 表达式，暂不支持: {cron_expr}")


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("pcrdb 任务调度器启动")
    logger.info("=" * 60)
    
    config = load_schedule_config()
    if not config:
        logger.error("无法加载配置文件，退出")
        return
    
    # 设置时区（如果需要）
    # scheduler_config = config.get('scheduler', {})
    # tz = scheduler_config.get('timezone', 'Asia/Shanghai')
    
    # 设置所有调度
    setup_schedules(config)
    
    logger.info("调度器已准备就绪，等待任务执行...")
    logger.info("按 Ctrl+C 停止调度器")
    
    # 主循环
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("收到停止信号，调度器退出")


if __name__ == '__main__':
    main()
