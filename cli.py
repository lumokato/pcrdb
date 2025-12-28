#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pcrdb 命令行入口
公主连结渠道服数据采集系统
"""
import argparse
import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))


def cmd_task(args):
    """运行采集任务"""
    from pcrdb.tasks import clan_sync, grand_sync, arena_deck_sync, player_profile_sync
    
    task_map = {
        'clan_sync': clan_sync.run,
        'grand_sync': grand_sync.run,
        'arena_deck_sync': arena_deck_sync.run,
        'player_profile_sync': player_profile_sync.run,
    }
    
    if args.task_name not in task_map:
        print(f"未知任务: {args.task_name}")
        print(f"可用任务: {list(task_map.keys())}")
        return 1
    
    # 解析参数
    kwargs = {}
    if args.args:
        for arg in args.args:
            if '=' in arg:
                k, v = arg.split('=', 1)
                kwargs[k] = int(v) if v.isdigit() else v
    
    print(f"运行任务: {args.task_name}")
    task_map[args.task_name](**kwargs)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='pcrdb - 公主连结渠道服数据采集系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
可用任务:
  clan_sync           同步公会数据
  grand_sync          同步PJJC排名数据
  arena_deck_sync     同步JJC防守阵容
  player_profile_sync 同步玩家档案

示例:
  python cli.py task clan_sync
  python cli.py task player_profile_sync --args mode=top_clans rank_limit=30
"""
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # task 命令
    task_parser = subparsers.add_parser('task', help='运行采集任务')
    task_parser.add_argument('task_name', help='任务名称')
    task_parser.add_argument('--args', nargs='*', help='任务参数 (key=value)')
    task_parser.set_defaults(func=cmd_task)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 0
    
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
