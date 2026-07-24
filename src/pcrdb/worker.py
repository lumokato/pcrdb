from __future__ import annotations

import asyncio
import calendar
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import yaml

from pcrdb.clan_battle.collector import collect_tick


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")
SCHEDULER_TIMEZONE = "Asia/Shanghai"


def _day_matches(day_expression: str) -> bool:
    if not day_expression.startswith("L"):
        return True
    offset = int(day_expression.split("-", 1)[1]) if "-" in day_expression else 0
    now = datetime.now(BEIJING)
    return now.day == calendar.monthrange(now.year, now.month)[1] - offset


def _run_pcrdb_task(task_name: str, task_config: dict[str, Any]) -> None:
    day_expression = task_config.get("schedule", "* * * * *").split()[2]
    if not _day_matches(day_expression):
        return

    logger.info("Starting scheduled task %s", task_name)
    if task_name == "clan_sync":
        from pcrdb.tasks.clan_sync import run

        run()
    elif task_name == "player_profile_sync":
        from pcrdb.tasks.player_profile_sync import run

        run(mode=task_config.get("mode", "top_clans"), **task_config.get("params", {}))
    elif task_name == "player_profile_sync_monthly":
        from pcrdb.tasks.player_profile_sync import run

        run(mode="active_all")
    elif task_name == "grand_sync":
        from pcrdb.tasks.grand_sync import run

        run()
    elif task_name == "arena_deck_sync":
        from pcrdb.tasks.arena_deck_sync import run

        run()
    else:
        logger.warning("Ignoring unknown scheduled task %s", task_name)


def _cron_trigger(expression: str) -> CronTrigger:
    minute, hour, day, month, weekday = expression.split()
    if day.startswith("L"):
        day = "*"
    return CronTrigger(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=weekday,
        timezone=SCHEDULER_TIMEZONE,
    )


def _run_clan_battle(trigger_name: str) -> None:
    asyncio.run(collect_tick(trigger_name))


def _is_enabled(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _add_arena_alert_job(scheduler: BlockingScheduler) -> None:
    if not _is_enabled("ARENA_ALERT_ENABLED"):
        return

    from pcrdb.tasks.arena_alert import ArenaAlertConfig, ArenaAlertMonitor

    try:
        config = ArenaAlertConfig.from_env()
    except ValueError as exc:
        logger.error("Arena alert is enabled but its configuration is invalid: %s", exc)
        return

    monitor = ArenaAlertMonitor(config)
    scheduler.add_job(
        monitor.run,
        IntervalTrigger(seconds=config.poll_seconds, timezone=SCHEDULER_TIMEZONE),
        next_run_time=datetime.now(BEIJING),
        id="arena_alert:poll",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )


def _load_schedule() -> dict[str, Any]:
    path = Path(os.getenv("PCRDB_SCHEDULE_PATH", "/app/config/schedule.yaml"))
    if not path.exists():
        logger.warning("Schedule config does not exist: %s", path)
        return {}
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream) or {}


def main() -> None:
    scheduler = BlockingScheduler(
        timezone=SCHEDULER_TIMEZONE,
        executors={"default": ThreadPoolExecutor(max_workers=2)},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
    )

    for task_name, task_config in _load_schedule().get("tasks", {}).items():
        if not task_config.get("enabled", False):
            continue
        scheduler.add_job(
            _run_pcrdb_task,
            _cron_trigger(task_config["schedule"]),
            id=f"pcrdb:{task_name}",
            kwargs={"task_name": task_name, "task_config": task_config},
            replace_existing=True,
        )

    scheduler.add_job(
        _run_clan_battle,
        CronTrigger(minute="0,30", timezone=SCHEDULER_TIMEZONE),
        id="clan_battle:tick",
        kwargs={"trigger_name": "cron"},
        replace_existing=True,
    )

    if os.getenv("CLAN_BATTLE_RUN_ON_START", "true").lower() == "true":
        scheduler.add_job(
            _run_clan_battle,
            "date",
            run_date=datetime.now(BEIJING),
            id="clan_battle:startup",
            kwargs={"trigger_name": "startup"},
        )

    _add_arena_alert_job(scheduler)

    logger.info("PCRDB worker started with %d jobs", len(scheduler.get_jobs()))
    scheduler.start()


if __name__ == "__main__":
    main()
