from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.models import CalendarBlock, Task, UserProfile

DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
PRIORITY_SCORE = {"critical": 4, "high": 3, "medium": 2, "low": 1}


def _to_minutes(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def _parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def _coerce_timezone(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _day_work_minutes(profile: UserProfile, target_date: date) -> tuple[int, datetime | None, datetime | None]:
    tz = ZoneInfo(profile.timezone)
    day_key = DAY_KEYS[target_date.weekday()]

    days = profile.working_hours.get("days", [])
    setting = next((day for day in days if day.get("day") == day_key), None)
    if setting is None:
        return 0, None, None

    start_h, start_m = _parse_hhmm(setting.get("start", "09:00"))
    end_h, end_m = _parse_hhmm(setting.get("end", "18:00"))

    start_dt = datetime(target_date.year, target_date.month, target_date.day, start_h, start_m, tzinfo=tz)
    end_dt = datetime(target_date.year, target_date.month, target_date.day, end_h, end_m, tzinfo=tz)

    total = _to_minutes(start_dt, end_dt)

    lunch = profile.working_hours.get("lunch") or {}
    if lunch.get("start") and lunch.get("end"):
        lh, lm = _parse_hhmm(lunch["start"])
        eh, em = _parse_hhmm(lunch["end"])
        lunch_start = datetime(target_date.year, target_date.month, target_date.day, lh, lm, tzinfo=tz)
        lunch_end = datetime(target_date.year, target_date.month, target_date.day, eh, em, tzinfo=tz)
        total -= max(0, _to_minutes(lunch_start, lunch_end))

    return max(0, total), start_dt, end_dt


def _find_first_free_slot(start_dt: datetime, end_dt: datetime, busy: list[tuple[datetime, datetime]]) -> tuple[datetime, datetime] | None:
    cursor = start_dt
    for b_start, b_end in sorted(busy, key=lambda x: x[0]):
        if b_end <= cursor:
            continue
        if b_start > cursor:
            slot_end = min(b_start, cursor + timedelta(minutes=90))
            if slot_end > cursor:
                return cursor, slot_end
        cursor = max(cursor, b_end)
    if cursor < end_dt:
        return cursor, min(end_dt, cursor + timedelta(minutes=90))
    return None


def build_daily_briefing(db: Session, profile: UserProfile, target_date: date) -> dict:
    tz = ZoneInfo(profile.timezone)
    day_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, tzinfo=tz)
    day_end = day_start + timedelta(days=1)

    tasks_stmt = select(Task).where(Task.status.in_(["todo", "in_progress"]))
    tasks = db.execute(tasks_stmt).scalars().all()

    def task_sort_key(task: Task) -> tuple[int, datetime]:
        due = _coerce_timezone(task.due, tz) if task.due else datetime.max.replace(tzinfo=tz)
        return (-PRIORITY_SCORE.get(task.priority, 1), due)

    tasks = sorted(tasks, key=task_sort_key)

    blocks_stmt = select(CalendarBlock).where(
        and_(
            CalendarBlock.start < day_end,
            CalendarBlock.end > day_start,
        )
    )
    blocks = db.execute(blocks_stmt).scalars().all()

    busy_ranges = []
    for block in blocks:
        start = _coerce_timezone(block.start, tz)
        end = _coerce_timezone(block.end, tz)
        busy_ranges.append((max(start, day_start), min(end, day_end)))
    busy_minutes = sum(max(0, _to_minutes(start, end)) for start, end in busy_ranges)
    focus_minutes = sum(
        max(0, _to_minutes(_coerce_timezone(block.start, tz), _coerce_timezone(block.end, tz)))
        for block in blocks
        if block.type in ("focus_block", "task_block")
    )
    meeting_minutes = sum(
        max(0, _to_minutes(_coerce_timezone(block.start, tz), _coerce_timezone(block.end, tz)))
        for block in blocks
        if block.type == "other" and block.source == "external"
    )

    work_minutes, work_start, work_end = _day_work_minutes(profile, target_date)
    free_minutes = max(0, work_minutes - busy_minutes)

    top_tasks = []
    for task in tasks[:5]:
        free_slot = _find_first_free_slot(work_start, work_end, busy_ranges) if work_start and work_end else None
        top_tasks.append(
            {
                "task_id": task.id,
                "title": task.title,
                "reason": f"우선순위={task.priority}, 예상소요={task.effort_minutes}분",
                "recommended_block": (
                    {
                        "start": free_slot[0],
                        "end": free_slot[1],
                    }
                    if free_slot
                    else None
                ),
            }
        )

    risks: list[str] = []
    overdue = [task for task in tasks if task.due and _coerce_timezone(task.due, tz) < day_start]
    if overdue:
        risks.append(f"기한 경과 작업 {len(overdue)}건")

    due_today = [task for task in tasks if task.due and day_start <= _coerce_timezone(task.due, tz) < day_end]
    if len(due_today) >= 3:
        risks.append("오늘 마감 작업이 3건 이상입니다")

    if free_minutes < 120:
        risks.append("가용 집중 시간이 2시간 미만입니다")

    reminders = []
    for task in due_today[:3]:
        reminders.append(f"{task.title} 마감이 오늘입니다")

    return {
        "date": target_date,
        "top_tasks": top_tasks,
        "risks": risks,
        "reminders": reminders,
        "snapshot": {
            "meeting_minutes": meeting_minutes,
            "focus_minutes": focus_minutes,
            "free_minutes": free_minutes,
        },
    }
