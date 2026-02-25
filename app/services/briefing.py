from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.models import ApprovalRequest, CalendarBlock, SyncStatus, Task, UserProfile

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


def _merge_ranges(ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not ranges:
        return []
    rows = sorted(ranges, key=lambda item: item[0])
    merged: list[tuple[datetime, datetime]] = [rows[0]]
    for start, end in rows[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def _free_minutes_between(
    window_start: datetime,
    window_end: datetime,
    busy_ranges: list[tuple[datetime, datetime]],
) -> int:
    if window_end <= window_start:
        return 0
    total_minutes = _to_minutes(window_start, window_end)
    clipped = []
    for start, end in busy_ranges:
        overlap_start = max(start, window_start)
        overlap_end = min(end, window_end)
        if overlap_end > overlap_start:
            clipped.append((overlap_start, overlap_end))
    busy_minutes = sum(_to_minutes(start, end) for start, end in _merge_ranges(clipped))
    return max(0, total_minutes - busy_minutes)


def _minutes_overlap(
    first_start: datetime,
    first_end: datetime,
    second_start: datetime,
    second_end: datetime,
) -> int:
    overlap_start = max(first_start, second_start)
    overlap_end = min(first_end, second_end)
    if overlap_end <= overlap_start:
        return 0
    return _to_minutes(overlap_start, overlap_end)


def _day_window(profile: UserProfile, target_date: date) -> tuple[datetime | None, datetime | None]:
    _, start_dt, end_dt = _day_work_minutes(profile, target_date)
    return start_dt, end_dt


def _weekly_workload_snapshot(
    profile: UserProfile,
    blocks: list[CalendarBlock],
    tz: ZoneInfo,
    target_date: date,
) -> dict[str, int]:
    week_start = target_date - timedelta(days=target_date.weekday())
    week_days = [week_start + timedelta(days=offset) for offset in range(7)]
    windows = {day: _day_window(profile, day) for day in week_days}

    meeting_minutes_week = 0
    overtime_minutes_week = 0
    fragmented_focus_blocks = 0

    for block in blocks:
        start = _coerce_timezone(block.start, tz)
        end = _coerce_timezone(block.end, tz)
        day_key = start.date()
        if day_key < week_start or day_key > week_start + timedelta(days=6):
            continue

        duration = max(0, _to_minutes(start, end))
        if block.type == "other" and block.source == "external":
            meeting_minutes_week += duration

        if block.type in ("focus_block", "task_block") and duration < 45:
            fragmented_focus_blocks += 1

        work_start, work_end = windows.get(day_key, (None, None))
        if work_start is None or work_end is None:
            overtime_minutes_week += duration
            continue

        in_hours = _minutes_overlap(start, end, work_start, work_end)
        overtime_minutes_week += max(0, duration - in_hours)

    return {
        "meeting_minutes_week": meeting_minutes_week,
        "overtime_minutes_week": overtime_minutes_week,
        "fragmented_focus_blocks": fragmented_focus_blocks,
    }


def _task_due_reminders(tasks: list[Task], tz: ZoneInfo, base_day: date, reminder_days: list[int]) -> list[str]:
    if not reminder_days:
        reminder_days = [2, 1, 0]
    normalized_days = sorted({max(0, int(day)) for day in reminder_days})
    reminders: list[str] = []

    for days_left in normalized_days:
        from_day = datetime(base_day.year, base_day.month, base_day.day, 0, 0, tzinfo=tz) + timedelta(days=days_left)
        to_day = from_day + timedelta(days=1)
        due_rows = [
            task
            for task in tasks
            if task.due and from_day <= _coerce_timezone(task.due, tz) < to_day
        ][:3]
        for task in due_rows:
            if days_left == 0:
                reminders.append(f"{task.title} 마감이 오늘입니다")
            else:
                reminders.append(f"{task.title} 마감 {days_left}일 전입니다")

    return reminders


def _approval_pending_reminders(db: Session, tz: ZoneInfo, now: datetime) -> list[str]:
    cutoff = now - timedelta(hours=2)
    rows = db.execute(
        select(ApprovalRequest).where(
            ApprovalRequest.status == "pending",
            ApprovalRequest.created_at <= cutoff,
        )
    ).scalars().all()
    reminders: list[str] = []
    for row in rows[:5]:
        requested_at = _coerce_timezone(row.created_at, tz).strftime("%H:%M")
        reminders.append(f"승인 요청 {row.id[:8]}... 이 {requested_at}부터 대기 중입니다")
    return reminders


def _block_start_reminders(
    blocks: list[CalendarBlock],
    tz: ZoneInfo,
    now: datetime,
    lead_minutes: int,
) -> list[str]:
    if lead_minutes <= 0:
        return []
    window_end = now + timedelta(minutes=lead_minutes)
    rows = sorted(
        [
            block
            for block in blocks
            if _coerce_timezone(block.start, tz) >= now and _coerce_timezone(block.start, tz) <= window_end
        ],
        key=lambda value: _coerce_timezone(value.start, tz),
    )
    reminders: list[str] = []
    for block in rows[:3]:
        start_local = _coerce_timezone(block.start, tz)
        left = int(max(0, (start_local - now).total_seconds() // 60))
        reminders.append(f"{block.title} 일정이 {left}분 후 시작됩니다")
    return reminders


def _format_duration(minutes: int) -> str:
    value = max(0, int(minutes))
    if value < 60:
        return f"{value}분"
    hours, remain = divmod(value, 60)
    if remain == 0:
        return f"{hours}시간"
    return f"{hours}시간 {remain}분"


def _near_due_task_reminders(tasks: list[Task], tz: ZoneInfo, now: datetime, horizon_minutes: int = 180) -> list[str]:
    if horizon_minutes <= 0:
        return []
    window_end = now + timedelta(minutes=horizon_minutes)
    rows = sorted(
        [
            task
            for task in tasks
            if task.due and now <= _coerce_timezone(task.due, tz) <= window_end
        ],
        key=lambda item: _coerce_timezone(item.due, tz),  # type: ignore[arg-type]
    )
    reminders: list[str] = []
    for task in rows[:3]:
        due = _coerce_timezone(task.due, tz)  # type: ignore[arg-type]
        left_minutes = int(max(0, (due - now).total_seconds() // 60))
        reminders.append(f"{task.title} 마감까지 {_format_duration(left_minutes)} 남았습니다")
    return reminders


def _active_block_reminders(blocks: list[CalendarBlock], tz: ZoneInfo, now: datetime) -> list[str]:
    rows = sorted(
        [
            block
            for block in blocks
            if _coerce_timezone(block.start, tz) <= now < _coerce_timezone(block.end, tz)
        ],
        key=lambda item: _coerce_timezone(item.end, tz),
    )
    reminders: list[str] = []
    for block in rows[:2]:
        end_local = _coerce_timezone(block.end, tz)
        left_minutes = int(max(0, (end_local - now).total_seconds() // 60))
        reminders.append(f"현재 {block.title} 일정 진행 중 · {_format_duration(left_minutes)} 남음")
    return reminders


def _count_overlapping_blocks(blocks: list[CalendarBlock], tz: ZoneInfo, day_start: datetime, day_end: datetime) -> int:
    rows = sorted(
        [
            (max(_coerce_timezone(block.start, tz), day_start), min(_coerce_timezone(block.end, tz), day_end))
            for block in blocks
            if _coerce_timezone(block.start, tz) < day_end and _coerce_timezone(block.end, tz) > day_start
        ],
        key=lambda item: item[0],
    )
    overlap_count = 0
    current_end: datetime | None = None
    for start, end in rows:
        if current_end is not None and start < current_end:
            overlap_count += 1
        if current_end is None or end > current_end:
            current_end = end
    return overlap_count


def _dedupe_messages(messages: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in messages:
        value = (item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
        if len(output) >= limit:
            break
    return output


def build_daily_briefing(db: Session, profile: UserProfile, target_date: date) -> dict:
    tz = ZoneInfo(profile.timezone)
    now_local = datetime.now(tz)
    is_today = target_date == now_local.date()
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

    week_start = target_date - timedelta(days=target_date.weekday())
    week_day_start = datetime(week_start.year, week_start.month, week_start.day, 0, 0, tzinfo=tz)
    week_day_end = week_day_start + timedelta(days=7)
    week_blocks_stmt = select(CalendarBlock).where(
        and_(
            CalendarBlock.start < week_day_end,
            CalendarBlock.end > week_day_start,
        )
    )
    week_blocks = db.execute(week_blocks_stmt).scalars().all()

    busy_ranges = []
    for block in blocks:
        start = _coerce_timezone(block.start, tz)
        end = _coerce_timezone(block.end, tz)
        busy_ranges.append((max(start, day_start), min(end, day_end)))
    merged_busy_ranges = _merge_ranges(busy_ranges)
    busy_minutes = sum(max(0, _to_minutes(start, end)) for start, end in merged_busy_ranges)
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
    free_minutes_remaining = free_minutes
    if is_today and work_start and work_end:
        window_start = max(now_local, work_start)
        free_minutes_remaining = _free_minutes_between(window_start, work_end, merged_busy_ranges)

    top_tasks = []
    for task in tasks[:5]:
        free_slot = _find_first_free_slot(work_start, work_end, merged_busy_ranges) if work_start and work_end else None
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
    overdue_cutoff = now_local if is_today else day_start
    overdue = [task for task in tasks if task.due and _coerce_timezone(task.due, tz) < overdue_cutoff]
    if overdue:
        risks.append(f"기한 경과 작업 {len(overdue)}건")

    due_today = [task for task in tasks if task.due and day_start <= _coerce_timezone(task.due, tz) < day_end]
    if is_today:
        due_remaining_today = [task for task in due_today if _coerce_timezone(task.due, tz) >= now_local]
        if len(due_remaining_today) >= 3:
            risks.append("오늘 남은 마감 작업이 3건 이상입니다")
        due_next_two_hours = [
            task for task in tasks if task.due and now_local <= _coerce_timezone(task.due, tz) <= now_local + timedelta(hours=2)
        ]
        if due_next_two_hours:
            risks.append(f"2시간 내 마감 작업 {len(due_next_two_hours)}건")
    elif len(due_today) >= 3:
        risks.append("오늘 마감 작업이 3건 이상입니다")

    if is_today:
        if free_minutes_remaining < 60:
            risks.append("남은 가용 집중 시간이 1시간 미만입니다")
        elif free_minutes_remaining < 120:
            risks.append("남은 가용 집중 시간이 2시간 미만입니다")
    elif free_minutes < 120:
        risks.append("가용 집중 시간이 2시간 미만입니다")

    weekly = _weekly_workload_snapshot(profile, week_blocks, tz, target_date)
    if weekly["meeting_minutes_week"] >= 900:
        risks.append("주간 회의 시간이 15시간 이상으로 과다합니다")
    if weekly["overtime_minutes_week"] >= 120:
        risks.append("야근/근무시간 외 일정이 2시간 이상입니다")
    if weekly["fragmented_focus_blocks"] >= 3:
        risks.append("집중 블록이 짧게 쪼개져 있어 몰입도가 떨어질 수 있습니다")

    overlap_count = _count_overlapping_blocks(blocks, tz, day_start, day_end)
    if overlap_count > 0:
        risks.append(f"오늘 일정 겹침 {overlap_count}건이 있습니다")

    sync_row = db.get(SyncStatus, 1)
    sync_delay_minutes = 0
    if sync_row and is_today and sync_row.graph_connected:
        if sync_row.last_delta_sync_at is None:
            risks.append("Outlook 동기화 이력이 없어 반영 지연 가능성이 있습니다")
        else:
            last_delta = _coerce_timezone(sync_row.last_delta_sync_at, tz)
            sync_delay_minutes = int(max(0, (now_local - last_delta).total_seconds() // 60))
            if sync_delay_minutes >= 15:
                risks.append(f"Outlook 동기화가 {sync_delay_minutes}분 지연되었습니다")
        if int(sync_row.recent_429_count or 0) >= 3:
            risks.append(f"Graph 요청 제한(429) {sync_row.recent_429_count}회로 반영이 늦을 수 있습니다")

    risks = _dedupe_messages(risks, limit=8)

    pref = profile.preferences or {}
    notify_pref = pref.get("notification_preferences") or {}
    due_reminder_days = notify_pref.get("due_reminders") or [2, 1, 0]
    block_start_lead = int(notify_pref.get("block_start_reminder_minutes") or 10)

    reminders = _task_due_reminders(tasks, tz, target_date, due_reminder_days)
    if is_today:
        reminders.extend(_near_due_task_reminders(tasks, tz, now_local, horizon_minutes=180))
        reminders.extend(_active_block_reminders(blocks, tz, now_local))
        reminders.extend(_block_start_reminders(blocks, tz, now_local, block_start_lead))
        if sync_delay_minutes >= 15:
            reminders.append(f"동기화가 {_format_duration(sync_delay_minutes)} 지연되었습니다. 필요 시 수동 동기화를 실행하세요")
    reminders.extend(_approval_pending_reminders(db, tz, now_local))
    reminders = _dedupe_messages(reminders, limit=10)

    return {
        "date": target_date,
        "top_tasks": top_tasks,
        "risks": risks,
        "reminders": reminders,
        "snapshot": {
            "meeting_minutes": meeting_minutes,
            "focus_minutes": focus_minutes,
            "free_minutes": free_minutes,
            "free_minutes_remaining": free_minutes_remaining,
            "meeting_minutes_week": weekly["meeting_minutes_week"],
            "overtime_minutes_week": weekly["overtime_minutes_week"],
            "fragmented_focus_blocks": weekly["fragmented_focus_blocks"],
        },
    }
