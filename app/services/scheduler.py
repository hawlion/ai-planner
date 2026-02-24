from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.models import CalendarBlock, SchedulingChange, SchedulingProposal, Task, UserProfile

DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
PRIORITY_SCORE = {"critical": 4, "high": 3, "medium": 2, "low": 1}


@dataclass
class Interval:
    start: datetime
    end: datetime

    @property
    def minutes(self) -> int:
        return int((self.end - self.start).total_seconds() // 60)


def _coerce_timezone(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _as_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":")
    return time(hour=int(hour), minute=int(minute))


def _merge_intervals(intervals: list[Interval]) -> list[Interval]:
    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda item: item.start)
    merged = [ordered[0]]
    for interval in ordered[1:]:
        prev = merged[-1]
        if interval.start <= prev.end:
            prev.end = max(prev.end, interval.end)
        else:
            merged.append(interval)
    return merged


def _subtract(base: list[Interval], busy: list[Interval]) -> list[Interval]:
    if not busy:
        return base

    result: list[Interval] = []
    busy_merged = _merge_intervals(busy)

    for window in base:
        cursors = [window]
        for taken in busy_merged:
            next_cursors: list[Interval] = []
            for cursor in cursors:
                if taken.end <= cursor.start or taken.start >= cursor.end:
                    next_cursors.append(cursor)
                    continue
                if taken.start > cursor.start:
                    next_cursors.append(Interval(cursor.start, min(taken.start, cursor.end)))
                if taken.end < cursor.end:
                    next_cursors.append(Interval(max(taken.end, cursor.start), cursor.end))
            cursors = [c for c in next_cursors if c.end > c.start]
        result.extend(cursors)
    return result


def _get_work_window_for_day(
    day: date,
    working_hours: dict,
    tz: ZoneInfo,
    horizon_start: datetime,
    horizon_end: datetime,
) -> list[Interval]:
    key = DAY_KEYS[day.weekday()]
    days = working_hours.get("days", [])
    match = next((item for item in days if item.get("day") == key), None)
    if match is None:
        return []

    day_start = datetime.combine(day, _parse_hhmm(match.get("start", "09:00")), tzinfo=tz)
    day_end = datetime.combine(day, _parse_hhmm(match.get("end", "18:00")), tzinfo=tz)

    day_start = max(day_start, horizon_start)
    day_end = min(day_end, horizon_end)
    if day_end <= day_start:
        return []

    work = [Interval(day_start, day_end)]

    lunch = working_hours.get("lunch") or {}
    if lunch.get("start") and lunch.get("end"):
        lunch_start = datetime.combine(day, _parse_hhmm(lunch["start"]), tzinfo=tz)
        lunch_end = datetime.combine(day, _parse_hhmm(lunch["end"]), tzinfo=tz)
        work = _subtract(work, [Interval(lunch_start, lunch_end)])

    return work


def _fetch_busy_intervals(db: Session, horizon_start: datetime, horizon_end: datetime) -> list[Interval]:
    stmt = select(CalendarBlock).where(
        and_(
            CalendarBlock.start < horizon_end,
            CalendarBlock.end > horizon_start,
        )
    )
    rows = db.execute(stmt).scalars().all()
    ref_tz = horizon_start.tzinfo

    intervals: list[Interval] = []
    for row in rows:
        start = row.start
        end = row.end
        if ref_tz and start.tzinfo is None:
            start = start.replace(tzinfo=ref_tz)
        if ref_tz and end.tzinfo is None:
            end = end.replace(tzinfo=ref_tz)
        intervals.append(Interval(start=start, end=end))

    return intervals


def _free_intervals(
    db: Session,
    working_hours: dict,
    horizon_start: datetime,
    horizon_end: datetime,
    timezone: str,
) -> list[Interval]:
    tz = ZoneInfo(timezone)
    horizon_start = _coerce_timezone(horizon_start, tz)
    horizon_end = _coerce_timezone(horizon_end, tz)
    start_day = horizon_start.astimezone(tz).date()
    end_day = horizon_end.astimezone(tz).date()

    windows: list[Interval] = []
    cursor = start_day
    while cursor <= end_day:
        windows.extend(_get_work_window_for_day(cursor, working_hours, tz, horizon_start, horizon_end))
        cursor += timedelta(days=1)

    busy = _fetch_busy_intervals(db, horizon_start, horizon_end)
    return [i for i in _subtract(windows, busy) if i.minutes >= 15]


def _task_order(strategy: str, tasks: list[Task]) -> list[Task]:
    def due_key(task: Task) -> float:
        if task.due is None:
            return float("inf")
        return _as_naive_utc(task.due).timestamp()

    if strategy == "urgent":
        return sorted(tasks, key=lambda t: (due_key(t), -PRIORITY_SCORE[t.priority]))
    if strategy == "focus":
        return sorted(tasks, key=lambda t: (-max(t.effort_minutes, 30), -PRIORITY_SCORE[t.priority], due_key(t)))
    return sorted(tasks, key=lambda t: (-PRIORITY_SCORE[t.priority], due_key(t)))


def _deep_windows(profile: UserProfile) -> list[tuple[str, time, time, float]]:
    windows = []
    for entry in profile.preferences.get("deep_work_windows", []):
        try:
            windows.append(
                (
                    entry["day"],
                    _parse_hhmm(entry.get("start", "10:00")),
                    _parse_hhmm(entry.get("end", "12:00")),
                    float(entry.get("weight", 0.7)),
                )
            )
        except (KeyError, ValueError):
            continue
    return windows


def _interval_focus_score(interval: Interval, windows: list[tuple[str, time, time, float]], tz: ZoneInfo) -> float:
    if not windows:
        return 0.0

    local_start = _coerce_timezone(interval.start, tz).astimezone(tz)
    local_end = _coerce_timezone(interval.end, tz).astimezone(tz)
    day_key = DAY_KEYS[local_start.weekday()]

    score = 0.0
    for win_day, win_start, win_end, weight in windows:
        if win_day != day_key:
            continue
        win_s = datetime.combine(local_start.date(), win_start, tzinfo=tz)
        win_e = datetime.combine(local_start.date(), win_end, tzinfo=tz)
        overlap = max(timedelta(0), min(local_end, win_e) - max(local_start, win_s))
        score += overlap.total_seconds() / 60 * weight
    return score


def _pick_interval(
    intervals: list[Interval],
    required_minutes: int,
    strategy: str,
    due: datetime | None,
    deep_windows: list[tuple[str, time, time, float]],
    tz: ZoneInfo,
) -> int | None:
    best_idx = None
    best_score = None
    due_norm = _as_naive_utc(due) if due else None

    for idx, interval in enumerate(intervals):
        if interval.minutes < required_minutes:
            continue

        if strategy == "stable":
            score = interval.start.timestamp()
        elif strategy == "urgent":
            lateness_penalty = 0.0
            interval_end_norm = _as_naive_utc(interval.end)
            if due_norm and interval_end_norm > due_norm:
                lateness_penalty = (interval_end_norm - due_norm).total_seconds() / 60 * 5.0
            score = interval.start.timestamp() + lateness_penalty
        else:
            focus_bonus = _interval_focus_score(interval, deep_windows, tz)
            score = interval.start.timestamp() - focus_bonus * 60

        if best_score is None or score < best_score:
            best_idx = idx
            best_score = score

    return best_idx


def _allocate_changes(
    tasks: list[Task],
    intervals: list[Interval],
    slot_minutes: int,
    strategy: str,
    profile: UserProfile,
) -> list[dict]:
    if not tasks:
        return []

    available = [Interval(i.start, i.end) for i in intervals]
    tz = ZoneInfo(profile.timezone)
    deep_windows = _deep_windows(profile)

    changes: list[dict] = []

    for task in _task_order(strategy, tasks):
        required = max(slot_minutes, int(math.ceil(task.effort_minutes / slot_minutes) * slot_minutes))
        required = min(required, 2 * 60)

        picked = _pick_interval(available, required, strategy, task.due, deep_windows, tz)
        if picked is None:
            continue

        chosen = available[picked]
        start = chosen.start
        end = start + timedelta(minutes=required)

        changes.append(
            {
                "kind": "create_block",
                "block": {
                    "type": "task_block" if required < 90 else "focus_block",
                    "title": f"{task.title} 집중 블록",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "task_id": task.id,
                    "locked": False,
                },
                "task": {"id": task.id, "title": task.title, "due": task.due.isoformat() if task.due else None},
            }
        )

        if end >= chosen.end:
            del available[picked]
        else:
            available[picked] = Interval(end, chosen.end)

    return changes


def _proposal_summary(strategy: str) -> str:
    if strategy == "stable":
        return "안정형 제안: 기존 일정 변경 최소"
    if strategy == "urgent":
        return "마감우선 제안: 임박한 업무 우선 배치"
    return "집중형 제안: 딥워크 블록 우선 확보"


def _score(changes: list[dict], tasks_by_id: dict[str, Task]) -> dict:
    lateness = 0
    deep_work = 0

    for change in changes:
        block = change["block"]
        start = datetime.fromisoformat(block["start"])
        end = datetime.fromisoformat(block["end"])
        duration = int((end - start).total_seconds() // 60)
        if duration >= 90:
            deep_work += duration

        task_id = block.get("task_id")
        task = tasks_by_id.get(task_id)
        if task and task.due:
            due = _as_naive_utc(task.due)
            end_norm = _as_naive_utc(end)
            if end_norm > due:
                lateness += int((end_norm - due).total_seconds() // 60)

    return {
        "objective_value": round(max(0, 1000 - lateness - len(changes) * 10 + deep_work * 0.5), 2),
        "lateness_minutes": lateness,
        "changes_count": len(changes),
        "deep_work_minutes": deep_work,
    }


def generate_proposals(
    db: Session,
    profile: UserProfile,
    horizon_from: datetime,
    horizon_to: datetime,
    task_ids: list[str] | None,
    slot_minutes: int,
    max_proposals: int,
) -> list[SchedulingProposal]:
    task_stmt = select(Task).where(Task.status.in_(["todo", "in_progress"]))
    if task_ids:
        task_stmt = task_stmt.where(Task.id.in_(task_ids))
    else:
        task_stmt = task_stmt.where(or_(Task.due.is_(None), Task.due <= horizon_to + timedelta(days=7)))

    tasks = db.execute(task_stmt).scalars().all()

    if not tasks:
        return []

    intervals = _free_intervals(db, profile.working_hours, horizon_from, horizon_to, profile.timezone)
    if not intervals:
        return []

    strategies = ["stable", "urgent", "focus"][:max_proposals]
    created: list[SchedulingProposal] = []

    tasks_by_id = {task.id: task for task in tasks}

    for strategy in strategies:
        changes_payload = _allocate_changes(tasks, intervals, slot_minutes, strategy, profile)
        proposal = SchedulingProposal(
            summary=_proposal_summary(strategy),
            explanation={
                "constraints_applied": [
                    "근무시간/점심시간 준수",
                    "기존 캘린더 블록 충돌 회피",
                    "슬롯 단위 배치",
                ],
                "tradeoffs": [
                    "변경 최소화와 마감 우선순위의 균형",
                    "딥워크 확보와 전체 처리량 간 균형",
                ],
                "notes": f"전략={strategy}",
            },
            score=_score(changes_payload, tasks_by_id),
            horizon_from=horizon_from,
            horizon_to=horizon_to,
            status="draft",
        )
        db.add(proposal)
        db.flush()

        for change in changes_payload:
            db.add(SchedulingChange(proposal_id=proposal.id, kind=change["kind"], payload=change))

        created.append(proposal)

    db.commit()

    for proposal in created:
        db.refresh(proposal)

    return created


def apply_proposal(db: Session, proposal: SchedulingProposal) -> tuple[list[CalendarBlock], list[CalendarBlock]]:
    created_blocks: list[CalendarBlock] = []
    updated_blocks: list[CalendarBlock] = []

    for change in proposal.changes:
        payload = change.payload
        kind = payload.get("kind")

        if kind != "create_block":
            continue

        block = payload.get("block", {})
        start = datetime.fromisoformat(block["start"])
        end = datetime.fromisoformat(block["end"])

        overlap = db.execute(
            select(CalendarBlock).where(
                and_(
                    CalendarBlock.start < end,
                    CalendarBlock.end > start,
                )
            )
        ).scalars().first()
        if overlap:
            continue

        row = CalendarBlock(
            type=block.get("type", "task_block"),
            title=block.get("title", "Planned block"),
            start=start,
            end=end,
            task_id=block.get("task_id"),
            locked=bool(block.get("locked", False)),
            source="aawo",
        )
        db.add(row)
        created_blocks.append(row)

    proposal.status = "applied"
    db.commit()

    for row in created_blocks:
        db.refresh(row)
    db.refresh(proposal)

    return created_blocks, updated_blocks
