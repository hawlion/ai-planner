from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import CalendarBlock, SchedulingChange, SchedulingProposal, Task, UserProfile

try:
    from ortools.sat.python import cp_model
except Exception:  # noqa: BLE001
    cp_model = None

DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
PRIORITY_SCORE = {"critical": 4, "high": 3, "medium": 2, "low": 1}
CP_SAT_STATUSES = {
    0: "unknown",
    1: "model_invalid",
    2: "feasible",
    3: "infeasible",
    4: "optimal",
}

logger = logging.getLogger(__name__)


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


def _task_required_minutes(task: Task, slot_minutes: int) -> int:
    required = max(slot_minutes, int(math.ceil(task.effort_minutes / slot_minutes) * slot_minutes))
    # 너무 긴 블록은 일정 배치 실패를 늘리므로 MVP에서는 2시간 상한으로 분할.
    return min(required, 2 * 60)


def _deep_overlap_minutes(
    start: datetime,
    end: datetime,
    windows: list[tuple[str, time, time, float]],
    tz: ZoneInfo,
) -> int:
    if not windows:
        return 0
    day_key = DAY_KEYS[start.astimezone(tz).weekday()]
    local_start = start.astimezone(tz)
    local_end = end.astimezone(tz)
    total = 0.0
    for win_day, win_start, win_end, weight in windows:
        if day_key != win_day:
            continue
        win_s = datetime.combine(local_start.date(), win_start, tzinfo=tz)
        win_e = datetime.combine(local_start.date(), win_end, tzinfo=tz)
        overlap = max(timedelta(0), min(local_end, win_e) - max(local_start, win_s))
        total += (overlap.total_seconds() / 60.0) * float(weight)
    return int(total)


def _slot_segments_from_intervals(
    intervals: list[Interval],
    horizon_start: datetime,
    slot_minutes: int,
) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    for interval in intervals:
        start_offset = (interval.start - horizon_start).total_seconds() / 60.0
        end_offset = (interval.end - horizon_start).total_seconds() / 60.0
        start_slot = int(math.ceil(start_offset / slot_minutes - 1e-9))
        end_slot = int(math.floor(end_offset / slot_minutes + 1e-9))
        if end_slot <= start_slot:
            continue
        segments.append((start_slot, end_slot))

    if not segments:
        return []

    segments.sort(key=lambda item: item[0])
    merged: list[tuple[int, int]] = [segments[0]]
    for start_slot, end_slot in segments[1:]:
        prev_start, prev_end = merged[-1]
        if start_slot <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end_slot))
        else:
            merged.append((start_slot, end_slot))
    return merged


def _candidate_score(
    task: Task,
    start: datetime,
    end: datetime,
    strategy: str,
    deep_windows: list[tuple[str, time, time, float]],
    tz: ZoneInfo,
    horizon_from: datetime,
) -> int:
    priority = PRIORITY_SCORE.get(task.priority, 1)
    minutes = int(max(0, (end - start).total_seconds() // 60))
    start_bias = int(max(0, (start - horizon_from).total_seconds() // 60))
    deep_bonus = _deep_overlap_minutes(start, end, deep_windows, tz)
    base_reward = 15000 + (priority * 2500) + min(minutes, 120) * 15

    lateness = 0
    due_urgency_bonus = 0
    if task.due:
        due = _as_naive_utc(task.due)
        end_norm = _as_naive_utc(end)
        start_norm = _as_naive_utc(start)
        lateness = max(0, int((end_norm - due).total_seconds() // 60))
        minutes_to_due = int((due - start_norm).total_seconds() // 60)
        due_urgency_bonus = max(0, (24 * 60 * 3 - minutes_to_due) // 6)

    if strategy == "urgent":
        return int(base_reward + due_urgency_bonus - (start_bias // 4) - lateness * (180 + priority * 30) + deep_bonus * 2)
    if strategy == "focus":
        long_block_bonus = 500 if minutes >= 90 else 0
        return int(base_reward + long_block_bonus - (start_bias // 10) - lateness * (90 + priority * 10) + deep_bonus * 12)
    return int(base_reward + due_urgency_bonus // 2 - (start_bias // 6) - lateness * (130 + priority * 20) + deep_bonus * 4)


def _allocate_changes_cpsat(
    tasks: list[Task],
    intervals: list[Interval],
    slot_minutes: int,
    strategy: str,
    profile: UserProfile,
    horizon_from: datetime,
    horizon_to: datetime,
) -> tuple[list[dict], dict]:
    if cp_model is None:
        return [], {"engine": "ortools_cp_sat", "status": "missing_dependency"}
    if not settings.scheduler_cpsat_enabled:
        return [], {"engine": "ortools_cp_sat", "status": "disabled"}
    if not tasks or not intervals:
        return [], {"engine": "ortools_cp_sat", "status": "empty_input"}

    slot_minutes = max(15, int(slot_minutes))
    segments = _slot_segments_from_intervals(intervals, horizon_from, slot_minutes)
    if not segments:
        return [], {"engine": "ortools_cp_sat", "status": "no_free_slots"}

    tz = ZoneInfo(profile.timezone)
    deep_windows = _deep_windows(profile)
    max_candidates = max(20, int(settings.scheduler_cpsat_max_candidates_per_task))

    model = cp_model.CpModel()
    slot_to_vars: dict[int, list] = defaultdict(list)
    task_var_map: dict[str, tuple[Task, list[tuple]]] = {}
    objective_terms: list = []

    ordered_tasks = _task_order(strategy, tasks)
    horizon_slots = int(math.ceil(max(0, (horizon_to - horizon_from).total_seconds() / 60.0) / slot_minutes))

    for task_idx, task in enumerate(ordered_tasks):
        required_minutes = _task_required_minutes(task, slot_minutes)
        required_slots = max(1, required_minutes // slot_minutes)
        candidates: list[tuple[int, int]] = []
        for seg_start, seg_end in segments:
            if seg_end - seg_start < required_slots:
                continue
            start_min = max(0, seg_start)
            start_max = min(seg_end - required_slots, horizon_slots - required_slots)
            if start_max < start_min:
                continue
            for slot_start in range(start_min, start_max + 1):
                slot_end = slot_start + required_slots
                candidates.append((slot_start, slot_end))

        if not candidates:
            continue

        scored: list[tuple[int, int, int, datetime, datetime]] = []
        for slot_start, slot_end in candidates:
            start_dt = horizon_from + timedelta(minutes=slot_start * slot_minutes)
            end_dt = horizon_from + timedelta(minutes=slot_end * slot_minutes)
            score = _candidate_score(task, start_dt, end_dt, strategy, deep_windows, tz, horizon_from)
            scored.append((score, slot_start, slot_end, start_dt, end_dt))

        scored.sort(key=lambda item: item[0], reverse=True)
        selected = scored[:max_candidates]
        if not selected:
            continue

        vars_for_task: list[tuple] = []
        for cand_idx, (score, slot_start, slot_end, start_dt, end_dt) in enumerate(selected):
            var = model.NewBoolVar(f"task_{task_idx}_{cand_idx}")
            vars_for_task.append((var, score, slot_start, slot_end, start_dt, end_dt, required_minutes))
            for slot in range(slot_start, slot_end):
                slot_to_vars[slot].append(var)
            objective_terms.append(score * var)

        model.Add(sum(item[0] for item in vars_for_task) <= 1)
        task_var_map[task.id] = (task, vars_for_task)

    if not objective_terms:
        return [], {"engine": "ortools_cp_sat", "status": "no_candidates"}

    for vars_at_slot in slot_to_vars.values():
        if len(vars_at_slot) > 1:
            model.Add(sum(vars_at_slot) <= 1)

    model.Maximize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max(1.0, float(settings.scheduler_cpsat_timeout_seconds))
    solver.parameters.num_search_workers = 8
    solver.parameters.random_seed = 42

    status = solver.Solve(model)
    status_name = CP_SAT_STATUSES.get(int(status), "unknown")
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        logger.info("CP-SAT solver did not produce a feasible plan: %s", status_name)
        return [], {"engine": "ortools_cp_sat", "status": status_name}

    changes: list[dict] = []
    for task_id, (task, vars_for_task) in task_var_map.items():
        chosen = next((item for item in vars_for_task if solver.Value(item[0]) == 1), None)
        if chosen is None:
            continue
        _, _, _, _, start_dt, end_dt, required_minutes = chosen
        changes.append(
            {
                "kind": "create_block",
                "block": {
                    "type": "task_block" if required_minutes < 90 else "focus_block",
                    "title": f"{task.title} 집중 블록",
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "task_id": task_id,
                    "locked": False,
                },
                "task": {"id": task.id, "title": task.title, "due": task.due.isoformat() if task.due else None},
            }
        )

    changes.sort(key=lambda item: item["block"]["start"])
    return changes, {"engine": "ortools_cp_sat", "status": status_name}


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
        required = _task_required_minutes(task, slot_minutes)

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


def _proposal_summary(strategy: str, *, engine: str) -> str:
    prefix = "CP-SAT 최적화" if engine == "ortools_cp_sat" else "휴리스틱"
    if strategy == "stable":
        return f"{prefix} 안정형 제안: 기존 일정 변경 최소"
    if strategy == "urgent":
        return f"{prefix} 마감우선 제안: 임박한 업무 우선 배치"
    return f"{prefix} 집중형 제안: 딥워크 블록 우선 확보"


def _proposal_explanation(strategy: str, *, engine: str, meta: dict | None = None) -> dict:
    explanation = {
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
        "engine": engine,
    }
    if meta:
        explanation["solver"] = meta
    return explanation


def _changes_signature(changes: list[dict]) -> tuple[tuple[str, str, str], ...]:
    parts: list[tuple[str, str, str]] = []
    for change in changes:
        block = change.get("block") or {}
        parts.append(
            (
                str(block.get("task_id") or ""),
                str(block.get("start") or ""),
                str(block.get("end") or ""),
            )
        )
    return tuple(sorted(parts))


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
    # Normalize horizon boundaries to a consistent timezone-aware basis so
    # downstream CP-SAT slot math does not mix naive/aware datetimes.
    tz = ZoneInfo(profile.timezone)
    horizon_from = _coerce_timezone(horizon_from, tz)
    horizon_to = _coerce_timezone(horizon_to, tz)
    if horizon_to <= horizon_from:
        return []

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
    signatures: set[tuple[tuple[str, str, str], ...]] = set()

    tasks_by_id = {task.id: task for task in tasks}

    if cp_model is not None and settings.scheduler_cpsat_enabled:
        for strategy in strategies:
            if len(created) >= max_proposals:
                break
            changes_payload, meta = _allocate_changes_cpsat(
                tasks,
                intervals,
                slot_minutes,
                strategy,
                profile,
                horizon_from,
                horizon_to,
            )
            if not changes_payload:
                continue

            signature = _changes_signature(changes_payload)
            if signature in signatures:
                continue
            signatures.add(signature)

            proposal = SchedulingProposal(
                summary=_proposal_summary(strategy, engine="ortools_cp_sat"),
                explanation=_proposal_explanation(strategy, engine="ortools_cp_sat", meta=meta),
                score={**_score(changes_payload, tasks_by_id), "engine": "ortools_cp_sat"},
                horizon_from=horizon_from,
                horizon_to=horizon_to,
                status="draft",
            )
            db.add(proposal)
            db.flush()
            for change in changes_payload:
                db.add(SchedulingChange(proposal_id=proposal.id, kind=change["kind"], payload=change))
            created.append(proposal)

    for strategy in strategies:
        if len(created) >= max_proposals:
            break
        changes_payload = _allocate_changes(tasks, intervals, slot_minutes, strategy, profile)
        if not changes_payload:
            continue

        signature = _changes_signature(changes_payload)
        if signature in signatures:
            continue
        signatures.add(signature)

        proposal = SchedulingProposal(
            summary=_proposal_summary(strategy, engine="heuristic"),
            explanation=_proposal_explanation(strategy, engine="heuristic"),
            score={**_score(changes_payload, tasks_by_id), "engine": "heuristic"},
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
    now_utc = datetime.utcnow()

    # Reuse existing future task/focus blocks for the same task_id so
    # rescheduling "moves" current blocks instead of cloning new duplicates.
    planned_task_ids = sorted(
        {
            str((change.payload or {}).get("block", {}).get("task_id") or "").strip()
            for change in proposal.changes
            if (change.payload or {}).get("kind") == "create_block"
            and str((change.payload or {}).get("block", {}).get("task_id") or "").strip()
        }
    )
    reusable_by_task: dict[str, list[CalendarBlock]] = {}
    if planned_task_ids:
        reusable_rows = db.execute(
            select(CalendarBlock)
            .where(
                CalendarBlock.task_id.in_(planned_task_ids),
                CalendarBlock.source != "external",
                CalendarBlock.locked.is_(False),
                CalendarBlock.type.in_(["task_block", "focus_block"]),
                CalendarBlock.end > now_utc,
            )
            .order_by(CalendarBlock.start.asc())
        ).scalars().all()
        for row in reusable_rows:
            if not row.task_id:
                continue
            reusable_by_task.setdefault(row.task_id, []).append(row)

    used_reusable_ids: set[str] = set()
    reusable_ids_all: set[str] = {
        row.id
        for rows in reusable_by_task.values()
        for row in rows
    }
    placed_ranges: list[tuple[datetime, datetime]] = []

    for change in proposal.changes:
        payload = change.payload
        kind = payload.get("kind")

        if kind != "create_block":
            continue

        block = payload.get("block", {})
        start = _as_naive_utc(datetime.fromisoformat(block["start"]))
        end = _as_naive_utc(datetime.fromisoformat(block["end"]))
        if end <= start:
            continue

        task_id = block.get("task_id")
        reusable = None
        if task_id:
            for candidate in reusable_by_task.get(str(task_id), []):
                if candidate.id not in used_reusable_ids:
                    reusable = candidate
                    break

        if any(start < placed_end and end > placed_start for placed_start, placed_end in placed_ranges):
            continue

        overlap_stmt = select(CalendarBlock).where(
            and_(
                CalendarBlock.start < end,
                CalendarBlock.end > start,
            )
        )
        if reusable_ids_all:
            overlap_stmt = overlap_stmt.where(CalendarBlock.id.notin_(list(reusable_ids_all)))
        overlap = db.execute(overlap_stmt).scalars().first()
        if overlap:
            continue

        if reusable:
            reusable.type = block.get("type", reusable.type or "task_block")
            reusable.title = block.get("title", reusable.title or "Planned block")
            reusable.start = start
            reusable.end = end
            reusable.task_id = block.get("task_id")
            reusable.locked = bool(block.get("locked", False))
            reusable.source = "aawo"
            reusable.version += 1
            used_reusable_ids.add(reusable.id)
            updated_blocks.append(reusable)
            placed_ranges.append((start, end))
        else:
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
            placed_ranges.append((start, end))

    proposal.status = "applied"
    db.commit()

    for row in created_blocks:
        db.refresh(row)
    for row in updated_blocks:
        db.refresh(row)
    db.refresh(proposal)

    return created_blocks, updated_blocks
