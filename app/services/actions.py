from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ActionItemCandidate, CalendarBlock, Task, UserProfile


def approve_candidate(
    db: Session,
    candidate: ActionItemCandidate,
    profile: UserProfile,
    *,
    title: str | None = None,
    due: datetime | None = None,
    effort_minutes: int | None = None,
    priority: str = "medium",
    create_time_block: bool = True,
) -> tuple[Task, list[CalendarBlock]]:
    task = Task(
        title=title or candidate.title,
        description=f"From meeting candidate {candidate.id}",
        due=due or candidate.due,
        effort_minutes=effort_minutes or candidate.effort_minutes,
        priority=priority,
        source="meeting",
        source_ref=candidate.meeting_id,
    )
    db.add(task)
    db.flush()

    created_blocks: list[CalendarBlock] = []
    if create_time_block:
        slot = _find_next_slot(db, profile, task.effort_minutes)
        if slot:
            block = CalendarBlock(
                type="task_block",
                title=f"{task.title} 실행",
                start=slot[0],
                end=slot[1],
                task_id=task.id,
                locked=False,
                source="aawo",
            )
            db.add(block)
            created_blocks.append(block)

    candidate.status = "approved"
    candidate.linked_task_id = task.id
    db.flush()

    return task, created_blocks


def reject_candidate(candidate: ActionItemCandidate) -> None:
    candidate.status = "rejected"


def _find_next_slot(db: Session, profile: UserProfile, effort_minutes: int) -> tuple[datetime, datetime] | None:
    tz = ZoneInfo(profile.timezone)
    now = datetime.now(tz=tz)
    horizon_end = now + timedelta(days=2)
    duration = timedelta(minutes=max(30, min(120, effort_minutes)))

    # 간단한 first-fit: 30분 단위로 충돌 없는 슬롯을 찾는다.
    cursor = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    while cursor < horizon_end:
        candidate_end = cursor + duration

        conflict = db.execute(
            select(CalendarBlock).where(CalendarBlock.start < candidate_end, CalendarBlock.end > cursor)
        ).scalars().first()
        if not conflict:
            return cursor, candidate_end

        cursor += timedelta(minutes=30)

    return None
