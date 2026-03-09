from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.db import engine, get_db
from app.models import ActionItemCandidate, ApprovalRequest, CalendarBlock, EmailTriage, SchedulingProposal, Task
from app.routers.assistant import _run_one_action
from app.schemas import ApprovalOut, ApprovalResolve
from app.services.actions import approve_candidate, reject_candidate
from app.services.core import ensure_profile
from app.services.graph_service import (
    OUTBOX_CALENDAR_EXPORT,
    OUTBOX_TODO_EXPORT,
    GraphApiError,
    enqueue_outbox_event,
    is_graph_connected,
)
from app.services.scheduler import apply_proposal

router = APIRouter(prefix="/approvals", tags=["approvals"])


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    local = parsed.astimezone(ZoneInfo(settings.timezone))
    return local.replace(tzinfo=None)


def _to_local_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo(settings.timezone)).replace(tzinfo=None)


def _ensure_email_triage_table(db: Session) -> None:
    try:
        db.execute(select(EmailTriage.id).limit(1))
    except OperationalError:
        db.rollback()
        EmailTriage.__table__.create(bind=engine, checkfirst=True)


def _queue_calendar_export_best_effort(db: Session, blocks: list[CalendarBlock]) -> bool:
    if not blocks or not is_graph_connected(db):
        return False

    starts: list[datetime] = []
    ends: list[datetime] = []
    for block in blocks:
        start = block.start
        end = block.end
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        else:
            start = start.astimezone(UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        else:
            end = end.astimezone(UTC)
        starts.append(start)
        ends.append(end)

    try:
        enqueue_outbox_event(
            db,
            OUTBOX_CALENDAR_EXPORT,
            {
                "start": (min(starts) - timedelta(hours=2)).isoformat(),
                "end": (max(ends) + timedelta(hours=2)).isoformat(),
            },
        )
        return True
    except GraphApiError:
        return False


def _queue_todo_export_best_effort(db: Session) -> bool:
    if not is_graph_connected(db):
        return False
    try:
        enqueue_outbox_event(db, OUTBOX_TODO_EXPORT, {})
        return True
    except GraphApiError:
        return False


@router.get("", response_model=list[ApprovalOut])
def list_approvals(status: str | None = None, db: Session = Depends(get_db)) -> list[ApprovalOut]:
    stmt = select(ApprovalRequest)
    if status:
        stmt = stmt.where(ApprovalRequest.status == status)
    rows = db.execute(stmt.order_by(ApprovalRequest.created_at.desc())).scalars().all()
    return [ApprovalOut.model_validate(row) for row in rows]


@router.post("/{approval_id}/resolve", response_model=ApprovalOut)
def resolve_approval(approval_id: str, payload: ApprovalResolve, db: Session = Depends(get_db)) -> ApprovalOut:
    approval = db.get(ApprovalRequest, approval_id)
    if approval is None:
        raise HTTPException(status_code=404, detail="Approval not found")

    if approval.status != "pending":
        raise HTTPException(status_code=409, detail=f"Approval already {approval.status}")

    if approval.type == "chat_clarification" and payload.decision == "approve":
        raise HTTPException(status_code=409, detail="명확화 요청은 AI 채팅창에 답변을 입력해 처리해 주세요.")

    decision = payload.decision
    approval.status = "approved" if decision == "approve" else "rejected"
    approval.reason = payload.reason
    approval.resolved_at = datetime.utcnow()
    queued_calendar_blocks: list[CalendarBlock] = []
    should_queue_todo_export = False

    has_edits = any(
        [
            bool((payload.task_title or "").strip()),
            payload.task_due is not None,
            bool((payload.event_title or "").strip()),
            payload.event_start is not None,
            payload.event_end is not None,
        ]
    )

    if approval.type == "chat_pending_action" and decision == "approve":
        payload_data = approval.payload if isinstance(approval.payload, dict) else {}
        action = payload_data.get("action") if isinstance(payload_data.get("action"), dict) else {}
        source_message = str(payload_data.get("source_message") or "").strip()
        _run_one_action(
            db,
            action,
            message=source_message,
            history_context=[],
            require_confirmation=False,
        )
        if not approval.reason:
            approval.reason = "resolved_via_approval_button"

    if approval.type == "action_item":
        candidate_id = approval.payload.get("candidate_id")
        candidate = db.get(ActionItemCandidate, candidate_id)
        if candidate and candidate.status == "pending":
            if decision == "approve":
                if (payload.task_title or "").strip():
                    candidate.title = (payload.task_title or "").strip()
                if payload.task_due is not None:
                    candidate.due = _to_local_naive(payload.task_due)
                profile = ensure_profile(db)
                _, blocks = approve_candidate(db, candidate, profile)
                queued_calendar_blocks.extend(blocks)
            else:
                reject_candidate(candidate)

    if approval.type == "reschedule" and decision == "approve":
        proposal_id = approval.payload.get("proposal_id")
        proposal = db.get(SchedulingProposal, proposal_id)
        if proposal and proposal.status == "draft":
            created_blocks, updated_blocks = apply_proposal(db, proposal)
            queued_calendar_blocks.extend([*created_blocks, *updated_blocks])

    if approval.type == "email_intake":
        _ensure_email_triage_table(db)
        triage = db.execute(select(EmailTriage).where(EmailTriage.approval_id == approval.id)).scalars().first()
        if decision == "approve":
            payload_data = approval.payload if isinstance(approval.payload, dict) else {}
            task_data = payload_data.get("task") if isinstance(payload_data.get("task"), dict) else None
            event_data = payload_data.get("event") if isinstance(payload_data.get("event"), dict) else None
            message_id = str(payload_data.get("message_id") or "").strip()

            if task_data is not None:
                task_data = dict(task_data)
            if event_data is not None:
                event_data = dict(event_data)

            if task_data is not None and (payload.task_title or "").strip():
                task_data["title"] = (payload.task_title or "").strip()
            if task_data is not None and payload.task_due is not None:
                task_data["due"] = _to_local_naive(payload.task_due).isoformat()

            if event_data is not None and (payload.event_title or "").strip():
                event_data["title"] = (payload.event_title or "").strip()

            event_start_override = _to_local_naive(payload.event_start)
            event_end_override = _to_local_naive(payload.event_end)
            if event_data is not None and event_start_override is not None:
                event_data["start"] = event_start_override.isoformat()
            if event_data is not None and event_end_override is not None:
                event_data["end"] = event_end_override.isoformat()

            created_task: Task | None = None
            created_block: CalendarBlock | None = None

            if task_data and str(task_data.get("title") or "").strip():
                priority = str(task_data.get("priority") or "medium").strip().lower()
                if priority not in {"low", "medium", "high", "critical"}:
                    priority = "medium"
                created_task = Task(
                    title=str(task_data.get("title") or "").strip(),
                    description=str(task_data.get("description") or "").strip() or None,
                    due=_parse_datetime(task_data.get("due")),
                    priority=priority,
                    source="email",
                    source_ref=message_id or None,
                    effort_minutes=60,
                )
                db.add(created_task)
                db.flush()
                should_queue_todo_export = True

            if event_data:
                start = _parse_datetime(event_data.get("start"))
                end = _parse_datetime(event_data.get("end"))
                if start and (end is None or end <= start):
                    end = start + timedelta(hours=1)
                if start and end and end > start:
                    created_block = CalendarBlock(
                        type="other",
                        title=str(event_data.get("title") or payload_data.get("subject") or "메일 기반 일정").strip(),
                        start=start,
                        end=end,
                        task_id=created_task.id if created_task else None,
                        locked=False,
                        source="aawo",
                    )
                    db.add(created_block)
                    db.flush()
                    queued_calendar_blocks.append(created_block)

            if triage is not None:
                triage.status = "approved"
                triage.created_task_id = created_task.id if created_task else None
                triage.created_block_id = created_block.id if created_block else None
        else:
            if triage is not None:
                triage.status = "rejected"

    if decision == "approve" and has_edits and not approval.reason:
        approval.reason = "approved_with_edits"

    db.commit()
    if should_queue_todo_export:
        _queue_todo_export_best_effort(db)
    if queued_calendar_blocks:
        _queue_calendar_export_best_effort(db, queued_calendar_blocks)
    db.refresh(approval)
    return ApprovalOut.model_validate(approval)
