from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal, get_db
from app.models import ActionItemCandidate, ApprovalRequest, Meeting
from app.schemas import (
    ActionItemCandidateOut,
    ApproveActionItemRequest,
    ApproveActionItemResult,
    CalendarBlockOut,
    MeetingIngest,
    MeetingOut,
    TaskOut,
)
from app.services.actions import approve_candidate, reject_candidate
from app.services.core import ensure_profile
from app.services.meeting_extractor import extract_action_items
from app.services.openai_client import OpenAIIntegrationError, extract_action_items_openai, is_openai_available

router = APIRouter(tags=["meetings"])
logger = logging.getLogger(__name__)


CONFIDENCE_THRESHOLD = 0.75
LARGE_EFFORT_MINUTES = 240


def _process_meeting(meeting_db_id: str) -> None:
    with SessionLocal() as db:
        meeting = db.get(Meeting, meeting_db_id)
        if meeting is None:
            return

        try:
            base_time = meeting.ended_at or datetime.utcnow()
            extraction_mode = "rule"
            if is_openai_available():
                try:
                    drafts = extract_action_items_openai(meeting.transcript, meeting.summary, base_dt=base_time)
                    extraction_mode = "openai"
                except OpenAIIntegrationError as exc:
                    logger.warning("OpenAI extraction failed, falling back to rule-based extractor: %s", exc)
                    drafts = extract_action_items(meeting.transcript, meeting.summary, base_dt=base_time)
            else:
                drafts = extract_action_items(meeting.transcript, meeting.summary, base_dt=base_time)

            for draft in drafts:
                candidate = ActionItemCandidate(
                    meeting_id=meeting.id,
                    title=draft.title,
                    assignee_name=draft.assignee_name,
                    due=draft.due,
                    effort_minutes=draft.effort_minutes,
                    confidence=draft.confidence,
                    rationale=f"[{extraction_mode}] {draft.rationale}",
                    status="pending",
                )
                db.add(candidate)
                db.flush()

                must_approve = draft.confidence < CONFIDENCE_THRESHOLD or draft.effort_minutes >= LARGE_EFFORT_MINUTES
                if must_approve:
                    db.add(
                        ApprovalRequest(
                            type="action_item",
                            status="pending",
                            payload={
                                "candidate_id": candidate.id,
                                "meeting_id": meeting.meeting_id,
                                "reason": "low_confidence_or_large_effort",
                            },
                        )
                    )

            meeting.extraction_status = "completed"
            db.commit()
        except Exception as exc:  # noqa: BLE001
            meeting.extraction_status = "failed"
            db.add(
                ApprovalRequest(
                    type="other",
                    status="pending",
                    payload={"meeting_id": meeting.meeting_id, "error": str(exc), "reason": "extraction_failed"},
                )
            )
            db.commit()


@router.post("/meetings", status_code=status.HTTP_202_ACCEPTED)
def ingest_meeting(payload: MeetingIngest, bg: BackgroundTasks, db: Session = Depends(get_db)) -> dict:
    row = Meeting(
        meeting_id=payload.meeting_id or f"meeting-{datetime.utcnow().timestamp()}",
        title=payload.title,
        started_at=payload.started_at,
        ended_at=payload.ended_at,
        summary=payload.summary,
        transcript=[item.model_dump() for item in payload.transcript],
        extraction_status="pending",
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    bg.add_task(_process_meeting, row.id)

    return {"meeting_id": row.id, "status": row.extraction_status}


@router.get("/meetings/{meeting_id}", response_model=MeetingOut)
def get_meeting(meeting_id: str, db: Session = Depends(get_db)) -> MeetingOut:
    row = db.get(Meeting, meeting_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Meeting not found")
    return MeetingOut.model_validate(row)


@router.get("/meetings/{meeting_id}/action-items", response_model=list[ActionItemCandidateOut])
def list_action_items(meeting_id: str, db: Session = Depends(get_db)) -> list[ActionItemCandidateOut]:
    meeting = db.get(Meeting, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=404, detail="Meeting not found")

    rows = db.execute(
        select(ActionItemCandidate)
        .where(ActionItemCandidate.meeting_id == meeting.id)
        .order_by(ActionItemCandidate.created_at.asc())
    ).scalars().all()
    return [ActionItemCandidateOut.model_validate(row) for row in rows]


@router.post("/action-items/{candidate_id}/approve", response_model=ApproveActionItemResult)
def approve_action_item(
    candidate_id: str,
    payload: ApproveActionItemRequest,
    db: Session = Depends(get_db),
) -> ApproveActionItemResult:
    candidate = db.get(ActionItemCandidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Action item candidate not found")
    if candidate.status != "pending":
        raise HTTPException(status_code=409, detail=f"Candidate already {candidate.status}")

    profile = ensure_profile(db)
    task, blocks = approve_candidate(
        db,
        candidate,
        profile,
        title=payload.title,
        due=payload.due,
        effort_minutes=payload.effort_minutes,
        priority=payload.priority,
        create_time_block=payload.create_time_block,
    )

    # 후보가 승인되면 연결된 pending approval도 해결 상태로 반영
    approvals = db.execute(
        select(ApprovalRequest).where(
            ApprovalRequest.type == "action_item",
            ApprovalRequest.status == "pending",
        )
    ).scalars().all()
    for approval in approvals:
        if approval.payload.get("candidate_id") == candidate.id:
            approval.status = "approved"
            approval.resolved_at = datetime.utcnow()
            approval.reason = "approved from action item endpoint"

    db.commit()
    db.refresh(task)
    for block in blocks:
        db.refresh(block)

    return ApproveActionItemResult(
        candidate_id=candidate.id,
        task=TaskOut.model_validate(task),
        created_blocks=[CalendarBlockOut.model_validate(block) for block in blocks],
        ms_todo_synced=False,
        outlook_synced=False,
    )


@router.post("/action-items/{candidate_id}/reject", status_code=status.HTTP_200_OK)
def reject_action_item(candidate_id: str, db: Session = Depends(get_db)) -> dict:
    candidate = db.get(ActionItemCandidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Action item candidate not found")
    if candidate.status != "pending":
        raise HTTPException(status_code=409, detail=f"Candidate already {candidate.status}")

    reject_candidate(candidate)
    approvals = db.execute(
        select(ApprovalRequest).where(
            ApprovalRequest.type == "action_item",
            ApprovalRequest.status == "pending",
        )
    ).scalars().all()
    for approval in approvals:
        if approval.payload.get("candidate_id") == candidate.id:
            approval.status = "rejected"
            approval.resolved_at = datetime.utcnow()
            approval.reason = "rejected from action item endpoint"

    db.commit()
    return {"candidate_id": candidate.id, "status": candidate.status}
