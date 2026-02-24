from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import ActionItemCandidate, ApprovalRequest, SchedulingProposal
from app.schemas import ApprovalOut, ApprovalResolve
from app.services.actions import approve_candidate, reject_candidate
from app.services.core import ensure_profile
from app.services.scheduler import apply_proposal

router = APIRouter(prefix="/approvals", tags=["approvals"])


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

    decision = payload.decision
    approval.status = "approved" if decision == "approve" else "rejected"
    approval.reason = payload.reason
    approval.resolved_at = datetime.utcnow()

    if approval.type == "action_item":
        candidate_id = approval.payload.get("candidate_id")
        candidate = db.get(ActionItemCandidate, candidate_id)
        if candidate and candidate.status == "pending":
            if decision == "approve":
                profile = ensure_profile(db)
                approve_candidate(db, candidate, profile)
            else:
                reject_candidate(candidate)

    if approval.type == "reschedule" and decision == "approve":
        proposal_id = approval.payload.get("proposal_id")
        proposal = db.get(SchedulingProposal, proposal_id)
        if proposal and proposal.status == "draft":
            apply_proposal(db, proposal)

    db.commit()
    db.refresh(approval)
    return ApprovalOut.model_validate(approval)
