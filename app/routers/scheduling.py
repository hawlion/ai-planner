from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import ApprovalRequest, SchedulingProposal
from app.schemas import ApplyProposalRequest, ScheduleProposalOut, SchedulingProposalRequest
from app.services.core import ensure_profile
from app.services.scheduler import apply_proposal, generate_proposals

router = APIRouter(prefix="/scheduling", tags=["scheduling"])


@router.post("/proposals", response_model=list[ScheduleProposalOut])
def create_proposals(payload: SchedulingProposalRequest, db: Session = Depends(get_db)) -> list[ScheduleProposalOut]:
    profile = ensure_profile(db)
    proposals = generate_proposals(
        db,
        profile,
        horizon_from=payload.horizon.from_,
        horizon_to=payload.horizon.to,
        task_ids=payload.task_ids,
        slot_minutes=payload.constraints.slot_minutes,
        max_proposals=payload.constraints.max_proposals,
    )
    return [ScheduleProposalOut.model_validate(proposal) for proposal in proposals]


@router.get("/proposals/{proposal_id}", response_model=ScheduleProposalOut)
def get_proposal(proposal_id: str, db: Session = Depends(get_db)) -> ScheduleProposalOut:
    proposal = db.get(SchedulingProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return ScheduleProposalOut.model_validate(proposal)


@router.post("/proposals/{proposal_id}/apply")
def apply_schedule_proposal(
    proposal_id: str,
    payload: ApplyProposalRequest,
    db: Session = Depends(get_db),
) -> dict:
    proposal = db.get(SchedulingProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="Proposal not found")

    if proposal.status != "draft":
        raise HTTPException(status_code=409, detail=f"Proposal already {proposal.status}")

    profile = ensure_profile(db)
    requires_approval = profile.autonomy_level in {"L0", "L1", "L2"}

    if requires_approval and not payload.approved:
        approval = ApprovalRequest(
            type="reschedule",
            status="pending",
            payload={"proposal_id": proposal.id, "summary": proposal.summary},
        )
        db.add(approval)
        db.commit()
        db.refresh(approval)
        return {
            "proposal_id": proposal.id,
            "applied": False,
            "approval_required": True,
            "approval_id": approval.id,
        }

    created_blocks, updated_blocks = apply_proposal(db, proposal)
    return {
        "proposal_id": proposal.id,
        "applied": True,
        "created_blocks": [
            {
                "id": block.id,
                "title": block.title,
                "start": block.start,
                "end": block.end,
                "task_id": block.task_id,
            }
            for block in created_blocks
        ],
        "updated_blocks": [
            {
                "id": block.id,
                "title": block.title,
                "start": block.start,
                "end": block.end,
                "task_id": block.task_id,
            }
            for block in updated_blocks
        ],
        "approval_required": False,
        "applied_at": datetime.utcnow(),
    }
