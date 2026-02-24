from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import DailyBriefingOut
from app.services.briefing import build_daily_briefing
from app.services.core import ensure_profile

router = APIRouter(prefix="/briefings", tags=["briefings"])


@router.get("/daily", response_model=DailyBriefingOut)
def get_daily_briefing(target_date: date | None = None, db: Session = Depends(get_db)) -> DailyBriefingOut:
    profile = ensure_profile(db)
    value = build_daily_briefing(db, profile, target_date or date.today())
    return DailyBriefingOut.model_validate(value)
