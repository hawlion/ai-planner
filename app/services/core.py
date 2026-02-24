from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.config import settings
from app.models import AuditLog, UserProfile


def now_local() -> datetime:
    return datetime.now(tz=ZoneInfo(settings.timezone))


def ensure_profile(db: Session) -> UserProfile:
    profile = db.query(UserProfile).order_by(UserProfile.created_at.asc()).first()
    if profile:
        return profile

    profile = UserProfile(timezone=settings.timezone)
    db.add(profile)
    db.commit()
    db.refresh(profile)
    return profile


def write_audit(db: Session, action: str, object_ref: str | None = None, actor: str = "user", meta: dict | None = None) -> None:
    db.add(AuditLog(action=action, object_ref=object_ref, actor=actor, meta=meta or {}))
    db.commit()
