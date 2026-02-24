from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import UserProfileOut, UserProfilePatch
from app.services.core import ensure_profile

router = APIRouter(prefix="/profile", tags=["profile"])


@router.get("", response_model=UserProfileOut)
def get_profile(db: Session = Depends(get_db)) -> UserProfileOut:
    profile = ensure_profile(db)
    return UserProfileOut.model_validate(profile)


@router.patch("", response_model=UserProfileOut)
def patch_profile(payload: UserProfilePatch, db: Session = Depends(get_db)) -> UserProfileOut:
    profile = ensure_profile(db)

    data = payload.model_dump(exclude_unset=True)
    if data.get("timezone") is not None:
        profile.timezone = data["timezone"]
    if data.get("autonomy_level") is not None:
        profile.autonomy_level = data["autonomy_level"]
    if data.get("working_hours") is not None:
        profile.working_hours = data["working_hours"]
    if data.get("preferences") is not None:
        profile.preferences = data["preferences"]

    profile.version += 1
    db.commit()
    db.refresh(profile)
    return UserProfileOut.model_validate(profile)
