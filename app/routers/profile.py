from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import UserProfileOut, UserProfilePatch
from app.services.core import add_audit, ensure_profile

router = APIRouter(prefix="/profile", tags=["profile"])


@router.get("", response_model=UserProfileOut)
def get_profile(db: Session = Depends(get_db)) -> UserProfileOut:
    profile = ensure_profile(db)
    return UserProfileOut.model_validate(profile)


@router.patch("", response_model=UserProfileOut)
def patch_profile(payload: UserProfilePatch, db: Session = Depends(get_db)) -> UserProfileOut:
    profile = ensure_profile(db)
    if payload.version is None:
        raise HTTPException(status_code=409, detail="Profile version is required")
    if payload.version != profile.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Profile version conflict",
                "expected_version": profile.version,
                "provided_version": payload.version,
            },
        )

    data = payload.model_dump(exclude_unset=True)
    changed_fields: list[str] = []
    if data.get("timezone") is not None:
        profile.timezone = data["timezone"]
        changed_fields.append("timezone")
    if data.get("autonomy_level") is not None:
        profile.autonomy_level = data["autonomy_level"]
        changed_fields.append("autonomy_level")
    if data.get("working_hours") is not None:
        profile.working_hours = data["working_hours"]
        changed_fields.append("working_hours")
    if data.get("preferences") is not None:
        profile.preferences = data["preferences"]
        changed_fields.append("preferences")

    profile.version += 1
    add_audit(
        db,
        action="profile.updated",
        object_ref=profile.id,
        meta={"changed_fields": changed_fields, "new_version": profile.version},
    )
    db.commit()
    db.refresh(profile)
    return UserProfileOut.model_validate(profile)
