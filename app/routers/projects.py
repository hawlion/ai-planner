from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, selectinload

from app.db import engine, get_db
from app.models import Project, ProjectMilestone
from app.schemas import ProjectCreate, ProjectOut, ProjectPatch
from app.services.core import add_audit

router = APIRouter(prefix="/projects", tags=["projects"])


def _ensure_project_milestone_table(db: Session) -> None:
    try:
        db.execute(select(ProjectMilestone.id).limit(1))
    except OperationalError:
        db.rollback()
        ProjectMilestone.__table__.create(bind=engine, checkfirst=True)


@router.get("", response_model=list[ProjectOut])
def list_projects(db: Session = Depends(get_db)) -> list[ProjectOut]:
    _ensure_project_milestone_table(db)
    rows = db.execute(
        select(Project).options(selectinload(Project.milestones)).order_by(Project.created_at.desc())
    ).scalars().all()
    return [ProjectOut.model_validate(row) for row in rows]


@router.post("", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
def create_project(payload: ProjectCreate, db: Session = Depends(get_db)) -> ProjectOut:
    _ensure_project_milestone_table(db)
    data = payload.model_dump(exclude={"milestones"})
    row = Project(**data)
    db.add(row)
    db.flush()
    for item in payload.milestones:
        db.add(ProjectMilestone(project_id=row.id, **item.model_dump()))
    add_audit(
        db,
        action="project.created",
        object_ref=row.id,
        meta={
            "title": row.title,
            "priority": row.priority,
            "milestone_count": len(payload.milestones),
        },
    )
    db.commit()
    db.expire_all()
    out = db.execute(
        select(Project).options(selectinload(Project.milestones)).where(Project.id == row.id)
    ).scalars().first()
    return ProjectOut.model_validate(out or row)


@router.get("/{project_id}", response_model=ProjectOut)
def get_project(project_id: str, db: Session = Depends(get_db)) -> ProjectOut:
    _ensure_project_milestone_table(db)
    row = db.execute(
        select(Project).options(selectinload(Project.milestones)).where(Project.id == project_id)
    ).scalars().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return ProjectOut.model_validate(row)


@router.patch("/{project_id}", response_model=ProjectOut)
def patch_project(project_id: str, payload: ProjectPatch, db: Session = Depends(get_db)) -> ProjectOut:
    _ensure_project_milestone_table(db)
    row = db.execute(
        select(Project).options(selectinload(Project.milestones)).where(Project.id == project_id)
    ).scalars().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if payload.version is None:
        raise HTTPException(status_code=409, detail="Project version is required")
    if payload.version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Project version conflict",
                "expected_version": row.version,
                "provided_version": payload.version,
            },
        )

    data = payload.model_dump(exclude_unset=True)
    milestones = data.pop("milestones", None)

    changed_fields: list[str] = []
    for field, value in data.items():
        if field == "version":
            continue
        setattr(row, field, value)
        changed_fields.append(field)

    if milestones is not None:
        for milestone in list(row.milestones):
            db.delete(milestone)
        for item in milestones:
            db.add(ProjectMilestone(project_id=row.id, **item))
        changed_fields.append("milestones")

    row.version += 1
    add_audit(
        db,
        action="project.updated",
        object_ref=row.id,
        meta={
            "changed_fields": changed_fields,
            "new_version": row.version,
            "milestone_count": len(milestones) if milestones is not None else len(row.milestones),
        },
    )
    db.commit()
    db.expire_all()
    out = db.execute(
        select(Project).options(selectinload(Project.milestones)).where(Project.id == row.id)
    ).scalars().first()
    return ProjectOut.model_validate(out or row)


@router.delete("/{project_id}")
def delete_project(project_id: str, version: int | None = None, db: Session = Depends(get_db)) -> dict:
    row = db.get(Project, project_id)
    if row is None:
        return {"deleted": False, "project_id": project_id}
    if version is not None and version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Project version conflict",
                "expected_version": row.version,
                "provided_version": version,
            },
        )
    add_audit(db, action="project.deleted", object_ref=row.id, meta={"title": row.title})
    db.delete(row)
    db.commit()
    return {"deleted": True, "project_id": project_id}
