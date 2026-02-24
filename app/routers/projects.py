from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Project
from app.schemas import ProjectCreate, ProjectOut, ProjectPatch

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=list[ProjectOut])
def list_projects(db: Session = Depends(get_db)) -> list[ProjectOut]:
    rows = db.execute(select(Project).order_by(Project.created_at.desc())).scalars().all()
    return [ProjectOut.model_validate(row) for row in rows]


@router.post("", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
def create_project(payload: ProjectCreate, db: Session = Depends(get_db)) -> ProjectOut:
    row = Project(**payload.model_dump())
    db.add(row)
    db.commit()
    db.refresh(row)
    return ProjectOut.model_validate(row)


@router.get("/{project_id}", response_model=ProjectOut)
def get_project(project_id: str, db: Session = Depends(get_db)) -> ProjectOut:
    row = db.get(Project, project_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return ProjectOut.model_validate(row)


@router.patch("/{project_id}", response_model=ProjectOut)
def patch_project(project_id: str, payload: ProjectPatch, db: Session = Depends(get_db)) -> ProjectOut:
    row = db.get(Project, project_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        if field == "version":
            continue
        setattr(row, field, value)

    row.version += 1
    db.commit()
    db.refresh(row)
    return ProjectOut.model_validate(row)


@router.delete("/{project_id}")
def delete_project(project_id: str, db: Session = Depends(get_db)) -> dict:
    row = db.get(Project, project_id)
    if row is None:
        return {"deleted": False, "project_id": project_id}
    db.delete(row)
    db.commit()
    return {"deleted": True, "project_id": project_id}
