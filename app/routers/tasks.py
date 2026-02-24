from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Task
from app.schemas import TaskCreate, TaskOut, TaskPatch

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", response_model=list[TaskOut])
def list_tasks(
    status_filter: str | None = Query(default=None, alias="status"),
    due_from: datetime | None = None,
    due_to: datetime | None = None,
    db: Session = Depends(get_db),
) -> list[TaskOut]:
    stmt = select(Task)
    filters = []
    if status_filter:
        filters.append(Task.status == status_filter)
    if due_from:
        filters.append(Task.due >= due_from)
    if due_to:
        filters.append(Task.due <= due_to)
    if filters:
        stmt = stmt.where(and_(*filters))
    rows = db.execute(stmt.order_by(Task.created_at.desc())).scalars().all()
    return [TaskOut.model_validate(row) for row in rows]


@router.post("", response_model=TaskOut, status_code=status.HTTP_201_CREATED)
def create_task(payload: TaskCreate, db: Session = Depends(get_db)) -> TaskOut:
    row = Task(**payload.model_dump())
    db.add(row)
    db.commit()
    db.refresh(row)
    return TaskOut.model_validate(row)


@router.get("/{task_id}", response_model=TaskOut)
def get_task(task_id: str, db: Session = Depends(get_db)) -> TaskOut:
    row = db.get(Task, task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskOut.model_validate(row)


@router.patch("/{task_id}", response_model=TaskOut)
def patch_task(task_id: str, payload: TaskPatch, db: Session = Depends(get_db)) -> TaskOut:
    row = db.get(Task, task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Task not found")

    data = payload.model_dump(exclude_unset=True)
    if "project_id" in data and data["project_id"] == "":
        data["project_id"] = None

    for field, value in data.items():
        if field == "version":
            continue
        setattr(row, field, value)

    row.version += 1
    db.commit()
    db.refresh(row)
    return TaskOut.model_validate(row)


@router.delete("/{task_id}")
def delete_task(task_id: str, db: Session = Depends(get_db)) -> dict:
    row = db.get(Task, task_id)
    if row is None:
        return {"deleted": False, "task_id": task_id}
    db.delete(row)
    db.commit()
    return {"deleted": True, "task_id": task_id}
