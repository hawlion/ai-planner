from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Task
from app.schemas import TaskCreate, TaskOut, TaskPatch
from app.services.core import add_audit, ensure_profile
from app.services.graph_service import (
    OUTBOX_TODO_EXPORT,
    GraphApiError,
    GraphAuthError,
    delete_task_from_todo,
    enqueue_outbox_event,
    is_graph_connected,
)
from app.services.learning import apply_learning_if_due, record_task_due_signal

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _queue_task_export_best_effort(db: Session) -> None:
    if not is_graph_connected(db):
        return
    try:
        enqueue_outbox_event(db, OUTBOX_TODO_EXPORT, {})
    except GraphApiError:
        pass


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
    db.flush()
    if row.due:
        profile = ensure_profile(db)
        record_task_due_signal(profile, row.due)
        apply_learning_if_due(profile)
    add_audit(
        db,
        action="task.created",
        object_ref=row.id,
        meta={"title": row.title, "priority": row.priority, "status": row.status},
    )
    db.commit()
    db.refresh(row)
    _queue_task_export_best_effort(db)
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
    if payload.version is None:
        raise HTTPException(status_code=409, detail="Task version is required")
    if payload.version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Task version conflict",
                "expected_version": row.version,
                "provided_version": payload.version,
            },
        )

    data = payload.model_dump(exclude_unset=True)
    if "project_id" in data and data["project_id"] == "":
        data["project_id"] = None

    old_due = row.due

    changed_fields: list[str] = []
    for field, value in data.items():
        if field == "version":
            continue
        setattr(row, field, value)
        changed_fields.append(field)

    row.version += 1
    if "due" in data and data["due"] != old_due and row.due is not None:
        profile = ensure_profile(db)
        record_task_due_signal(profile, row.due)
        apply_learning_if_due(profile)

    add_audit(
        db,
        action="task.updated",
        object_ref=row.id,
        meta={"changed_fields": changed_fields, "new_version": row.version, "status": row.status},
    )
    db.commit()
    _queue_task_export_best_effort(db)
    db.refresh(row)
    return TaskOut.model_validate(row)


@router.delete("/{task_id}")
def delete_task(task_id: str, version: int | None = None, db: Session = Depends(get_db)) -> dict:
    row = db.get(Task, task_id)
    if row is None:
        return {"deleted": False, "task_id": task_id}
    if version is not None and version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Task version conflict",
                "expected_version": row.version,
                "provided_version": version,
            },
        )

    if is_graph_connected(db):
        try:
            delete_task_from_todo(db, row)
        except (GraphAuthError, GraphApiError):
            db.rollback()

    add_audit(db, action="task.deleted", object_ref=row.id, meta={"title": row.title, "status": row.status})
    db.delete(row)
    db.commit()
    return {"deleted": True, "task_id": task_id}
