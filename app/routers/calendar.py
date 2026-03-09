from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import CalendarBlock
from app.schemas import CalendarBlockCreate, CalendarBlockOut, CalendarBlockPatch
from app.services.core import add_audit, ensure_profile
from app.services.learning import apply_learning_if_due, record_event_start_signal
from app.services.graph_service import (
    GraphApiError,
    GraphAuthError,
    OUTBOX_CALENDAR_EXPORT,
    delete_blocks_from_outlook,
    enqueue_outbox_event,
    is_graph_connected,
)

router = APIRouter(prefix="/calendar/blocks", tags=["calendar"])


def _enqueue_export_fallback(db: Session, *, anchor: CalendarBlock | None = None) -> None:
    start: datetime = datetime.now(UTC)
    end: datetime = start + timedelta(days=14)

    if anchor is not None:
        start = anchor.start
        end = anchor.end
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        start = start.astimezone(UTC) - timedelta(hours=2)
        end = end.astimezone(UTC) + timedelta(hours=2)

    try:
        enqueue_outbox_event(
            db,
            OUTBOX_CALENDAR_EXPORT,
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
    except GraphApiError:
        pass


def _sync_blocks_to_outlook_or_queue(db: Session, blocks: list[CalendarBlock]) -> None:
    if not blocks:
        return

    if not is_graph_connected(db):
        return

    _enqueue_export_fallback(db, anchor=blocks[0])


def _check_overlap(db: Session, start: datetime, end: datetime, exclude_id: str | None = None) -> None:
    stmt = select(CalendarBlock).where(and_(CalendarBlock.start < end, CalendarBlock.end > start))
    if exclude_id:
        stmt = stmt.where(CalendarBlock.id != exclude_id)
    hit = db.execute(stmt).scalars().first()
    if hit is not None:
        raise HTTPException(status_code=409, detail=f"Calendar conflict with block {hit.id}")


@router.get("", response_model=list[CalendarBlockOut])
def list_blocks(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[CalendarBlockOut]:
    stmt = select(CalendarBlock)
    if start and end:
        stmt = stmt.where(and_(CalendarBlock.start < end, CalendarBlock.end > start))
    rows = db.execute(stmt.order_by(CalendarBlock.start.asc())).scalars().all()
    return [CalendarBlockOut.model_validate(row) for row in rows]


@router.post("", response_model=CalendarBlockOut, status_code=status.HTTP_201_CREATED)
def create_block(payload: CalendarBlockCreate, db: Session = Depends(get_db)) -> CalendarBlockOut:
    if payload.end <= payload.start:
        raise HTTPException(status_code=422, detail="end must be later than start")

    _check_overlap(db, payload.start, payload.end)
    profile = ensure_profile(db)

    row = CalendarBlock(**payload.model_dump())
    db.add(row)
    db.flush()
    if row.source != "external":
        record_event_start_signal(profile, row.start)
        apply_learning_if_due(profile)

    add_audit(
        db,
        action="calendar_block.created",
        object_ref=row.id,
        meta={"title": row.title, "start": row.start.isoformat(), "end": row.end.isoformat()},
    )
    should_queue_export = row.source != "external"

    db.commit()
    db.refresh(row)
    if should_queue_export:
        _sync_blocks_to_outlook_or_queue(db, [row])
    return CalendarBlockOut.model_validate(row)


@router.get("/{block_id}", response_model=CalendarBlockOut)
def get_block(block_id: str, db: Session = Depends(get_db)) -> CalendarBlockOut:
    row = db.get(CalendarBlock, block_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Calendar block not found")
    return CalendarBlockOut.model_validate(row)


@router.patch("/{block_id}", response_model=CalendarBlockOut)
def patch_block(block_id: str, payload: CalendarBlockPatch, db: Session = Depends(get_db)) -> CalendarBlockOut:
    row = db.get(CalendarBlock, block_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Calendar block not found")
    if payload.version is None:
        raise HTTPException(status_code=409, detail="Calendar block version is required")
    if payload.version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Calendar block version conflict",
                "expected_version": row.version,
                "provided_version": payload.version,
            },
        )

    data = payload.model_dump(exclude_unset=True)
    new_start = data.get("start", row.start)
    new_end = data.get("end", row.end)
    if new_end <= new_start:
        raise HTTPException(status_code=422, detail="end must be later than start")

    if new_start != row.start or new_end != row.end:
        _check_overlap(db, new_start, new_end, exclude_id=row.id)

    profile = ensure_profile(db)
    should_record_event_start = row.source != "external" and (new_start != row.start)
    changed_fields: list[str] = []
    for field, value in data.items():
        if field == "version":
            continue
        setattr(row, field, value)
        changed_fields.append(field)
    if should_record_event_start:
        record_event_start_signal(profile, row.start)
        apply_learning_if_due(profile)

    row.version += 1
    add_audit(
        db,
        action="calendar_block.updated",
        object_ref=row.id,
        meta={"changed_fields": changed_fields, "new_version": row.version},
    )
    should_queue_export = row.source != "external"

    db.commit()
    db.refresh(row)
    if should_queue_export:
        _sync_blocks_to_outlook_or_queue(db, [row])
    return CalendarBlockOut.model_validate(row)


@router.delete("/{block_id}")
def delete_block(block_id: str, version: int | None = None, db: Session = Depends(get_db)) -> dict:
    row = db.get(CalendarBlock, block_id)
    if row is None:
        return {"deleted": False, "block_id": block_id}
    if version is not None and version != row.version:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Calendar block version conflict",
                "expected_version": row.version,
                "provided_version": version,
            },
        )

    outlook_deleted = 0
    outlook_event_id = (row.outlook_event_id or "").strip()
    if outlook_event_id:
        if not is_graph_connected(db):
            raise HTTPException(
                status_code=409,
                detail="Outlook 연동이 끊겨 일정 원본을 삭제할 수 없습니다. 다시 연결 후 삭제해 주세요.",
            )
        try:
            result = delete_blocks_from_outlook(db, [row])
        except (GraphAuthError, GraphApiError) as exc:
            raise HTTPException(status_code=502, detail=f"Outlook 일정 삭제 실패: {exc}") from exc
        if int(result.get("failed", 0)) > 0:
            raise HTTPException(status_code=502, detail="Outlook 일정 삭제에 실패했습니다. 잠시 후 다시 시도해 주세요.")
        outlook_deleted = int(result.get("deleted", 0))
    add_audit(
        db,
        action="calendar_block.deleted",
        object_ref=row.id,
        meta={"title": row.title, "outlook_deleted": outlook_deleted},
    )
    should_queue_export = not outlook_event_id and row.source != "external"
    anchor = row if should_queue_export else None
    db.delete(row)
    db.commit()
    if should_queue_export:
        _enqueue_export_fallback(db, anchor=anchor)
    return {"deleted": True, "block_id": block_id, "outlook_deleted": outlook_deleted}
