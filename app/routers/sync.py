from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import SyncStatus
from app.schemas import SyncStatusOut
from app.services.graph_connector import GraphConnector

router = APIRouter(prefix="/sync", tags=["sync"])
connector = GraphConnector()


def _ensure_sync_status(db: Session) -> SyncStatus:
    row = db.get(SyncStatus, 1)
    if row:
        return row
    row = SyncStatus(id=1, graph_connected=False, recent_429_count=0)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/status", response_model=SyncStatusOut)
def get_sync_status(db: Session = Depends(get_db)) -> SyncStatusOut:
    row = _ensure_sync_status(db)
    row.recent_429_count = connector.recent_429_count
    row.last_429_at = connector.last_429_at
    db.commit()
    db.refresh(row)

    return SyncStatusOut(
        graph_connected=row.graph_connected,
        last_delta_sync_at=row.last_delta_sync_at,
        webhook={"enabled": False, "last_received_at": row.last_webhook_at},
        throttling={"last_429_at": row.last_429_at, "recent_429_count": row.recent_429_count},
    )


@router.post("/ping")
def ping_graph(db: Session = Depends(get_db)) -> dict:
    row = _ensure_sync_status(db)
    result = connector.call_with_backoff()
    row.graph_connected = result.ok
    row.last_delta_sync_at = datetime.utcnow() if result.ok else row.last_delta_sync_at
    row.recent_429_count = connector.recent_429_count
    row.last_429_at = connector.last_429_at
    db.commit()

    return {
        "ok": result.ok,
        "status_code": result.status_code,
        "retry_after": result.retry_after,
        "recent_429_count": row.recent_429_count,
    }
