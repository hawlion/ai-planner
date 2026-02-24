from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import SyncStatus
from app.schemas import SyncStatusOut
from app.services.graph_service import GraphApiError, GraphAuthError, _ensure_sync_status, ping_me, status_payload

router = APIRouter(prefix="/sync", tags=["sync"])


@router.get("/status", response_model=SyncStatusOut)
def get_sync_status(db: Session = Depends(get_db)) -> SyncStatusOut:
    row = db.get(SyncStatus, 1) or _ensure_sync_status(db)
    status = status_payload(db)

    return SyncStatusOut(
        graph_connected=status["connected"],
        last_delta_sync_at=row.last_delta_sync_at,
        webhook={"enabled": False, "last_received_at": row.last_webhook_at},
        throttling={"last_429_at": row.last_429_at, "recent_429_count": row.recent_429_count},
    )


@router.post("/ping")
def ping_graph(db: Session = Depends(get_db)) -> dict:
    try:
        result = ping_me(db)
        return {
            "ok": True,
            "status_code": 200,
            "retry_after": None,
            "recent_429_count": (_ensure_sync_status(db)).recent_429_count,
            "me": result.get("me"),
        }
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
