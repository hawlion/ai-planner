from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import SyncStatus
from app.schemas import SyncStatusOut
from app.services.core import add_audit
from app.services.graph_service import (
    GraphApiError,
    GraphAuthError,
    GraphConfigError,
    _ensure_sync_status,
    create_or_renew_event_subscription,
    delete_event_subscription,
    ping_me,
    process_outbox,
    process_outbox_in_new_session,
    record_lifecycle_notifications,
    record_webhook_notifications,
    status_payload,
    sync_calendar_delta_to_local,
    sync_mail_delta_to_local,
    sync_todo_delta_to_local,
    webhook_status_payload,
)

router = APIRouter(prefix="/sync", tags=["sync"])


@router.get("/status", response_model=SyncStatusOut)
def get_sync_status(db: Session = Depends(get_db)) -> SyncStatusOut:
    row = db.get(SyncStatus, 1) or _ensure_sync_status(db)
    status = status_payload(db)
    webhook = webhook_status_payload(db)

    return SyncStatusOut(
        graph_connected=status["connected"],
        last_delta_sync_at=row.last_delta_sync_at,
        webhook={**webhook, "last_received_at": row.last_webhook_at},
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


@router.post("/calendar/delta")
def sync_calendar_delta(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    reset: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    try:
        return sync_calendar_delta_to_local(db, start=start, end=end, reset=reset)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/todo/delta")
def sync_todo_delta(
    list_id: str | None = Query(default=None),
    reset: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    try:
        return sync_todo_delta_to_local(db, list_id=list_id, reset=reset)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/mail/delta")
def sync_mail_delta(
    reset: bool = Query(default=False),
    unread_only: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    try:
        result = sync_mail_delta_to_local(db, reset=reset, unread_only=unread_only)
        add_audit(db, action="sync.mail.delta", meta=result)
        db.commit()
        return result
    except GraphConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/webhook/status")
def get_webhook_status(db: Session = Depends(get_db)) -> dict:
    return webhook_status_payload(db)


@router.post("/webhook/subscribe")
def subscribe_webhook(
    force_new: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    try:
        result = create_or_renew_event_subscription(db, force_new=force_new)
        add_audit(db, action="sync.webhook.subscribed", object_ref=result.get("subscription_id"), meta=result)
        db.commit()
        return result
    except GraphConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/webhook/renew")
def renew_webhook(db: Session = Depends(get_db)) -> dict:
    try:
        result = create_or_renew_event_subscription(db, force_new=False)
        add_audit(db, action="sync.webhook.renewed", object_ref=result.get("subscription_id"), meta=result)
        db.commit()
        return result
    except GraphConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/webhook/unsubscribe")
def unsubscribe_webhook(db: Session = Depends(get_db)) -> dict:
    try:
        result = delete_event_subscription(db)
        add_audit(db, action="sync.webhook.unsubscribed", object_ref=result.get("subscription_id"), meta=result)
        db.commit()
        return result
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/webhook/notifications")
async def webhook_notifications(
    request: Request,
    background_tasks: BackgroundTasks,
    validationToken: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    if validationToken:
        return PlainTextResponse(content=validationToken)

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}

    notifications = payload.get("value") if isinstance(payload, dict) else []
    if not isinstance(notifications, list):
        notifications = []

    result = record_webhook_notifications(db, notifications)
    if int(result.get("queued", 0)) > 0:
        background_tasks.add_task(process_outbox_in_new_session, 20)

    add_audit(db, action="sync.webhook.notification.received", meta=result)
    db.commit()
    return {"received": len(notifications), **result}


@router.get("/webhook/notifications")
def webhook_notifications_validation(validationToken: str | None = Query(default=None)):
    if validationToken is None:
        raise HTTPException(status_code=400, detail="Missing validationToken")
    return PlainTextResponse(content=validationToken)


@router.post("/webhook/lifecycle")
async def webhook_lifecycle_notifications(
    request: Request,
    background_tasks: BackgroundTasks,
    validationToken: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    if validationToken:
        return PlainTextResponse(content=validationToken)

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}

    notifications = payload.get("value") if isinstance(payload, dict) else []
    if not isinstance(notifications, list):
        notifications = []

    result = record_lifecycle_notifications(db, notifications)
    if int(result.get("queued", 0)) > 0:
        background_tasks.add_task(process_outbox_in_new_session, 20)

    add_audit(db, action="sync.webhook.lifecycle.received", meta=result)
    db.commit()
    return {"received": len(notifications), **result}


@router.get("/webhook/lifecycle")
def webhook_lifecycle_validation(validationToken: str | None = Query(default=None)):
    if validationToken is None:
        raise HTTPException(status_code=400, detail="Missing validationToken")
    return PlainTextResponse(content=validationToken)


@router.post("/outbox/process")
def process_outbox_jobs(
    limit: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> dict:
    result = process_outbox(db, limit=limit)
    add_audit(db, action="sync.outbox.processed", meta={"limit": limit, **result})
    db.commit()
    return result
