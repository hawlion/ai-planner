from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import GraphAuthUrlOut, GraphCalendarEventCreate, GraphStatusOut, GraphTodoTaskCreate
from app.services.graph_service import (
    GraphApiError,
    GraphAuthError,
    GraphConfigError,
    complete_auth_code,
    create_auth_url,
    create_calendar_event,
    create_todo_task,
    disconnect_graph,
    format_graph_datetime,
    import_calendar_to_local,
    import_todo_to_local,
    list_calendar_events,
    list_todo_lists,
    list_todo_tasks,
    ping_me,
    status_payload,
)

router = APIRouter(prefix="/graph", tags=["graph"])


@router.get("/auth/url", response_model=GraphAuthUrlOut)
def auth_url(db: Session = Depends(get_db)) -> GraphAuthUrlOut:
    result = create_auth_url(db)
    return GraphAuthUrlOut(
        configured=result.configured,
        auth_url=result.auth_url,
        missing_settings=result.missing_settings,
        redirect_uri=status_payload(db)["redirect_uri"],
    )


@router.get("/auth/callback")
def auth_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
    as_json: bool = False,
    db: Session = Depends(get_db),
):
    if error:
        detail = error_description or error
        if as_json:
            raise HTTPException(status_code=400, detail=detail)
        return RedirectResponse(url=f"/?graph_error={detail}", status_code=302)

    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code/state in callback")

    try:
        result = complete_auth_code(db, code, state)
    except GraphConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    if as_json:
        return result
    return RedirectResponse(url="/?graph=connected", status_code=302)


@router.get("/status", response_model=GraphStatusOut)
def graph_status(db: Session = Depends(get_db)) -> GraphStatusOut:
    data = status_payload(db)
    return GraphStatusOut(**data)


@router.post("/disconnect")
def graph_disconnect(db: Session = Depends(get_db)) -> dict:
    return disconnect_graph(db)


@router.post("/ping")
def graph_ping(db: Session = Depends(get_db)) -> dict:
    try:
        return ping_me(db)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/calendar/events")
def graph_calendar_events(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict]:
    if start is None:
        start = datetime.now(UTC)
    if end is None:
        end = start + timedelta(days=7)

    try:
        return list_calendar_events(db, start, end)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/calendar/events")
def graph_create_event(payload: GraphCalendarEventCreate, db: Session = Depends(get_db)) -> dict:
    if payload.end <= payload.start:
        raise HTTPException(status_code=422, detail="end must be later than start")

    body: dict = {
        "subject": payload.subject,
        "start": format_graph_datetime(payload.start),
        "end": format_graph_datetime(payload.end),
    }
    if payload.body:
        body["body"] = {"contentType": "text", "content": payload.body}
    if payload.location:
        body["location"] = {"displayName": payload.location}

    try:
        return create_calendar_event(db, body)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/calendar/import")
def graph_import_calendar(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    if start is None:
        start = datetime.now(UTC)
    if end is None:
        end = start + timedelta(days=14)

    try:
        return import_calendar_to_local(db, start, end)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/todo/lists")
def graph_todo_lists(db: Session = Depends(get_db)) -> list[dict]:
    try:
        return list_todo_lists(db)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/todo/lists/{list_id}/tasks")
def graph_todo_tasks(list_id: str, db: Session = Depends(get_db)) -> list[dict]:
    try:
        return list_todo_tasks(db, list_id)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/todo/lists/{list_id}/tasks")
def graph_create_todo_task(list_id: str, payload: GraphTodoTaskCreate, db: Session = Depends(get_db)) -> dict:
    body: dict = {"title": payload.title}
    if payload.body:
        body["body"] = {"contentType": "text", "content": payload.body}
    if payload.due:
        body["dueDateTime"] = format_graph_datetime(payload.due)

    try:
        return create_todo_task(db, list_id, body)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post("/todo/lists/{list_id}/import")
def graph_import_todo(list_id: str, db: Session = Depends(get_db)) -> dict:
    try:
        return import_todo_to_local(db, list_id)
    except GraphAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GraphApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
