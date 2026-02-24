from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
from msal import ConfidentialClientApplication, SerializableTokenCache
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import CalendarBlock, GraphConnection, SyncStatus, Task

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
MSAL_RESERVED_SCOPES = {"openid", "profile", "offline_access"}


class GraphConfigError(RuntimeError):
    pass


class GraphAuthError(RuntimeError):
    pass


class GraphApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class GraphAuthResult:
    configured: bool
    auth_url: str | None
    missing_settings: list[str]



def configured_scopes() -> list[str]:
    return [scope.strip() for scope in settings.ms_scopes.split() if scope.strip()]


def graph_scopes() -> list[str]:
    scopes = [scope for scope in configured_scopes() if scope.lower() not in MSAL_RESERVED_SCOPES]
    return scopes or ["User.Read"]



def _missing_settings() -> list[str]:
    missing: list[str] = []
    if not settings.ms_client_id:
        missing.append("MS_CLIENT_ID")
    if not settings.ms_client_secret:
        missing.append("MS_CLIENT_SECRET")
    if not settings.ms_redirect_uri:
        missing.append("MS_REDIRECT_URI")
    return missing



def is_graph_configured() -> bool:
    return len(_missing_settings()) == 0



def _ensure_sync_status(db: Session) -> SyncStatus:
    row = db.get(SyncStatus, 1)
    if row:
        return row
    row = SyncStatus(id=1, graph_connected=False, recent_429_count=0)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row



def _update_sync_status(
    db: Session,
    *,
    connected: bool | None = None,
    ping_success: bool = False,
    throttled: bool = False,
) -> None:
    row = _ensure_sync_status(db)
    now = datetime.now(UTC)

    if connected is not None:
        row.graph_connected = connected
    if ping_success:
        row.last_delta_sync_at = now
    if throttled:
        row.last_429_at = now
        row.recent_429_count += 1

    db.commit()



def ensure_graph_connection(db: Session) -> GraphConnection:
    row = db.get(GraphConnection, 1)
    if row:
        return row

    row = GraphConnection(id=1, connected=False, token_cache="", scopes=settings.ms_scopes)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row



def _token_cache(row: GraphConnection) -> SerializableTokenCache:
    cache = SerializableTokenCache()
    if row.token_cache:
        try:
            cache.deserialize(row.token_cache)
        except Exception:  # noqa: BLE001
            cache = SerializableTokenCache()
    return cache



def _msal_app(cache: SerializableTokenCache) -> ConfidentialClientApplication:
    authority = f"https://login.microsoftonline.com/{settings.ms_tenant_id or 'common'}"
    return ConfidentialClientApplication(
        client_id=settings.ms_client_id,
        client_credential=settings.ms_client_secret,
        authority=authority,
        token_cache=cache,
    )



def create_auth_url(db: Session) -> GraphAuthResult:
    missing = _missing_settings()
    if missing:
        return GraphAuthResult(configured=False, auth_url=None, missing_settings=missing)

    row = ensure_graph_connection(db)
    cache = _token_cache(row)
    app = _msal_app(cache)

    state = uuid4().hex
    url = app.get_authorization_request_url(
        scopes=graph_scopes(),
        state=state,
        redirect_uri=settings.ms_redirect_uri,
        prompt="select_account",
    )

    row.pending_state = state
    row.scopes = settings.ms_scopes
    if cache.has_state_changed:
        row.token_cache = cache.serialize()
    db.commit()

    return GraphAuthResult(configured=True, auth_url=url, missing_settings=[])



def complete_auth_code(db: Session, code: str, state: str) -> dict:
    missing = _missing_settings()
    if missing:
        raise GraphConfigError(f"Microsoft Graph is not configured. Missing: {', '.join(missing)}")

    row = ensure_graph_connection(db)
    if not row.pending_state or row.pending_state != state:
        raise GraphAuthError("Invalid OAuth state. Please start sign-in again.")

    cache = _token_cache(row)
    app = _msal_app(cache)

    result = app.acquire_token_by_authorization_code(
        code=code,
        scopes=graph_scopes(),
        redirect_uri=settings.ms_redirect_uri,
    )

    if "access_token" not in result:
        error = result.get("error_description") or result.get("error") or "Authorization code exchange failed"
        _update_sync_status(db, connected=False)
        raise GraphAuthError(error)

    claims = result.get("id_token_claims") or {}
    accounts = app.get_accounts()
    home_account_id = accounts[0].get("home_account_id") if accounts else None

    row.connected = True
    row.pending_state = None
    row.username = claims.get("preferred_username") or claims.get("email") or row.username
    row.tenant_id = claims.get("tid") or settings.ms_tenant_id
    row.home_account_id = home_account_id
    row.scopes = settings.ms_scopes
    if cache.has_state_changed:
        row.token_cache = cache.serialize()

    db.commit()
    _update_sync_status(db, connected=True, ping_success=True)

    return {
        "connected": True,
        "username": row.username,
        "tenant_id": row.tenant_id,
        "scopes": graph_scopes(),
    }



def disconnect_graph(db: Session) -> dict:
    row = ensure_graph_connection(db)
    row.connected = False
    row.username = None
    row.tenant_id = None
    row.home_account_id = None
    row.pending_state = None
    row.token_cache = ""
    db.commit()

    _update_sync_status(db, connected=False)
    return {"connected": False}



def _acquire_access_token(db: Session, *, force_refresh: bool = False) -> str:
    row = ensure_graph_connection(db)
    if not row.connected:
        raise GraphAuthError("Microsoft account is not connected.")

    cache = _token_cache(row)
    app = _msal_app(cache)

    accounts = app.get_accounts(username=row.username) or app.get_accounts()
    if not accounts:
        raise GraphAuthError("No cached Microsoft account found. Please reconnect.")

    account = accounts[0]
    if row.home_account_id:
        for candidate in accounts:
            if candidate.get("home_account_id") == row.home_account_id:
                account = candidate
                break

    result = app.acquire_token_silent(graph_scopes(), account=account, force_refresh=force_refresh)

    if not result or "access_token" not in result:
        description = "Could not acquire access token silently. Please reconnect."
        if isinstance(result, dict):
            description = result.get("error_description") or result.get("error") or description
        raise GraphAuthError(description)

    row.connected = True
    row.username = row.username or account.get("username")
    if cache.has_state_changed:
        row.token_cache = cache.serialize()
    db.commit()

    return result["access_token"]



def graph_request(
    db: Session,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: dict | None = None,
    headers: dict | None = None,
) -> dict:
    token = _acquire_access_token(db)
    attempts = 0

    while attempts < 4:
        attempts += 1
        request_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if headers:
            request_headers.update(headers)

        with httpx.Client(timeout=30.0) as client:
            response = client.request(
                method=method,
                url=f"{GRAPH_BASE_URL}{path}",
                params=params,
                json=json_body,
                headers=request_headers,
            )

        if response.status_code == 401 and attempts == 1:
            token = _acquire_access_token(db, force_refresh=True)
            continue

        if response.status_code == 429:
            _update_sync_status(db, connected=True, throttled=True)
            retry_after = 2
            try:
                retry_after = int(response.headers.get("Retry-After", "2"))
            except ValueError:
                retry_after = 2
            time.sleep(min(max(retry_after, 1), 10))
            continue

        if response.status_code >= 400:
            _update_sync_status(db, connected=False if response.status_code in (401, 403) else None)
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001
                payload = {"error": response.text}
            raise GraphApiError(response.status_code, str(payload))

        _update_sync_status(db, connected=True, ping_success=True)

        if response.status_code == 204 or not response.content:
            return {}

        try:
            return response.json()
        except Exception:  # noqa: BLE001
            return {}

    raise GraphApiError(429, "Graph request failed repeatedly due to throttling")



def parse_graph_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    tz = ZoneInfo(settings.timezone)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)



def format_graph_datetime(dt: datetime) -> dict:
    tz = ZoneInfo(settings.timezone)
    value = dt.astimezone(tz) if dt.tzinfo else dt.replace(tzinfo=tz)
    return {
        "dateTime": value.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S"),
        "timeZone": settings.timezone,
    }



def status_payload(db: Session) -> dict:
    row = ensure_graph_connection(db)
    return {
        "configured": is_graph_configured(),
        "connected": row.connected,
        "username": row.username,
        "tenant_id": row.tenant_id,
        "scopes": configured_scopes(),
        "missing_settings": _missing_settings(),
        "redirect_uri": settings.ms_redirect_uri,
    }



def ping_me(db: Session) -> dict:
    data = graph_request(db, "GET", "/me", params={"$select": "id,displayName,userPrincipalName"})
    return {
        "ok": True,
        "me": data,
    }



def list_calendar_events(db: Session, start: datetime, end: datetime) -> list[dict]:
    data = graph_request(
        db,
        "GET",
        "/me/calendar/calendarView",
        params={
            "startDateTime": start.astimezone(UTC).isoformat(),
            "endDateTime": end.astimezone(UTC).isoformat(),
            "$top": 80,
            "$orderby": "start/dateTime",
        },
        headers={"Prefer": f'outlook.timezone="{settings.timezone}"'},
    )
    return data.get("value", [])



def create_calendar_event(db: Session, payload: dict) -> dict:
    return graph_request(db, "POST", "/me/events", json_body=payload)


def delete_calendar_event(db: Session, event_id: str) -> None:
    graph_request(db, "DELETE", f"/me/events/{event_id}")


def delete_blocks_from_outlook(db: Session, blocks: list[CalendarBlock]) -> dict:
    deleted = 0
    skipped = 0
    failed = 0

    for block in blocks:
        event_id = (block.outlook_event_id or "").strip()
        if not event_id:
            skipped += 1
            continue

        try:
            delete_calendar_event(db, event_id)
            deleted += 1
        except GraphApiError as exc:
            if exc.status_code == 404:
                deleted += 1
            else:
                failed += 1
                continue

        block.outlook_event_id = None

    db.commit()
    return {"blocks": len(blocks), "deleted": deleted, "skipped": skipped, "failed": failed}


def _block_event_payload(block: CalendarBlock) -> dict:
    payload: dict = {
        "subject": block.title or "AAWO Block",
        "start": format_graph_datetime(block.start),
        "end": format_graph_datetime(block.end),
        "body": {
            "contentType": "text",
            "content": f"Synced from AI Planner block {block.id}",
        },
    }

    if block.task_id:
        payload["categories"] = ["AAWO"]
    return payload


def is_graph_connected(db: Session) -> bool:
    row = ensure_graph_connection(db)
    return bool(row.connected)


def sync_blocks_to_outlook(db: Session, blocks: list[CalendarBlock]) -> dict:
    created = 0
    updated = 0
    skipped = 0
    synced = 0

    for block in blocks:
        if block.source == "external":
            skipped += 1
            continue

        payload = _block_event_payload(block)

        if block.outlook_event_id:
            try:
                graph_request(db, "PATCH", f"/me/events/{block.outlook_event_id}", json_body=payload)
                updated += 1
                synced += 1
                continue
            except GraphApiError as exc:
                if exc.status_code != 404:
                    raise
                block.outlook_event_id = None

        create_payload = {
            **payload,
            "transactionId": f"aawo-block-{block.id}",
        }
        event = create_calendar_event(db, create_payload)
        event_id = event.get("id")
        if event_id:
            block.outlook_event_id = event_id
            created += 1
            synced += 1
        else:
            skipped += 1

    db.commit()
    return {
        "blocks": len(blocks),
        "synced": synced,
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }


def export_calendar_to_outlook(db: Session, start: datetime, end: datetime) -> dict:
    rows = db.execute(
        select(CalendarBlock).where(
            and_(
                CalendarBlock.start < end,
                CalendarBlock.end > start,
                CalendarBlock.source != "external",
            )
        )
    ).scalars().all()

    result = sync_blocks_to_outlook(db, rows)
    return {**result, "window_start": start.isoformat(), "window_end": end.isoformat()}



def list_todo_lists(db: Session) -> list[dict]:
    data = graph_request(db, "GET", "/me/todo/lists")
    return data.get("value", [])



def list_todo_tasks(db: Session, list_id: str) -> list[dict]:
    data = graph_request(db, "GET", f"/me/todo/lists/{list_id}/tasks")
    return data.get("value", [])



def create_todo_task(db: Session, list_id: str, payload: dict) -> dict:
    return graph_request(db, "POST", f"/me/todo/lists/{list_id}/tasks", json_body=payload)



def import_calendar_to_local(db: Session, start: datetime, end: datetime) -> dict:
    events = list_calendar_events(db, start, end)
    imported = 0

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        start_raw = (event.get("start") or {}).get("dateTime")
        end_raw = (event.get("end") or {}).get("dateTime")
        start_dt = parse_graph_datetime(start_raw)
        end_dt = parse_graph_datetime(end_raw)
        if not start_dt or not end_dt:
            continue

        row = db.execute(
            select(CalendarBlock).where(CalendarBlock.outlook_event_id == event_id)
        ).scalars().first()

        if row is None:
            row = CalendarBlock(
                title=event.get("subject") or "Outlook Event",
                type="other",
                start=start_dt,
                end=end_dt,
                source="external",
                locked=True,
                outlook_event_id=event_id,
            )
            db.add(row)
            imported += 1
            continue

        row.title = event.get("subject") or row.title
        row.start = start_dt
        row.end = end_dt
        if row.source == "external":
            row.source = "external"
            row.locked = True
        imported += 1

    db.commit()
    return {"imported": imported, "events": len(events)}



def import_todo_to_local(db: Session, list_id: str) -> dict:
    remote_tasks = list_todo_tasks(db, list_id)
    imported = 0

    for item in remote_tasks:
        task_id = item.get("id")
        if not task_id:
            continue

        due_raw = ((item.get("dueDateTime") or {}).get("dateTime"))
        due_dt = parse_graph_datetime(due_raw)

        row = db.execute(select(Task).where(Task.ms_todo_task_id == task_id)).scalars().first()
        if row is None:
            row = Task(
                title=item.get("title") or "Microsoft To Do Task",
                description=((item.get("body") or {}).get("content")) or None,
                due=due_dt,
                source="manual",
                source_ref="graph_todo",
                ms_todo_task_id=task_id,
                ms_todo_list_id=list_id,
                priority="medium",
                effort_minutes=60,
            )
            db.add(row)
            imported += 1
            continue

        row.title = item.get("title") or row.title
        row.description = ((item.get("body") or {}).get("content")) or row.description
        row.due = due_dt
        row.ms_todo_list_id = list_id
        imported += 1

    db.commit()
    return {"imported": imported, "tasks": len(remote_tasks)}
