from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import re
import threading
from uuid import uuid4
from zoneinfo import ZoneInfo

import dateparser
import httpx
from msal import ConfidentialClientApplication, SerializableTokenCache
from sqlalchemy import and_, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal, engine
from app.models import (
    ApprovalRequest,
    CalendarBlock,
    EmailTriage,
    GraphConnection,
    GraphDeltaState,
    GraphSubscription,
    IntegrationOutbox,
    SyncStatus,
    Task,
)
from app.services.openai_client import OpenAIIntegrationError, is_openai_available, parse_email_triage_openai

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_BETA_BASE_URL = "https://graph.microsoft.com/beta"
MSAL_RESERVED_SCOPES = {"openid", "profile", "offline_access"}
CALENDAR_DELTA_RESOURCE = "calendar_events"
TODO_LIST_DELTA_RESOURCE = "todo_lists"
TODO_TASK_DELTA_RESOURCE = "todo_tasks"
MAIL_INBOX_DELTA_RESOURCE = "mail_inbox_messages"
OUTBOX_CALENDAR_EXPORT = "calendar.export.window"
OUTBOX_CALENDAR_DELTA = "calendar.delta"
OUTBOX_TODO_EXPORT = "todo.export"
OUTBOX_TODO_DELTA = "todo.delta"
OUTBOX_MAIL_DELTA = "mail.delta"
_GRAPH_HTTP_CLIENT: httpx.Client | None = None
_GRAPH_HTTP_CLIENT_LOCK = threading.Lock()


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


def _scope_enabled(scope_prefix: str) -> bool:
    prefix = scope_prefix.strip().lower()
    if not prefix:
        return False
    return any(scope.strip().lower().startswith(prefix) for scope in configured_scopes())



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



def _ensure_delta_state(db: Session, resource_type: str, resource_key: str = "default") -> GraphDeltaState:
    query = select(GraphDeltaState).where(
        and_(
            GraphDeltaState.resource_type == resource_type,
            GraphDeltaState.resource_key == resource_key,
        )
    )
    try:
        row = db.execute(query).scalars().first()
    except OperationalError:
        # Existing deployments can have an old DB schema before restart/migration.
        db.rollback()
        GraphDeltaState.__table__.create(bind=engine, checkfirst=True)
        row = db.execute(query).scalars().first()
    if row:
        return row

    row = GraphDeltaState(resource_type=resource_type, resource_key=resource_key)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _resolve_sync_window(
    start: datetime | None,
    end: datetime | None,
    state: GraphDeltaState,
) -> tuple[datetime, datetime]:
    if start is None:
        start = state.window_start or datetime.now(UTC) - timedelta(days=14)
    if end is None:
        end = state.window_end or (start + timedelta(days=30))
    if end <= start:
        raise ValueError("end must be later than start")
    return start, end


def _todo_status_from_local(status: str | None) -> str:
    mapping = {
        "todo": "notStarted",
        "in_progress": "inProgress",
        "done": "completed",
        "blocked": "waitingOnOthers",
        "canceled": "deferred",
    }
    return mapping.get((status or "").strip().lower(), "notStarted")


def _todo_status_to_local(status: str | None) -> str:
    mapping = {
        "notstarted": "todo",
        "inprogress": "in_progress",
        "completed": "done",
        "waitingonothers": "blocked",
        "deferred": "blocked",
    }
    return mapping.get((status or "").strip().lower(), "todo")


def _todo_priority_from_local(priority: str | None) -> str:
    value = (priority or "").strip().lower()
    if value == "critical":
        return "high"
    if value == "high":
        return "high"
    if value == "low":
        return "low"
    return "normal"


def _todo_priority_to_local(priority: str | None) -> str:
    value = (priority or "").strip().lower()
    if value == "high":
        return "high"
    if value == "low":
        return "low"
    return "medium"


def _email_sender(item: dict) -> str | None:
    sender = (item.get("from") or {}).get("emailAddress") or {}
    name = (sender.get("name") or "").strip()
    address = (sender.get("address") or "").strip()
    if name and address:
        return f"{name} <{address}>"
    return address or name or None


def _email_received_at(item: dict) -> datetime:
    dt = _parse_datetime(item.get("receivedDateTime"))
    if dt is not None:
        return dt
    return datetime.now(UTC)


def _clean_email_subject(subject: str) -> str:
    text = (subject or "").strip()
    prefixes = ("re:", "fw:", "fwd:")
    while text.lower().startswith(prefixes):
        parts = text.split(":", 1)
        text = parts[1].strip() if len(parts) == 2 else text
    return text or "메일 후속 작업"


def _is_generic_email_title(value: str) -> bool:
    lowered = (value or "").strip().lower()
    if not lowered:
        return True
    generic = {
        "메일",
        "이메일",
        "회신",
        "답장",
        "fyi",
        "notice",
        "notification",
        "update",
        "announcement",
        "meeting",
        "request",
        "문의",
        "요청",
        "알림",
    }
    return lowered in generic


_TIME_HINT_RE = re.compile(
    r"\b(?:오전|오후)\s*\d{1,2}(?:\s*시)?(?:\s*\d{1,2}\s*분)?|"
    r"\b(?:am\.?|pm\.?|a\.m\.?|p\.m\.?)\b|"
    r"\d{1,2}:\d{2}(?:\s*[ap]m)?|\d{1,2}\s*[시]\b|\d{1,2}\s*시\s*\d{1,2}\s*분|\d{1,2}\s*시\s*반",
    re.IGNORECASE,
)
_ISO_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_MIDNIGHT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T00:00(?::00)?(?:\.\d+)?(?:z|[+-]\d{2}:?\d{2})?$",
    re.IGNORECASE,
)
_KOREAN_MONTH_DAY_RE = re.compile(
    r"(?:(?P<year>\d{4})\s*년\s*)?(?P<month>\d{1,2})\s*월\s*(?P<day>\d{1,2})\s*일"
)
_TIME_KO_RE = re.compile(r"(?:오전|오후)\s*(?P<hour>\d{1,2})(?:\s*시)?(?:\s*(?P<minute>\d{1,2})\s*분|\s*반)?", re.IGNORECASE)
_TIME_RE = re.compile(
    r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?(?:\s*(?P<ampm>am|pm|a\.m\.?|p\.m\.?))?",
    re.IGNORECASE,
)
_DATE_CONTEXT_KO = {
    "오늘",
    "내일",
    "모레",
    "다음주",
    "다음 주",
    "이번주",
    "월요일",
    "화요일",
    "수요일",
    "목요일",
    "금요일",
    "토요일",
    "일요일",
}


def _contains_explicit_time(text: str | None) -> bool:
    if not text:
        return False
    return bool(_TIME_HINT_RE.search(text))


def _looks_time_missing(raw: str | None, parsed: datetime | None) -> bool:
    if not raw or parsed is None:
        return False
    if parsed.hour != 0 or parsed.minute != 0 or parsed.second != 0 or parsed.microsecond != 0:
        return False

    lowered = raw.strip().lower()
    if not _contains_explicit_time(lowered):
        return True
    if _ISO_DATE_ONLY_RE.match(lowered):
        return True
    if _ISO_MIDNIGHT_RE.match(lowered):
        return True
    return False


def _parse_time_only(raw: str, base_dt: datetime) -> datetime | None:
    text = (raw or "").strip()
    if any(token in text for token in _DATE_CONTEXT_KO):
        return None

    m = _TIME_KO_RE.search(text)
    if m:
        hour = int(m.group("hour"))
        minute = int(m.group("minute") or 0)
        if "반" in m.group(0):
            minute = 30
        if "오후" in m.group(0) and hour < 12:
            hour += 12
        local = base_dt.astimezone(ZoneInfo(settings.timezone))
        parsed = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if parsed <= local:
            parsed = parsed + timedelta(days=1)
        return parsed

    m = _TIME_RE.search(text)
    if not m:
        return None
    hour = int(m.group("hour"))
    minute = int(m.group("minute") or 0)
    ampm = (m.group("ampm") or "").lower()
    if ampm in {"pm", "p.m", "p.m."} and hour < 12:
        hour += 12
    if ampm in {"am", "a.m", "a.m."} and hour == 12:
        hour = 0

    local = base_dt.astimezone(ZoneInfo(settings.timezone))
    parsed = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if parsed <= local:
        parsed = parsed + timedelta(days=1)
    return parsed


def _parse_month_day_with_time(raw: str, base_dt: datetime) -> datetime | None:
    text = str(raw or "").strip()
    md_match = _KOREAN_MONTH_DAY_RE.search(text)
    if not md_match:
        return None

    year = md_match.group("year")
    month = int(md_match.group("month"))
    day = int(md_match.group("day"))

    if month <= 0 or month > 12 or day <= 0 or day > 31:
        return None

    ko_match = _TIME_KO_RE.search(text)
    tm_match = None
    if ko_match:
        tm_match = ko_match
        hour = int(ko_match.group("hour"))
        minute = int(ko_match.group("minute") or 0)
        if "반" in ko_match.group(0):
            minute = 30
        if "오후" in ko_match.group(0) and hour < 12:
            hour += 12
    else:
        tm_match = _TIME_RE.search(text)
        if not tm_match:
            return None
        hour = int(tm_match.group("hour"))
        minute = int(tm_match.group("minute") or 0)
        ampm = (tm_match.group("ampm") or "").lower()
        if ampm in {"pm", "p.m", "p.m."} and hour < 12:
            hour += 12
        if ampm in {"am", "a.m", "a.m."} and hour == 12:
            hour = 0

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    tz = ZoneInfo(settings.timezone)
    local_base = base_dt.astimezone(tz)
    try:
        target = datetime(
            int(year) if year else local_base.year,
            month,
            day,
            hour,
            minute,
            0,
            tzinfo=tz,
        )
    except ValueError:
        return None

    if not year and target < local_base:
        try:
            target = target.replace(year=target.year + 1)
        except ValueError:
            return None

    return target


def _extract_first_datetime_with_time_hint(text: str | None, base_dt: datetime) -> datetime | None:
    if not text:
        return None
    for candidate in re.split(r"[\n,;\|()]|\r?\n|\.", text):
        candidate = candidate.strip()
        if not candidate or not _contains_explicit_time(candidate):
            continue
        parsed = _parse_loose_datetime(candidate, base_dt, require_time=True)
        if parsed is not None:
            return parsed
    return None


def _parse_loose_datetime(
    value: str | None,
    base_dt: datetime,
    *,
    require_time: bool = False,
) -> datetime | None:
    parsed = _parse_datetime(value)
    if parsed is not None:
        if require_time and _looks_time_missing(value, parsed):
            return None
        if require_time and not _contains_explicit_time(value):
            return None
        return parsed
    if not value:
        return None
    guessed = dateparser.parse(
        value,
        languages=["ko", "en"],
        settings={
            "PREFER_DATES_FROM": "future",
            "RELATIVE_BASE": base_dt.astimezone(ZoneInfo(settings.timezone)),
            "TIMEZONE": settings.timezone,
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
    )

    if guessed is None:
        guessed = _parse_month_day_with_time(value, base_dt)

    if guessed is None:
        guessed = _parse_time_only(value, base_dt)

    if guessed is None:
        return None
    if require_time and not _contains_explicit_time(value):
        return None
    if require_time and _looks_time_missing(value, guessed):
        return None

    if guessed.tzinfo is None:
        return guessed.replace(tzinfo=ZoneInfo(settings.timezone)).astimezone(UTC)
    return guessed.astimezone(UTC)


def _fallback_email_triage(item: dict) -> dict:
    subject = _clean_email_subject(item.get("subject") or "")
    preview = (item.get("bodyPreview") or "").strip()
    sender = _email_sender(item) or ""
    received_at = _email_received_at(item)
    combined = f"{subject}\n{preview}".strip()
    lowered = combined.lower()

    no_action_tokens = [
        "unsubscribe",
        "newsletter",
        "promotion",
        "promotional",
        "광고",
        "홍보",
        "공지사항",
        "announcement",
        "fyi",
        "for your information",
        "receipt",
        "invoice",
        "영수증",
        "자동 알림",
        "do not reply",
        "noreply",
        "digest",
    ]
    task_tokens = [
        "action required",
        "please",
        "요청",
        "부탁",
        "검토",
        "작성",
        "제출",
        "회신",
        "reply",
        "respond",
        "follow up",
        "todo",
        "to-do",
        "처리",
    ]
    event_tokens = [
        "meeting",
        "미팅",
        "회의",
        "call",
        "일정",
        "schedule",
        "calendar",
        "zoom",
        "teams",
        "인터뷰",
        "약속",
    ]

    no_action_hit = any(token in lowered for token in no_action_tokens)
    task_hit = any(token in lowered for token in task_tokens)
    event_hit = any(token in lowered for token in event_tokens)

    inferred_dt = _parse_loose_datetime(combined, received_at, require_time=True)
    if inferred_dt is None and _contains_explicit_time(combined):
        inferred_dt = _extract_first_datetime_with_time_hint(combined, received_at)
    if inferred_dt is not None:
        event_hit = True

    classification = "no_action"
    confidence = 0.62 if no_action_hit else 0.45
    reason = "정보성/공지성 메일로 판단되어 자동 반영하지 않습니다."
    task_candidate = None
    event_candidate = None

    if event_hit and task_hit:
        classification = "task_and_event"
        confidence = 0.64
        reason = "업무 요청과 일정 관련 문맥이 함께 감지되었습니다."
    elif event_hit:
        classification = "event"
        confidence = 0.67
        reason = "일정/회의 관련 문맥이 감지되었습니다."
    elif task_hit:
        classification = "task"
        confidence = 0.66
        reason = "업무 요청 문맥이 감지되었습니다."

    if no_action_hit and not (task_hit or event_hit):
        classification = "no_action"

    if classification in {"task", "task_and_event"}:
        task_title = subject if not _is_generic_email_title(subject) else (preview[:90] or "메일 후속 조치")
        task_candidate = {
            "title": task_title.strip() or "메일 후속 조치",
            "description": f"[메일] {subject}\n보낸사람: {sender}\n요약: {preview}".strip(),
            "due": None,
            "priority": "medium",
        }

    if classification in {"event", "task_and_event"} and inferred_dt is not None:
        event_start = inferred_dt
        event_end = inferred_dt + timedelta(hours=1)
        event_candidate = {
            "title": subject if not _is_generic_email_title(subject) else "메일 기반 일정",
            "start": event_start.isoformat(),
            "end": event_end.isoformat(),
            "location": None,
        }

    return {
        "classification": classification,
        "reason": reason,
        "confidence": confidence,
        "task": task_candidate,
        "event": event_candidate,
    }


def _classify_email_message(item: dict) -> dict:
    subject = _clean_email_subject(item.get("subject") or "")
    preview = (item.get("bodyPreview") or "").strip()
    sender = _email_sender(item)
    received_at = _email_received_at(item)

    fallback = _fallback_email_triage(item)
    if not is_openai_available():
        return fallback

    try:
        parsed = parse_email_triage_openai(
            subject=subject,
            sender=sender,
            body_preview=preview,
            received_at=received_at,
        )
    except OpenAIIntegrationError:
        return fallback

    task_candidate = None
    event_candidate = None
    classification = parsed.classification

    task_title = (parsed.task_title or "").strip()
    if classification in {"task", "task_and_event"} and task_title:
        if _is_generic_email_title(task_title):
            task_title = subject if not _is_generic_email_title(subject) else task_title
        task_due = _parse_loose_datetime(parsed.task_due, received_at)
        task_candidate = {
            "title": task_title,
            "description": (parsed.task_description or f"[메일] {subject}\n요약: {preview}").strip(),
            "due": task_due.isoformat() if task_due else None,
            "priority": parsed.task_priority or "medium",
        }

    if classification in {"event", "task_and_event"}:
        raw_event_start = f"{subject}\n{preview}".strip()
        explicit_time = _contains_explicit_time(raw_event_start)
        event_start = _parse_loose_datetime(parsed.event_start, received_at, require_time=True)
        event_end = _parse_loose_datetime(parsed.event_end, received_at, require_time=True)
        if explicit_time and (event_start is None or _looks_time_missing(parsed.event_start, event_start)):
            event_start = _extract_first_datetime_with_time_hint(raw_event_start, received_at)
            if event_start and event_end is not None and _looks_time_missing(parsed.event_end, event_end):
                event_end = None
        if event_start and event_end is None:
            event_end = event_start + timedelta(hours=1)
        if event_start and event_end and event_end > event_start:
            title = (parsed.event_title or subject or "메일 기반 일정").strip()
            event_candidate = {
                "title": title,
                "start": event_start.isoformat(),
                "end": event_end.isoformat(),
                "location": (parsed.event_location or None),
            }

    # 분류 결과가 액션형인데 후보가 비어있으면 불명확으로 내린다.
    if classification in {"task", "event", "task_and_event"} and not (task_candidate or event_candidate):
        classification = "unclear"

    return {
        "classification": classification,
        "reason": parsed.reason,
        "confidence": float(parsed.confidence),
        "task": task_candidate,
        "event": event_candidate,
    }


def _normalized_webhook_resource() -> str:
    resource = (settings.ms_webhook_resource or "/me/events").strip()
    if not resource:
        resource = "/me/events"
    return resource.lstrip("/")


def _webhook_subscription_config_errors() -> list[str]:
    missing: list[str] = []
    url = (settings.ms_webhook_notification_url or "").strip()
    if not url:
        missing.append("MS_WEBHOOK_NOTIFICATION_URL")
    elif not url.startswith("https://"):
        missing.append("MS_WEBHOOK_NOTIFICATION_URL(https required)")
    return missing


def _ensure_graph_subscription(db: Session) -> GraphSubscription:
    resource = _normalized_webhook_resource()
    query = select(GraphSubscription).where(GraphSubscription.resource == resource).order_by(GraphSubscription.id.asc())
    try:
        row = db.execute(query).scalars().first()
    except OperationalError:
        db.rollback()
        GraphSubscription.__table__.create(bind=engine, checkfirst=True)
        row = db.execute(query).scalars().first()
    if row:
        return row

    row = GraphSubscription(
        resource=resource,
        change_type=(settings.ms_webhook_change_type or "created,updated,deleted").strip() or "created,updated,deleted",
        notification_url=(settings.ms_webhook_notification_url or "").strip() or None,
        lifecycle_url=(settings.ms_webhook_lifecycle_url or "").strip() or None,
        client_state=(settings.ms_webhook_client_state or "").strip() or None,
        status="inactive",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _ensure_outbox_table(db: Session) -> None:
    try:
        db.execute(select(IntegrationOutbox.id).limit(1))
    except OperationalError:
        db.rollback()
        IntegrationOutbox.__table__.create(bind=engine, checkfirst=True)


def _ensure_email_triage_table(db: Session) -> None:
    try:
        db.execute(select(EmailTriage.id).limit(1))
    except OperationalError:
        db.rollback()
        EmailTriage.__table__.create(bind=engine, checkfirst=True)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_graph_utc(dt: datetime) -> str:
    value = dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _next_retry_delay_seconds(retry_count: int) -> int:
    # Bounded exponential backoff for outbox retries.
    return min(3600, 2 ** min(10, max(1, retry_count)))


def _update_sync_status(
    db: Session,
    *,
    connected: bool | None = None,
    ping_success: bool = False,
    throttled: bool = False,
) -> None:
    row = _ensure_sync_status(db)
    now = datetime.now(UTC)
    changed = False

    if connected is not None:
        if row.graph_connected != connected:
            row.graph_connected = connected
            changed = True
    if ping_success:
        if row.last_delta_sync_at is None or (now - row.last_delta_sync_at).total_seconds() >= 15:
            row.last_delta_sync_at = now
            changed = True
    if throttled:
        row.last_429_at = now
        row.recent_429_count += 1
        changed = True

    if changed:
        db.commit()


def _graph_http_client() -> httpx.Client:
    global _GRAPH_HTTP_CLIENT
    if _GRAPH_HTTP_CLIENT is not None:
        return _GRAPH_HTTP_CLIENT

    with _GRAPH_HTTP_CLIENT_LOCK:
        if _GRAPH_HTTP_CLIENT is None:
            _GRAPH_HTTP_CLIENT = httpx.Client(
                http2=True,
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                timeout=httpx.Timeout(connect=5.0, read=10.0, write=12.0, pool=5.0),
            )
    return _GRAPH_HTTP_CLIENT


def _graph_request_timeout(method: str, path: str) -> httpx.Timeout:
    upper = method.upper()
    target = (path or "").lower()
    if upper == "GET" and ("calendarview" in target or "/calendar/" in target or "/me/events" in target):
        return httpx.Timeout(connect=5.0, read=8.0, write=10.0, pool=5.0)
    if upper == "GET":
        return httpx.Timeout(connect=5.0, read=9.0, write=10.0, pool=5.0)
    return httpx.Timeout(connect=5.0, read=12.0, write=12.0, pool=5.0)



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

        url = path if path.startswith("http://") or path.startswith("https://") else f"{GRAPH_BASE_URL}{path}"
        response = _graph_http_client().request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=request_headers,
            timeout=_graph_request_timeout(method, url),
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
            time.sleep(min(max(retry_after, 1), 4))
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



def webhook_status_payload(db: Session) -> dict:
    row = _ensure_graph_subscription(db)
    now = datetime.now(UTC)
    expires_at = row.expiration_at.astimezone(UTC) if row.expiration_at else None
    active = row.status == "active" and bool(expires_at and expires_at > now)
    missing = _webhook_subscription_config_errors()

    return {
        "configured": len(missing) == 0,
        "missing_settings": missing,
        "resource": row.resource,
        "change_type": row.change_type,
        "subscription_id": row.subscription_id,
        "notification_url": row.notification_url,
        "expiration_at": expires_at.isoformat() if expires_at else None,
        "status": row.status,
        "enabled": active,
        "last_notification_at": row.last_notification_at.isoformat() if row.last_notification_at else None,
        "last_error": row.last_error,
    }


def create_or_renew_event_subscription(db: Session, *, force_new: bool = False) -> dict:
    errors = _webhook_subscription_config_errors()
    if errors:
        raise GraphConfigError(f"Webhook subscription is not configured: {', '.join(errors)}")

    row = _ensure_graph_subscription(db)
    resource = _normalized_webhook_resource()
    change_type = (settings.ms_webhook_change_type or row.change_type or "created,updated,deleted").strip()
    client_state = (settings.ms_webhook_client_state or row.client_state or "").strip() or None
    notification_url = (settings.ms_webhook_notification_url or row.notification_url or "").strip()
    lifecycle_url = (settings.ms_webhook_lifecycle_url or row.lifecycle_url or notification_url).strip() or None
    ttl = max(45, min(int(settings.ms_subscription_ttl_minutes), 4230))
    expiration_at = datetime.now(UTC) + timedelta(minutes=ttl)

    if row.subscription_id and not force_new:
        try:
            result = graph_request(
                db,
                "PATCH",
                f"/subscriptions/{row.subscription_id}",
                json_body={"expirationDateTime": _to_graph_utc(expiration_at)},
            )
            row.status = "active"
            row.expiration_at = _parse_datetime(result.get("expirationDateTime")) or expiration_at
            row.last_error = None
            row.notification_url = notification_url
            row.lifecycle_url = lifecycle_url
            row.client_state = client_state
            row.change_type = change_type
            db.commit()
            db.refresh(row)
            return {
                "action": "renewed",
                "subscription_id": row.subscription_id,
                "resource": row.resource,
                "expiration_at": row.expiration_at.isoformat() if row.expiration_at else None,
                "status": row.status,
            }
        except GraphApiError as exc:
            if exc.status_code != 404:
                row.last_error = str(exc)
                db.commit()
                raise
            row.subscription_id = None
            row.status = "inactive"
            db.commit()

    payload: dict = {
        "changeType": change_type,
        "notificationUrl": notification_url,
        "resource": resource,
        "expirationDateTime": _to_graph_utc(expiration_at),
    }
    if client_state:
        payload["clientState"] = client_state
    if lifecycle_url:
        payload["lifecycleNotificationUrl"] = lifecycle_url

    result = graph_request(db, "POST", "/subscriptions", json_body=payload)
    row.subscription_id = result.get("id")
    row.resource = result.get("resource") or resource
    row.change_type = result.get("changeType") or change_type
    row.notification_url = notification_url
    row.lifecycle_url = lifecycle_url
    row.client_state = client_state
    row.expiration_at = _parse_datetime(result.get("expirationDateTime")) or expiration_at
    row.status = "active"
    row.last_error = None
    db.commit()
    db.refresh(row)

    return {
        "action": "created",
        "subscription_id": row.subscription_id,
        "resource": row.resource,
        "expiration_at": row.expiration_at.isoformat() if row.expiration_at else None,
        "status": row.status,
    }


def delete_event_subscription(db: Session) -> dict:
    row = _ensure_graph_subscription(db)
    sub_id = (row.subscription_id or "").strip()
    if sub_id:
        try:
            graph_request(db, "DELETE", f"/subscriptions/{sub_id}")
        except GraphApiError as exc:
            if exc.status_code != 404:
                row.last_error = str(exc)
                db.commit()
                raise

    row.status = "inactive"
    row.subscription_id = None
    row.expiration_at = None
    row.last_error = None
    db.commit()
    return {"deleted": True, "subscription_id": sub_id or None}


def enqueue_outbox_event(db: Session, event_type: str, payload: dict | None = None) -> dict:
    _ensure_outbox_table(db)
    row = IntegrationOutbox(
        event_type=event_type,
        status="pending",
        payload=payload or {},
        retry_count=0,
        max_retries=12,
        next_retry_at=datetime.now(UTC),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"id": row.id, "event_type": row.event_type, "status": row.status}


def _run_outbox_job(db: Session, row: IntegrationOutbox) -> dict:
    payload = row.payload or {}
    if row.event_type == OUTBOX_CALENDAR_DELTA:
        return sync_calendar_delta_to_local(
            db,
            start=_parse_datetime(payload.get("start")),
            end=_parse_datetime(payload.get("end")),
            reset=bool(payload.get("reset", False)),
        )
    if row.event_type == OUTBOX_TODO_DELTA:
        return sync_todo_delta_to_local(
            db,
            list_id=(payload.get("list_id") or None),
            reset=bool(payload.get("reset", False)),
        )
    if row.event_type == OUTBOX_CALENDAR_EXPORT:
        start = _parse_datetime(payload.get("start"))
        end = _parse_datetime(payload.get("end"))
        if start is None:
            start = datetime.now(UTC)
        if end is None:
            end = start + timedelta(days=14)
        return export_calendar_to_outlook(db, start, end)
    if row.event_type == OUTBOX_TODO_EXPORT:
        return export_tasks_to_todo(db, list_id=(payload.get("list_id") or None))
    if row.event_type == OUTBOX_MAIL_DELTA:
        return sync_mail_delta_to_local(
            db,
            reset=bool(payload.get("reset", False)),
            unread_only=bool(payload.get("unread_only", True)),
        )

    raise ValueError(f"Unsupported outbox event_type: {row.event_type}")


def process_outbox(db: Session, *, limit: int = 20) -> dict:
    _ensure_outbox_table(db)
    now = datetime.now(UTC)
    rows = db.execute(
        select(IntegrationOutbox)
        .where(
            IntegrationOutbox.status == "pending",
            (IntegrationOutbox.next_retry_at.is_(None)) | (IntegrationOutbox.next_retry_at <= now),
        )
        .order_by(IntegrationOutbox.created_at.asc(), IntegrationOutbox.id.asc())
        .limit(max(1, min(limit, 200)))
    ).scalars().all()

    processed = 0
    succeeded = 0
    failed = 0
    dead_letter = 0

    for row in rows:
        processed += 1
        row.status = "processing"
        db.commit()

        try:
            _run_outbox_job(db, row)
            row.status = "done"
            row.last_error = None
            row.processed_at = datetime.now(UTC)
            row.next_retry_at = None
            db.commit()
            succeeded += 1
        except (GraphApiError, GraphAuthError, ValueError) as exc:
            row.retry_count += 1
            row.last_error = str(exc)
            if row.retry_count >= row.max_retries:
                row.status = "dead_letter"
                row.processed_at = datetime.now(UTC)
                row.next_retry_at = None
                dead_letter += 1
            else:
                row.status = "pending"
                row.next_retry_at = datetime.now(UTC) + timedelta(seconds=_next_retry_delay_seconds(row.retry_count))
                failed += 1
            db.commit()
        except Exception as exc:  # noqa: BLE001
            row.retry_count += 1
            row.last_error = str(exc)
            if row.retry_count >= row.max_retries:
                row.status = "dead_letter"
                row.processed_at = datetime.now(UTC)
                row.next_retry_at = None
                dead_letter += 1
            else:
                row.status = "pending"
                row.next_retry_at = datetime.now(UTC) + timedelta(seconds=_next_retry_delay_seconds(row.retry_count))
                failed += 1
            db.commit()

    return {
        "processed": processed,
        "succeeded": succeeded,
        "failed": failed,
        "dead_letter": dead_letter,
    }


def process_outbox_in_new_session(limit: int = 20) -> dict:
    with SessionLocal() as db:
        return process_outbox(db, limit=limit)


def record_webhook_notifications(db: Session, notifications: list[dict]) -> dict:
    row = _ensure_graph_subscription(db)
    sync = _ensure_sync_status(db)

    accepted = 0
    rejected = 0
    queued = 0
    now = datetime.now(UTC)
    expected_state = (row.client_state or "").strip() or None

    for item in notifications:
        client_state = (item.get("clientState") or "").strip() or None
        subscription_id = (item.get("subscriptionId") or "").strip() or None
        if expected_state and client_state and client_state != expected_state:
            rejected += 1
            continue
        if row.subscription_id and subscription_id and subscription_id != row.subscription_id:
            rejected += 1
            continue

        accepted += 1
        row.last_notification_at = now
        sync.last_webhook_at = now

        resource = (item.get("resource") or row.resource or "").strip().lower()
        if "mail" in resource or "message" in resource:
            enqueue_outbox_event(db, OUTBOX_MAIL_DELTA, {"unread_only": True})
            queued += 1
        elif "todo" in resource:
            enqueue_outbox_event(db, OUTBOX_TODO_DELTA, {})
            queued += 1
        else:
            enqueue_outbox_event(db, OUTBOX_CALENDAR_DELTA, {})
            queued += 1

    db.commit()
    return {"accepted": accepted, "rejected": rejected, "queued": queued}


def record_lifecycle_notifications(db: Session, notifications: list[dict]) -> dict:
    row = _ensure_graph_subscription(db)
    sync = _ensure_sync_status(db)

    accepted = 0
    rejected = 0
    queued = 0
    events: dict[str, int] = {}
    now = datetime.now(UTC)

    for item in notifications:
        subscription_id = (item.get("subscriptionId") or "").strip() or None
        if row.subscription_id and subscription_id and subscription_id != row.subscription_id:
            rejected += 1
            continue

        accepted += 1
        row.last_notification_at = now
        sync.last_webhook_at = now

        event_name = (item.get("lifecycleEvent") or item.get("changeType") or "").strip().lower()
        events[event_name or "unknown"] = events.get(event_name or "unknown", 0) + 1

        if event_name in {"reauthorizationrequired", "missed"}:
            row.status = "stale"
            row.last_error = f"lifecycle:{event_name}"
            enqueue_outbox_event(db, OUTBOX_CALENDAR_DELTA, {"reset": False})
            enqueue_outbox_event(db, OUTBOX_TODO_DELTA, {"reset": False})
            if _scope_enabled("mail.read"):
                enqueue_outbox_event(db, OUTBOX_MAIL_DELTA, {"reset": False, "unread_only": True})
                queued += 3
            else:
                queued += 2
        elif event_name in {"subscriptionremoved", "removed"}:
            row.status = "inactive"
            row.subscription_id = None
            row.expiration_at = None
            row.last_error = f"lifecycle:{event_name}"

    db.commit()
    return {"accepted": accepted, "rejected": rejected, "queued": queued, "events": events}


def ping_me(db: Session) -> dict:
    data = graph_request(db, "GET", "/me", params={"$select": "id,displayName,userPrincipalName"})
    return {
        "ok": True,
        "me": data,
    }



def list_calendar_events(db: Session, start: datetime, end: datetime, *, max_pages: int = 80) -> list[dict]:
    if end <= start:
        raise GraphApiError(422, "end must be later than start")

    params = {
        "startDateTime": start.astimezone(UTC).isoformat(),
        "endDateTime": end.astimezone(UTC).isoformat(),
        "$top": 80,
        "$orderby": "start/dateTime",
    }
    path = "/me/calendar/calendarView"
    data: list[dict] = []
    pages_left = max(1, int(max_pages))

    for _ in range(pages_left):
        payload = graph_request(
            db,
            "GET",
            path,
            params=params,
            headers={"Prefer": f'outlook.timezone="{settings.timezone}"'},
        )
        data.extend(payload.get("value", []))

        next_link = payload.get("@odata.nextLink")
        if next_link:
            path = next_link
            params = None
            continue
        break

    if pages_left and next_link:
        raise GraphApiError(500, "Calendar view page limit exceeded")

    return data



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



def update_todo_task(db: Session, list_id: str, task_id: str, payload: dict) -> dict:
    return graph_request(db, "PATCH", f"/me/todo/lists/{list_id}/tasks/{task_id}", json_body=payload)


def delete_todo_task(db: Session, list_id: str, task_id: str) -> None:
    graph_request(db, "DELETE", f"/me/todo/lists/{list_id}/tasks/{task_id}")


def _select_default_todo_list_id(db: Session) -> str:
    lists = list_todo_lists(db)
    if not lists:
        raise GraphApiError(404, "No Microsoft To Do list is available.")

    for item in lists:
        if (item.get("wellknownListName") or "").strip() == "defaultList":
            list_id = item.get("id")
            if list_id:
                return list_id

    fallback = lists[0].get("id")
    if not fallback:
        raise GraphApiError(404, "No valid Microsoft To Do list id found.")
    return fallback


def _task_to_todo_payload(task: Task) -> dict:
    payload: dict = {
        "title": task.title,
        "status": _todo_status_from_local(task.status),
        "importance": _todo_priority_from_local(task.priority),
    }
    if task.description:
        payload["body"] = {"contentType": "text", "content": task.description}
    if task.due:
        payload["dueDateTime"] = format_graph_datetime(task.due)
    payload["linkedResources"] = [
        {
            "applicationName": "AAWO",
            "displayName": "AAWO Task",
            "externalId": task.id,
            "webUrl": "https://localhost/aawo",
        }
    ]
    return payload


def sync_task_to_todo(db: Session, task: Task, *, preferred_list_id: str | None = None) -> dict:
    list_id = (preferred_list_id or task.ms_todo_list_id or "").strip()
    if not list_id:
        list_id = _select_default_todo_list_id(db)

    payload = _task_to_todo_payload(task)
    todo_id = (task.ms_todo_task_id or "").strip()
    action = "created"

    if todo_id:
        try:
            update_todo_task(db, list_id, todo_id, payload)
            action = "updated"
        except GraphApiError as exc:
            recovered = False
            if exc.status_code in (400, 422) and "linkedResources" in payload:
                fallback_payload = dict(payload)
                fallback_payload.pop("linkedResources", None)
                try:
                    update_todo_task(db, list_id, todo_id, fallback_payload)
                    action = "updated"
                    recovered = True
                except GraphApiError:
                    pass
            if recovered:
                pass
            elif exc.status_code != 404:
                raise
            else:
                task.ms_todo_task_id = None
                todo_id = ""

    if not todo_id:
        try:
            created = create_todo_task(db, list_id, payload)
        except GraphApiError as exc:
            if exc.status_code not in (400, 422) or "linkedResources" not in payload:
                raise
            fallback_payload = dict(payload)
            fallback_payload.pop("linkedResources", None)
            created = create_todo_task(db, list_id, fallback_payload)
        new_id = created.get("id")
        if not new_id:
            raise GraphApiError(502, "To Do task creation did not return id")
        task.ms_todo_task_id = new_id

    task.ms_todo_list_id = list_id
    db.commit()
    db.refresh(task)
    return {
        "action": action,
        "task_id": task.id,
        "todo_task_id": task.ms_todo_task_id,
        "todo_list_id": task.ms_todo_list_id,
    }


def delete_task_from_todo(db: Session, task: Task) -> dict:
    task_id = (task.ms_todo_task_id or "").strip()
    list_id = (task.ms_todo_list_id or "").strip()
    if not task_id or not list_id:
        return {"deleted": False, "reason": "not_linked"}

    try:
        delete_todo_task(db, list_id, task_id)
    except GraphApiError as exc:
        if exc.status_code != 404:
            raise

    task.ms_todo_task_id = None
    task.ms_todo_list_id = None
    db.commit()
    return {"deleted": True, "todo_task_id": task_id, "todo_list_id": list_id}


def export_tasks_to_todo(db: Session, list_id: str | None = None) -> dict:
    rows = db.execute(
        select(Task).where(Task.status != "canceled").order_by(Task.updated_at.desc())
    ).scalars().all()

    created = 0
    updated = 0
    failed = 0

    for row in rows:
        try:
            result = sync_task_to_todo(db, row, preferred_list_id=list_id)
        except (GraphApiError, GraphAuthError):
            failed += 1
            continue

        if result.get("action") == "updated":
            updated += 1
        else:
            created += 1

    return {"tasks": len(rows), "created": created, "updated": updated, "failed": failed}


def _apply_calendar_event_to_local(db: Session, event: dict) -> str:
    event_id = event.get("id")
    if not event_id:
        return "skipped"

    row = db.execute(select(CalendarBlock).where(CalendarBlock.outlook_event_id == event_id)).scalars().first()
    if event.get("@removed"):
        if row is None:
            return "skipped"
        db.delete(row)
        return "deleted"

    is_cancelled = bool((event.get("isCancelled") or False))
    if is_cancelled:
        if row is None:
            return "skipped"
        db.delete(row)
        return "deleted"

    start_raw = (event.get("start") or {}).get("dateTime")
    end_raw = (event.get("end") or {}).get("dateTime")
    start_dt = parse_graph_datetime(start_raw)
    end_dt = parse_graph_datetime(end_raw)
    if not start_dt or not end_dt:
        return "skipped"

    if row is None:
        db.add(
            CalendarBlock(
                title=event.get("subject") or "Outlook Event",
                type="other",
                start=start_dt,
                end=end_dt,
                source="external",
                locked=True,
                outlook_event_id=event_id,
            )
        )
        return "created"

    row.title = event.get("subject") or row.title
    row.start = start_dt
    row.end = end_dt
    if row.source == "external":
        row.locked = True
    row.version += 1
    return "updated"


def _reconcile_calendar_events_with_remote(
    db: Session,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    remote_events = list_calendar_events(db, window_start, window_end, max_pages=160)
    remote_ids: set[str] = set()
    created = 0
    updated = 0
    skipped = 0
    deleted = 0

    for event in remote_events:
        event_id = (event.get("id") or "").strip()
        if not event_id:
            skipped += 1
            continue
        remote_ids.add(event_id)

        outcome = _apply_calendar_event_to_local(db, event)
        if outcome == "created":
            created += 1
        elif outcome == "updated":
            updated += 1
        elif outcome == "deleted":
            deleted += 1
        else:
            skipped += 1

    local_rows = db.execute(
        select(CalendarBlock).where(
            CalendarBlock.outlook_event_id.is_not(None),
            CalendarBlock.start < window_end,
            CalendarBlock.end > window_start,
        )
    ).scalars().all()

    reconciled_deleted = 0
    for row in local_rows:
        local_event_id = (row.outlook_event_id or "").strip()
        if local_event_id and local_event_id not in remote_ids:
            db.delete(row)
            reconciled_deleted += 1

    return {
        "remote_events": len(remote_events),
        "remote_created": created,
        "remote_updated": updated,
        "remote_skipped": skipped,
        "remote_deleted": deleted,
        "reconciled_deleted": reconciled_deleted,
    }


def sync_calendar_delta_to_local(
    db: Session,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    reset: bool = False,
    reconcile: bool = False,
) -> dict:
    state = _ensure_delta_state(db, CALENDAR_DELTA_RESOURCE)
    window_start, window_end = _resolve_sync_window(start, end, state)

    use_reset = reset or not state.delta_link
    if not use_reset and (start is not None or end is not None):
        if state.window_start != window_start or state.window_end != window_end:
            use_reset = True

    path = state.delta_link if not use_reset and state.delta_link else "/me/calendar/calendarView/delta"
    params = None
    if path == "/me/calendar/calendarView/delta":
        params = {
            "startDateTime": window_start.astimezone(UTC).isoformat(),
            "endDateTime": window_end.astimezone(UTC).isoformat(),
            "$select": "id,subject,start,end,isCancelled,lastModifiedDateTime",
        }

    created = 0
    updated = 0
    deleted = 0
    skipped = 0

    delta_reconcile_summary: dict[str, int] | None = None

    for _ in range(40):
        try:
            payload = graph_request(
                db,
                "GET",
                path,
                params=params,
                headers={"Prefer": f'outlook.timezone="{settings.timezone}"'},
            )
        except GraphApiError as exc:
            if exc.status_code == 410 and not reset:
                state.delta_link = None
                state.window_start = window_start
                state.window_end = window_end
                db.commit()
                return sync_calendar_delta_to_local(
                    db,
                    start=window_start,
                    end=window_end,
                    reset=True,
                    reconcile=reconcile,
                )
            raise

        for event in payload.get("value", []):
            outcome = _apply_calendar_event_to_local(db, event)
            if outcome == "created":
                created += 1
            elif outcome == "updated":
                updated += 1
            elif outcome == "deleted":
                deleted += 1
            else:
                skipped += 1

        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        if next_link:
            path = next_link
            params = None
            continue

        if reconcile:
            delta_reconcile_summary = _reconcile_calendar_events_with_remote(db, window_start, window_end)

        if delta_link:
            state.delta_link = delta_link
        state.window_start = window_start
        state.window_end = window_end
        state.last_synced_at = datetime.now(UTC)
        db.commit()
        return {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "created": created,
            "updated": updated,
            "deleted": deleted,
            "skipped": skipped,
            "reconciled": delta_reconcile_summary,
            "delta_link_saved": bool(state.delta_link),
            "reset": use_reset,
        }

    db.commit()
    raise GraphApiError(500, "Calendar delta sync page limit exceeded")


def _apply_todo_item_to_local(db: Session, list_id: str, item: dict) -> str:
    task_id = item.get("id")
    if not task_id:
        return "skipped"

    row = db.execute(select(Task).where(Task.ms_todo_task_id == task_id)).scalars().first()
    if item.get("@removed"):
        if row is None:
            return "skipped"
        if (row.source_ref or "").strip() == "graph_todo":
            db.delete(row)
            return "deleted"
        row.ms_todo_task_id = None
        row.ms_todo_list_id = None
        row.version += 1
        return "unlinked"

    due_raw = ((item.get("dueDateTime") or {}).get("dateTime"))
    due_dt = parse_graph_datetime(due_raw)
    local_status = _todo_status_to_local(item.get("status"))
    local_priority = _todo_priority_to_local(item.get("importance"))

    if row is None:
        db.add(
            Task(
                title=item.get("title") or "Microsoft To Do Task",
                description=((item.get("body") or {}).get("content")) or None,
                due=due_dt,
                status=local_status,
                source="manual",
                source_ref="graph_todo",
                ms_todo_task_id=task_id,
                ms_todo_list_id=list_id,
                priority=local_priority,
                effort_minutes=60,
            )
        )
        return "created"

    row.title = item.get("title") or row.title
    row.description = ((item.get("body") or {}).get("content")) or row.description
    row.due = due_dt
    row.status = local_status
    row.priority = local_priority
    row.ms_todo_list_id = list_id
    row.version += 1
    return "updated"


def _cleanup_removed_todo_list_local(db: Session, list_id: str) -> dict:
    rows = db.execute(select(Task).where(Task.ms_todo_list_id == list_id)).scalars().all()
    deleted = 0
    unlinked = 0

    for row in rows:
        if (row.source_ref or "").strip() == "graph_todo":
            db.delete(row)
            deleted += 1
            continue
        row.ms_todo_task_id = None
        row.ms_todo_list_id = None
        row.version += 1
        unlinked += 1

    return {"deleted": deleted, "unlinked": unlinked}


def _sync_todo_lists_delta(db: Session, *, reset: bool = False) -> dict:
    state = _ensure_delta_state(db, TODO_LIST_DELTA_RESOURCE)
    use_reset = reset or not state.delta_link or "/beta/" not in (state.delta_link or "")
    base_path = f"{GRAPH_BETA_BASE_URL}/me/todo/lists/delta"
    path = state.delta_link if not use_reset and state.delta_link else base_path
    params = None

    active_ids: set[str] = set()
    removed_ids: set[str] = set()
    changed = 0
    skipped = 0

    for _ in range(20):
        try:
            payload = graph_request(db, "GET", path, params=params)
        except GraphApiError as exc:
            if exc.status_code == 410 and not reset:
                state.delta_link = None
                state.last_synced_at = None
                db.commit()
                return _sync_todo_lists_delta(db, reset=True)
            raise

        for item in payload.get("value", []):
            list_id = item.get("id")
            if not list_id:
                skipped += 1
                continue
            if item.get("@removed"):
                removed_ids.add(list_id)
            else:
                active_ids.add(list_id)
            changed += 1

        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        if next_link:
            path = next_link
            params = None
            continue

        if delta_link:
            state.delta_link = delta_link
        state.last_synced_at = datetime.now(UTC)
        db.commit()
        return {
            "active_list_ids": sorted(active_ids),
            "removed_list_ids": sorted(removed_ids),
            "lists_changed": changed,
            "skipped": skipped,
            "delta_link_saved": bool(state.delta_link),
            "reset": use_reset,
        }

    db.commit()
    raise GraphApiError(500, "To Do list delta sync page limit exceeded")


def _sync_todo_tasks_delta_for_list(db: Session, list_id: str, *, reset: bool = False) -> dict:
    state = _ensure_delta_state(db, TODO_TASK_DELTA_RESOURCE, resource_key=list_id)
    use_reset = reset or not state.delta_link or "/beta/" not in (state.delta_link or "")
    base_path = f"{GRAPH_BETA_BASE_URL}/me/todo/lists/{list_id}/tasks/delta"
    path = state.delta_link if not use_reset and state.delta_link else base_path
    params = None

    created = 0
    updated = 0
    deleted = 0
    unlinked = 0
    skipped = 0

    for _ in range(40):
        try:
            payload = graph_request(db, "GET", path, params=params)
        except GraphApiError as exc:
            if exc.status_code == 404:
                cleanup = _cleanup_removed_todo_list_local(db, list_id)
                db.commit()
                return {
                    "list_id": list_id,
                    "created": 0,
                    "updated": 0,
                    "deleted": cleanup["deleted"],
                    "unlinked": cleanup["unlinked"],
                    "skipped": 0,
                    "removed_list": True,
                    "delta_link_saved": False,
                    "reset": use_reset,
                }
            if exc.status_code == 410 and not reset:
                state.delta_link = None
                state.last_synced_at = None
                db.commit()
                return _sync_todo_tasks_delta_for_list(db, list_id, reset=True)
            raise

        for item in payload.get("value", []):
            outcome = _apply_todo_item_to_local(db, list_id, item)
            if outcome == "created":
                created += 1
            elif outcome == "updated":
                updated += 1
            elif outcome == "deleted":
                deleted += 1
            elif outcome == "unlinked":
                unlinked += 1
            else:
                skipped += 1

        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        if next_link:
            path = next_link
            params = None
            continue

        if delta_link:
            state.delta_link = delta_link
        state.last_synced_at = datetime.now(UTC)
        db.commit()
        return {
            "list_id": list_id,
            "created": created,
            "updated": updated,
            "deleted": deleted,
            "unlinked": unlinked,
            "skipped": skipped,
            "removed_list": False,
            "delta_link_saved": bool(state.delta_link),
            "reset": use_reset,
        }

    db.commit()
    raise GraphApiError(500, "To Do task delta sync page limit exceeded")


def sync_todo_delta_to_local(db: Session, *, list_id: str | None = None, reset: bool = False) -> dict:
    removed_list_ids: list[str] = []
    per_list_results: list[dict] = []
    total_created = 0
    total_updated = 0
    total_deleted = 0
    total_unlinked = 0
    total_skipped = 0
    list_delta_result: dict | None = None

    if list_id:
        target_list_ids = [list_id]
    else:
        list_delta_result = _sync_todo_lists_delta(db, reset=reset)
        removed_list_ids = list_delta_result["removed_list_ids"]
        tracked_ids = db.execute(
            select(GraphDeltaState.resource_key).where(GraphDeltaState.resource_type == TODO_TASK_DELTA_RESOURCE)
        ).scalars().all()
        target_set = {value for value in tracked_ids if value}
        target_set.update(list_delta_result["active_list_ids"])
        target_set.difference_update(removed_list_ids)
        if not target_set:
            target_set.update([(item.get("id") or "").strip() for item in list_todo_lists(db)])
            target_set.discard("")
        target_list_ids = sorted(target_set)

    removed_cleanup_deleted = 0
    removed_cleanup_unlinked = 0
    for removed_id in removed_list_ids:
        cleanup = _cleanup_removed_todo_list_local(db, removed_id)
        removed_cleanup_deleted += cleanup["deleted"]
        removed_cleanup_unlinked += cleanup["unlinked"]
        state_row = db.execute(
            select(GraphDeltaState).where(
                and_(
                    GraphDeltaState.resource_type == TODO_TASK_DELTA_RESOURCE,
                    GraphDeltaState.resource_key == removed_id,
                )
            )
        ).scalars().first()
        if state_row is not None:
            db.delete(state_row)
    if removed_list_ids:
        db.commit()

    for target_id in target_list_ids:
        result = _sync_todo_tasks_delta_for_list(db, target_id, reset=reset)
        per_list_results.append(result)
        total_created += int(result.get("created", 0))
        total_updated += int(result.get("updated", 0))
        total_deleted += int(result.get("deleted", 0))
        total_unlinked += int(result.get("unlinked", 0))
        total_skipped += int(result.get("skipped", 0))

    total_deleted += removed_cleanup_deleted
    total_unlinked += removed_cleanup_unlinked

    return {
        "target_lists": target_list_ids,
        "removed_lists": removed_list_ids,
        "lists_synced": len(per_list_results),
        "created": total_created,
        "updated": total_updated,
        "deleted": total_deleted,
        "unlinked": total_unlinked,
        "skipped": total_skipped,
        "details": per_list_results,
        "list_delta": list_delta_result,
    }


def sync_mail_delta_to_local(
    db: Session,
    *,
    reset: bool = False,
    unread_only: bool = True,
) -> dict:
    if not _scope_enabled("mail.read"):
        raise GraphConfigError("Mail sync requires Mail.Read (or Mail.ReadWrite) scope in MS_SCOPES.")

    _ensure_email_triage_table(db)
    state = _ensure_delta_state(db, MAIL_INBOX_DELTA_RESOURCE)
    use_reset = reset or not state.delta_link
    if not use_reset and state.delta_link and "/mailfolders/" not in state.delta_link.lower():
        use_reset = True

    base_path = "/me/mailFolders/inbox/messages/delta"
    path = state.delta_link if (not use_reset and state.delta_link) else base_path
    params = None
    if path == base_path:
        params = {
            "$select": "id,subject,bodyPreview,from,receivedDateTime,isRead,internetMessageId,webLink",
            "$top": 50,
        }

    processed = 0
    created_approvals = 0
    ignored = 0
    skipped_existing = 0
    skipped_read = 0
    processed_read_actionable = 0
    removed = 0
    classification_counts: dict[str, int] = {
        "no_action": 0,
        "task": 0,
        "event": 0,
        "task_and_event": 0,
        "unclear": 0,
    }

    for _ in range(30):
        try:
            payload = graph_request(db, "GET", path, params=params)
        except GraphApiError as exc:
            if exc.status_code == 410 and not reset:
                state.delta_link = None
                state.last_synced_at = None
                db.commit()
                return sync_mail_delta_to_local(db, reset=True, unread_only=unread_only)
            raise

        values = payload.get("value", [])
        for item in values:
            if item.get("@removed"):
                removed += 1
                continue

            message_id = (item.get("id") or "").strip()
            if not message_id:
                continue

            exists = db.execute(select(EmailTriage).where(EmailTriage.ms_message_id == message_id)).scalars().first()
            if exists is not None:
                skipped_existing += 1
                continue

            # unread_only 모드에서도 읽음 메일이 일정/업무 요청으로 보이면 놓치지 않도록
            # 최소 휴리스틱으로 한 번 더 확인한다.
            if unread_only and bool(item.get("isRead")):
                quick = _fallback_email_triage(item)
                quick_class = str(quick.get("classification") or "no_action")
                quick_task = quick.get("task") if isinstance(quick.get("task"), dict) else None
                quick_event = quick.get("event") if isinstance(quick.get("event"), dict) else None
                quick_actionable = quick_class in {"task", "event", "task_and_event"} and (quick_task or quick_event)
                if not quick_actionable:
                    skipped_read += 1
                    continue
                processed_read_actionable += 1

            triage = _classify_email_message(item)
            classification = str(triage.get("classification") or "unclear")
            if classification not in classification_counts:
                classification = "unclear"
            classification_counts[classification] += 1

            task_candidate = triage.get("task") if isinstance(triage.get("task"), dict) else None
            event_candidate = triage.get("event") if isinstance(triage.get("event"), dict) else None
            actionable = classification in {"task", "event", "task_and_event"} and (task_candidate or event_candidate)

            triage_row = EmailTriage(
                ms_message_id=message_id,
                internet_message_id=(item.get("internetMessageId") or None),
                subject=_clean_email_subject(item.get("subject") or ""),
                sender=_email_sender(item),
                preview=(item.get("bodyPreview") or None),
                received_at=_email_received_at(item),
                classification=classification,
                reason=str(triage.get("reason") or ""),
                confidence=max(0.0, min(1.0, float(triage.get("confidence") or 0.0))),
                status="pending" if actionable else "ignored",
            )
            db.add(triage_row)
            db.flush()

            if actionable:
                approval_payload = {
                    "message_id": message_id,
                    "internet_message_id": item.get("internetMessageId"),
                    "subject": triage_row.subject,
                    "sender": triage_row.sender,
                    "received_at": triage_row.received_at.isoformat() if triage_row.received_at else None,
                    "classification": classification,
                    "reason": triage_row.reason,
                    "confidence": triage_row.confidence,
                    "task": task_candidate,
                    "event": event_candidate,
                    "mail_link": item.get("webLink"),
                }
                approval = ApprovalRequest(
                    type="email_intake",
                    status="pending",
                    payload=approval_payload,
                    reason="new_email_action_candidate",
                )
                db.add(approval)
                db.flush()
                triage_row.approval_id = approval.id
                created_approvals += 1
            else:
                ignored += 1

            processed += 1

        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        if next_link:
            path = next_link
            params = None
            db.commit()
            continue

        if delta_link:
            state.delta_link = delta_link
        state.last_synced_at = datetime.now(UTC)
        db.commit()
        return {
            "processed": processed,
            "created_approvals": created_approvals,
            "ignored": ignored,
            "skipped_existing": skipped_existing,
            "skipped_read": skipped_read,
            "processed_read_actionable": processed_read_actionable,
            "removed": removed,
            "classification_counts": classification_counts,
            "delta_link_saved": bool(state.delta_link),
            "reset": use_reset,
            "unread_only": unread_only,
        }

    db.commit()
    raise GraphApiError(500, "Mail delta sync page limit exceeded")


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
        local_status = _todo_status_to_local(item.get("status"))
        local_priority = _todo_priority_to_local(item.get("importance"))

        row = db.execute(select(Task).where(Task.ms_todo_task_id == task_id)).scalars().first()
        if row is None:
            row = Task(
                title=item.get("title") or "Microsoft To Do Task",
                description=((item.get("body") or {}).get("content")) or None,
                due=due_dt,
                status=local_status,
                source="manual",
                source_ref="graph_todo",
                ms_todo_task_id=task_id,
                ms_todo_list_id=list_id,
                priority=local_priority,
                effort_minutes=60,
            )
            db.add(row)
            imported += 1
            continue

        row.title = item.get("title") or row.title
        row.description = ((item.get("body") or {}).get("content")) or row.description
        row.due = due_dt
        row.status = local_status
        row.priority = local_priority
        row.ms_todo_list_id = list_id
        imported += 1

    db.commit()
    return {"imported": imported, "tasks": len(remote_tasks)}
