from __future__ import annotations

import json
from queue import Empty, Queue
import threading
from datetime import UTC, datetime, timedelta
import re
from typing import Callable
from zoneinfo import ZoneInfo

import dateparser
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal, engine, get_db
from app.models import ActionItemCandidate, ApprovalRequest, CalendarBlock, EmailTriage, Meeting, SchedulingProposal, Task
from app.schemas import AssistantActionOut, AssistantChatRequest, AssistantChatResponse
from app.services.actions import approve_candidate, reject_candidate
from app.services.core import ensure_profile
from app.services.learning import apply_learning_if_due, record_event_start_signal, record_task_due_signal
from app.services.graph_service import (
    OUTBOX_CALENDAR_EXPORT,
    OUTBOX_TODO_EXPORT,
    GraphApiError,
    GraphAuthError,
    delete_blocks_from_outlook,
    enqueue_outbox_event,
    is_graph_connected,
)
from app.services.meeting_extractor import extract_action_items
from app.services.openai_client import (
    OpenAIIntegrationError,
    extract_action_items_openai,
    is_openai_available,
    parse_assistant_plan_openai,
    parse_nli_openai,
)
from app.services.scheduler import apply_proposal, generate_proposals

router = APIRouter(prefix="/assistant", tags=["assistant"])

ChatProgressEmitter = Callable[[str, dict], None]

CONFIDENCE_THRESHOLD = 0.75
LARGE_EFFORT_MINUTES = 240
PRIORITY_MAP = {
    "긴급": "critical",
    "높음": "high",
    "중간": "medium",
    "낮음": "low",
    "critical": "critical",
    "high": "high",
    "medium": "medium",
    "low": "low",
}
PRIORITY_RANK = {"low": 1, "medium": 2, "high": 3, "critical": 4}
STATUS_RANK = {"in_progress": 5, "todo": 4, "blocked": 3, "done": 2, "canceled": 1}
YES_TOKENS = {"응", "네", "예", "승인", "확인", "좋아", "진행", "ok", "yes", "approve", "go ahead"}
NO_TOKENS = {"아니", "아니요", "거절", "취소", "중단", "안해", "no", "nope", "reject", "cancel", "stop"}
CHAT_CLARIFICATION_TYPE = "chat_clarification"
CHAT_CONFIRM_TYPE = "chat_pending_action"
CHAT_APPROVABLE_TYPES = {CHAT_CONFIRM_TYPE, "reschedule", "action_item", "email_intake"}
GENERIC_KEYWORDS = {
    "작업",
    "업무",
    "task",
    "일정",
    "할일",
    "고객",
    "회의",
    "미팅",
    "보고서",
    "준비",
}
GENERIC_EVENT_TITLES = {
    "일정",
    "스케줄",
    "캘린더",
    "미팅",
    "회의",
    "약속",
    "event",
    "calendar",
    "meeting",
    "call",
    "sync",
}
GENERIC_EVENT_FILTERS = GENERIC_EVENT_TITLES | {"미팅", "회의", "일정", "약속", "event", "meeting", "calendar", "schedule"}
GENERIC_EVENT_FILTER_KEYS = {re.sub(r"[^0-9a-zA-Z가-힣]+", "", item.lower()) for item in GENERIC_EVENT_FILTERS}
EVENT_CONTAINER_TOKENS = {"일정", "스케줄", "캘린더", "event", "calendar"}
GENERIC_TASK_TITLES = {
    "작업",
    "업무",
    "할일",
    "할 일",
    "태스크",
    "task",
    "todo",
    "to-do",
}
TITLE_DROP_TOKENS = {
    "추가",
    "등록",
    "생성",
    "만들어",
    "만들어줘",
    "만들",
    "잡아",
    "잡아줘",
    "해줘",
    "해주세요",
    "부탁",
    "좀",
    "일정",
    "스케줄",
    "캘린더",
    "event",
    "calendar",
    "할일",
    "할",
    "일",
    "태스크",
    "task",
    "todo",
    "to-do",
    "오늘",
    "내일",
    "모레",
    "이번주",
    "다음주",
    "금주",
    "오전",
    "오후",
    "저녁",
    "아침",
    "밤",
    "새벽",
    "이후",
    "부터",
    "까지",
    "에",
    "로",
    "으로",
}
REFERENCE_TOKENS = {
    "그거",
    "그 일정",
    "그 작업",
    "이거",
    "방금",
    "아까",
    "that",
    "it",
    "those",
}
CREATE_ADD_KEYWORDS = {
    "추가",
    "등록",
    "생성",
    "추가해줘",
    "추가해",
    "만들어",
    "만들어줘",
    "잡아",
    "잡아줘",
    "create",
    "add",
}
CREATE_EVENT_KEYWORDS = {
    "일정",
    "스케줄",
    "캘린더",
    "meeting",
    "calendar",
    "미팅",
    "회의",
    "약속",
    "call",
}
CREATE_TASK_KEYWORDS = {
    "할일",
    "할 일",
    "태스크",
    "task",
    "todo",
    "to-do",
    "업무",
    "작업",
}
TIME_HINT_KEYWORDS = {
    "오전",
    "오후",
    "저녁",
    "아침",
    "밤",
    "새벽",
    "today",
    "tomorrow",
    "next week",
    "this week",
    "다음주",
    "이번주",
    "오늘",
    "내일",
    "모레",
    "월요일",
    "화요일",
    "수요일",
    "목요일",
    "금요일",
    "토요일",
    "일요일",
}
EVENT_QUERY_DROP_TOKENS = TITLE_DROP_TOKENS | EVENT_CONTAINER_TOKENS | {
    "중복",
    "중복된",
    "중복으로",
    "duplicate",
    "제목",
    "이름",
    "변경",
    "변경해줘",
    "변경해주세요",
    "수정",
    "수정해줘",
    "수정해주세요",
    "이동",
    "이동해줘",
    "이동해주세요",
    "옮겨",
    "옮겨줘",
    "옮겨주세요",
    "조정",
    "조정해줘",
    "조정해주세요",
    "바꿔",
    "바꿔줘",
    "바꿔주세요",
    "바꾸어",
    "move",
    "change",
    "update",
    "reschedule",
    "shift",
    "삭제",
    "지워",
    "remove",
    "delete",
}
EVENT_TIME_CHANGE_RE = re.compile(
    r"^(?P<source>.+?)\s+(?P<dest>.+?)\s*(?:으로|로)\s*"
    r"(?:변경(?:해줘|해주세요)?|수정(?:해줘|해주세요)?|이동(?:해줘|해주세요)?|옮겨(?:줘|주세요)?|조정(?:해줘|해주세요)?|바꿔(?:줘|주세요)?)\s*$",
    re.IGNORECASE,
)
WEEKDAY_KO = {
    "월요일": 0,
    "화요일": 1,
    "수요일": 2,
    "목요일": 3,
    "금요일": 4,
    "토요일": 5,
    "일요일": 6,
    "월": 0,
    "화": 1,
    "수": 2,
    "목": 3,
    "금": 4,
    "토": 5,
    "일": 6,
}


def _queue_calendar_export_if_connected(db: Session, blocks: list[CalendarBlock]) -> bool:
    if not blocks or not is_graph_connected(db):
        return False

    start = min(block.start for block in blocks)
    end = max(block.end for block in blocks)

    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)

    try:
        enqueue_outbox_event(
            db,
            OUTBOX_CALENDAR_EXPORT,
            {
                "start": (start.astimezone(UTC) - timedelta(hours=2)).isoformat(),
                "end": (end.astimezone(UTC) + timedelta(hours=2)).isoformat(),
            },
        )
    except GraphApiError:
        return False
    return True


def _queue_todo_export_if_connected(db: Session) -> bool:
    if not is_graph_connected(db):
        return False
    try:
        enqueue_outbox_event(db, OUTBOX_TODO_EXPORT, {})
    except GraphApiError:
        return False
    return True


def _has_reference_phrase(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in REFERENCE_TOKENS)


def _is_affirmative(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if lowered in YES_TOKENS:
        return True
    if lowered.startswith("승인") or lowered.startswith("approve"):
        return True
    return any(token in lowered for token in ["승인해", "진행해", "yes", "approve", "go ahead"])


def _is_negative(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if lowered in NO_TOKENS:
        return True
    return any(token in lowered for token in ["거절", "취소", "cancel", "reject", "멈춰"])


def _extract_uuid(text: str) -> str | None:
    match = re.search(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", text.lower())
    return match.group(0) if match else None


def _looks_like_due_change(text: str) -> bool:
    lowered = text.lower()
    has_due = "마감" in text or "due" in lowered or "deadline" in lowered
    has_change = (
        any(token in text for token in ["변경", "옮겨", "바꿔", "조정", "미뤄", "당겨"])
        or any(token in lowered for token in ["change", "move", "shift"])
    )
    return has_due and has_change


def _extract_cutoff_hour(value: int | None, text: str) -> int | None:
    if isinstance(value, int) and 0 <= value <= 23:
        return value

    lowered = text.lower()

    ampm_match = re.search(r"(\d{1,2})\s*(am|pm)", lowered)
    if ampm_match:
        hour = int(ampm_match.group(1))
        if ampm_match.group(2) == "pm" and hour < 12:
            hour += 12
        if ampm_match.group(2) == "am" and hour == 12:
            hour = 0
        if 0 <= hour <= 23:
            return hour

    match = re.search(r"(\d{1,2})\s*시", text)
    if match:
        hour = int(match.group(1))
        if "오후" in text and hour < 12:
            hour += 12
        if "오전" in text and hour == 12:
            hour = 0
        if 0 <= hour <= 23:
            return hour

    after_match = re.search(r"(?:after|이후)\s*(\d{1,2})", lowered)
    if after_match:
        hour = int(after_match.group(1))
        if 0 <= hour <= 23:
            return hour

    if "저녁" in text:
        return 18
    return None


def _extract_duration_minutes_from_message(text: str) -> int | None:
    lowered = text.lower()
    raw_match = re.search(r"(\d{1,2})\s*시간\s*(\d{1,2})?\s*(?:분|분이란|분짜리)?", lowered)
    if raw_match:
        hours = int(raw_match.group(1))
        minutes = int(raw_match.group(2) or 0)
        total = hours * 60 + minutes
        if total > 0:
            return max(15, min(8 * 60, total))

    minute_match = re.search(r"(\d{1,4})\s*(?:분|min|mins|minutes)\b", lowered)
    if minute_match:
        return max(15, min(8 * 60, int(minute_match.group(1))))

    if "반" in lowered and any(token in lowered for token in CREATE_ADD_KEYWORDS):
        return 30

    return None


def _extract_relative_shift_minutes(text: str) -> int | None:
    lowered = text.lower()
    minutes = _extract_duration_minutes_from_message(text)
    if minutes is None:
        return None

    positive_tokens = [
        "늦춰",
        "늦춰줘",
        "미뤄",
        "미뤄줘",
        "연기",
        "연기해",
        "뒤로",
        "postpone",
        "delay",
        "push back",
    ]
    negative_tokens = [
        "당겨",
        "당겨줘",
        "앞당겨",
        "앞당겨줘",
        "빨리",
        "earlier",
        "pull forward",
        "move up",
    ]

    if any(token in lowered for token in positive_tokens):
        return minutes
    if any(token in lowered for token in negative_tokens):
        return -minutes
    return None


def _extract_target_duration_minutes(text: str) -> int | None:
    lowered = text.lower()
    if not any(token in lowered for token in ["으로", "로", "duration", "길이", "시간", "분"]):
        return None

    minutes = _extract_duration_minutes_from_message(text)
    if minutes is None:
        return None

    target_patterns = [
        r"\d{1,2}\s*시간\s*(?:\d{1,2}\s*분)?\s*(?:으로|로)",
        r"\d{1,4}\s*(?:분|min|mins|minutes)\s*(?:으로|로)",
        r"\d{1,2}\s*시간\s*(?:짜리|동안)",
        r"\d{1,4}\s*분\s*(?:짜리|동안)",
    ]
    if any(re.search(pattern, lowered) for pattern in target_patterns):
        return minutes
    return None


def _extract_duration_delta_minutes(text: str) -> int | None:
    lowered = text.lower()
    minutes = _extract_duration_minutes_from_message(text)
    if minutes is None:
        return None

    positive_tokens = ["연장", "늘려", "늘려줘", "길게", "extend", "longer"]
    negative_tokens = ["줄여", "줄여줘", "단축", "짧게", "shorten", "reduce"]
    if any(token in lowered for token in positive_tokens):
        return minutes
    if any(token in lowered for token in negative_tokens):
        return -minutes
    return None


def _looks_like_schedule_question(text: str) -> bool:
    lowered = text.lower()
    question_tokens = [
        "뭐야",
        "뭐지",
        "알려",
        "보여",
        "있어?",
        "있나",
        "언제",
        "몇 개",
        "what",
        "show",
        "list",
        "when",
        "free",
        "available",
    ]
    return any(token in lowered for token in question_tokens)


def _infer_schedule_fast_action(message: str) -> dict | None:
    raw = str(message or "").strip()
    if not raw:
        return None
    lowered = raw.lower()

    if any(token in lowered for token in ["중복", "duplicate"]) and any(
        token in lowered for token in ["삭제", "지워", "제거", "정리", "remove", "delete", "cleanup"]
    ) and any(token in lowered for token in ["일정", "미팅", "회의", "약속", "스케줄", "캘린더", "event", "meeting"]):
        return {
            "intent": "delete_duplicate_events",
            "task_keyword": _extract_duplicate_event_keyword(raw),
            "title": raw,
        }

    source_segment, dest_segment = _split_event_time_change_message(raw)
    if source_segment and dest_segment and _contains_datetime_phrase(dest_segment):
        return {
            "intent": "move_event",
            "title": source_segment,
            "task_keyword": source_segment,
            "start": dest_segment,
        }

    is_event_like = any(token in lowered for token in ["일정", "스케줄", "캘린더", "미팅", "회의", "약속", "event", "meeting"])
    if not is_event_like and not _extract_event_date_window(raw):
        return None

    if any(token in lowered for token in ["빈시간", "빈 시간", "비는 시간", "가용", "가능한 시간", "free time", "available"]) or (
        _looks_like_schedule_question(raw) and any(token in lowered for token in ["비어", "비었", "가능", "시간 돼", "free", "available"])
    ):
        return {
            "intent": "find_free_time",
            "target_date": raw,
            "duration_minutes": _extract_duration_minutes_from_message(raw) or 60,
        }

    if _looks_like_schedule_question(raw) and any(
        token in lowered for token in ["일정", "스케줄", "캘린더", "미팅", "회의", "agenda", "calendar", "schedule"]
    ):
        return {"intent": "list_events", "target_date": raw, "limit": 12}

    if any(token in lowered for token in ["삭제", "지워", "취소", "remove", "delete", "cancel"]) and is_event_like:
        return {"intent": "delete_event", "title": raw, "task_keyword": raw}

    duration_delta = _extract_duration_delta_minutes(raw)
    if duration_delta is not None and is_event_like:
        return {
            "intent": "update_event",
            "title": raw,
            "task_keyword": raw,
            "duration_delta_minutes": duration_delta,
        }

    target_duration = _extract_target_duration_minutes(raw)
    if target_duration is not None and is_event_like and any(
        token in lowered for token in ["변경", "수정", "조정", "바꿔", "으로", "로", "duration"]
    ):
        return {
            "intent": "update_event",
            "title": raw,
            "task_keyword": raw,
            "duration_minutes": target_duration,
        }

    relative_shift = _extract_relative_shift_minutes(raw)
    if relative_shift is not None and is_event_like:
        return {
            "intent": "move_event",
            "title": raw,
            "task_keyword": raw,
            "shift_minutes": relative_shift,
        }

    if any(token in lowered for token in ["제목", "이름", "rename", "title"]) and any(
        token in lowered for token in ["변경", "수정", "바꿔", "rename", "update"]
    ):
        return {"intent": "update_event", "title": raw, "task_keyword": raw}

    return None


def _extract_priority_from_message(text: str) -> str | None:
    lowered = text.lower()
    if "긴급" in lowered or "critical" in lowered:
        return "critical"
    if "높음" in lowered or "high" in lowered:
        return "high"
    if "중간" in lowered or "medium" in lowered:
        return "medium"
    if "낮음" in lowered or "low" in lowered:
        return "low"
    return None


def _infer_local_create_or_task_action(message: str) -> dict | None:
    lowered = message.lower()
    if not lowered.strip():
        return None

    has_add_verb = any(token in lowered for token in CREATE_ADD_KEYWORDS)
    if not has_add_verb:
        return None

    is_event_hint = any(token in lowered for token in CREATE_EVENT_KEYWORDS)
    is_task_hint = any(token in lowered for token in CREATE_TASK_KEYWORDS)
    is_time_hint = any(token in lowered for token in TIME_HINT_KEYWORDS) or _contains_datetime_phrase(message)

    # Time-stamped commands without explicit task marker are usually schedules.
    if is_event_hint or (is_time_hint and not is_task_hint):
        parsed = _parse_due(message, message)
        return {
            "intent": "create_event",
            "title": message,
            "due": parsed.isoformat() if parsed else message,
            "duration_minutes": _extract_duration_minutes_from_message(message) or 60,
        }

    if is_task_hint:
        parsed = _parse_due(message, message)
        return {
            "intent": "create_task",
            "title": message,
            "due": parsed.isoformat() if parsed else None,
            "effort_minutes": _extract_duration_minutes_from_message(message) or 60,
            "priority": _extract_priority_from_message(message) or "medium",
        }

    # Fallback: if the message only has verb + unknown subject, try to infer from intent wording first.
    if any(token in lowered for token in TIME_HINT_KEYWORDS) or is_time_hint:
        parsed = _parse_due(message, message)
        return {
            "intent": "create_event",
            "title": message,
            "due": parsed.isoformat() if parsed else message,
            "duration_minutes": _extract_duration_minutes_from_message(message) or 60,
        }

    return {
        "intent": "create_task",
        "title": message,
        "due": None,
        "effort_minutes": _extract_duration_minutes_from_message(message) or 60,
        "priority": _extract_priority_from_message(message) or "medium",
    }


def _parse_due(value: str | None, fallback_text: str) -> datetime | None:
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz=tz)

    def parse_general(text: str) -> datetime | None:
        return dateparser.parse(
            text,
            languages=["ko", "en"],
            settings={
                "PREFER_DATES_FROM": "future",
                "TIMEZONE": settings.timezone,
                "RETURN_AS_TIMEZONE_AWARE": True,
            },
        )

    def parse_korean_hint(text: str) -> datetime | None:
        lowered = text.lower()
        day_offset = None
        if "내일" in text or "tomorrow" in lowered:
            day_offset = 1
        elif "모레" in text:
            day_offset = 2
        elif "오늘" in text or "today" in lowered:
            day_offset = 0

        weekday_match = None
        for token in sorted(WEEKDAY_KO.keys(), key=len, reverse=True):
            if token in text:
                weekday_match = WEEKDAY_KO[token]
                break

        hour = 9
        minute = 0
        hm = re.search(r"(\d{1,2})\s*시(?:\s*(\d{1,2})\s*분)?", text)
        if hm:
            hour = int(hm.group(1))
            minute = int(hm.group(2) or 0)

        if "오후" in text and hour < 12:
            hour += 12
        if "오전" in text and hour == 12:
            hour = 0
        if "밤" in text and hour < 12:
            hour += 12

        if day_offset is not None:
            target = now + timedelta(days=day_offset)
            return target.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if weekday_match is not None:
            monday = now - timedelta(days=now.weekday())
            week_offset = 1 if ("다음주" in text or "next week" in lowered) else 0
            target = (monday + timedelta(days=weekday_match, weeks=week_offset)).replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )
            if target < now and week_offset == 0:
                target = target + timedelta(days=7)
            return target

        return None

    if value and value.strip():
        hinted_value = parse_korean_hint(value)
        if hinted_value is not None and _contains_datetime_phrase(value):
            return hinted_value

        parsed_value = parse_general(value)
        if parsed_value is not None:
            return parsed_value

    hint_due = parse_korean_hint(fallback_text)
    if hint_due is not None:
        return hint_due

    return parse_general(fallback_text)


def _looks_like_meeting_note(text: str) -> bool:
    lowered = text.lower()
    if "회의록" in text or "meeting notes" in lowered or "회의 내용" in text:
        return True
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    speaker_like = 0
    for line in lines:
        if ":" not in line:
            continue
        speaker = line.split(":", 1)[0].strip().lower()
        if speaker in {"추가정보", "additional", "context"}:
            continue
        if len(speaker) <= 20:
            speaker_like += 1
    return len(lines) >= 3 and speaker_like >= 2


def _to_transcript(text: str) -> list[dict]:
    cleaned = text.strip()
    for prefix in ["회의록:", "회의록", "meeting notes:", "meeting notes"]:
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix) :].strip()
            break

    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    if not lines:
        lines = [cleaned]

    transcript: list[dict] = []
    for idx, line in enumerate(lines):
        if ":" in line:
            speaker, utterance = line.split(":", 1)
            speaker = speaker.strip() or "참석자"
            utterance = utterance.strip() or line
        else:
            speaker = "참석자"
            utterance = line
        transcript.append({"ts_ms": idx * 20000, "speaker": speaker, "text": utterance})
    return transcript


def _normalize_text(value: str | None) -> str:
    text = (value or "").lower()
    return re.sub(r"[^0-9a-zA-Z가-힣]+", "", text)


def _normalize_title_key(value: str | None) -> str:
    return _normalize_text(value)


def _task_match_score(task: Task, keyword: str) -> float:
    title = task.title or ""
    description = task.description or ""
    raw = f"{title} {description}".lower()
    keyword_raw = keyword.lower()
    keyword_norm = _normalize_text(keyword)
    text_norm = _normalize_text(raw)
    title_norm = _normalize_text(title)

    score = 0.0
    if keyword_norm and keyword_norm == title_norm:
        score += 150.0
    if keyword_raw and keyword_raw in raw:
        score += 90.0
    if keyword_norm and keyword_norm in title_norm:
        score += 100.0
    elif keyword_norm and keyword_norm in text_norm:
        score += 80.0

    key_tokens = [token for token in keyword_raw.split() if len(token) >= 2]
    if key_tokens:
        hit = sum(1 for token in key_tokens if token in title.lower())
        ratio = hit / len(key_tokens)
        score += ratio * 40.0
        if len(key_tokens) >= 2 and ratio == 1.0:
            score += 35.0
    return score


def _is_generic_keyword(keyword: str) -> bool:
    tokens = [token for token in re.split(r"\s+", keyword.lower()) if token]
    if not tokens:
        return True
    if len(tokens) == 1:
        token = tokens[0]
        if token in GENERIC_KEYWORDS or len(_normalize_text(token)) <= 2:
            return True
    return False


def _find_task(
    db: Session,
    keyword: str | None,
    *,
    statuses: tuple[str, ...] = ("todo", "in_progress"),
    allow_latest_fallback: bool = True,
) -> Task | None:
    rows = db.execute(select(Task).where(Task.status.in_(list(statuses))).order_by(Task.updated_at.desc())).scalars().all()
    if not rows:
        return None

    if not keyword or not keyword.strip():
        return rows[0] if allow_latest_fallback else None

    key = keyword.strip()
    if _is_generic_keyword(key):
        return rows[0] if allow_latest_fallback else None

    key_tokens = [token for token in re.split(r"\s+", key) if token]
    allow_strict = (len(key_tokens) >= 2 or len(_normalize_text(key)) >= 6) and not _is_generic_keyword(key)
    if allow_strict:
        strict = db.execute(
            select(Task)
            .where(Task.status.in_(list(statuses)))
            .where(or_(Task.title.contains(key), Task.description.contains(key)))
            .order_by(Task.updated_at.desc())
        ).scalars().first()
        if strict:
            return strict

    scored = [(row, _task_match_score(row, key)) for row in rows]
    scored.sort(key=lambda item: item[1], reverse=True)
    if scored and scored[0][1] >= 45.0:
        return scored[0][0]
    return None


def _task_context(db: Session) -> list[dict]:
    rows = db.execute(select(Task).order_by(Task.updated_at.desc()).limit(15)).scalars().all()
    return [
        {
            "id": row.id,
            "title": row.title,
            "status": row.status,
            "priority": row.priority,
            "due": row.due.isoformat() if row.due else None,
        }
        for row in rows
    ]


def _task_context_for_message(db: Session, message: str) -> list[dict]:
    rows = db.execute(select(Task).order_by(Task.updated_at.desc()).limit(24)).scalars().all()
    if not rows:
        return []

    keyword = _extract_task_keyword(message)
    if not keyword or _is_generic_keyword(keyword):
        rows = rows[:10]
    else:
        scored = sorted(((row, _task_match_score(row, keyword)) for row in rows), key=lambda item: item[1], reverse=True)
        filtered = [row for row, score in scored if score >= 35.0][:8]
        rows = filtered or rows[:8]

    return [
        {
            "id": row.id,
            "title": row.title,
            "status": row.status,
            "priority": row.priority,
            "due": row.due.isoformat() if row.due else None,
        }
        for row in rows
    ]


def _calendar_context(db: Session) -> list[dict]:
    now = datetime.utcnow() - timedelta(days=1)
    rows = db.execute(
        select(CalendarBlock)
        .where(CalendarBlock.end >= now)
        .order_by(CalendarBlock.start.asc())
        .limit(20)
    ).scalars().all()
    return [
        {
            "id": row.id,
            "title": row.title,
            "start": row.start.isoformat() if row.start else None,
            "end": row.end.isoformat() if row.end else None,
            "source": row.source,
            "task_id": row.task_id,
        }
        for row in rows
    ]


def _calendar_context_for_message(db: Session, message: str) -> list[dict]:
    now = datetime.utcnow() - timedelta(days=2)
    horizon = datetime.utcnow() + timedelta(days=21)
    rows = db.execute(
        select(CalendarBlock)
        .where(CalendarBlock.end >= now, CalendarBlock.start <= horizon)
        .order_by(CalendarBlock.start.asc())
        .limit(48)
    ).scalars().all()
    if not rows:
        return []

    keyword = _extract_event_keyword(message)
    window = _extract_event_date_window(message)
    source_minutes = _extract_clock_minutes(message)

    if keyword or window is not None:
        scored = sorted(
            (
                (row, _event_match_score(row, keyword, window=window, source_minutes=source_minutes))
                for row in rows
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        threshold = 15.0 if window is not None and _is_generic_event_keyword(keyword) else 35.0
        filtered = [row for row, score in scored if score >= threshold][:10]
        rows = filtered or rows[:10]
    else:
        rows = rows[:10]

    return [
        {
            "id": row.id,
            "title": row.title,
            "start": row.start.isoformat() if row.start else None,
            "end": row.end.isoformat() if row.end else None,
            "source": row.source,
            "task_id": row.task_id,
        }
        for row in rows
    ]


def _pending_approval_context(db: Session) -> list[dict]:
    rows = db.execute(
        select(ApprovalRequest)
        .where(ApprovalRequest.status == "pending")
        .order_by(ApprovalRequest.created_at.desc())
        .limit(8)
    ).scalars().all()
    items: list[dict] = []
    for row in rows:
        payload = row.payload or {}
        summary = (
            str(payload.get("summary") or "").strip()
            or str(payload.get("reason") or "").strip()
            or str(row.reason or "").strip()
            or row.type
        )
        items.append({"id": row.id, "type": row.type, "summary": summary})
    return items


def _plan_actions_with_llm(
    db: Session,
    message: str,
    history_context: list[dict],
) -> tuple[list[dict], str | None]:
    plan = parse_assistant_plan_openai(
        message,
        base_dt=datetime.now(UTC),
        task_context=_task_context_for_message(db, message),
        history=history_context,
        calendar_context=_calendar_context_for_message(db, message),
        pending_approvals=_pending_approval_context(db),
    )
    return [item.model_dump() for item in plan.actions], plan.note


def _latest_pending_approval(
    db: Session,
    *,
    types: tuple[str, ...] | None = None,
    approval_id: str | None = None,
) -> ApprovalRequest | None:
    stmt = select(ApprovalRequest).where(ApprovalRequest.status == "pending")
    if types:
        stmt = stmt.where(ApprovalRequest.type.in_(list(types)))
    if approval_id:
        stmt = stmt.where(ApprovalRequest.id == approval_id)
    return db.execute(stmt.order_by(ApprovalRequest.created_at.desc())).scalars().first()


def _parse_email_approval_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(settings.timezone))
    local = parsed.astimezone(ZoneInfo(settings.timezone))
    return local


def _ensure_email_triage_table(db: Session) -> None:
    try:
        db.execute(select(EmailTriage.id).limit(1))
    except OperationalError:
        db.rollback()
        EmailTriage.__table__.create(bind=engine, checkfirst=True)


def _queue_chat_confirmation(db: Session, action: dict, summary: str, source_message: str) -> ApprovalRequest:
    approval = ApprovalRequest(
        type=CHAT_CONFIRM_TYPE,
        status="pending",
        payload={"action": action, "summary": summary, "source_message": source_message},
        reason="assistant_confirmation_required",
    )
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


def _queue_chat_clarification(db: Session, question: str, original_message: str) -> ApprovalRequest:
    approval = ApprovalRequest(
        type=CHAT_CLARIFICATION_TYPE,
        status="pending",
        payload={"question": question, "original_message": original_message},
        reason="assistant_needs_clarification",
    )
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


def _clarification_question(message: str, plan_note: str | None = None) -> str:
    if plan_note and plan_note.strip():
        return plan_note.strip()

    lowered = message.lower()
    if "재배치" in message or "reschedule" in lowered:
        return "재배치 범위를 알려주세요. 예: '내일 오후 6시 이후 일정만 재배치' 또는 '이번 주 금요일까지'."
    if "마감" in message or "due" in lowered:
        return "어떤 작업의 마감을 어떻게 바꿀지 알려주세요. 예: '분기보고서 마감을 내일 오후 5시로'."
    if "우선순위" in message or "priority" in lowered:
        return "대상 작업과 우선순위를 알려주세요. 예: '고객 제안서 우선순위를 높음으로'."
    if "삭제" in message or "remove" in lowered or "delete" in lowered:
        return "무엇을 삭제할지 구체적으로 알려주세요. 예: '중복 태스크만 정리해줘'."
    return "의도를 정확히 파악하지 못했습니다. 작업 대상과 시간/조건을 한 문장으로 더 구체적으로 알려주세요."


def _is_ambiguous_short_request(message: str) -> bool:
    lowered = message.lower()
    normalized_len = len(_normalize_text(message))
    vague_verbs = ["정리", "처리", "해줘", "부탁", "fix", "do", "handle"]
    concrete_tokens = [
        "중복",
        "우선순위",
        "마감",
        "일정",
        "회의록",
        "태스크",
        "작업",
        "재배치",
        "삭제",
        "추가",
        "duplicate",
        "priority",
        "deadline",
        "schedule",
        "task",
        "meeting",
    ]
    if normalized_len > 12:
        return False
    if not any(token in lowered for token in vague_verbs):
        return False
    return not any(token in lowered for token in concrete_tokens)


def _extract_task_keyword(raw_text: str) -> str | None:
    cleaned = raw_text
    for token in [
        "완료",
        "처리",
        "해주세요",
        "해줘",
        "우선순위",
        "priority",
        "바꿔줘",
        "변경",
        "설정",
        "으로",
        "로",
        "를",
        "을",
        "높음",
        "중간",
        "낮음",
        "긴급",
        "high",
        "medium",
        "low",
        "critical",
        "작업",
        "할일",
        ":",
    ]:
        cleaned = cleaned.replace(token, " ")

    drop_tokens = {
        "오늘",
        "내일",
        "모레",
        "이번주",
        "다음주",
        "월요일",
        "화요일",
        "수요일",
        "목요일",
        "금요일",
        "토요일",
        "일요일",
        "오전",
        "오후",
        "밤",
        "아침",
        "저녁",
        "까지",
        "마감",
    }

    parts: list[str] = []
    for token in cleaned.split():
        normalized = re.sub(r"[^\w가-힣]", "", token.lower())
        normalized = re.sub(r"(은|는|이|가|을|를|에|에서|로|으로)$", "", normalized)
        if not normalized or normalized in drop_tokens:
            continue
        parts.append(normalized)

    keyword = " ".join(parts).strip()
    return keyword if len(keyword) >= 2 else None


def _extract_clock_minutes(text: str | None) -> int | None:
    if not text:
        return None

    colon_match = re.search(r"\b(\d{1,2})\s*:\s*(\d{2})\b", text)
    if colon_match:
        hour = int(colon_match.group(1))
        minute = int(colon_match.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour * 60 + minute

    hm = re.search(r"(\d{1,2})\s*시(?:\s*(\d{1,2})\s*분?)?", text)
    if not hm:
        return None

    hour = int(hm.group(1))
    minute = int(hm.group(2) or 0)
    if "오후" in text and hour < 12:
        hour += 12
    if "오전" in text and hour == 12:
        hour = 0
    if any(token in text for token in ["저녁", "밤"]) and hour < 12:
        hour += 12
    if "새벽" in text and hour == 12:
        hour = 0
    if hour >= 24 or minute >= 60:
        return None
    return hour * 60 + minute


def _split_event_time_change_message(message: str) -> tuple[str | None, str | None]:
    cleaned = str(message or "").strip()
    if not cleaned:
        return (None, None)
    body_match = re.match(
        r"^(?P<body>.+?)\s*(?:으로|로)\s*"
        r"(?:변경(?:해줘|해주세요)?|수정(?:해줘|해주세요)?|이동(?:해줘|해주세요)?|옮겨(?:줘|주세요)?|조정(?:해줘|해주세요)?|바꿔(?:줘|주세요)?)\s*$",
        cleaned.rstrip(".?! "),
        re.IGNORECASE,
    )
    if not body_match:
        return (None, None)
    body = str(body_match.group("body") or "").strip()
    if not body:
        return (None, None)

    anchor_pattern = re.compile(
        r"(오늘|내일|모레|이번주|다음주|금주|today|tomorrow|this week|next week|"
        r"월요일|화요일|수요일|목요일|금요일|토요일|일요일|"
        r"오전|오후|아침|저녁|밤|새벽|\b(?:am|pm)\b|\d{1,2}\s*:\s*\d{2}|\d{1,2}\s*시(?:\s*\d{1,2}\s*분)?)",
        re.IGNORECASE,
    )
    for match in anchor_pattern.finditer(body):
        start = match.start()
        if start <= 0:
            continue
        source = body[:start].strip()
        dest = body[start:].strip()
        if not source or not dest:
            continue
        if _parse_due(dest, dest) is None:
            continue
        if not (_extract_event_keyword(source) or _extract_event_date_window(source) or any(token in source for token in ["일정", "미팅", "회의", "캘린더"])):
            continue
        return (source, dest)

    match = EVENT_TIME_CHANGE_RE.match(cleaned.rstrip(".?! "))
    if not match:
        return (None, None)
    source = str(match.group("source") or "").strip()
    dest = str(match.group("dest") or "").strip()
    return (source or None, dest or None)


def _extract_event_keyword(raw_text: str | None) -> str | None:
    text = _clean_candidate_title(raw_text)
    if not text:
        return None

    quoted_match = re.search(r"[\"'“”‘’]([^\"'“”‘’]{2,120})[\"'“”‘’]", text)
    if quoted_match:
        quoted = _clean_candidate_title(quoted_match.group(1))
        if quoted:
            return quoted

    tokens: list[str] = []
    for raw_token in re.split(r"\s+", text):
        trimmed = re.sub(r"^[^0-9A-Za-z가-힣]+|[^0-9A-Za-z가-힣]+$", "", raw_token)
        if not trimmed:
            continue
        lowered = _strip_korean_particle(trimmed.lower())
        if not lowered:
            continue
        if lowered in EVENT_QUERY_DROP_TOKENS:
            continue
        if lowered in {"am", "pm"} or _looks_like_datetime_token(lowered):
            continue
        tokens.append(trimmed)

    candidate = _clean_candidate_title(" ".join(tokens))
    if not candidate:
        return None
    return candidate if len(_normalize_text(candidate)) >= 2 else None


def _extract_duplicate_event_keyword(raw_text: str | None) -> str | None:
    text = str(raw_text or "").strip()
    if not text:
        return None
    cleaned = text
    for token in [
        "중복된",
        "중복으로",
        "중복",
        "duplicate",
        "삭제",
        "지워",
        "제거",
        "정리",
        "취소",
        "remove",
        "delete",
        "cleanup",
    ]:
        cleaned = re.sub(token, " ", cleaned, flags=re.IGNORECASE)
    keyword = _extract_event_keyword(cleaned)
    if not keyword:
        return None
    normalized = _normalize_text(keyword)
    if normalized in GENERIC_EVENT_FILTER_KEYS:
        return keyword
    return keyword


def _localize_dt(value: datetime) -> datetime:
    tz = ZoneInfo(settings.timezone)
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def _day_window(base: datetime) -> tuple[datetime, datetime]:
    start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    return (start, start + timedelta(days=1))


def _format_window_label(window: tuple[datetime, datetime]) -> str:
    start, end = window
    end_display = end - timedelta(seconds=1)
    if (end - start) <= timedelta(days=1):
        return start.strftime("%Y-%m-%d")
    return f"{start.strftime('%Y-%m-%d')} ~ {end_display.strftime('%Y-%m-%d')}"


def _extract_event_date_window(text: str | None) -> tuple[datetime, datetime] | None:
    if not text:
        return None

    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz=tz)
    lowered = text.lower()

    if "오늘" in text or "today" in lowered:
        return _day_window(now)
    if "내일" in text or "tomorrow" in lowered:
        return _day_window(now + timedelta(days=1))
    if "모레" in text:
        return _day_window(now + timedelta(days=2))

    week_offset = 1 if ("다음주" in text or "next week" in lowered) else 0
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    weekday_match = None
    for token in sorted(WEEKDAY_KO.keys(), key=len, reverse=True):
        if token in text:
            weekday_match = WEEKDAY_KO[token]
            break

    if weekday_match is not None:
        target = monday + timedelta(days=weekday_match, weeks=week_offset)
        if week_offset == 0 and not any(token in text for token in ["이번주", "금주"]) and "this week" not in lowered:
            if target.date() < now.date():
                target += timedelta(days=7)
        return _day_window(target)

    if any(token in text for token in ["이번주", "금주"]) or "this week" in lowered or week_offset:
        start = monday + timedelta(weeks=week_offset)
        return (start, start + timedelta(days=7))

    if _contains_datetime_phrase(text):
        parsed = _parse_due(text, text)
        if parsed is not None:
            return _day_window(_localize_dt(parsed))
    return None


def _is_generic_event_keyword(keyword: str | None) -> bool:
    tokens = [token for token in re.split(r"\s+", str(keyword or "").lower()) if token]
    if not tokens:
        return True
    if len(tokens) == 1:
        token = tokens[0]
        if token in GENERIC_EVENT_TITLES or len(_normalize_text(token)) <= 2:
            return True
    return False


def _event_match_score(
    block: CalendarBlock,
    keyword: str | None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> float:
    local_start = _localize_dt(block.start)
    local_end = _localize_dt(block.end)
    if window is not None:
        window_start, window_end = window
        if local_start >= window_end or local_end <= window_start:
            return -1.0

    score = 0.0
    if window is not None:
        score += 55.0

    title = block.title or ""
    title_raw = title.lower()
    title_norm = _normalize_text(title)

    if keyword:
        keyword_raw = keyword.lower()
        keyword_norm = _normalize_text(keyword)
        if keyword_norm and keyword_norm == title_norm:
            score += 180.0
        if keyword_raw and keyword_raw in title_raw:
            score += 100.0
        if keyword_norm and keyword_norm in title_norm:
            score += 120.0

        key_tokens = [token for token in keyword_raw.split() if len(token) >= 2]
        if key_tokens:
            hit = sum(1 for token in key_tokens if token in title_raw)
            ratio = hit / len(key_tokens)
            score += ratio * 45.0
            if len(key_tokens) >= 2 and ratio == 1.0:
                score += 25.0
    elif window is None:
        return -1.0

    if source_minutes is not None:
        start_minutes = local_start.hour * 60 + local_start.minute
        diff = abs(start_minutes - source_minutes)
        if diff <= 10:
            score += 45.0
        elif diff <= 30:
            score += 35.0
        elif diff <= 90:
            score += 20.0
        elif diff <= 180:
            score += 8.0
        else:
            score -= min(24.0, diff / 30.0)

    return score


def _search_events(
    db: Session,
    keyword: str | None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> list[tuple[CalendarBlock, float]]:
    rows = db.execute(select(CalendarBlock).order_by(CalendarBlock.start.asc())).scalars().all()
    if not rows:
        return []

    generic_keyword = _is_generic_event_keyword(keyword)
    matches: list[tuple[CalendarBlock, float]] = []
    for row in rows:
        score = _event_match_score(row, keyword, window=window, source_minutes=source_minutes)
        if score < 0:
            continue
        if keyword and not generic_keyword and score < 45.0:
            continue
        if keyword and generic_keyword and window is None and score < 70.0:
            continue
        matches.append((row, score))

    matches.sort(
        key=lambda item: (
            item[1],
            _localize_dt(item[0].start).timestamp() * -1,
        ),
        reverse=True,
    )
    return matches


def _resolve_event_match(
    db: Session,
    keyword: str | None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> tuple[CalendarBlock | None, str | None]:
    matches = _search_events(db, keyword, window=window, source_minutes=source_minutes)
    if not matches:
        return (None, "not_found")
    if len(matches) == 1:
        return (matches[0][0], None)

    top_row, top_score = matches[0]
    second_score = matches[1][1]
    generic_keyword = _is_generic_event_keyword(keyword)
    if not generic_keyword and (top_score >= 150.0 or top_score - second_score >= 12.0):
        return (top_row, None)
    if window is not None and source_minutes is not None and top_score - second_score >= 15.0:
        return (top_row, None)
    if window is not None and not generic_keyword and top_score - second_score >= 8.0:
        return (top_row, None)
    return (None, "ambiguous")


def _resolve_event_lookup(
    action: dict,
    message: str,
    *,
    prefer_source_segment: bool = False,
) -> tuple[str | None, tuple[datetime, datetime] | None, int | None]:
    base_text = str(action.get("task_keyword") or action.get("title") or "").strip()
    source_segment, _ = _split_event_time_change_message(message)

    keyword_candidates: list[str] = []
    if base_text:
        keyword_candidates.append(base_text)
    if source_segment:
        keyword_candidates.append(source_segment)
    if not prefer_source_segment and message.strip():
        keyword_candidates.append(message)

    window_candidates: list[str] = []
    if source_segment:
        window_candidates.append(source_segment)
    if base_text:
        window_candidates.append(base_text)
    if not prefer_source_segment and message.strip():
        window_candidates.append(message)

    keyword = next((resolved for candidate in keyword_candidates if (resolved := _extract_event_keyword(candidate))), None)
    window = next((resolved for candidate in window_candidates if (resolved := _extract_event_date_window(candidate))), None)
    source_minutes = _extract_clock_minutes(source_segment or base_text)
    return (keyword, window, source_minutes)


def _strip_korean_particle(token: str) -> str:
    return re.sub(r"(은|는|이|가|을|를|에|에게|와|과|도|로|으로|부터|까지|에서)$", "", token)


def _contains_datetime_phrase(text: str) -> bool:
    lowered = text.lower()
    if re.search(r"\d{1,2}\s*:\s*\d{2}", lowered):
        return True
    if re.search(r"\d{1,2}\s*시(?:\s*\d{1,2}\s*분)?", text):
        return True
    if any(token in lowered for token in ["today", "tomorrow", "next week", "this week"]):
        return True
    if re.search(r"\b(am|pm)\b", lowered):
        return True
    if any(token in text for token in ["오늘", "내일", "모레", "이번주", "다음주", "오전", "오후", "아침", "저녁", "밤", "새벽"]):
        return True
    return any(token in text for token in ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"])


def _clean_candidate_title(raw_title: str | None) -> str:
    title = str(raw_title or "").replace("\n", " ").strip()
    if not title:
        return ""

    title = re.sub(r"\s+", " ", title)
    title = title.strip(" '\"`[](){}.,:;")
    # Remove command-like suffix to keep only the semantic title.
    title = re.sub(
        r"(?:일정|스케줄|캘린더|event|calendar)\s*(?:추가|등록|생성|만들어줘|만들어|만들|잡아줘|잡아)$",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    title = re.sub(
        r"(?:할일|할 일|태스크|task|todo|to-do)\s*(?:추가|등록|생성|만들어줘|만들어|만들)$",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:120].strip()


def _looks_like_datetime_token(token: str) -> bool:
    if not token:
        return False
    if token in WEEKDAY_KO or token.endswith("요일"):
        return True
    if token in {"오늘", "내일", "모레", "이번주", "다음주", "금주", "today", "tomorrow"}:
        return True
    if re.fullmatch(r"\d{1,2}(?::\d{2})?", token):
        return True
    if re.fullmatch(r"(?:am|pm)?\d{1,2}(?::\d{2})?(?:am|pm)?", token.lower()):
        return True
    if re.fullmatch(r"(?:오전|오후|아침|저녁|밤|새벽)?\d{1,2}시(?:\d{1,2}분?)?", token):
        return True
    return bool(re.search(r"\d{1,2}\s*시|\d{1,2}\s*:\s*\d{2}", token))


def _extract_title_from_message(message: str, *, intent: str) -> str | None:
    text = _clean_candidate_title(message)
    if not text:
        return None

    quoted_match = re.search(r"[\"'“”‘’]([^\"'“”‘’]{2,120})[\"'“”‘’]", text)
    if quoted_match:
        quoted = _clean_candidate_title(quoted_match.group(1))
        if quoted:
            return quoted

    tokens: list[str] = []
    for raw_token in re.split(r"\s+", text):
        trimmed = re.sub(r"^[^0-9A-Za-z가-힣]+|[^0-9A-Za-z가-힣]+$", "", raw_token)
        if not trimmed:
            continue

        lowered = _strip_korean_particle(trimmed.lower())
        if not lowered:
            continue
        if lowered in TITLE_DROP_TOKENS:
            continue
        if lowered in {"am", "pm"} or _looks_like_datetime_token(lowered):
            continue

        tokens.append(trimmed)

    candidate = _clean_candidate_title(" ".join(tokens))
    if not candidate:
        return None

    if intent == "create_task":
        candidate = re.sub(r"^(?:할일|할 일|task|todo)\s+", "", candidate, flags=re.IGNORECASE).strip()

    return candidate or None


def _is_ambiguous_creation_title(title: str | None, *, intent: str) -> bool:
    cleaned = _clean_candidate_title(title)
    if not cleaned:
        return True

    tokens: list[str] = []
    for raw in re.split(r"\s+", cleaned):
        lowered = _strip_korean_particle(re.sub(r"[^0-9A-Za-z가-힣]+", "", raw.lower()))
        if not lowered:
            continue
        if lowered in TITLE_DROP_TOKENS:
            continue
        if _looks_like_datetime_token(lowered):
            continue
        if intent == "create_event" and lowered in {"일정", "스케줄", "캘린더", "event", "calendar"}:
            continue
        if intent == "create_task" and lowered in {"작업", "업무", "할일", "할", "일", "태스크", "task", "todo", "to-do"}:
            continue
        tokens.append(lowered)

    if not tokens:
        return True

    if len(tokens) == 1:
        token = tokens[0]
        generic_pool = GENERIC_EVENT_TITLES if intent == "create_event" else GENERIC_TASK_TITLES
        if token in generic_pool:
            return True
        if len(_normalize_text(token)) <= 2:
            return True

    return False


def _title_quality_score(title: str, *, intent: str) -> int:
    cleaned = _clean_candidate_title(title)
    if not cleaned:
        return -999

    score = 0
    if not _is_ambiguous_creation_title(cleaned, intent=intent):
        score += 50

    token_count = len([token for token in cleaned.split() if token.strip()])
    if 1 <= token_count <= 5:
        score += 10
    elif token_count >= 9:
        score -= 10

    if len(cleaned) <= 30:
        score += 8
    elif len(cleaned) >= 50:
        score -= 12

    lowered = cleaned.lower()
    if any(token in lowered for token in ["추가", "등록", "생성", "만들", "잡아", "해줘", "해주세요"]):
        score -= 24
    if _contains_datetime_phrase(cleaned):
        score -= 20
    return score


def _resolve_creation_title(action: dict, message: str, *, intent: str) -> str | None:
    raw_candidates = [
        str(action.get("title") or "").strip(),
        str(action.get("task_keyword") or "").strip(),
        _extract_title_from_message(message, intent=intent) or "",
    ]

    best_title: str | None = None
    best_score = -999
    seen: set[str] = set()

    for raw in raw_candidates:
        cleaned = _clean_candidate_title(raw)
        if not cleaned:
            continue
        normalized = _normalize_text(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)

        score = _title_quality_score(cleaned, intent=intent)
        if score > best_score:
            best_title = cleaned
            best_score = score

    return best_title


def _extract_titles_from_assistant_text(text: str) -> list[str]:
    markers = [
        "할일을 생성했습니다:",
        "완료 처리했습니다:",
        "이미 완료 상태입니다:",
        "우선순위를 변경했습니다:",
        "마감일을 변경했습니다:",
    ]
    titles: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"^\d+\.\s*", "", raw_line.strip())
        if not line:
            continue
        for marker in markers:
            if marker not in line:
                continue
            value = line.split(marker, 1)[1].strip()
            value = value.split("->", 1)[0].strip()
            value = value.split("(요청:", 1)[0].strip()
            if value:
                titles.append(value)
            break
    return titles


def _infer_keyword_from_history(
    db: Session,
    message: str,
    history: list[dict],
    *,
    statuses: tuple[str, ...],
) -> str | None:
    has_reference = _has_reference_phrase(message)
    if not has_reference and len(_normalize_text(message)) > 6:
        return None

    for turn in reversed(history[-12:]):
        role = str(turn.get("role") or "").strip().lower()
        text = str(turn.get("text") or "").strip()
        if role not in {"user", "assistant"} or not text:
            continue

        candidates: list[str] = []
        if role == "assistant":
            candidates.extend(_extract_titles_from_assistant_text(text))
        else:
            keyword = _extract_task_keyword(text)
            if keyword:
                candidates.append(keyword)

        for candidate in candidates:
            keyword = candidate.strip()
            if not keyword or _is_generic_keyword(keyword):
                continue
            matched = _find_task(
                db,
                keyword,
                statuses=statuses,
                allow_latest_fallback=False,
            )
            if matched:
                return keyword
    return None


def _needs_clarification_for_action(
    db: Session,
    action: dict,
    message: str,
    history_context: list[dict],
) -> str | None:
    intent = str(action.get("intent") or "")
    if intent == "unknown":
        return _clarification_question(message, None)

    if intent == "register_meeting_note" and not _looks_like_meeting_note(message):
        return "회의록 등록 요청인지 다른 작업 요청인지 불명확합니다. 예: '회의록: ...' 또는 '중복 태스크 정리'."

    if intent == "create_event":
        due = _parse_due(action.get("start") or action.get("due"), message)
        if due is None:
            return "일정 시간을 알려주세요. 예: '이번주 목요일 오후 3시 미팅 일정 추가'."
        title = _resolve_creation_title(action, message, intent="create_event")
        if _is_ambiguous_creation_title(title, intent="create_event"):
            return "일정 제목이 모호합니다. 어떤 일정인지 한 번 더 알려주세요. 예: '공인알림 미팅'."
        return None

    if intent == "create_task":
        title = _resolve_creation_title(action, message, intent="create_task")
        if _is_ambiguous_creation_title(title, intent="create_task"):
            return "할일 제목이 모호합니다. 어떤 작업인지 한 번 더 알려주세요. 예: '고객사 견적서 검토'."
        return None

    if intent in {"delete_task", "start_task", "update_task"}:
        base_keyword = action.get("task_keyword") or action.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        if not keyword:
            return "대상 할일 제목을 알려주세요."
        found = _find_task(
            db,
            keyword,
            statuses=("todo", "in_progress", "blocked", "done", "canceled"),
            allow_latest_fallback=False,
        )
        if not found:
            return f"'{keyword}' 할일을 찾지 못했습니다. 정확한 제목으로 다시 알려주세요."

    if intent in {"list_tasks", "list_events", "find_free_time"}:
        return None

    if intent == "delete_duplicate_events":
        return None

    if intent == "move_event":
        keyword, window, source_minutes = _resolve_event_lookup(action, message, prefer_source_segment=True)
        if not keyword and window is None:
            return "이동할 일정 제목을 알려주세요. 예: '오늘 고객 미팅 내일 오후 4시로 변경'."
        new_start = _parse_due(action.get("start") or action.get("due"), message)
        shift_minutes = action.get("shift_minutes")
        if new_start is None and shift_minutes is None:
            return "어느 시간으로 이동할지 알려주세요. 예: '목요일 4시로 이동'."
        target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
        if target is None:
            if reason == "ambiguous":
                return "대상 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요."
            return f"'{keyword}' 일정을 찾지 못했습니다. 정확한 제목이나 날짜를 더 알려주세요."

    if intent == "reschedule_after_hour":
        cutoff = _extract_cutoff_hour(action.get("cutoff_hour"), message)
        if cutoff is None:
            return "몇 시 이후 일정을 재배치할까요? 예: '오후 6시 이후 일정 재배치'."
        return None

    if intent == "update_priority":
        priority = action.get("priority")
        if not priority:
            return "변경할 우선순위를 알려주세요. 낮음/중간/높음/긴급 중 하나입니다."

    if intent == "update_due":
        due = _parse_due(action.get("due"), message)
        if due is None:
            return "새 마감일을 이해하지 못했습니다. 예: '내일 오후 5시'처럼 알려주세요."

    if intent in {"complete_task", "update_priority", "update_due"}:
        base_keyword = action.get("task_keyword") or action.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        if not keyword or _is_generic_keyword(keyword):
            keyword = _infer_keyword_from_history(
                db,
                message,
                history_context,
                statuses=("todo", "in_progress", "blocked", "done"),
            )
        if not keyword:
            return "대상 작업이 불명확합니다. 작업 제목을 포함해 다시 말해 주세요."
        found = _find_task(
            db,
            keyword,
            statuses=("todo", "in_progress", "blocked", "done"),
            allow_latest_fallback=False,
        )
        if not found:
            return f"'{keyword}' 작업을 찾지 못했습니다. 정확한 제목으로 다시 알려주세요."

    if intent == "delete_event":
        keyword, window, source_minutes = _resolve_event_lookup(action, message)
        if not keyword and window is None:
            return "삭제할 일정을 지정해 주세요. 예: '주간회의 일정 삭제'."
        target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
        if target is None:
            if reason == "ambiguous":
                return "삭제할 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요."
            if keyword:
                return f"'{keyword}' 일정을 찾지 못했습니다."
            return "해당 조건의 일정을 찾지 못했습니다."

    if intent == "update_event":
        keyword, window, source_minutes = _resolve_event_lookup(action, message)
        if not keyword and window is None:
            return "수정할 일정 제목을 알려주세요. 예: '고객미팅 일정을 주간회의로 변경'."
        target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
        if target is None:
            if reason == "ambiguous":
                return "수정할 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요."
            if keyword:
                return f"'{keyword}' 일정을 찾지 못했습니다."
            return "해당 조건의 일정을 찾지 못했습니다."
        new_title = str(action.get("new_title") or "").strip()
        has_resize = action.get("duration_minutes") is not None or action.get("duration_delta_minutes") is not None or action.get("end")
        if not new_title and not has_resize:
            return "일정의 무엇을 바꿀지 알려주세요. 예: '주간회의로 변경' 또는 '30분 연장'."

    if intent == "reschedule_request":
        lowered = message.lower()
        if len(message.strip()) <= 8 or ("재배치" in message and not any(token in lowered for token in ["오늘", "내일", "이번주", "다음주", "tomorrow", "week"])):
            return "재배치 범위를 알려주세요. 예: '내일 오전 일정만 재배치' 또는 '이번주 금요일까지'."

    return None


def _register_meeting_and_apply(db: Session, note_text: str) -> tuple[str, list[AssistantActionOut], list[str]]:
    transcript = _to_transcript(note_text)
    summary = transcript[0]["text"][:200] if transcript else None

    meeting = Meeting(
        meeting_id=f"chat-meeting-{datetime.now(UTC).timestamp()}",
        title="Chat Meeting Note",
        started_at=datetime.utcnow(),
        ended_at=datetime.utcnow(),
        summary=summary,
        transcript=transcript,
        extraction_status="pending",
    )
    db.add(meeting)
    db.flush()

    base_time = meeting.ended_at or datetime.utcnow()
    extraction_mode = "rule"
    if is_openai_available():
        try:
            drafts = extract_action_items_openai(meeting.transcript, meeting.summary, base_dt=base_time)
            extraction_mode = "openai" if drafts else "rule"
            if not drafts:
                drafts = extract_action_items(meeting.transcript, meeting.summary, base_dt=base_time)
        except OpenAIIntegrationError:
            drafts = extract_action_items(meeting.transcript, meeting.summary, base_dt=base_time)
    else:
        drafts = extract_action_items(meeting.transcript, meeting.summary, base_dt=base_time)

    profile = ensure_profile(db)
    auto_tasks = 0
    approvals = 0
    created_blocks = []
    approval_actions: list[AssistantActionOut] = []

    for draft in drafts:
        candidate = ActionItemCandidate(
            meeting_id=meeting.id,
            title=draft.title,
            assignee_name=draft.assignee_name,
            due=draft.due,
            effort_minutes=draft.effort_minutes,
            confidence=draft.confidence,
            rationale=f"[{extraction_mode}] {draft.rationale}",
            status="pending",
        )
        db.add(candidate)
        db.flush()

        must_approve = draft.confidence < CONFIDENCE_THRESHOLD or draft.effort_minutes >= LARGE_EFFORT_MINUTES
        if must_approve:
            approval = ApprovalRequest(
                type="action_item",
                status="pending",
                payload={
                    "candidate_id": candidate.id,
                    "meeting_id": meeting.meeting_id,
                    "reason": "low_confidence_or_large_effort",
                },
            )
            db.add(approval)
            db.flush()
            approval_actions.append(
                AssistantActionOut(
                    type="approval_requested",
                    detail={
                        "approval_id": approval.id,
                        "type": "action_item",
                        "summary": f"액션아이템 승인: {candidate.title}",
                    },
                )
            )
            approvals += 1
            continue

        _, blocks = approve_candidate(
            db,
            candidate,
            profile,
            title=draft.title,
            due=draft.due,
            effort_minutes=draft.effort_minutes,
            priority="medium",
            create_time_block=True,
        )
        auto_tasks += 1
        created_blocks.extend(blocks)

    meeting.extraction_status = "completed"
    db.commit()
    calendar_sync_queued = _queue_calendar_export_if_connected(db, created_blocks)

    reply = (
        f"회의록을 등록했고 액션아이템 {len(drafts)}건을 처리했습니다. "
        f"자동 반영 {auto_tasks}건, 승인 대기 {approvals}건"
    )
    if calendar_sync_queued:
        reply += ", Outlook 동기화 예약"
    reply += "."

    return (
        reply,
        [
            AssistantActionOut(type="meeting_registered", detail={"meeting_id": meeting.id}),
            AssistantActionOut(
                type="action_items_processed",
                detail={"detected": len(drafts), "auto_tasks": auto_tasks, "approval_pending": approvals},
            ),
            *approval_actions,
        ],
        ["calendar", "tasks", "approvals"],
    )


def _create_task_from_message(
    db: Session, title: str, due: datetime | None, effort_minutes: int, priority: str
) -> tuple[str, list[AssistantActionOut], list[str]]:
    profile = ensure_profile(db)
    task = Task(
        title=title.strip() or "새 작업",
        due=due,
        effort_minutes=max(15, min(8 * 60, effort_minutes)),
        priority=priority if priority in {"low", "medium", "high", "critical"} else "medium",
        source="chat",
    )
    db.add(task)
    if task.due:
        record_task_due_signal(profile, task.due)
        apply_learning_if_due(profile)
    db.commit()
    db.refresh(task)
    return (
        f"할일을 생성했습니다: {task.title}",
        [AssistantActionOut(type="task_created", detail={"task_id": task.id, "title": task.title})],
        ["tasks"],
    )


def _create_event_from_message(
    db: Session,
    title: str,
    start: datetime | None,
    duration_minutes: int = 60,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    if start is None:
        return ("일정 시간을 이해하지 못했습니다. 예: '목요일 오후 3시 미팅 일정 추가'.", [], [])

    event_title = title.strip() or "새 일정"
    duration = max(15, min(8 * 60, int(duration_minutes or 60)))
    end = start + timedelta(minutes=duration)

    conflict = db.execute(
        select(CalendarBlock)
        .where(
            and_(
                CalendarBlock.start < end,
                CalendarBlock.end > start,
            )
        )
        .order_by(CalendarBlock.start.asc())
    ).scalars().first()
    if conflict:
        return (
            (
                f"요청 시간에 기존 일정 '{conflict.title}'과 겹칩니다. "
                "다른 시간으로 다시 요청해 주세요."
            ),
            [],
            ["calendar"],
        )

    block = CalendarBlock(
        type="other",
        title=event_title,
        start=start,
        end=end,
        locked=False,
        source="aawo",
    )
    db.add(block)
    profile = ensure_profile(db)
    if block.source != "external":
        record_event_start_signal(profile, block.start)
        apply_learning_if_due(profile)
    db.flush()

    db.commit()
    db.refresh(block)
    sync_queued = _queue_calendar_export_if_connected(db, [block])

    reply = f"일정을 생성했습니다: {block.title} ({start.strftime('%Y-%m-%d %H:%M')} - {end.strftime('%H:%M')})"
    if sync_queued:
        reply += " · Outlook 동기화 예약"

    return (
        reply,
        [
            AssistantActionOut(
                type="event_created",
                detail={
                    "block_id": block.id,
                    "title": block.title,
                    "start": block.start.isoformat(),
                    "end": block.end.isoformat(),
                    "outlook_synced": False,
                    "outlook_sync_queued": sync_queued,
                },
            )
        ],
        ["calendar"],
    )


def _reschedule_from_message(db: Session, hint: str) -> tuple[str, list[AssistantActionOut], list[str]]:
    profile = ensure_profile(db)
    horizon_from = datetime.utcnow()
    horizon_to = datetime.utcnow() + timedelta(days=2)
    proposals = generate_proposals(
        db,
        profile,
        horizon_from=horizon_from,
        horizon_to=horizon_to,
        task_ids=None,
        slot_minutes=30,
        max_proposals=1,
    )
    if not proposals:
        return ("재배치할 제안을 만들지 못했습니다. 기간을 더 넓혀 다시 요청해 주세요.", [], ["calendar"])

    proposal = proposals[0]
    requires_approval = profile.autonomy_level in {"L0", "L1", "L2"}
    if requires_approval:
        approval = ApprovalRequest(
            type="reschedule",
            status="pending",
            payload={"proposal_id": proposal.id, "summary": proposal.summary, "reason": "assistant_chat_request"},
        )
        db.add(approval)
        db.commit()
        db.refresh(approval)
        return (
            (
                f"재배치 제안을 만들었습니다. 채팅에서 '승인' 또는 '취소'로 결정해 주세요. "
                f"(approval {approval.id})"
            ),
            [
                AssistantActionOut(
                    type="reschedule_approval_requested",
                    detail={"approval_id": approval.id, "proposal_id": proposal.id, "type": "reschedule"},
                )
            ],
            ["approvals", "calendar"],
        )

    created_blocks, updated_blocks = apply_proposal(db, proposal)
    changed_blocks = [*created_blocks, *updated_blocks]
    if changed_blocks:
        for block in changed_blocks:
            if block.source != "external":
                record_event_start_signal(profile, block.start)
        apply_learning_if_due(profile)

    sync_queued = _queue_calendar_export_if_connected(db, changed_blocks)

    reply = f"재배치를 적용했습니다. 일정 이동 {len(updated_blocks)}건"
    if created_blocks:
        reply += f", 신규 {len(created_blocks)}건 생성"
    if sync_queued:
        reply += ", Outlook 동기화 예약"
    if hint.strip():
        reply += f" (요청: {hint.strip()})"
    reply += "."
    return (
        reply,
        [
            AssistantActionOut(
                type="reschedule_applied",
                detail={
                    "proposal_id": proposal.id,
                    "created_blocks": len(created_blocks),
                    "updated_blocks": len(updated_blocks),
                },
            )
        ],
        ["calendar"],
    )


def _complete_task(db: Session, keyword: str | None) -> tuple[str, list[AssistantActionOut], list[str]]:
    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked", "done"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("완료 처리할 할일을 찾지 못했습니다. 작업 제목을 조금 더 구체적으로 말해 주세요.", [], [])
    if task.status == "done":
        return (
            f"이미 완료 상태입니다: {task.title}",
            [AssistantActionOut(type="task_already_done", detail={"task_id": task.id, "title": task.title})],
            ["tasks"],
        )
    task.status = "done"
    task.version += 1
    db.commit()
    return (
        f"완료 처리했습니다: {task.title}",
        [AssistantActionOut(type="task_completed", detail={"task_id": task.id, "title": task.title})],
        ["tasks"],
    )


def _update_priority(db: Session, keyword: str | None, priority: str | None) -> tuple[str, list[AssistantActionOut], list[str]]:
    if not priority:
        return ("우선순위 값을 찾지 못했습니다. 예: '보고서 작업 우선순위 높음으로 변경'", [], [])
    mapped = PRIORITY_MAP.get(priority.strip().lower()) or PRIORITY_MAP.get(priority.strip())
    if not mapped:
        return ("지원하지 않는 우선순위입니다. 낮음/중간/높음/긴급 중 하나로 요청해 주세요.", [], [])

    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("우선순위를 바꿀 할일을 찾지 못했습니다.", [], [])

    task.priority = mapped
    task.version += 1
    db.commit()
    return (
        f"우선순위를 변경했습니다: {task.title} -> {mapped}",
        [AssistantActionOut(type="task_priority_updated", detail={"task_id": task.id, "priority": mapped})],
        ["tasks"],
    )


def _update_due(db: Session, keyword: str | None, due: datetime | None) -> tuple[str, list[AssistantActionOut], list[str]]:
    if due is None:
        return ("새 마감일을 찾지 못했습니다. 예: '보고서 마감을 내일 오후 5시로 변경'", [], [])

    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("마감일을 변경할 할일을 찾지 못했습니다.", [], [])

    task.due = due
    profile = ensure_profile(db)
    if task.due:
        record_task_due_signal(profile, task.due)
        apply_learning_if_due(profile)
    task.version += 1
    db.commit()
    return (
        f"마감일을 변경했습니다: {task.title} -> {due.strftime('%Y-%m-%d %H:%M')}",
        [AssistantActionOut(type="task_due_updated", detail={"task_id": task.id, "due": due.isoformat()})],
        ["tasks"],
    )


def _update_task_from_message(
    db: Session,
    keyword: str | None,
    *,
    new_title: str | None = None,
    due: datetime | None = None,
    priority: str | None = None,
    status: str | None = None,
    effort_minutes: int | None = None,
    description: str | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked", "done", "canceled"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("수정할 할일을 찾지 못했습니다. 정확한 작업 제목을 알려주세요.", [], [])

    old_due = task.due
    changed: list[str] = []
    if new_title and new_title.strip() and new_title.strip() != task.title:
        task.title = new_title.strip()
        changed.append("제목")
    if due is not None:
        task.due = due
        if task.due != old_due and task.due:
            profile = ensure_profile(db)
            record_task_due_signal(profile, task.due)
            apply_learning_if_due(profile)
        changed.append("마감")
    if priority:
        mapped = PRIORITY_MAP.get(priority.strip().lower()) or PRIORITY_MAP.get(priority.strip())
        if mapped:
            task.priority = mapped
            changed.append("우선순위")
    if status and status in {"todo", "in_progress", "done", "blocked", "canceled"}:
        task.status = status
        changed.append("상태")
    if isinstance(effort_minutes, int):
        task.effort_minutes = max(15, min(8 * 60, effort_minutes))
        changed.append("소요시간")
    if description is not None:
        task.description = description
        changed.append("설명")

    if not changed:
        return ("변경할 필드를 이해하지 못했습니다. 제목/마감/우선순위/상태 중 하나를 지정해 주세요.", [], [])

    task.version += 1
    db.commit()

    return (
        f"할일을 수정했습니다: {task.title} ({', '.join(changed)})",
        [AssistantActionOut(type="task_updated", detail={"task_id": task.id, "changed": changed})],
        ["tasks"],
    )


def _delete_task_from_message(db: Session, keyword: str | None) -> tuple[str, list[AssistantActionOut], list[str]]:
    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked", "done", "canceled"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("삭제할 할일을 찾지 못했습니다.", [], [])

    blocks = db.execute(select(CalendarBlock).where(CalendarBlock.task_id == task.id)).scalars().all()
    for block in blocks:
        block.task_id = None
        block.version += 1

    deleted_id = task.id
    deleted_title = task.title
    db.delete(task)
    db.commit()

    return (
        f"할일을 삭제했습니다: {deleted_title}",
        [AssistantActionOut(type="task_deleted", detail={"task_id": deleted_id, "title": deleted_title})],
        ["tasks", "calendar"],
    )


def _start_task_from_message(db: Session, keyword: str | None) -> tuple[str, list[AssistantActionOut], list[str]]:
    task = _find_task(
        db,
        keyword,
        statuses=("todo", "in_progress", "blocked"),
        allow_latest_fallback=False,
    )
    if not task:
        return ("시작할 할일을 찾지 못했습니다.", [], [])
    if task.status == "in_progress":
        return (
            f"이미 진행중입니다: {task.title}",
            [AssistantActionOut(type="task_already_in_progress", detail={"task_id": task.id, "title": task.title})],
            ["tasks"],
        )

    task.status = "in_progress"
    task.version += 1
    db.commit()
    return (
        f"진행중으로 변경했습니다: {task.title}",
        [AssistantActionOut(type="task_started", detail={"task_id": task.id, "title": task.title})],
        ["tasks"],
    )


def _list_tasks_from_message(db: Session, limit: int | None = None) -> tuple[str, list[AssistantActionOut], list[str]]:
    size = max(1, min(20, int(limit or 7)))
    rows = db.execute(select(Task).order_by(Task.updated_at.desc()).limit(size)).scalars().all()
    if not rows:
        return ("현재 등록된 할일이 없습니다.", [], ["tasks"])

    lines = ["최근 할일입니다:"]
    for idx, row in enumerate(rows, start=1):
        due_text = row.due.strftime("%m-%d %H:%M") if row.due else "마감없음"
        lines.append(f"{idx}. {row.title} · {row.status} · {row.priority} · {due_text}")
    return (
        "\n".join(lines),
        [AssistantActionOut(type="tasks_listed", detail={"count": len(rows)})],
        ["tasks"],
    )


def _list_events_from_message(
    db: Session,
    target_date: str | None,
    message: str,
    limit: int | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    window = _extract_event_date_window(target_date or message)
    if window is None:
        base = _parse_due(target_date, message) if (target_date or message) else None
        date_base = _localize_dt(base or datetime.now(ZoneInfo(settings.timezone)))
        start, end = _day_window(date_base)
    else:
        start, end = window
    size = max(1, min(20, int(limit or 10)))

    rows = db.execute(
        select(CalendarBlock)
        .where(CalendarBlock.start < end, CalendarBlock.end > start)
        .order_by(CalendarBlock.start.asc())
        .limit(size)
    ).scalars().all()
    if not rows:
        return ("해당 기간에 등록된 일정이 없습니다.", [], ["calendar"])

    multi_day = (end - start) > timedelta(days=1)
    lines = [f"{_format_window_label((start, end))} 일정입니다:"]
    for idx, row in enumerate(rows, start=1):
        row_start = _localize_dt(row.start)
        row_end = _localize_dt(row.end)
        time_label = (
            f"{row_start.strftime('%m-%d %H:%M')} - {row_end.strftime('%m-%d %H:%M')}"
            if multi_day
            else f"{row_start.strftime('%H:%M')}-{row_end.strftime('%H:%M')}"
        )
        lines.append(f"{idx}. {row.title} ({time_label})")

    return (
        "\n".join(lines),
        [AssistantActionOut(type="events_listed", detail={"count": len(rows), "date": start.date().isoformat()})],
        ["calendar"],
    )


def _find_free_time_from_message(
    db: Session,
    target_date: str | None,
    message: str,
    duration_minutes: int | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    tz = ZoneInfo(settings.timezone)
    window = _extract_event_date_window(target_date or message)
    if window is None:
        base = _parse_due(target_date, message) if (target_date or message) else None
        date_base = _localize_dt(base or datetime.now(tz))
        window = _day_window(date_base)
    need = timedelta(minutes=max(15, min(8 * 60, int(duration_minutes or 60))))
    window_start, window_end = window

    rows = db.execute(
        select(CalendarBlock)
        .where(CalendarBlock.start < window_end, CalendarBlock.end > window_start)
        .order_by(CalendarBlock.start.asc())
    ).scalars().all()

    slots: list[tuple[datetime, datetime]] = []
    current_day = window_start
    while current_day < window_end and len(slots) < 12:
        day_start = current_day.replace(hour=9, minute=0, second=0, microsecond=0)
        day_end = current_day.replace(hour=18, minute=0, second=0, microsecond=0)
        if day_end <= window_start:
            current_day += timedelta(days=1)
            continue
        slot_start = max(day_start, window_start)
        slot_end = min(day_end, window_end)
        if slot_end <= slot_start:
            current_day += timedelta(days=1)
            continue

        day_rows = [
            row
            for row in rows
            if _localize_dt(row.start) < slot_end and _localize_dt(row.end) > slot_start
        ]

        cursor = slot_start
        for row in day_rows:
            row_start = max(_localize_dt(row.start), slot_start)
            row_end = min(_localize_dt(row.end), slot_end)
            if row_start - cursor >= need:
                slots.append((cursor, row_start))
            if row_end > cursor:
                cursor = row_end
        if slot_end - cursor >= need:
            slots.append((cursor, slot_end))
        current_day += timedelta(days=1)

    if not slots:
        return ("요청한 길이의 빈 시간이 없습니다.", [], ["calendar"])

    top = slots[:5]
    multi_day = (window_end - window_start) > timedelta(days=1)
    lines = [f"{_format_window_label(window)} 기준 추천 빈 시간:"]
    for idx, (s, e) in enumerate(top, start=1):
        label = (
            f"{s.strftime('%m-%d %H:%M')} - {e.strftime('%m-%d %H:%M')}"
            if multi_day
            else f"{s.strftime('%H:%M')} - {e.strftime('%H:%M')}"
        )
        lines.append(f"{idx}. {label}")

    return (
        "\n".join(lines),
        [AssistantActionOut(type="free_time_found", detail={"count": len(top), "date": window_start.date().isoformat()})],
        ["calendar"],
    )


def _find_event_by_keyword(
    db: Session,
    keyword: str | None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> CalendarBlock | None:
    target, _ = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
    return target


def _move_event_from_message(
    db: Session,
    keyword: str | None,
    new_start: datetime | None,
    new_end: datetime | None = None,
    duration_minutes: int | None = None,
    shift_minutes: int | None = None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
    if not target:
        if reason == "ambiguous":
            return ("대상 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요.", [], [])
        return ("이동할 일정을 찾지 못했습니다.", [], [])
    if target.source == "external":
        return ("Outlook 원본 일정은 앱에서 직접 이동할 수 없습니다.", [], [])

    if new_start is None and shift_minutes is None:
        return ("이동할 새 시간을 알려주세요. 예: '목요일 4시로 이동'.", [], [])

    if new_start is None and shift_minutes is not None:
        delta = timedelta(minutes=shift_minutes)
        new_start = _localize_dt(target.start) + delta
        if new_end is None:
            new_end = _localize_dt(target.end) + delta

    duration = (
        (new_end - new_start)
        if new_end and new_end > new_start
        else timedelta(minutes=max(15, min(8 * 60, int(duration_minutes or ((target.end - target.start).total_seconds() // 60 or 60)))))
    )
    end = new_start + duration

    conflict = db.execute(
        select(CalendarBlock)
        .where(
            CalendarBlock.id != target.id,
            CalendarBlock.start < end,
            CalendarBlock.end > new_start,
        )
    ).scalars().first()
    if conflict:
        return (f"'{conflict.title}' 일정과 겹쳐 이동할 수 없습니다.", [], ["calendar"])

    target.start = new_start
    target.end = end
    if target.source != "external":
        profile = ensure_profile(db)
        record_event_start_signal(profile, target.start)
        apply_learning_if_due(profile)
    target.version += 1
    db.commit()
    db.refresh(target)
    sync_queued = _queue_calendar_export_if_connected(db, [target])

    reply = f"일정을 이동했습니다: {target.title} ({new_start.strftime('%m-%d %H:%M')} - {end.strftime('%H:%M')})"
    if sync_queued:
        reply += " · Outlook 동기화 예약"
    return (
        reply,
        [AssistantActionOut(type="event_moved", detail={"block_id": target.id, "title": target.title})],
        ["calendar"],
    )


def _reschedule_after_hour(
    db: Session,
    cutoff_hour: int | None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    if cutoff_hour is None or cutoff_hour < 0 or cutoff_hour > 23:
        return ("기준 시간을 파악하지 못했습니다. 예: '오후 6시 이후 일정 재배치'", [], [])

    profile = ensure_profile(db)
    tz = ZoneInfo(profile.timezone or settings.timezone)
    now_local = datetime.now(tz)
    now_utc = datetime.utcnow()

    rows = db.execute(
        select(CalendarBlock)
        .where(
            and_(
                CalendarBlock.source != "external",
                CalendarBlock.end > now_utc,
            )
        )
        .order_by(CalendarBlock.start.asc())
    ).scalars().all()

    targets: list[CalendarBlock] = []
    for row in rows:
        start = row.start.replace(tzinfo=tz) if row.start.tzinfo is None else row.start.astimezone(tz)
        end = row.end.replace(tzinfo=tz) if row.end.tzinfo is None else row.end.astimezone(tz)
        if end <= now_local:
            continue
        start_hour = start.hour + start.minute / 60.0
        end_hour = end.hour + end.minute / 60.0
        if start_hour >= cutoff_hour or end_hour > cutoff_hour:
            targets.append(row)

    if not targets:
        return (f"{cutoff_hour:02d}:00 이후 일정이 없어 재배치할 항목이 없습니다.", [], ["calendar"])

    task_ids = sorted({row.task_id for row in targets if row.task_id})
    skipped_unlinked = sum(1 for row in targets if not row.task_id)
    if not task_ids:
        return (
            "재배치 대상 일정은 찾았지만 연결된 할일(task_id)이 없어 자동 재배치를 적용하지 못했습니다.",
            [],
            ["calendar"],
        )

    proposals = generate_proposals(
        db,
        profile,
        horizon_from=now_local,
        horizon_to=now_local + timedelta(days=14),
        task_ids=task_ids,
        slot_minutes=30,
        max_proposals=1,
    )
    if not proposals:
        return ("재배치 가능한 제안을 만들지 못했습니다. 근무시간 또는 기존 일정 충돌을 확인해 주세요.", [], ["calendar"])

    proposal = proposals[0]
    created_blocks, updated_blocks = apply_proposal(db, proposal)
    changed_blocks = [*created_blocks, *updated_blocks]
    if not changed_blocks:
        return ("재배치 제안을 만들었지만 적용 가능한 슬롯이 없어 변경하지 못했습니다.", [], ["calendar"])

    if changed_blocks:
        for block in changed_blocks:
            if block.source != "external":
                record_event_start_signal(profile, block.start)
        apply_learning_if_due(profile)

    updated_ids = {row.id for row in updated_blocks}
    removed_blocks = [row for row in targets if row.task_id in task_ids and row.id not in updated_ids]

    deleted_outlook = 0
    if removed_blocks and is_graph_connected(db):
        try:
            deleted = delete_blocks_from_outlook(db, removed_blocks)
            deleted_outlook = int(deleted.get("deleted", 0))
        except (GraphAuthError, GraphApiError):
            deleted_outlook = 0

    removed_count = 0
    for row in removed_blocks:
        db.delete(row)
        removed_count += 1
    db.commit()

    sync_queued = _queue_calendar_export_if_connected(db, changed_blocks)

    reply = (
        f"{cutoff_hour:02d}:00 이후 일정 재배치를 적용했습니다. "
        f"기존 {removed_count}건 정리, 일정 이동 {len(updated_blocks)}건"
    )
    if created_blocks:
        reply += f", 새 일정 {len(created_blocks)}건 생성"
    if skipped_unlinked:
        reply += f", 미연결 일정 {skipped_unlinked}건 제외"
    if sync_queued:
        reply += ", Outlook 반영 예약"
    if deleted_outlook:
        reply += f", Outlook 기존일정 삭제 {deleted_outlook}건"
    reply += "."

    return (
        reply,
        [
            AssistantActionOut(
                type="after_hour_rescheduled",
                detail={
                    "cutoff_hour": cutoff_hour,
                    "removed_blocks": removed_count,
                    "created_blocks": len(created_blocks),
                    "updated_blocks": len(updated_blocks),
                    "skipped_unlinked": skipped_unlinked,
                },
            )
        ],
        ["calendar", "tasks"],
    )


def _delete_duplicate_tasks(db: Session) -> tuple[str, list[AssistantActionOut], list[str]]:
    rows = db.execute(
        select(Task)
        .where(Task.status.in_(["todo", "in_progress", "blocked", "done"]))
        .order_by(Task.updated_at.desc())
    ).scalars().all()

    grouped: dict[str, list[Task]] = {}
    for row in rows:
        key = _normalize_title_key(row.title)
        if len(key) < 3:
            continue
        grouped.setdefault(key, []).append(row)

    duplicate_groups = [group for group in grouped.values() if len(group) >= 2]
    if not duplicate_groups:
        return ("중복으로 판단되는 태스크가 없습니다.", [], ["tasks"])

    canceled = 0
    merged = 0
    relinked = 0
    groups = 0

    def keeper_score(task: Task) -> tuple[float, float, float, float, float]:
        status_score = float(STATUS_RANK.get(task.status, 0))
        priority_score = float(PRIORITY_RANK.get(task.priority, 0))
        due_score = 1.0 if task.due else 0.0
        desc_score = float(len((task.description or "").strip()))
        updated_score = task.updated_at.timestamp() if task.updated_at else 0.0
        return (status_score, priority_score, due_score, desc_score, updated_score)

    for group in duplicate_groups:
        keeper = max(group, key=keeper_score)
        keeper_changed = False
        for row in group:
            if row.id == keeper.id:
                continue

            if not keeper.description and row.description:
                keeper.description = row.description
                keeper_changed = True
                merged += 1
            if keeper.due is None and row.due is not None:
                keeper.due = row.due
                keeper_changed = True
                merged += 1
            if PRIORITY_RANK.get(row.priority, 0) > PRIORITY_RANK.get(keeper.priority, 0):
                keeper.priority = row.priority
                keeper_changed = True
                merged += 1

            blocks = db.execute(select(CalendarBlock).where(CalendarBlock.task_id == row.id)).scalars().all()
            for block in blocks:
                block.task_id = keeper.id
                block.version += 1
                relinked += 1

            if row.status != "canceled":
                row.status = "canceled"
                row.version += 1
                canceled += 1

        if keeper_changed:
            keeper.version += 1
        groups += 1

    db.commit()

    return (
        f"중복 태스크를 정리했습니다. 그룹 {groups}개, 취소 {canceled}건, 일정 재연결 {relinked}건.",
        [
            AssistantActionOut(
                type="duplicate_tasks_cleaned",
                detail={"groups": groups, "canceled": canceled, "relinked_blocks": relinked, "merged_fields": merged},
            )
        ],
        ["tasks", "calendar"],
    )


def _delete_duplicate_events(db: Session, keyword: str | None = None) -> tuple[str, list[AssistantActionOut], list[str]]:
    rows = db.execute(select(CalendarBlock).order_by(CalendarBlock.updated_at.desc())).scalars().all()
    if not rows:
        return ("중복으로 판단되는 일정이 없습니다.", [], ["calendar"])

    filter_keyword = _extract_duplicate_event_keyword(keyword)
    filter_norm = _normalize_text(filter_keyword) if filter_keyword else ""

    def include_row(row: CalendarBlock) -> bool:
        if not filter_norm:
            return True
        title_norm = _normalize_text(row.title)
        if not title_norm:
            return False
        if filter_norm in GENERIC_EVENT_FILTER_KEYS:
            return filter_norm in title_norm
        return filter_norm in title_norm

    strict_grouped: dict[tuple[str, str, str, int], list[CalendarBlock]] = {}
    task_linked_grouped: dict[tuple[str, str, int], list[CalendarBlock]] = {}
    for row in rows:
        if not include_row(row):
            continue
        title_key = _normalize_title_key(row.title)
        if len(title_key) < 2:
            continue
        start = _localize_dt(row.start)
        end = _localize_dt(row.end)
        duration = max(1, int((end - start).total_seconds() // 60))
        strict_key = (
            title_key,
            start.date().isoformat(),
            start.strftime("%H:%M"),
            duration,
        )
        strict_grouped.setdefault(strict_key, []).append(row)

        # When the same task-linked meeting exists multiple times, it is usually a reschedule/copy bug.
        # Treat those as duplicates even if the time/date changed.
        if row.task_id and row.source != "external":
            task_key = (
                str(row.task_id),
                title_key,
                duration,
            )
            task_linked_grouped.setdefault(task_key, []).append(row)

    duplicate_groups: list[list[CalendarBlock]] = [group for group in strict_grouped.values() if len(group) >= 2]
    seen_ids = {row.id for group in duplicate_groups for row in group}
    for group in task_linked_grouped.values():
        remaining = [row for row in group if row.id not in seen_ids]
        if len(remaining) >= 2:
            duplicate_groups.append(remaining)
            for row in remaining:
                seen_ids.add(row.id)
    if not duplicate_groups:
        if filter_keyword:
            return (f"'{filter_keyword}' 기준 중복 일정이 없습니다.", [], ["calendar"])
        return ("중복으로 판단되는 일정이 없습니다.", [], ["calendar"])

    deleted_outlook = 0
    deleted_local = 0
    group_count = 0
    kept_titles: list[str] = []

    now = datetime.now(ZoneInfo(settings.timezone))

    def keeper_score(block: CalendarBlock) -> tuple[int, int, int, float, float]:
        local_preferred = 1 if block.source != "external" else 0
        task_linked = 1 if block.task_id else 0
        synced = 1 if (block.outlook_event_id or "").strip() else 0
        future_preferred = 1 if _localize_dt(block.end) >= now else 0
        updated = block.updated_at.timestamp() if block.updated_at else 0.0
        start_score = _localize_dt(block.start).timestamp()
        return (local_preferred, task_linked, future_preferred, synced, updated + start_score * 1e-9)

    for group in duplicate_groups:
        keeper = max(group, key=keeper_score)
        duplicates = [row for row in group if row.id != keeper.id]
        remote_candidates = [row for row in duplicates if (row.outlook_event_id or "").strip()]
        if remote_candidates and is_graph_connected(db):
            try:
                deleted = delete_blocks_from_outlook(db, remote_candidates)
                deleted_outlook += int(deleted.get("deleted", 0))
            except (GraphAuthError, GraphApiError):
                pass

        for row in duplicates:
            db.delete(row)
            deleted_local += 1
        group_count += 1
        if keeper.title:
            kept_titles.append(keeper.title)

    db.commit()

    title_note = ""
    if kept_titles:
        sample = ", ".join(kept_titles[:2])
        title_note = f" 유지: {sample}"
        if len(kept_titles) > 2:
            title_note += " 외"

    reply = f"중복 일정을 정리했습니다. 그룹 {group_count}개, 로컬 삭제 {deleted_local}건"
    if deleted_outlook:
        reply += f", Outlook 삭제 {deleted_outlook}건"
    reply += "."
    if title_note:
        reply += title_note

    return (
        reply,
        [
            AssistantActionOut(
                type="duplicate_events_cleaned",
                detail={
                    "groups": group_count,
                    "deleted_local": deleted_local,
                    "deleted_outlook": deleted_outlook,
                    "keyword": filter_keyword,
                },
            )
        ],
        ["calendar"],
    )


def _delete_event(
    db: Session,
    keyword: str | None,
    *,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    if not keyword and window is None:
        return ("삭제할 일정을 지정해주세요.", [], [])

    target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
    if not target:
        if reason == "ambiguous":
            return ("삭제할 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요.", [], [])
        if keyword:
            return (f"'{keyword}'에 해당하는 일정을 찾지 못했습니다.", [], [])
        return ("해당 조건의 일정을 찾지 못했습니다.", [], [])

    db.delete(target)
    db.commit()

    return (
        f"일정 '{target.title}'을(를) 삭제했습니다.",
        [
            AssistantActionOut(
                type="event_deleted",
                detail={"deleted_id": target.id, "title": target.title},
            )
        ],
        ["calendar"],
    )

def _update_event(
    db: Session,
    keyword: str | None,
    new_title: str | None,
    *,
    duration_minutes: int | None = None,
    duration_delta_minutes: int | None = None,
    new_end: datetime | None = None,
    window: tuple[datetime, datetime] | None = None,
    source_minutes: int | None = None,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    if not keyword and window is None:
        return ("변경할 일정을 지정해주세요.", [], [])

    target, reason = _resolve_event_match(db, keyword, window=window, source_minutes=source_minutes)
    if not target:
        if reason == "ambiguous":
            return ("수정할 일정이 여러 건입니다. 일정 제목이나 날짜를 더 구체적으로 알려주세요.", [], [])
        if keyword:
            return (f"'{keyword}'에 해당하는 일정을 찾지 못했습니다.", [], [])
        return ("해당 조건의 일정을 찾지 못했습니다.", [], [])

    changed: list[str] = []
    old_title = target.title

    if new_title and new_title.strip() and new_title.strip() != target.title:
        target.title = new_title.strip()
        changed.append("제목")

    current_start = _localize_dt(target.start)
    current_end = _localize_dt(target.end)
    desired_end = current_end
    if new_end and new_end > current_start:
        desired_end = new_end
    elif duration_minutes is not None:
        desired_end = current_start + timedelta(minutes=max(15, min(8 * 60, int(duration_minutes))))
    elif duration_delta_minutes is not None:
        current_minutes = max(15, int((current_end - current_start).total_seconds() // 60))
        resized_minutes = max(15, min(8 * 60, current_minutes + int(duration_delta_minutes)))
        desired_end = current_start + timedelta(minutes=resized_minutes)

    if desired_end != current_end:
        conflict = db.execute(
            select(CalendarBlock)
            .where(
                CalendarBlock.id != target.id,
                CalendarBlock.start < desired_end,
                CalendarBlock.end > current_start,
            )
        ).scalars().first()
        if conflict:
            return (f"'{conflict.title}' 일정과 겹쳐 기간을 조정할 수 없습니다.", [], ["calendar"])
        target.end = desired_end
        changed.append("길이")

    if not changed:
        return (f"일정 '{target.title}'을 어떻게 수정할지 알려주세요.", [], [])

    target.version += 1
    db.commit()
    db.refresh(target)
    sync_queued = _queue_calendar_export_if_connected(db, [target])

    reply = f"일정을 수정했습니다: {target.title} ({', '.join(changed)})"
    if "제목" in changed and old_title != target.title:
        reply = f"일정 이름을 '{old_title}'에서 '{target.title}'(으)로 변경했습니다."
        if len(changed) > 1:
            reply += f" 추가 변경: {', '.join([item for item in changed if item != '제목'])}"
    if sync_queued:
        reply += " · Outlook 동기화 예약"

    return (
        reply,
        [
            AssistantActionOut(
                type="event_updated",
                detail={"updated_id": target.id, "new_title": target.title, "changed": changed},
            )
        ],
        ["calendar"],
    )

def _run_one_action(
    db: Session,
    parsed: dict,
    message: str,
    history_context: list[dict],
    *,
    require_confirmation: bool,
) -> tuple[str, list[AssistantActionOut], list[str]]:
    intent = parsed.get("intent", "unknown")
    if intent == "register_meeting_note":
        return _register_meeting_and_apply(db, parsed.get("meeting_note") or message)

    if intent == "create_task":
        title = _resolve_creation_title(parsed, message, intent="create_task") or "새 작업"
        due = _parse_due(parsed.get("due"), message)
        effort = int(parsed.get("effort_minutes") or 60)
        priority = str(parsed.get("priority") or "medium")
        return _create_task_from_message(db, title, due, effort, priority)

    if intent == "create_event":
        title = _resolve_creation_title(parsed, message, intent="create_event") or "새 일정"
        start = _parse_due(parsed.get("start") or parsed.get("due"), message)
        duration = int(parsed.get("duration_minutes") or parsed.get("effort_minutes") or 60)
        return _create_event_from_message(db, title, start, duration)

    if intent == "update_task":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        due = _parse_due(parsed.get("due"), message) if parsed.get("due") else None
        return _update_task_from_message(
            db,
            keyword,
            new_title=parsed.get("new_title"),
            due=due,
            priority=parsed.get("priority"),
            status=parsed.get("status"),
            effort_minutes=parsed.get("effort_minutes"),
            description=parsed.get("description"),
        )

    if intent == "delete_task":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        return _delete_task_from_message(db, keyword)

    if intent == "start_task":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        return _start_task_from_message(db, keyword)

    if intent == "list_tasks":
        return _list_tasks_from_message(db, limit=parsed.get("limit"))

    if intent == "list_events":
        return _list_events_from_message(
            db,
            target_date=parsed.get("target_date") or parsed.get("due"),
            message=message,
            limit=parsed.get("limit"),
        )

    if intent == "find_free_time":
        return _find_free_time_from_message(
            db,
            target_date=parsed.get("target_date") or parsed.get("due"),
            message=message,
            duration_minutes=parsed.get("duration_minutes") or parsed.get("effort_minutes"),
        )

    if intent == "move_event":
        keyword, window, source_minutes = _resolve_event_lookup(parsed, message, prefer_source_segment=True)
        new_start = _parse_due(parsed.get("start") or parsed.get("due"), message)
        new_end = _parse_due(parsed.get("end"), message) if parsed.get("end") else None
        return _move_event_from_message(
            db,
            keyword,
            new_start,
            new_end,
            duration_minutes=parsed.get("duration_minutes") or parsed.get("effort_minutes"),
            shift_minutes=parsed.get("shift_minutes"),
            window=window,
            source_minutes=source_minutes,
        )

    if intent == "reschedule_after_hour":
        cutoff_hour = _extract_cutoff_hour(parsed.get("cutoff_hour"), message)
        if cutoff_hour is None:
            return ("기준 시간을 파악하지 못했습니다. 예: '오후 6시 이후 일정 재배치'", [], [])
        if require_confirmation:
            approval = _queue_chat_confirmation(
                db,
                {"intent": "reschedule_after_hour", "cutoff_hour": cutoff_hour},
                summary=f"{cutoff_hour:02d}:00 이후 일정 재배치",
                source_message=message,
            )
            return (
                (
                    f"{cutoff_hour:02d}:00 이후 일정을 재배치하려고 합니다. "
                    "채팅에 '승인' 또는 '취소'라고 답해 주세요."
                ),
                [AssistantActionOut(type="approval_requested", detail={"approval_id": approval.id, "type": "chat"})],
                ["approvals"],
            )
        return _reschedule_after_hour(db, cutoff_hour)

    if intent == "reschedule_request":
        hint = str(parsed.get("reschedule_hint") or parsed.get("time_hint") or parsed.get("title") or message)
        return _reschedule_from_message(db, hint)

    if intent == "delete_duplicate_tasks":
        if require_confirmation:
            approval = _queue_chat_confirmation(
                db,
                {"intent": "delete_duplicate_tasks"},
                summary="중복 태스크 정리(중복 항목 취소 및 일정 재연결)",
                source_message=message,
            )
            return (
                "중복 태스크를 정리하려고 합니다. 채팅에 '승인' 또는 '취소'라고 답해 주세요.",
                [AssistantActionOut(type="approval_requested", detail={"approval_id": approval.id, "type": "chat"})],
                ["approvals"],
            )
        return _delete_duplicate_tasks(db)

    if intent == "delete_duplicate_events":
        keyword = parsed.get("task_keyword") or parsed.get("title")
        if require_confirmation:
            approval = _queue_chat_confirmation(
                db,
                {"intent": "delete_duplicate_events", "task_keyword": keyword},
                summary=f"중복 일정 정리{f' ({keyword})' if keyword else ''}",
                source_message=message,
            )
            return (
                "중복 일정을 정리하려고 합니다. 채팅에 '승인' 또는 '취소'라고 답해 주세요.",
                [AssistantActionOut(type="approval_requested", detail={"approval_id": approval.id, "type": "chat"})],
                ["approvals"],
            )
        return _delete_duplicate_events(db, str(keyword or "").strip() or None)

    if intent == "complete_task":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        history_keyword = _infer_keyword_from_history(
            db,
            message,
            history_context,
            statuses=("todo", "in_progress", "blocked", "done"),
        )
        if _has_reference_phrase(message) and history_keyword:
            keyword = history_keyword
        elif not keyword or _is_generic_keyword(keyword):
            keyword = history_keyword
        return _complete_task(db, keyword)

    if intent == "update_priority":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        history_keyword = _infer_keyword_from_history(
            db,
            message,
            history_context,
            statuses=("todo", "in_progress", "blocked"),
        )
        if _has_reference_phrase(message) and history_keyword:
            keyword = history_keyword
        elif not keyword or _is_generic_keyword(keyword):
            keyword = history_keyword
        return _update_priority(db, keyword, parsed.get("priority"))

    if intent == "update_due":
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        keyword = _extract_task_keyword(str(base_keyword or "")) or _extract_task_keyword(message)
        history_keyword = _infer_keyword_from_history(
            db,
            message,
            history_context,
            statuses=("todo", "in_progress", "blocked"),
        )
        if _has_reference_phrase(message) and history_keyword:
            keyword = history_keyword
        if _has_reference_phrase(message) and history_keyword:
            keyword = history_keyword
        elif not keyword or _is_generic_keyword(keyword):
            keyword = history_keyword
        due = _parse_due(parsed.get("due"), message)
        return _update_due(db, keyword, due)

    if intent == "delete_event":
        keyword, window, source_minutes = _resolve_event_lookup(parsed, message)
        if not keyword and window is None:
            return ("어떤 일정을 삭제할지 알려주세요.", [], [])
        return _delete_event(db, keyword, window=window, source_minutes=source_minutes)

    if intent == "update_event":
        # we parse out the current title and the new title from the request.
        # Example prompt: "미팅 일정을 주간회의로 바꿔줘" -> target="미팅", new_title="주간회의"
        base_keyword = parsed.get("task_keyword") or parsed.get("title")
        new_title = str(parsed.get("new_title") or "").strip()
        has_duration_change = (
            parsed.get("duration_minutes") is not None
            or parsed.get("duration_delta_minutes") is not None
            or parsed.get("end") is not None
        )
        if not new_title and not has_duration_change:
            new_title = (
                message.replace(str(base_keyword or ""), "")
                .replace("바꿔줘", "")
                .replace("수정", "")
                .replace("변경", "")
                .replace("일정", "")
                .replace("으로", "")
                .replace("를", "")
                .replace("을", "")
                .replace("로", "")
                .strip()
            )

        keyword, window, source_minutes = _resolve_event_lookup(parsed, message)
        if not keyword and window is None:
            return ("어떤 일정을 수정할지 알려주세요.", [], [])
        return _update_event(
            db,
            keyword,
            new_title,
            duration_minutes=parsed.get("duration_minutes"),
            duration_delta_minutes=parsed.get("duration_delta_minutes"),
            new_end=_parse_due(parsed.get("end"), message) if parsed.get("end") else None,
            window=window,
            source_minutes=source_minutes,
        )

    return ("요청 의도를 처리하지 못했습니다.", [], [])


def _resolve_pending_approval_by_chat(
    db: Session,
    approval: ApprovalRequest,
    *,
    approve: bool,
    message: str,
    history_context: list[dict],
) -> tuple[str, list[AssistantActionOut], list[str]]:
    approval.status = "approved" if approve else "rejected"
    approval.resolved_at = datetime.utcnow()
    approval.reason = "resolved_via_chat"

    if not approve:
        if approval.type == "action_item":
            candidate_id = approval.payload.get("candidate_id")
            candidate = db.get(ActionItemCandidate, candidate_id)
            if candidate and candidate.status == "pending":
                reject_candidate(candidate)
        elif approval.type == "email_intake":
            _ensure_email_triage_table(db)
            triage = db.execute(select(EmailTriage).where(EmailTriage.approval_id == approval.id)).scalars().first()
            if triage is not None:
                triage.status = "rejected"
        db.commit()
        return (
            "요청한 작업을 취소했습니다.",
            [AssistantActionOut(type="approval_rejected", detail={"approval_id": approval.id, "approval_type": approval.type})],
            ["approvals"],
        )

    if approval.type == CHAT_CONFIRM_TYPE:
        action = approval.payload.get("action") or {}
        source_message = str(approval.payload.get("source_message") or "").strip()
        combined_message = f"{source_message}\n{message}".strip() if source_message else message
        reply, actions, refresh = _run_one_action(
            db,
            action,
            message=combined_message,
            history_context=history_context,
            require_confirmation=False,
        )
        return (
            f"승인되었습니다.\n{reply}",
            [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id, "approval_type": approval.type})]
            + actions,
            sorted(set(["approvals", *refresh])),
        )

    if approval.type == "action_item":
        candidate_id = approval.payload.get("candidate_id")
        candidate = db.get(ActionItemCandidate, candidate_id)
        if candidate and candidate.status == "pending":
            profile = ensure_profile(db)
            _, blocks = approve_candidate(db, candidate, profile)
            db.commit()
            sync_queued = _queue_calendar_export_if_connected(db, blocks)
            reply = f"승인되었습니다. 액션아이템을 할일로 반영했습니다: {candidate.title}"
            if sync_queued:
                reply += " (Outlook 동기화 예약)"
            return (
                reply,
                [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id, "approval_type": approval.type})],
                ["approvals", "tasks", "calendar"],
            )
        db.commit()
        return ("승인할 액션아이템을 찾지 못했습니다.", [], ["approvals"])

    if approval.type == "reschedule":
        proposal_id = approval.payload.get("proposal_id")
        proposal = db.get(SchedulingProposal, proposal_id)
        if proposal and proposal.status == "draft":
            created_blocks, updated_blocks = apply_proposal(db, proposal)
            changed_blocks = [*created_blocks, *updated_blocks]
            db.commit()
            sync_queued = _queue_calendar_export_if_connected(db, changed_blocks)
            reply = f"승인되었습니다. 재배치를 적용해 일정 {len(updated_blocks)}건을 이동했습니다."
            if created_blocks:
                reply += f" (신규 {len(created_blocks)}건 생성)"
            if sync_queued:
                reply += " (Outlook 동기화 예약)"
            return (
                reply,
                [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id, "approval_type": approval.type})],
                ["approvals", "calendar"],
            )
        db.commit()
        return ("승인할 재배치 제안을 찾지 못했습니다.", [], ["approvals"])

    if approval.type == "email_intake":
        _ensure_email_triage_table(db)
        triage = db.execute(select(EmailTriage).where(EmailTriage.approval_id == approval.id)).scalars().first()
        payload_data = approval.payload if isinstance(approval.payload, dict) else {}
        task_data = payload_data.get("task") if isinstance(payload_data.get("task"), dict) else None
        event_data = payload_data.get("event") if isinstance(payload_data.get("event"), dict) else None
        message_id = str(payload_data.get("message_id") or "").strip()

        created_task: Task | None = None
        created_block: CalendarBlock | None = None
        todo_sync_queued = False
        calendar_sync_queued = False

        if task_data and str(task_data.get("title") or "").strip():
            priority = str(task_data.get("priority") or "medium").strip().lower()
            if priority not in {"low", "medium", "high", "critical"}:
                priority = "medium"
            created_task = Task(
                title=str(task_data.get("title") or "").strip(),
                description=str(task_data.get("description") or "").strip() or None,
                due=_parse_email_approval_datetime(task_data.get("due")),
                priority=priority,
                source="email",
                source_ref=message_id or None,
                effort_minutes=60,
            )
            db.add(created_task)
            db.flush()

        if event_data:
            start = _parse_email_approval_datetime(event_data.get("start"))
            end = _parse_email_approval_datetime(event_data.get("end"))
            if start and (end is None or end <= start):
                end = start + timedelta(hours=1)
            if start and end and end > start:
                created_block = CalendarBlock(
                    type="other",
                    title=str(event_data.get("title") or payload_data.get("subject") or "메일 기반 일정").strip(),
                    start=start,
                    end=end,
                    task_id=created_task.id if created_task else None,
                    locked=False,
                    source="aawo",
                )
                db.add(created_block)
                db.flush()

        if triage is not None:
            triage.status = "approved"
            triage.created_task_id = created_task.id if created_task else None
            triage.created_block_id = created_block.id if created_block else None

        db.commit()
        if created_task:
            todo_sync_queued = _queue_todo_export_if_connected(db)
        if created_block:
            calendar_sync_queued = _queue_calendar_export_if_connected(db, [created_block])

        created_labels: list[str] = []
        refresh_keys = ["approvals"]
        if created_task:
            created_labels.append(f"할일 '{created_task.title}'")
            refresh_keys.append("tasks")
        if created_block:
            created_labels.append(f"일정 '{created_block.title}'")
            refresh_keys.append("calendar")

        if created_labels:
            reply = f"승인되었습니다. 메일 기반 {', '.join(created_labels)}을 생성했습니다."
        else:
            reply = "승인되었습니다. 다만 생성 가능한 일정/할일 정보가 부족해 변경 내역이 없습니다."

        sync_notes: list[str] = []
        if todo_sync_queued:
            sync_notes.append("To Do 동기화 예약")
        if calendar_sync_queued:
            sync_notes.append("Outlook 캘린더 동기화 예약")
        if sync_notes:
            reply += f" ({', '.join(sync_notes)})"

        return (
            reply,
            [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id, "approval_type": approval.type})],
            sorted(set(refresh_keys)),
        )

    db.commit()
    return ("승인되었습니다.", [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id})], ["approvals"])


def _fallback_classify(text: str, *, allow_openai_nli: bool = True) -> dict:
    lowered = text.lower()
    source_segment, dest_segment = _split_event_time_change_message(text)
    schedule_fast = _infer_schedule_fast_action(text)
    if schedule_fast:
        return schedule_fast
    if (
        source_segment
        and dest_segment
        and any(token in text for token in ["일정", "미팅", "회의", "캘린더"])
        and _contains_datetime_phrase(dest_segment)
    ):
        return {"intent": "move_event", "title": source_segment, "task_keyword": source_segment, "start": dest_segment}
    if _looks_like_meeting_note(text):
        return {"intent": "register_meeting_note", "meeting_note": text}
    if any(token in text for token in ["중복", "duplicate"]) and any(
        token in text or token in lowered for token in ["삭제", "정리", "제거", "dedup", "cleanup", "merge"]
    ) and any(token in text for token in ["일정", "미팅", "회의", "약속", "캘린더", "스케줄"]):
        return {"intent": "delete_duplicate_events", "task_keyword": _extract_duplicate_event_keyword(text), "title": text}
    if any(token in text for token in ["중복", "duplicate"]) and any(
        token in text or token in lowered for token in ["삭제", "정리", "제거", "dedup", "merge"]
    ):
        return {"intent": "delete_duplicate_tasks"}
    if any(token in text for token in ["재배치", "옮겨", "조정"]) or "reschedule" in lowered:
        cutoff = _extract_cutoff_hour(None, text)
        if cutoff is not None and ("이후" in text or "after" in lowered or "저녁" in text):
            return {"intent": "reschedule_after_hour", "cutoff_hour": cutoff}
    if "마감" in text or "due" in lowered:
        if any(token in text for token in ["변경", "옮겨", "조정", "바꿔"]) or "change" in lowered:
            return {"intent": "update_due", "title": text, "due": text}
    if "우선순위" in text or "priority" in lowered:
        priority = None
        for token in PRIORITY_MAP:
            if token in text or token in lowered:
                priority = token
                break
        return {"intent": "update_priority", "title": text, "priority": priority}
    if any(token in text for token in ["시작", "착수"]) or "start task" in lowered:
        return {"intent": "start_task", "title": text}
    if any(token in text for token in ["완료"]) or "done" in lowered:
        return {"intent": "complete_task", "title": text}
    if any(token in text for token in ["삭제", "지워", "remove", "delete"]) and any(
        token in text for token in ["할일", "태스크", "task", "todo", "to-do", "업무"]
    ):
        return {"intent": "delete_task", "title": text}
    if any(token in text for token in ["수정", "바꿔", "변경", "update", "modify"]) and any(
        token in text for token in ["할일", "태스크", "task", "todo", "to-do", "업무"]
    ):
        return {"intent": "update_task", "title": text}
    if any(token in text for token in ["삭제", "지워", "delete", "remove"]) and ("일정" in text or "미팅" in text or "회의" in text):
        return {"intent": "delete_event", "title": text}
    if (
        any(token in text for token in ["수정", "바꿔", "변경", "update", "modify"])
        and ("일정" in text or "미팅" in text or "회의" in text)
        and _contains_datetime_phrase(text)
    ):
        return {"intent": "move_event", "title": source_segment or text, "task_keyword": source_segment or text, "start": dest_segment or text}
    if any(token in text for token in ["수정", "바꿔", "변경", "update", "modify"]) and ("일정" in text or "미팅" in text or "회의" in text):
        return {"intent": "update_event", "title": text}
    if any(token in text for token in ["이동", "옮겨"]) and ("일정" in text or "미팅" in text or "회의" in text):
        return {"intent": "move_event", "title": text, "start": text}
    if any(token in text for token in ["빈시간", "빈 시간", "free time", "available slot", "가용시간", "가능한 시간"]):
        return {"intent": "find_free_time", "target_date": text, "duration_minutes": 60}
    if any(token in text for token in ["일정 보여", "일정 알려", "스케줄 보여", "agenda", "calendar"]) and any(
        token in text for token in ["오늘", "내일", "이번주", "다음주", "목록", "list", "보여", "알려"]
    ):
        return {"intent": "list_events", "target_date": text, "limit": 10}
    if any(token in text for token in ["할일 보여", "할 일 보여", "태스크 보여", "task list", "todo list", "목록"]):
        return {"intent": "list_tasks", "limit": 10}
    if allow_openai_nli and is_openai_available():
        try:
            parsed = parse_nli_openai(text, base_dt=datetime.utcnow())
            return {
                "intent": parsed.intent,
                "title": parsed.title,
                "due": parsed.due,
                "effort_minutes": parsed.effort_minutes,
                "priority": parsed.priority,
                "target_date": parsed.time_hint,
                "time_hint": parsed.time_hint,
            }
        except OpenAIIntegrationError:
            pass

    if (
        any(token in text for token in ["일정", "미팅", "회의", "캘린더"])
        and any(token in text for token in ["추가", "등록", "생성", "만들", "잡아", "잡아줘"])
    ) or "create event" in lowered:
        return {"intent": "create_event", "title": text, "due": text, "effort_minutes": 60}
    if any(keyword in text for keyword in ["할일", "태스크", "task", "todo", "to-do", "업무"]) and any(
        keyword in text for keyword in ["추가", "만들", "등록", "create"]
    ):
        return {"intent": "create_task", "title": text, "effort_minutes": 60, "priority": "medium"}
    if any(keyword in text for keyword in ["추가", "만들", "등록"]) or "create task" in lowered:
        return {"intent": "create_task", "title": text, "effort_minutes": 60, "priority": "medium"}
    if any(keyword in text for keyword in ["일정", "재배치", "조정"]) or "reschedule" in lowered:
        return {"intent": "reschedule_request", "time_hint": text}
    return {"intent": "unknown"}


def _fast_plan_actions(message: str) -> list[dict]:
    """Fast rule-based plan extraction without LLM.

    Handles deterministic commands quickly and only falls back to LLM for ambiguous cases.
    """
    raw = (message or "").strip()
    if not raw:
        return []

    schedule_action = _infer_schedule_fast_action(raw)
    if schedule_action:
        return [schedule_action]

    inferred = _infer_local_create_or_task_action(raw)
    if inferred and inferred.get("intent") in {"create_task", "create_event"}:
        return [inferred]

    primary = _fallback_classify(raw, allow_openai_nli=False)
    if str(primary.get("intent") or "unknown") != "unknown":
        return [primary]

    segments = [segment.strip() for segment in re.split(r"[;\n\r]+| 그리고 ", raw) if segment.strip()]
    if len(segments) <= 1:
        return []

    actions: list[dict] = []
    for segment in segments:
        schedule_segment = _infer_schedule_fast_action(segment)
        if schedule_segment:
            actions.append(schedule_segment)
            continue

        inferred_segment = _infer_local_create_or_task_action(segment)
        if inferred_segment and inferred_segment.get("intent") in {"create_task", "create_event"}:
            actions.append(inferred_segment)
            continue

        parsed = _fallback_classify(segment, allow_openai_nli=False)
        if str(parsed.get("intent") or "unknown") != "unknown":
            actions.append(parsed)
    return actions


FAST_PATH_SAFE_INTENTS = {
    "create_task",
    "create_event",
    "update_due",
    "update_priority",
    "move_event",
    "delete_event",
    "update_event",
    "list_tasks",
    "list_events",
    "find_free_time",
    "reschedule_after_hour",
    "delete_duplicate_tasks",
    "delete_duplicate_events",
    "register_meeting_note",
}


def _can_accept_fast_actions(actions: list[dict], message: str) -> bool:
    if not actions:
        return False
    if _has_reference_phrase(message):
        return False

    for action in actions:
        intent = str(action.get("intent") or "unknown")
        if intent not in FAST_PATH_SAFE_INTENTS:
            return False
        if intent == "create_event":
            title = _resolve_creation_title(action, message, intent="create_event")
            if _is_ambiguous_creation_title(title, intent="create_event"):
                return False
        if intent == "create_task":
            title = _resolve_creation_title(action, message, intent="create_task")
            if _is_ambiguous_creation_title(title, intent="create_task"):
                return False
    return True


def _quick_plan_actions(message: str) -> list[dict] | None:
    raw = (message or "").strip()
    if not raw:
        return None

    schedule_action = _infer_schedule_fast_action(raw)
    if schedule_action and _can_accept_fast_actions([schedule_action], raw):
        return [schedule_action]

    inferred = _infer_local_create_or_task_action(raw)
    if inferred and _can_accept_fast_actions([inferred], raw):
        return [inferred]

    fast_actions = _fast_plan_actions(raw)
    if _can_accept_fast_actions(fast_actions, raw):
        return fast_actions
    return None


def _should_try_nli_first(message: str) -> bool:
    if _is_multi_intent_message(message) or _has_reference_phrase(message):
        return False
    normalized_len = len(_normalize_text(message))
    if normalized_len > 80:
        return False
    lowered = message.lower()
    if any(token in lowered for token in ["회의록", "transcript", "summary"]):
        return False
    return True


def _detect_new_command_while_clarifying(message: str, llm_available: bool) -> list[dict]:
    quick_actions = _quick_plan_actions(message)
    if quick_actions:
        return quick_actions
    if llm_available and _should_try_nli_first(message):
        return _nli_plan_actions(message) or []
    return []


def _is_multi_intent_message(message: str) -> bool:
    lowered = message.lower()
    markers = [";", "\n", ",", " 그리고 ", " 또한 ", " 그리고요 ", " 및 ", "&", " and ", " then ", " next ", " 또 "]
    return any(marker in lowered for marker in markers)


def _map_nli_to_plan_action(message: str, parsed) -> dict | None:
    intent = str(getattr(parsed, "intent", "unknown") or "unknown")
    if intent == "unknown":
        return None

    title = getattr(parsed, "title", None)
    due = getattr(parsed, "due", None)
    effort_minutes = getattr(parsed, "effort_minutes", None)
    priority = getattr(parsed, "priority", None)
    time_hint = getattr(parsed, "time_hint", None)
    task_keyword = getattr(parsed, "task_keyword", None)
    task_title = getattr(parsed, "task_title", None)
    start = getattr(parsed, "start", None)
    end = getattr(parsed, "end", None)
    new_title = getattr(parsed, "new_title", None)
    cutoff_hour = getattr(parsed, "cutoff_hour", None)
    duration_minutes = getattr(parsed, "duration_minutes", None)

    target_keyword = task_keyword or task_title or title

    if intent == "create_task":
        return {
            "intent": "create_task",
            "title": title,
            "due": due,
            "effort_minutes": effort_minutes,
            "priority": priority,
        }
    if intent == "create_event":
        return {
            "intent": "create_event",
            "title": title,
            "due": due or start,
            "duration_minutes": duration_minutes or effort_minutes,
        }
    if intent == "update_task":
        return {
            "intent": "update_task",
            "task_keyword": target_keyword,
            "title": title,
            "due": due,
            "priority": priority,
        }
    if intent == "update_due":
        return {
            "intent": "update_due",
            "task_keyword": target_keyword,
            "title": target_keyword,
            "due": due,
        }
    if intent == "update_priority":
        return {
            "intent": "update_priority",
            "task_keyword": target_keyword,
            "title": target_keyword,
            "priority": priority,
        }
    if intent == "move_event":
        return {
            "intent": "move_event",
            "task_keyword": target_keyword,
            "title": target_keyword,
            "start": start or due,
            "end": end,
            "duration_minutes": duration_minutes,
        }
    if intent == "update_event":
        if start or due or end:
            return {
                "intent": "move_event",
                "task_keyword": target_keyword,
                "title": target_keyword,
                "start": start or due,
                "end": end,
                "duration_minutes": duration_minutes,
            }
        return {
            "intent": "update_event",
            "task_keyword": target_keyword,
            "title": target_keyword,
            "new_title": new_title,
            "duration_minutes": duration_minutes,
            "end": end,
        }
    if intent == "reschedule_request":
        hint = time_hint or due or message
        return {"intent": "reschedule_request", "reschedule_hint": hint, "time_hint": time_hint}
    if intent == "list_tasks":
        return {"intent": "list_tasks", "limit": getattr(parsed, "limit", None)}
    if intent == "list_events":
        return {"intent": "list_events", "target_date": time_hint or due, "limit": getattr(parsed, "limit", None)}
    if intent == "find_free_time":
        return {
            "intent": "find_free_time",
            "target_date": time_hint or due,
            "duration_minutes": effort_minutes,
        }
    if intent == "reschedule_after_hour":
        resolved_cutoff = cutoff_hour
        if resolved_cutoff is None:
            resolved_cutoff = _extract_cutoff_hour(None, str(time_hint or due or message or ""))
        return {
            "intent": "reschedule_after_hour",
            "cutoff_hour": resolved_cutoff,
            "reschedule_hint": time_hint,
            "title": title,
        }
    if intent == "delete_duplicate_tasks":
        return {
            "intent": "delete_duplicate_tasks",
            "title": title,
        }
    if intent == "delete_duplicate_events":
        return {
            "intent": "delete_duplicate_events",
            "task_keyword": target_keyword,
            "title": target_keyword,
        }
    if intent == "delete_event":
        return {"intent": "delete_event", "task_keyword": target_keyword, "title": target_keyword}
    if intent == "complete_task":
        return {"intent": "complete_task", "task_keyword": target_keyword, "title": target_keyword}
    if intent == "delete_task":
        return {"intent": "delete_task", "task_keyword": target_keyword, "title": target_keyword}
    return None


def _nli_plan_actions(message: str) -> list[dict] | None:
    # Avoid LLM-only NLI parser for context-dependent references.
    if _has_reference_phrase(message):
        return None

    if not is_openai_available():
        return None

    try:
        parsed = parse_nli_openai(message, base_dt=datetime.utcnow())
    except OpenAIIntegrationError:
        return None

    mapped = _map_nli_to_plan_action(message, parsed)
    return [mapped] if mapped else None


def _plan_actions_with_fallback(
    db: Session,
    message: str,
    history_context: list[dict],
) -> tuple[list[dict], str | None]:
    quick_actions = _quick_plan_actions(message)
    if quick_actions:
        return quick_actions, None

    if settings.assistant_llm_only:
        if _should_try_nli_first(message):
            nli_actions = _nli_plan_actions(message)
            if nli_actions:
                return nli_actions, None
        # In LLM-first mode, use the planner directly and avoid heuristic pre-parsing
        # to reduce branch drift and keep behavior anchored to the current state prompt.
        try:
            return _plan_actions_with_llm(db, message, history_context)
        except Exception as exc:
            # Temporary hardening: fallback to deterministic parsers on transient LLM failures
            # (e.g., timeout) so the assistant remains usable.
            fast_actions = _fast_plan_actions(message)
            if fast_actions:
                return fast_actions, None
            if _should_try_nli_first(message):
                nli_actions = _nli_plan_actions(message)
                if nli_actions:
                    return nli_actions, None

            # rethrow for upstream llm_only failure response when no fallback can parse
            raise

    fast_actions = _fast_plan_actions(message)
    if fast_actions:
        return fast_actions, None

    # Use NLI parser for single-intent commands to reduce latency and keep intent quality stable.
    if _should_try_nli_first(message):
        nli_actions = _nli_plan_actions(message)
        if nli_actions:
            return nli_actions, None

    return _plan_actions_with_llm(db, message, history_context)


def _emit_chat_progress(emit: ChatProgressEmitter | None, event: str, payload: dict) -> None:
    if emit is None:
        return
    try:
        emit(event, payload)
    except Exception:
        return


def _build_chat_response(
    payload: AssistantChatRequest,
    db: Session,
    emit: ChatProgressEmitter | None = None,
) -> AssistantChatResponse:
    message = payload.message.strip()
    history_context = [
        {"role": turn.role, "text": turn.text.strip()}
        for turn in payload.history
        if turn.text.strip()
    ]
    llm_available = is_openai_available()
    _emit_chat_progress(emit, "status", {"message": "요청 분석 중..."})

    if settings.assistant_llm_only and not llm_available:
        return AssistantChatResponse(
            reply="현재 LLM 연결을 사용할 수 없습니다. OPENAI 키/모델 설정을 확인해 주세요.",
            actions=[AssistantActionOut(type="llm_unavailable", detail={})],
            refresh=[],
        )

    pending_clarification = _latest_pending_approval(db, types=(CHAT_CLARIFICATION_TYPE,))
    if pending_clarification:
        clarification_id = pending_clarification.id
        typed_approval_id = _extract_uuid(message)

        if _is_negative(message) and (typed_approval_id in {None, clarification_id}):
            pending_clarification.status = "rejected"
            pending_clarification.resolved_at = datetime.utcnow()
            pending_clarification.reason = "clarification_canceled_by_user"
            db.commit()
            return AssistantChatResponse(
                reply="요청을 취소했습니다. 새로 요청해 주세요.",
                actions=[AssistantActionOut(type="clarification_canceled", detail={"clarification_id": pending_clarification.id})],
                refresh=["approvals"],
            )

        if _is_affirmative(message) and (typed_approval_id in {None, clarification_id}):
            question = str(pending_clarification.payload.get("question") or "").strip() or "질문에 답해 주세요."
            return AssistantChatResponse(
                reply=f"이 항목은 승인 요청이 아니라 추가 정보가 필요합니다.\n{question}",
                actions=[AssistantActionOut(type="clarification_requested", detail={"clarification_id": clarification_id})],
                refresh=["approvals"],
            )

        if typed_approval_id and typed_approval_id != clarification_id and (_is_affirmative(message) or _is_negative(message)):
            pending_clarification = None

    if pending_clarification:
        incoming_actions = _detect_new_command_while_clarifying(message, llm_available)

        has_new_command = any(str(item.get("intent") or "unknown") != "unknown" for item in incoming_actions)
        if has_new_command:
            pending_clarification.status = "rejected"
            pending_clarification.resolved_at = datetime.utcnow()
            pending_clarification.reason = "clarification_superseded_by_new_command"
            db.commit()
            pending_clarification = None

    if pending_clarification:
        original_message = str(pending_clarification.payload.get("original_message") or "").strip()
        pending_clarification.status = "approved"
        pending_clarification.resolved_at = datetime.utcnow()
        pending_clarification.reason = "clarification_resolved_via_chat"
        db.commit()

        if original_message:
            if _is_ambiguous_short_request(original_message):
                message = message
            else:
                message = f"{original_message}\n{message}"

    approval_id = _extract_uuid(message)
    if _is_affirmative(message) or _is_negative(message):
        pending_approval = _latest_pending_approval(
            db,
            types=tuple(CHAT_APPROVABLE_TYPES),
            approval_id=approval_id,
        )
        if pending_approval:
            _emit_chat_progress(emit, "status", {"message": "승인 요청 처리 중..."})
            reply, actions, refresh = _resolve_pending_approval_by_chat(
                db,
                pending_approval,
                approve=_is_affirmative(message),
                message=message,
                history_context=history_context,
            )
            return AssistantChatResponse(reply=reply, actions=actions, refresh=sorted(set(refresh)))

    planned_actions: list[dict] = []
    plan_note: str | None = None

    if llm_available:
        try:
            _emit_chat_progress(emit, "status", {"message": "의도 해석 중..."})
            planned_actions, plan_note = _plan_actions_with_fallback(db, message, history_context)
        except OpenAIIntegrationError as exc:
            if settings.assistant_llm_only:
                return AssistantChatResponse(
                    reply="LLM 호출에 실패했습니다. 잠시 후 다시 시도해 주세요.",
                    actions=[AssistantActionOut(type="llm_error", detail={"reason": str(exc)[:200]})],
                    refresh=[],
                )
            planned_actions = []
            plan_note = None

    if not planned_actions:
        if settings.assistant_llm_only and llm_available:
            question = _clarification_question(message, plan_note)
            clarification_req = _queue_chat_clarification(db, question, message)
            return AssistantChatResponse(
                reply=f"{question}\n답변해 주시면 이어서 처리합니다.",
                actions=[AssistantActionOut(type="clarification_requested", detail={"clarification_id": clarification_req.id})],
                refresh=["approvals"],
            )
        planned_actions = [_fallback_classify(message, allow_openai_nli=False)]

    if settings.assistant_llm_only and llm_available:
        executable_actions = [item for item in planned_actions if str(item.get("intent") or "unknown") != "unknown"]
        if not executable_actions:
            question = _clarification_question(message, plan_note)
            clarification_req = _queue_chat_clarification(db, question, message)
            return AssistantChatResponse(
                reply=f"{question}\n답변해 주시면 이어서 처리합니다.",
                actions=[AssistantActionOut(type="clarification_requested", detail={"clarification_id": clarification_req.id})],
                refresh=["approvals"],
            )
        planned_actions = executable_actions

    has_meeting_intent = any(item.get("intent") == "register_meeting_note" for item in planned_actions)
    if has_meeting_intent:
        planned_actions = [item for item in planned_actions if item.get("intent") == "register_meeting_note"][:1]

    singleton_intents = {"register_meeting_note", "reschedule_after_hour", "delete_duplicate_tasks", "delete_duplicate_events"}
    seen_singletons: set[str] = set()
    unique_actions: list[dict] = []
    for item in planned_actions:
        intent = str(item.get("intent") or "")
        if intent in singleton_intents:
            if intent in seen_singletons:
                continue
            seen_singletons.add(intent)
        unique_actions.append(item)
    planned_actions = unique_actions

    if not (settings.assistant_llm_only and llm_available) and _looks_like_due_change(message):
        rewritten: list[dict] = []
        for item in planned_actions:
            if item.get("intent") == "reschedule_request":
                converted = dict(item)
                converted["intent"] = "update_due"
                if not converted.get("due"):
                    converted["due"] = message
                if not converted.get("title") and not converted.get("task_keyword"):
                    converted["title"] = message
                rewritten.append(converted)
                continue
            rewritten.append(item)
        planned_actions = rewritten

    merged_actions: list[AssistantActionOut] = []
    refresh_set: set[str] = set()
    reply_parts: list[str] = []
    reference_mode = _has_reference_phrase(message)
    reference_action_processed = False

    executable_actions = planned_actions[:5]
    if executable_actions:
        _emit_chat_progress(emit, "status", {"message": "작업 실행 중..."})

    for index, parsed in enumerate(executable_actions, start=1):
        intent = parsed.get("intent", "unknown")
        if intent == "unknown":
            continue
        if reference_mode and intent in {"complete_task", "update_priority", "update_due"} and reference_action_processed:
            continue

        clarification = _needs_clarification_for_action(db, parsed, message, history_context)
        if clarification:
            clarification_req = _queue_chat_clarification(db, clarification, message)
            return AssistantChatResponse(
                reply=f"{clarification}\n답변해 주시면 이어서 처리합니다.",
                actions=[AssistantActionOut(type="clarification_requested", detail={"clarification_id": clarification_req.id})],
                refresh=["approvals"],
            )

        reply, actions, refresh = _run_one_action(
            db,
            parsed,
            message=message,
            history_context=history_context,
            require_confirmation=True,
        )
        reply_parts.append(reply)
        merged_actions.extend(actions)
        refresh_set.update(refresh)
        _emit_chat_progress(
            emit,
            "progress",
            {
                "message": f"작업 {index}/{len(executable_actions)} 처리 완료",
                "reply": reply,
                "index": index,
            },
        )

        if reference_mode and intent in {"complete_task", "update_priority", "update_due"}:
            reference_action_processed = True

    if not reply_parts:
        question = _clarification_question(message, plan_note)
        clarification_req = _queue_chat_clarification(db, question, message)
        return AssistantChatResponse(
            reply=f"{question}\n답변해 주시면 이어서 처리합니다.",
            actions=[AssistantActionOut(type="clarification_requested", detail={"clarification_id": clarification_req.id})],
            refresh=["approvals"],
        )

    merged_reply = "\n".join(f"{index + 1}. {part}" for index, part in enumerate(reply_parts))
    return AssistantChatResponse(reply=merged_reply, actions=merged_actions, refresh=sorted(refresh_set))


def _sse_event(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


@router.post("/chat", response_model=AssistantChatResponse)
def chat(payload: AssistantChatRequest, db: Session = Depends(get_db)) -> AssistantChatResponse:
    return _build_chat_response(payload, db)


@router.post("/chat/stream")
def chat_stream(payload: AssistantChatRequest) -> StreamingResponse:
    queue: Queue[tuple[str, dict] | None] = Queue()

    def emit(event: str, event_payload: dict) -> None:
        queue.put((event, event_payload))

    def worker() -> None:
        with SessionLocal() as db:
            try:
                result = _build_chat_response(payload, db, emit=emit)
                queue.put(("result", result.model_dump(mode="json")))
            except Exception as exc:
                queue.put(("error", {"message": str(exc) or "assistant stream failed"}))
            finally:
                queue.put(None)

    threading.Thread(target=worker, name="assistant-chat-stream", daemon=True).start()

    def event_stream():
        while True:
            try:
                item = queue.get(timeout=15.0)
            except Empty:
                yield ": keepalive\n\n"
                continue
            if item is None:
                yield _sse_event("done", {"ok": True})
                break
            event, event_payload = item
            yield _sse_event(event, event_payload)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
