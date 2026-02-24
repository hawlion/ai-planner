from __future__ import annotations

from datetime import UTC, datetime, timedelta
import re
from zoneinfo import ZoneInfo

import dateparser
from fastapi import APIRouter, Depends
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_db
from app.models import ActionItemCandidate, ApprovalRequest, CalendarBlock, Meeting, SchedulingProposal, Task
from app.schemas import AssistantActionOut, AssistantChatRequest, AssistantChatResponse
from app.services.actions import approve_candidate
from app.services.core import ensure_profile
from app.services.graph_service import (
    GraphApiError,
    GraphAuthError,
    delete_blocks_from_outlook,
    is_graph_connected,
    sync_blocks_to_outlook,
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
CHAT_APPROVABLE_TYPES = {CHAT_CONFIRM_TYPE, "reschedule", "action_item"}
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


def _has_reference_phrase(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in REFERENCE_TOKENS)


def _is_affirmative(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if lowered in YES_TOKENS:
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

    hint_due = parse_korean_hint(fallback_text)
    if hint_due is not None:
        return hint_due

    if value and value.strip():
        parsed_value = parse_general(value)
        if parsed_value is not None:
            return parsed_value

    return parse_general(fallback_text)


def _looks_like_meeting_note(text: str) -> bool:
    lowered = text.lower()
    if "회의록" in text or "meeting notes" in lowered or "회의 내용" in text:
        return True
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    speaker_like = sum(1 for line in lines if ":" in line and len(line.split(":", 1)[0]) <= 20)
    return len(lines) >= 2 and speaker_like >= 1


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
    rows = db.execute(select(Task).order_by(Task.updated_at.desc()).limit(40)).scalars().all()
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
            db.add(
                ApprovalRequest(
                    type="action_item",
                    status="pending",
                    payload={
                        "candidate_id": candidate.id,
                        "meeting_id": meeting.meeting_id,
                        "reason": "low_confidence_or_large_effort",
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

    outlook_synced = 0
    if created_blocks and is_graph_connected(db):
        try:
            sync_result = sync_blocks_to_outlook(db, created_blocks)
            outlook_synced = int(sync_result["synced"])
        except (GraphAuthError, GraphApiError):
            outlook_synced = 0

    meeting.extraction_status = "completed"
    db.commit()

    reply = (
        f"회의록을 등록했고 액션아이템 {len(drafts)}건을 처리했습니다. "
        f"자동 반영 {auto_tasks}건, 승인 대기 {approvals}건"
    )
    if outlook_synced:
        reply += f", Outlook 동기화 {outlook_synced}건"
    reply += "."

    return (
        reply,
        [
            AssistantActionOut(type="meeting_registered", detail={"meeting_id": meeting.id}),
            AssistantActionOut(
                type="action_items_processed",
                detail={"detected": len(drafts), "auto_tasks": auto_tasks, "approval_pending": approvals},
            ),
        ],
        ["calendar", "tasks", "approvals"],
    )


def _create_task_from_message(
    db: Session, title: str, due: datetime | None, effort_minutes: int, priority: str
) -> tuple[str, list[AssistantActionOut], list[str]]:
    task = Task(
        title=title.strip() or "새 작업",
        due=due,
        effort_minutes=max(15, min(8 * 60, effort_minutes)),
        priority=priority if priority in {"low", "medium", "high", "critical"} else "medium",
        source="chat",
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return (
        f"할일을 생성했습니다: {task.title}",
        [AssistantActionOut(type="task_created", detail={"task_id": task.id, "title": task.title})],
        ["tasks"],
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
            [AssistantActionOut(type="reschedule_approval_requested", detail={"approval_id": approval.id})],
            ["approvals", "calendar"],
        )

    created_blocks, _ = apply_proposal(db, proposal)
    synced = 0
    if created_blocks and is_graph_connected(db):
        try:
            sync_result = sync_blocks_to_outlook(db, created_blocks)
            synced = int(sync_result["synced"])
        except (GraphAuthError, GraphApiError):
            synced = 0

    reply = f"재배치를 적용했습니다. 새 일정 {len(created_blocks)}건 생성"
    if synced:
        reply += f", Outlook 동기화 {synced}건"
    if hint.strip():
        reply += f" (요청: {hint.strip()})"
    reply += "."
    return (
        reply,
        [AssistantActionOut(type="reschedule_applied", detail={"proposal_id": proposal.id, "created_blocks": len(created_blocks)})],
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
    task.version += 1
    db.commit()
    return (
        f"마감일을 변경했습니다: {task.title} -> {due.strftime('%Y-%m-%d %H:%M')}",
        [AssistantActionOut(type="task_due_updated", detail={"task_id": task.id, "due": due.isoformat()})],
        ["tasks"],
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
    created_blocks, _ = apply_proposal(db, proposal)
    if not created_blocks:
        return ("재배치 제안을 만들었지만 적용 가능한 새 일정 슬롯이 없어 변경하지 못했습니다.", [], ["calendar"])

    removed_blocks = [row for row in targets if row.task_id in task_ids]

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

    synced = 0
    if created_blocks and is_graph_connected(db):
        try:
            sync_result = sync_blocks_to_outlook(db, created_blocks)
            synced = int(sync_result["synced"])
        except (GraphAuthError, GraphApiError):
            synced = 0

    reply = (
        f"{cutoff_hour:02d}:00 이후 일정 재배치를 적용했습니다. "
        f"기존 {removed_count}건 정리, 새 일정 {len(created_blocks)}건 생성"
    )
    if skipped_unlinked:
        reply += f", 미연결 일정 {skipped_unlinked}건 제외"
    if synced:
        reply += f", Outlook 반영 {synced}건"
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
        title = (parsed.get("title") or parsed.get("task_keyword") or message).strip()
        due = _parse_due(parsed.get("due"), message)
        effort = int(parsed.get("effort_minutes") or 60)
        priority = str(parsed.get("priority") or "medium")
        return _create_task_from_message(db, title, due, effort, priority)

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
        elif not keyword or _is_generic_keyword(keyword):
            keyword = history_keyword
        due = _parse_due(parsed.get("due"), message)
        return _update_due(db, keyword, due)

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
        db.commit()
        return (
            "요청한 작업을 취소했습니다.",
            [AssistantActionOut(type="approval_rejected", detail={"approval_id": approval.id, "approval_type": approval.type})],
            ["approvals"],
        )

    if approval.type == CHAT_CONFIRM_TYPE:
        action = approval.payload.get("action") or {}
        reply, actions, refresh = _run_one_action(
            db,
            action,
            message=approval.payload.get("source_message") or message,
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
            synced = 0
            if blocks and is_graph_connected(db):
                try:
                    sync_result = sync_blocks_to_outlook(db, blocks)
                    synced = int(sync_result["synced"])
                except (GraphAuthError, GraphApiError):
                    synced = 0
            db.commit()
            reply = f"승인되었습니다. 액션아이템을 할일로 반영했습니다: {candidate.title}"
            if synced:
                reply += f" (Outlook 동기화 {synced}건)"
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
            created_blocks, _ = apply_proposal(db, proposal)
            synced = 0
            if created_blocks and is_graph_connected(db):
                try:
                    sync_result = sync_blocks_to_outlook(db, created_blocks)
                    synced = int(sync_result["synced"])
                except (GraphAuthError, GraphApiError):
                    synced = 0
            reply = f"승인되었습니다. 재배치를 적용해 일정 {len(created_blocks)}건을 생성했습니다."
            if synced:
                reply += f" (Outlook 동기화 {synced}건)"
            return (
                reply,
                [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id, "approval_type": approval.type})],
                ["approvals", "calendar"],
            )
        db.commit()
        return ("승인할 재배치 제안을 찾지 못했습니다.", [], ["approvals"])

    db.commit()
    return ("승인되었습니다.", [AssistantActionOut(type="approval_approved", detail={"approval_id": approval.id})], ["approvals"])


def _fallback_classify(text: str, *, allow_openai_nli: bool = True) -> dict:
    lowered = text.lower()
    if _looks_like_meeting_note(text):
        return {"intent": "register_meeting_note", "meeting_note": text}
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
    if "완료" in text or "done" in lowered:
        return {"intent": "complete_task", "title": text}
    if allow_openai_nli and is_openai_available():
        try:
            parsed = parse_nli_openai(text, base_dt=datetime.utcnow())
            return {
                "intent": parsed.intent,
                "title": parsed.title,
                "due": parsed.due,
                "effort_minutes": parsed.effort_minutes,
                "priority": parsed.priority,
                "time_hint": parsed.time_hint,
            }
        except OpenAIIntegrationError:
            pass

    if any(keyword in text for keyword in ["추가", "만들", "등록"]) or "create task" in lowered:
        return {"intent": "create_task", "title": text, "effort_minutes": 60, "priority": "medium"}
    if any(keyword in text for keyword in ["일정", "재배치", "조정"]) or "reschedule" in lowered:
        return {"intent": "reschedule_request", "time_hint": text}
    return {"intent": "unknown"}


@router.post("/chat", response_model=AssistantChatResponse)
def chat(payload: AssistantChatRequest, db: Session = Depends(get_db)) -> AssistantChatResponse:
    message = payload.message.strip()
    history_context = [
        {"role": turn.role, "text": turn.text.strip()}
        for turn in payload.history
        if turn.text.strip()
    ]
    pending_clarification = _latest_pending_approval(db, types=(CHAT_CLARIFICATION_TYPE,))
    if pending_clarification:
        if _is_negative(message):
            pending_clarification.status = "rejected"
            pending_clarification.resolved_at = datetime.utcnow()
            pending_clarification.reason = "clarification_canceled_by_user"
            db.commit()
            return AssistantChatResponse(
                reply="요청을 취소했습니다. 새로 요청해 주세요.",
                actions=[AssistantActionOut(type="clarification_canceled", detail={"clarification_id": pending_clarification.id})],
                refresh=["approvals"],
            )

        original_message = str(pending_clarification.payload.get("original_message") or "").strip()
        pending_clarification.status = "approved"
        pending_clarification.resolved_at = datetime.utcnow()
        pending_clarification.reason = "clarification_resolved_via_chat"
        db.commit()

        if original_message:
            message = f"{original_message}\n추가정보: {message}"

    approval_id = _extract_uuid(message)
    if _is_affirmative(message) or _is_negative(message):
        pending_approval = _latest_pending_approval(
            db,
            types=tuple(CHAT_APPROVABLE_TYPES),
            approval_id=approval_id,
        )
        if pending_approval:
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

    quick_rule = _fallback_classify(message, allow_openai_nli=False)
    quick_intent = str(quick_rule.get("intent") or "unknown")
    if quick_intent in {
        "register_meeting_note",
        "create_task",
        "reschedule_after_hour",
        "delete_duplicate_tasks",
        "update_due",
        "update_priority",
        "complete_task",
        "reschedule_request",
    }:
        planned_actions = [quick_rule]
    elif is_openai_available():
        try:
            plan = parse_assistant_plan_openai(
                message,
                base_dt=datetime.now(UTC),
                task_context=_task_context(db),
                history=history_context,
            )
            planned_actions = [item.model_dump() for item in plan.actions]
            plan_note = plan.note
        except OpenAIIntegrationError:
            planned_actions = []
            plan_note = None

    if not planned_actions:
        planned_actions = [_fallback_classify(message, allow_openai_nli=False)]

    has_meeting_intent = any(item.get("intent") == "register_meeting_note" for item in planned_actions)
    if has_meeting_intent:
        planned_actions = [item for item in planned_actions if item.get("intent") == "register_meeting_note"][:1]

    singleton_intents = {"register_meeting_note", "reschedule_after_hour", "delete_duplicate_tasks"}
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

    if _looks_like_due_change(message):
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

    for parsed in planned_actions[:5]:
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
