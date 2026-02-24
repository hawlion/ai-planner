from __future__ import annotations

from datetime import UTC, datetime, timedelta

import dateparser
from fastapi import APIRouter, Depends
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_db
from app.models import ActionItemCandidate, ApprovalRequest, Meeting, Task
from app.schemas import AssistantActionOut, AssistantChatRequest, AssistantChatResponse
from app.services.actions import approve_candidate
from app.services.core import ensure_profile
from app.services.graph_service import GraphApiError, GraphAuthError, is_graph_connected, sync_blocks_to_outlook
from app.services.meeting_extractor import extract_action_items
from app.services.openai_client import (
    OpenAIIntegrationError,
    extract_action_items_openai,
    is_openai_available,
    parse_assistant_action_openai,
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


def _parse_due(value: str | None, fallback_text: str) -> datetime | None:
    source = value if value and value.strip() else fallback_text
    parsed = dateparser.parse(
        source,
        languages=["ko", "en"],
        settings={
            "PREFER_DATES_FROM": "future",
            "TIMEZONE": settings.timezone,
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
    )
    return parsed


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


def _find_task(db: Session, keyword: str | None) -> Task | None:
    if keyword and keyword.strip():
        key = keyword.strip()
        row = db.execute(
            select(Task)
            .where(or_(Task.title.contains(key), Task.description.contains(key)))
            .order_by(Task.updated_at.desc())
        ).scalars().first()
        if row:
            return row

    return db.execute(select(Task).where(Task.status.in_(["todo", "in_progress"])).order_by(Task.updated_at.desc())).scalars().first()


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
            f"재배치 제안을 생성했고 승인 요청을 올렸습니다. (approval {approval.id})",
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
    task = _find_task(db, keyword)
    if not task:
        return ("완료 처리할 할일을 찾지 못했습니다. 작업 제목을 조금 더 구체적으로 말해 주세요.", [], [])
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

    task = _find_task(db, keyword)
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


def _fallback_classify(text: str) -> dict:
    lowered = text.lower()
    if _looks_like_meeting_note(text):
        return {"intent": "register_meeting_note", "meeting_note": text}
    if "우선순위" in text or "priority" in lowered:
        priority = None
        for token in PRIORITY_MAP:
            if token in text or token in lowered:
                priority = token
                break
        return {"intent": "update_priority", "title": text, "priority": priority}
    if "완료" in text or "done" in lowered:
        return {"intent": "complete_task", "title": text}

    if is_openai_available():
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
    actions: list[AssistantActionOut] = []
    refresh: list[str] = []

    parsed: dict
    if is_openai_available():
        try:
            llm = parse_assistant_action_openai(message, base_dt=datetime.now(UTC))
            parsed = llm.model_dump()
        except OpenAIIntegrationError:
            parsed = _fallback_classify(message)
    else:
        parsed = _fallback_classify(message)

    intent = parsed.get("intent", "unknown")

    if intent == "register_meeting_note":
        reply, actions, refresh = _register_meeting_and_apply(db, parsed.get("meeting_note") or message)
        return AssistantChatResponse(reply=reply, actions=actions, refresh=refresh)

    if intent == "create_task":
        title = (parsed.get("title") or message).strip()
        due = _parse_due(parsed.get("due"), message)
        effort = int(parsed.get("effort_minutes") or 60)
        priority = str(parsed.get("priority") or "medium")
        reply, actions, refresh = _create_task_from_message(db, title, due, effort, priority)
        return AssistantChatResponse(reply=reply, actions=actions, refresh=refresh)

    if intent == "reschedule_request":
        hint = str(parsed.get("time_hint") or message)
        reply, actions, refresh = _reschedule_from_message(db, hint)
        return AssistantChatResponse(reply=reply, actions=actions, refresh=refresh)

    if intent == "complete_task":
        reply, actions, refresh = _complete_task(db, parsed.get("title") or message)
        return AssistantChatResponse(reply=reply, actions=actions, refresh=refresh)

    if intent == "update_priority":
        reply, actions, refresh = _update_priority(db, parsed.get("title") or message, parsed.get("priority"))
        return AssistantChatResponse(reply=reply, actions=actions, refresh=refresh)

    return AssistantChatResponse(
        reply=(
            "요청 의도를 명확히 파악하지 못했습니다. 예시: "
            "'내일 오전 보고서 작업 추가', '보고서 작업 완료 처리', "
            "'이번주 일정 재배치', '회의록: ...'"
        ),
        actions=[],
        refresh=[],
    )
