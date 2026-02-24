from __future__ import annotations

from datetime import datetime, timedelta

import dateparser
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Task
from app.schemas import NLIRequest, NLIResponse
from app.services.openai_client import OpenAIIntegrationError, is_openai_available, parse_nli_openai

router = APIRouter(prefix="/nli", tags=["nli"])


def _fallback_nli_parse(text: str) -> dict:
    lowered = text.lower()
    due = None
    if "마감" in text or "due" in lowered or "까지" in text:
        due = dateparser.parse(text, languages=["ko", "en"], settings={"PREFER_DATES_FROM": "future"})

    if any(keyword in text for keyword in ["추가", "만들", "등록"]) or "create task" in lowered:
        title = text
        for token in ["할일", "작업", "task", "추가", "만들어줘", "만들기", "등록", ":"]:
            title = title.replace(token, "")
        title = title.strip() or "새 작업"
        return {
            "intent": "create_task",
            "title": title,
            "due": due,
            "effort_minutes": 60,
            "priority": "medium",
            "note": "규칙 기반 파서로 작업 생성 요청을 해석했습니다.",
        }

    if any(keyword in text for keyword in ["오늘", "내일", "다음 주", "오후", "오전"]):
        return {
            "intent": "reschedule_request",
            "time_hint": text,
            "window": {
                "from": datetime.utcnow().isoformat(),
                "to": (datetime.utcnow() + timedelta(days=2)).isoformat(),
            },
            "note": "규칙 기반 파서로 시간 조정 요청을 해석했습니다.",
        }

    return {
        "intent": "unknown",
        "note": "규칙 기반 파서에서 명확한 의도를 찾지 못했습니다.",
    }


def _parse_due_value(due_value: str | datetime | None, text_fallback: str) -> datetime | None:
    if isinstance(due_value, datetime):
        return due_value
    if isinstance(due_value, str) and due_value.strip():
        parsed = dateparser.parse(
            due_value,
            languages=["ko", "en"],
            settings={"PREFER_DATES_FROM": "future", "RETURN_AS_TIMEZONE_AWARE": True},
        )
        if parsed is not None:
            return parsed

    return dateparser.parse(
        text_fallback,
        languages=["ko", "en"],
        settings={"PREFER_DATES_FROM": "future", "RETURN_AS_TIMEZONE_AWARE": True},
    )


@router.post("/command", response_model=NLIResponse)
def command(payload: NLIRequest, db: Session = Depends(get_db)) -> NLIResponse:
    text = payload.text.strip()
    parsed = None

    if is_openai_available():
        try:
            llm = parse_nli_openai(text, base_dt=datetime.utcnow())
            parsed = {
                "intent": llm.intent,
                "title": llm.title,
                "due": llm.due,
                "effort_minutes": llm.effort_minutes or 60,
                "priority": llm.priority or "medium",
                "time_hint": llm.time_hint,
                "note": llm.note or "OpenAI 기반 파서 결과입니다.",
            }
        except OpenAIIntegrationError:
            parsed = _fallback_nli_parse(text)
    else:
        parsed = _fallback_nli_parse(text)

    if parsed["intent"] == "create_task":
        title = (parsed.get("title") or text).strip()
        parsed_due = _parse_due_value(parsed.get("due"), text)
        task = Task(
            title=title,
            due=parsed_due,
            effort_minutes=int(parsed.get("effort_minutes") or 60),
            priority=str(parsed.get("priority") or "medium"),
            source="chat",
        )
        db.add(task)
        db.commit()
        db.refresh(task)

        return NLIResponse(
            intent="create_task",
            extracted={"task_id": task.id, "title": task.title, "due": task.due},
            note=parsed.get("note") or "자연어 요청을 작업 생성으로 적용했습니다.",
        )

    if parsed["intent"] == "reschedule_request":
        return NLIResponse(
            intent="reschedule_request",
            extracted={
                "time_hint": parsed.get("time_hint") or text,
                "window": {
                    "from": datetime.utcnow().isoformat(),
                    "to": (datetime.utcnow() + timedelta(days=2)).isoformat(),
                },
            },
            note=(parsed.get("note") or "시간 조정 요청으로 해석했습니다.") + " /scheduling/proposals API를 호출해 제안을 받으세요.",
        )

    return NLIResponse(
        intent="unknown",
        extracted={"raw": text},
        note=parsed.get("note") or "명확한 의도를 찾지 못했습니다. 예: '내일 오전에 보고서 작성 작업 추가해줘'",
    )
