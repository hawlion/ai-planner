from __future__ import annotations

from datetime import datetime, timedelta

import dateparser
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Task
from app.schemas import NLIRequest, NLIResponse

router = APIRouter(prefix="/nli", tags=["nli"])


@router.post("/command", response_model=NLIResponse)
def command(payload: NLIRequest, db: Session = Depends(get_db)) -> NLIResponse:
    text = payload.text.strip()
    lowered = text.lower()

    if "마감" in text or "due" in lowered or "까지" in text:
        parsed_due = dateparser.parse(text, languages=["ko", "en"], settings={"PREFER_DATES_FROM": "future"})
    else:
        parsed_due = None

    if any(keyword in text for keyword in ["추가", "만들", "등록"]) or "create task" in lowered:
        title = text
        for token in ["할일", "작업", "task", "추가", "만들어줘", "만들기", "등록", ":"]:
            title = title.replace(token, "")
        title = title.strip() or "새 작업"

        task = Task(title=title, due=parsed_due, effort_minutes=60, priority="medium", source="chat")
        db.add(task)
        db.commit()
        db.refresh(task)

        return NLIResponse(
            intent="create_task",
            extracted={"task_id": task.id, "title": task.title, "due": task.due},
            note="자연어 요청을 작업 생성으로 적용했습니다.",
        )

    if any(keyword in text for keyword in ["오늘", "내일", "다음 주", "오후", "오전"]):
        return NLIResponse(
            intent="reschedule_request",
            extracted={
                "time_hint": text,
                "window": {
                    "from": datetime.utcnow().isoformat(),
                    "to": (datetime.utcnow() + timedelta(days=2)).isoformat(),
                },
            },
            note="시간 조정 요청으로 해석했습니다. /scheduling/proposals API를 호출해 제안을 받으세요.",
        )

    return NLIResponse(
        intent="unknown",
        extracted={"raw": text},
        note="명확한 의도를 찾지 못했습니다. 예: '내일 오전에 보고서 작성 작업 추가해줘'",
    )
