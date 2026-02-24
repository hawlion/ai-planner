from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import dateparser

from app.config import settings

ACTION_HINTS = (
    "해야",
    "해주세요",
    "해줘",
    "작성",
    "정리",
    "검토",
    "전달",
    "공유",
    "준비",
    "fix",
    "review",
    "send",
    "prepare",
    "update",
)

ASSIGNEE_RE = re.compile(r"(?P<name>[A-Za-z가-힣0-9_]{2,20})(?:님|이|가|는|은|께서)")
EFFORT_HOURS_RE = re.compile(r"(?P<hours>\d+)\s*시간")
EFFORT_MIN_RE = re.compile(r"(?P<mins>\d+)\s*분")
DUE_KEYWORDS_RE = re.compile(
    r"(오늘|내일|모레|이번\s*주\s*[월화수목금토일]요일|다음\s*주\s*[월화수목금토일]요일|\d{1,2}/\d{1,2}|\d{4}-\d{2}-\d{2})"
)


@dataclass
class DraftActionItem:
    title: str
    assignee_name: str | None
    due: datetime | None
    effort_minutes: int
    confidence: float
    rationale: str



def _parse_due(text: str, base_dt: datetime) -> datetime | None:
    match = DUE_KEYWORDS_RE.search(text)
    if not match:
        return None

    parsed = dateparser.parse(
        match.group(1),
        settings={
            "PREFER_DATES_FROM": "future",
            "RELATIVE_BASE": base_dt,
            "TIMEZONE": settings.timezone,
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
        languages=["ko", "en"],
    )
    return parsed


def _parse_effort(text: str) -> int:
    hour_match = EFFORT_HOURS_RE.search(text)
    if hour_match:
        hours = int(hour_match.group("hours"))
        return max(30, min(8 * 60, hours * 60))

    min_match = EFFORT_MIN_RE.search(text)
    if min_match:
        minutes = int(min_match.group("mins"))
        return max(15, min(8 * 60, minutes))

    return 60


def _extract_title(line: str) -> str:
    cleaned = re.sub(r"\s+", " ", line).strip()
    cleaned = re.sub(r"^(그러면|그럼|일단|음|어)\s*", "", cleaned)
    if len(cleaned) > 120:
        cleaned = cleaned[:117] + "..."
    return cleaned


def _confidence(has_due: bool, has_assignee: bool, has_action_hint: bool, effort_minutes: int) -> float:
    score = 0.35
    if has_action_hint:
        score += 0.25
    if has_due:
        score += 0.2
    if has_assignee:
        score += 0.15
    if effort_minutes > 180:
        score -= 0.1
    return max(0.2, min(score, 0.95))


def extract_action_items(
    transcript: list[dict],
    summary: str | None,
    base_dt: datetime | None = None,
) -> list[DraftActionItem]:
    if base_dt is None:
        base_dt = datetime.now(tz=ZoneInfo(settings.timezone))

    candidates: list[DraftActionItem] = []

    lines: list[tuple[str, str]] = []
    for utterance in transcript:
        speaker = utterance.get("speaker") or "참석자"
        text = (utterance.get("text") or "").strip()
        if text:
            lines.append((speaker, text))

    if summary:
        lines.append(("summary", summary))

    seen_titles: set[str] = set()

    for speaker, text in lines:
        lowered = text.lower()
        has_action_hint = any(keyword in lowered or keyword in text for keyword in ACTION_HINTS)
        if not has_action_hint and "까지" not in text:
            continue

        assignee_match = ASSIGNEE_RE.search(text)
        assignee = assignee_match.group("name") if assignee_match else speaker
        due = _parse_due(text, base_dt)
        effort = _parse_effort(text)
        title = _extract_title(text)
        if len(title) < 6:
            continue

        dedupe_key = title.lower()
        if dedupe_key in seen_titles:
            continue
        seen_titles.add(dedupe_key)

        confidence = _confidence(bool(due), bool(assignee_match), has_action_hint, effort)

        rationale_parts = []
        if has_action_hint:
            rationale_parts.append("행동 동사/요청 표현 감지")
        if due:
            rationale_parts.append("마감 관련 표현 감지")
        if assignee_match:
            rationale_parts.append("담당자 표현 감지")

        if not rationale_parts:
            rationale_parts.append("회의 맥락에서 후속 액션 가능성")

        candidates.append(
            DraftActionItem(
                title=title,
                assignee_name=assignee,
                due=due,
                effort_minutes=effort,
                confidence=confidence,
                rationale=", ".join(rationale_parts),
            )
        )

    return candidates
