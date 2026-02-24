from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Literal

import dateparser
from pydantic import BaseModel, Field, ValidationError

from app.config import settings
from app.services.meeting_extractor import DraftActionItem

try:
    from openai import OpenAI
except Exception:  # noqa: BLE001
    OpenAI = None


logger = logging.getLogger(__name__)


class OpenAIIntegrationError(RuntimeError):
    pass


class ActionItemOutput(BaseModel):
    title: str = Field(min_length=3, max_length=180)
    assignee_name: str | None = None
    due: str | None = None
    effort_minutes: int = Field(default=60, ge=15, le=8 * 60)
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    rationale: str = Field(default="Action item extracted from meeting context")


class ActionItemsEnvelope(BaseModel):
    items: list[ActionItemOutput] = Field(default_factory=list)


class NLIOutput(BaseModel):
    intent: Literal["create_task", "reschedule_request", "unknown"]
    title: str | None = None
    due: str | None = None
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    time_hint: str | None = None
    note: str = ""



def is_openai_available() -> bool:
    return bool(settings.openai_api_key and OpenAI is not None)



def _client() -> OpenAI:
    if OpenAI is None:
        raise OpenAIIntegrationError("openai package not installed")
    if not settings.openai_api_key:
        raise OpenAIIntegrationError("OPENAI_API_KEY is not configured")
    return OpenAI(api_key=settings.openai_api_key, timeout=settings.openai_timeout_seconds)



def _chat_json(system_prompt: str, user_prompt: str) -> dict:
    client = _client()

    try:
        response = client.chat.completions.create(
            model=settings.openai_model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        raise OpenAIIntegrationError(f"OpenAI API request failed: {exc}") from exc

    content = ""
    try:
        content = response.choices[0].message.content or "{}"
        if isinstance(content, list):
            content = "".join(
                part.get("text", "") if isinstance(part, dict) else str(part)
                for part in content
            )
        return json.loads(content)
    except Exception as exc:  # noqa: BLE001
        raise OpenAIIntegrationError(f"OpenAI response parse failed: {exc}; raw={content!r}") from exc



def _parse_due(value: str | None, base_dt: datetime) -> datetime | None:
    if not value:
        return None

    parsed = dateparser.parse(
        value,
        settings={
            "PREFER_DATES_FROM": "future",
            "RELATIVE_BASE": base_dt,
            "TIMEZONE": settings.timezone,
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
        languages=["ko", "en"],
    )
    return parsed



def extract_action_items_openai(transcript: list[dict], summary: str | None, base_dt: datetime) -> list[DraftActionItem]:
    if not transcript and not summary:
        return []

    transcript_lines = []
    for utterance in transcript[:180]:
        speaker = utterance.get("speaker") or "참석자"
        text = (utterance.get("text") or "").strip()
        if text:
            transcript_lines.append(f"- {speaker}: {text}")

    system_prompt = (
        "You extract concrete meeting action items only."
        " Return strict JSON object only with shape:"
        ' {"items":[{"title":string,"assignee_name":string|null,"due":string|null,'
        '"effort_minutes":int,"confidence":number,"rationale":string}]}. '
        "Exclude vague ideas. Use null when unknown."
        " confidence must be between 0 and 1."
        " due should be ISO-8601 datetime if inferable, else null."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"base_datetime={base_dt.isoformat()}\n"
        f"summary={(summary or '').strip()}\n"
        "transcript:\n"
        f"{'\n'.join(transcript_lines)}"
    )

    payload = _chat_json(system_prompt, user_prompt)
    try:
        envelope = ActionItemsEnvelope.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI action items schema validation failed: {exc}") from exc

    items: list[DraftActionItem] = []
    seen: set[str] = set()

    for item in envelope.items:
        key = item.title.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)

        due_dt = _parse_due(item.due, base_dt)
        items.append(
            DraftActionItem(
                title=item.title.strip(),
                assignee_name=item.assignee_name,
                due=due_dt,
                effort_minutes=item.effort_minutes,
                confidence=item.confidence,
                rationale=item.rationale or "LLM extraction",
            )
        )

    return items



def parse_nli_openai(text: str, base_dt: datetime) -> NLIOutput:
    system_prompt = (
        "You parse Korean/English planning commands into intent JSON."
        " Return strict JSON only with fields:"
        ' intent(create_task|reschedule_request|unknown), title, due, effort_minutes, priority, time_hint, note. '
        "Use null for unknown values."
        " due should be ISO-8601 datetime when possible."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"base_datetime={base_dt.isoformat()}\n"
        f"command={text}"
    )

    payload = _chat_json(system_prompt, user_prompt)
    try:
        parsed = NLIOutput.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI NLI schema validation failed: {exc}") from exc

    return parsed
