from __future__ import annotations

import json
import logging
import time
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


def _clip_text(value: str | None, limit: int = 160) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _is_retryable_openai_error(exc: Exception) -> bool:
    text = str(exc).lower()
    retry_tokens = [
        "timeout",
        "timed out",
        "rate limit",
        "429",
        "503",
        "502",
        "500",
        "overloaded",
        "temporar",
        "connection",
        "network",
        "service unavailable",
        "retry",
    ]
    return any(token in text for token in retry_tokens)


class ActionItemOutput(BaseModel):
    title: str = Field(min_length=3, max_length=180)
    assignee_name: str | None = None
    due: str | None = None
    effort_minutes: int = Field(default=60, ge=0, le=8 * 60)
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    rationale: str | None = Field(default="Action item extracted from meeting context")


class ActionItemsEnvelope(BaseModel):
    items: list[ActionItemOutput] = Field(default_factory=list)


class NLIOutput(BaseModel):
    intent: Literal[
        "create_task",
        "create_event",
        "update_task",
        "delete_task",
        "move_event",
        "reschedule_request",
        "delete_event",
        "update_event",
        "list_tasks",
        "list_events",
        "find_free_time",
        "unknown",
    ]
    title: str | None = None
    due: str | None = None
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    time_hint: str | None = None
    note: str | None = ""


class AssistantActionOutput(BaseModel):
    intent: Literal[
        "create_task",
        "create_event",
        "update_task",
        "delete_task",
        "start_task",
        "reschedule_request",
        "complete_task",
        "update_priority",
        "list_tasks",
        "list_events",
        "find_free_time",
        "move_event",
        "register_meeting_note",
        "delete_event",
        "update_event",
        "unknown",
    ]
    title: str | None = None
    due: str | None = None
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    meeting_note: str | None = None
    note: str | None = ""


class AssistantPlanAction(BaseModel):
    intent: Literal[
        "create_task",
        "create_event",
        "update_task",
        "delete_task",
        "start_task",
        "reschedule_request",
        "reschedule_after_hour",
        "complete_task",
        "update_priority",
        "update_due",
        "list_tasks",
        "list_events",
        "find_free_time",
        "move_event",
        "delete_duplicate_tasks",
        "register_meeting_note",
        "delete_event",
        "update_event",
        "unknown",
    ]
    title: str | None = None
    task_keyword: str | None = None
    due: str | None = None
    cutoff_hour: int | None = Field(default=None, ge=0, le=23)
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    status: Literal["todo", "in_progress", "done", "blocked", "canceled"] | None = None
    meeting_note: str | None = None
    reschedule_hint: str | None = None
    new_title: str | None = None
    start: str | None = None
    end: str | None = None
    duration_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    description: str | None = None
    target_date: str | None = None
    limit: int | None = Field(default=None, ge=1, le=20)


class AssistantPlanOutput(BaseModel):
    actions: list[AssistantPlanAction] = Field(default_factory=list)
    note: str | None = ""


class EmailTriageOutput(BaseModel):
    classification: Literal["no_action", "task", "event", "task_and_event", "unclear"]
    reason: str = Field(min_length=3, max_length=600)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    task_title: str | None = None
    task_due: str | None = None
    task_priority: Literal["low", "medium", "high", "critical"] | None = None
    task_description: str | None = None
    event_title: str | None = None
    event_start: str | None = None
    event_end: str | None = None
    event_location: str | None = None



MODEL_PURPOSE = Literal["default", "assistant", "nli", "extraction"]


def _model_candidates(purpose: MODEL_PURPOSE) -> list[str]:
    candidates: list[str] = []
    if purpose == "assistant":
        candidates.append(settings.openai_assistant_model)
    elif purpose == "nli":
        candidates.append(settings.openai_nli_model)
    elif purpose == "extraction":
        candidates.append(settings.openai_extraction_model)

    candidates.extend([settings.openai_model, settings.openai_fallback_model])

    seen: set[str] = set()
    deduped: list[str] = []
    for model in candidates:
        name = (model or "").strip()
        if not name or name in seen:
            continue
        deduped.append(name)
        seen.add(name)
    return deduped


def is_openai_available() -> bool:
    return bool(settings.openai_api_key and OpenAI is not None)



def _client() -> OpenAI:
    if OpenAI is None:
        raise OpenAIIntegrationError("openai package not installed")
    if not settings.openai_api_key:
        raise OpenAIIntegrationError("OPENAI_API_KEY is not configured")
    return OpenAI(api_key=settings.openai_api_key, timeout=settings.openai_timeout_seconds)



def _chat_json(
    system_prompt: str,
    user_prompt: str,
    *,
    purpose: MODEL_PURPOSE = "default",
    temperature: float | None = None,
) -> dict:
    client = _client()
    models = _model_candidates(purpose)
    if not models:
        raise OpenAIIntegrationError("No OpenAI model candidates configured")

    temp = settings.openai_temperature if temperature is None else float(temperature)
    last_error: Exception | None = None
    started_at = time.monotonic()
    # Cap total wait so model fallback/retries do not multiply user-facing latency.
    max_total_wait = max(12.0, float(settings.openai_timeout_seconds) + 10.0)
    max_retries = 2

    for model in models:
        for attempt in range(max_retries + 1):
            content = ""
            try:
                request_args = {
                    "model": model,
                    "temperature": temp,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                }
                response = client.chat.completions.create(**request_args)
                content = response.choices[0].message.content or "{}"
                if isinstance(content, list):
                    content = "".join(
                        part.get("text", "") if isinstance(part, dict) else str(part)
                        for part in content
                    )
                return json.loads(content)
            except Exception as exc:  # noqa: BLE001
                # Some models (e.g. gpt-5-mini) only allow default temperature.
                text = str(exc)
                if "temperature" in text and ("Unsupported value" in text or "does not support" in text):
                    try:
                        retry_args = {
                            "model": model,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_prompt},
                            ],
                        }
                        response = client.chat.completions.create(**retry_args)
                        content = response.choices[0].message.content or "{}"
                        if isinstance(content, list):
                            content = "".join(
                                part.get("text", "") if isinstance(part, dict) else str(part)
                                for part in content
                            )
                        return json.loads(content)
                    except Exception as retry_exc:  # noqa: BLE001
                        exc = retry_exc
                last_error = exc
                logger.warning(
                    "OpenAI request failed for purpose=%s model=%s attempt=%s: %s",
                    purpose,
                    model,
                    attempt + 1,
                    exc,
                )

                elapsed = time.monotonic() - started_at
                exhausted = attempt >= max_retries
                retryable = _is_retryable_openai_error(exc)
                if elapsed >= max_total_wait:
                    logger.warning(
                        "OpenAI retries stopped by total timeout budget: purpose=%s elapsed=%.2fs",
                        purpose,
                        elapsed,
                    )
                    exhausted = True
                if exhausted or not retryable:
                    break

                backoff = min(1.5 * (attempt + 1), 3.0)
                time.sleep(backoff)

        if time.monotonic() - started_at >= max_total_wait:
            break

    raise OpenAIIntegrationError(
        f"OpenAI API request failed for all models={models}: {last_error}"
    ) from last_error



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


def _parse_datetime_value(value: str | None, base_dt: datetime) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=base_dt.tzinfo)
        return parsed
    except ValueError:
        pass
    return dateparser.parse(
        value,
        settings={
            "PREFER_DATES_FROM": "future",
            "RELATIVE_BASE": base_dt,
            "TIMEZONE": settings.timezone,
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
        languages=["ko", "en"],
    )



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

    payload = _chat_json(system_prompt, user_prompt, purpose="extraction")
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
        effort = item.effort_minutes if item.effort_minutes >= 15 else 60
        confidence = max(0.0, min(1.0, float(item.confidence)))
        items.append(
            DraftActionItem(
                title=item.title.strip(),
                assignee_name=item.assignee_name,
                due=due_dt,
                effort_minutes=effort,
                confidence=confidence,
                rationale=item.rationale or "LLM extraction",
            )
        )

    return items



def parse_nli_openai(text: str, base_dt: datetime) -> NLIOutput:
    system_prompt = (
        "You parse Korean/English planning commands into intent JSON."
        " Return strict JSON only with fields:"
        " intent(create_task|create_event|update_task|delete_task|move_event|reschedule_request|"
        "delete_event|update_event|list_tasks|list_events|find_free_time|unknown),"
        " title, due, effort_minutes, priority, time_hint, note. "
        " If user asks to add a schedule/meeting/calendar entry, use create_event."
        " Use create_task only for to-do/task requests."
        " For create_event/create_task, title must be a concise semantic subject"
        " (strip date/time words and command words like 추가/등록/생성)."
        " Example: '이번주 목요일 오후3시에 공인알림 미팅 일정 추가' -> title='공인알림 미팅'."
        " If only a generic title is available (e.g., 미팅/회의/일정/task), keep it generic and do not invent details."
        "Use null for unknown values."
        " due should be ISO-8601 datetime when possible."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"base_datetime={base_dt.isoformat()}\n"
        f"command={text}"
    )

    payload = _chat_json(system_prompt, user_prompt, purpose="nli")
    try:
        parsed = NLIOutput.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI NLI schema validation failed: {exc}") from exc

    return parsed


def parse_email_triage_openai(
    *,
    subject: str,
    sender: str | None,
    body_preview: str | None,
    received_at: datetime,
) -> EmailTriageOutput:
    system_prompt = (
        "You classify incoming work email for an AI planner."
        " Return strict JSON only with fields:"
        " classification(no_action|task|event|task_and_event|unclear),"
        " reason, confidence, task_title, task_due, task_priority, task_description,"
        " event_title, event_start, event_end, event_location."
        " Rules:"
        " 1) no_action if message is informational only, newsletter/promotional,"
        "    FYI/announcement, automated receipt/notification, already-resolved thread,"
        "    or does not require recipient action."
        " 2) task/event/task_and_event only when explicit action or schedule commitment exists."
        " 3) Do not hallucinate missing details."
        " 4) task_title/event_title should be concise and concrete."
        " 5) Datetime fields should be ISO-8601 when inferable; otherwise null."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"received_at={received_at.isoformat()}\n"
        f"sender={(sender or '').strip()}\n"
        f"subject={subject.strip()}\n"
        f"body_preview={(body_preview or '').strip()}\n"
    )

    payload = _chat_json(system_prompt, user_prompt, purpose="assistant", temperature=settings.openai_assistant_temperature)
    try:
        parsed = EmailTriageOutput.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI email triage schema validation failed: {exc}") from exc

    # Normalize inferred datetimes if model returns natural language.
    task_due = _parse_datetime_value(parsed.task_due, received_at)
    event_start = _parse_datetime_value(parsed.event_start, received_at)
    event_end = _parse_datetime_value(parsed.event_end, received_at)

    return EmailTriageOutput(
        classification=parsed.classification,
        reason=parsed.reason,
        confidence=float(parsed.confidence),
        task_title=parsed.task_title,
        task_due=task_due.isoformat() if task_due else None,
        task_priority=parsed.task_priority,
        task_description=parsed.task_description,
        event_title=parsed.event_title,
        event_start=event_start.isoformat() if event_start else None,
        event_end=event_end.isoformat() if event_end else None,
        event_location=parsed.event_location,
    )


def parse_assistant_action_openai(text: str, base_dt: datetime) -> AssistantActionOutput:
    system_prompt = (
        "You are an assistant action parser for a work planner."
        " Return strict JSON only with fields: "
        "intent(create_task|create_event|update_task|delete_task|start_task|reschedule_request|complete_task|"
        "update_priority|list_tasks|list_events|find_free_time|move_event|register_meeting_note|unknown), "
        "title, due, effort_minutes, priority, meeting_note, note. "
        "For meeting-note style text, use register_meeting_note and copy full note text into meeting_note."
        " For task completion/priority update, title should be the target task title or keyword."
        " For schedule/meeting add requests, use create_event."
        " For create_event/create_task, title must be a concise semantic subject"
        " (exclude date/time text and command words)."
        " If only generic title words are available, keep them as-is and do not hallucinate."
        " Use null for unknown values."
        " due should be ISO-8601 datetime when possible."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"base_datetime={base_dt.isoformat()}\n"
        f"user_message={text}"
    )

    payload = _chat_json(
        system_prompt,
        user_prompt,
        purpose="assistant",
        temperature=settings.openai_assistant_temperature,
    )
    try:
        parsed = AssistantActionOutput.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI assistant action schema validation failed: {exc}") from exc

    return parsed


def parse_assistant_plan_openai(
    text: str,
    base_dt: datetime,
    task_context: list[dict],
    history: list[dict] | None = None,
    calendar_context: list[dict] | None = None,
    pending_approvals: list[dict] | None = None,
) -> AssistantPlanOutput:
    context_lines: list[str] = []
    for item in task_context[:30]:
        context_lines.append(
            "- title={title} | status={status} | priority={priority} | due={due}".format(
                title=_clip_text(str(item.get("title") or ""), 90),
                status=_clip_text(str(item.get("status") or ""), 24),
                priority=_clip_text(str(item.get("priority") or ""), 24),
                due=_clip_text(str(item.get("due") or ""), 40),
            )
        )

    history_lines: list[str] = []
    for turn in (history or [])[-6:]:
        role = str(turn.get("role") or "").strip().lower()
        text_value = _clip_text(str(turn.get("text") or ""), 280)
        if role not in {"user", "assistant"} or not text_value:
            continue
        history_lines.append(f"{role}: {text_value}")

    event_lines: list[str] = []
    for item in (calendar_context or [])[:30]:
        event_lines.append(
            "- title={title} | start={start} | end={end} | source={source}".format(
                title=_clip_text(str(item.get("title") or ""), 90),
                start=_clip_text(str(item.get("start") or ""), 40),
                end=_clip_text(str(item.get("end") or ""), 40),
                source=_clip_text(str(item.get("source") or ""), 24),
            )
        )

    approval_lines: list[str] = []
    for item in (pending_approvals or [])[:15]:
        approval_lines.append(
            "- id={id} | type={type} | summary={summary}".format(
                id=_clip_text(str(item.get("id") or ""), 48),
                type=_clip_text(str(item.get("type") or ""), 24),
                summary=_clip_text(str(item.get("summary") or ""), 120),
            )
        )

    system_prompt = (
        "You are an action planner for a Korean/English work assistant."
        " Return strict JSON only with shape:"
        ' {"actions":[{"intent":string,"title":string|null,"task_keyword":string|null,"due":string|null,'
        '"cutoff_hour":int|null,"effort_minutes":int|null,"priority":string|null,"status":string|null,'
        '"meeting_note":string|null,"reschedule_hint":string|null,"new_title":string|null,'
        '"start":string|null,"end":string|null,"duration_minutes":int|null,'
        '"description":string|null,"target_date":string|null,"limit":int|null}],'
        '"note":string|null}.'
        " Supported intent values are:"
        " create_task, create_event, update_task, delete_task, start_task, reschedule_request, reschedule_after_hour,"
        " complete_task, update_priority, update_due, list_tasks, list_events, find_free_time, move_event,"
        " delete_duplicate_tasks, register_meeting_note, delete_event, update_event, unknown."
        " Parse multiple requests in one message into multiple actions in order."
        " CRITICAL: if user asks to add schedule/meeting/calendar event (일정/미팅/회의/캘린더 + 추가/등록/잡아줘),"
        " you MUST output create_event, not create_task."
        " Use create_task only when user explicitly asks for to-do/task/할일."
        " For create_event/create_task, title must be a concise semantic subject, not the whole sentence."
        " Remove date/time words and command words from title."
        " Example: '이번주 목요일 오후3시에 공인알림 미팅 일정 추가' => title='공인알림 미팅'."
        " If you only know a generic title (미팅/회의/일정/task), keep it generic; do not invent details."
        " For update_task, put changed fields into priority/status/due/description/effort_minutes/new_title."
        " For move_event, set task_keyword to existing event title and set start (and optionally end or duration_minutes)."
        " For list_events/list_tasks/find_free_time, use target_date/limit/duration_minutes when inferable."
        " If user asks to show/list tasks, MUST output list_tasks."
        " If user asks to show/list schedule/calendar/events, MUST output list_events."
        " If user asks for available/free time slots, MUST output find_free_time."
        " For complete/update actions, choose task_keyword from existing task titles and make it specific."
        " For delete_event or update_event, choose task_keyword from existing event titles when possible."
        " For update_event, set new_title when user asks to rename the event."
        " If user message is approval intent and contains approval id, still return unknown and ask a Korean clarification"
        " question in note only when target approval cannot be inferred."
        " Never use a generic one-word keyword like '작업', '고객', '미팅'."
        " For requests like 'after 6pm' or '오후 6시 이후', use reschedule_after_hour and set cutoff_hour."
        " For duplicate cleanup requests, use delete_duplicate_tasks."
        " If matching is uncertain, keep intent as unknown and set note as one concise clarification question in Korean."
        " due should be ISO-8601 datetime when inferable, else null."
        " If message is meeting notes/transcript, use register_meeting_note with full note in meeting_note."
        " For meeting-note messages, do not generate extra create_task actions."
        " Resolve references like '그거/방금 거/that one' using recent conversation when possible."
        " Do not answer user-facing content in note."
        " note is only for one short clarification question when all actions are unknown."
        " Prefer executable actions over unknown when evidence exists in contexts."
        " Keep actions concise and executable."
    )

    user_prompt = (
        f"timezone={settings.timezone}\n"
        f"base_datetime={base_dt.isoformat()}\n"
        f"recent_conversation:\n{'\n'.join(history_lines) if history_lines else '(none)'}\n"
        f"existing_tasks:\n{'\n'.join(context_lines)}\n"
        f"existing_events:\n{'\n'.join(event_lines) if event_lines else '(none)'}\n"
        f"pending_approvals:\n{'\n'.join(approval_lines) if approval_lines else '(none)'}\n"
        f"user_message={text}"
    )

    payload = _chat_json(
        system_prompt,
        user_prompt,
        purpose="assistant",
        temperature=settings.openai_assistant_temperature,
    )
    try:
        parsed = AssistantPlanOutput.model_validate(payload)
    except ValidationError as exc:
        raise OpenAIIntegrationError(f"OpenAI assistant plan schema validation failed: {exc}") from exc

    return parsed
