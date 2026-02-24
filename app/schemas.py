from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ApiMessage(BaseModel):
    message: str


class WorkingDay(BaseModel):
    day: Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    start: str = "09:00"
    end: str = "18:00"


class LunchWindow(BaseModel):
    start: str = "12:00"
    end: str = "13:00"


class WorkingHours(BaseModel):
    days: list[WorkingDay]
    lunch: LunchWindow = LunchWindow()


class UserProfileBase(BaseModel):
    timezone: str = "Asia/Seoul"
    autonomy_level: Literal["L0", "L1", "L2", "L3", "L4"] = "L2"
    working_hours: dict[str, Any] | None = None
    preferences: dict[str, Any] | None = None


class UserProfilePatch(UserProfileBase):
    version: int | None = None


class UserProfileOut(UserProfileBase):
    id: str
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProjectCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str | None = None
    kpi: str | None = None
    priority: Literal["low", "medium", "high", "critical"] = "medium"


class ProjectPatch(BaseModel):
    title: str | None = None
    description: str | None = None
    kpi: str | None = None
    priority: Literal["low", "medium", "high", "critical"] | None = None
    version: int | None = None


class ProjectOut(BaseModel):
    id: str
    title: str
    description: str | None
    kpi: str | None
    priority: str
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TaskCreate(BaseModel):
    title: str = Field(min_length=1, max_length=250)
    description: str | None = None
    due: datetime | None = None
    effort_minutes: int = Field(default=60, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] = "medium"
    project_id: str | None = None
    source: Literal["manual", "meeting", "email", "chat"] = "manual"
    source_ref: str | None = None


class TaskPatch(BaseModel):
    title: str | None = None
    description: str | None = None
    status: Literal["todo", "in_progress", "done", "blocked", "canceled"] | None = None
    due: datetime | None = None
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    project_id: str | None = None
    version: int | None = None


class TaskOut(BaseModel):
    id: str
    title: str
    description: str | None
    status: str
    priority: str
    due: datetime | None
    effort_minutes: int
    project_id: str | None
    source: str
    source_ref: str | None
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CalendarBlockCreate(BaseModel):
    type: Literal["task_block", "focus_block", "buffer", "personal", "other"] = "task_block"
    title: str = "Focused Work"
    start: datetime
    end: datetime
    task_id: str | None = None
    locked: bool = False


class CalendarBlockPatch(BaseModel):
    title: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    task_id: str | None = None
    locked: bool | None = None
    version: int | None = None


class CalendarBlockOut(BaseModel):
    id: str
    type: str
    title: str
    start: datetime
    end: datetime
    task_id: str | None
    locked: bool
    source: str
    outlook_event_id: str | None
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TranscriptUtterance(BaseModel):
    ts_ms: int = Field(ge=0)
    speaker: str | None = None
    text: str = Field(min_length=1)


class MeetingIngest(BaseModel):
    meeting_id: str | None = None
    title: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    summary: str | None = None
    transcript: list[TranscriptUtterance]


class MeetingOut(BaseModel):
    id: str
    meeting_id: str
    title: str | None
    started_at: datetime | None
    ended_at: datetime | None
    summary: str | None
    extraction_status: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ActionItemCandidateOut(BaseModel):
    id: str
    meeting_id: str
    title: str
    assignee_name: str | None
    assignee_email: str | None
    due: datetime | None
    effort_minutes: int
    confidence: float
    rationale: str | None
    status: str
    linked_task_id: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ApproveActionItemRequest(BaseModel):
    title: str | None = None
    due: datetime | None = None
    effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] = "medium"
    create_time_block: bool = True


class ApproveActionItemResult(BaseModel):
    candidate_id: str
    task: TaskOut
    created_blocks: list[CalendarBlockOut]
    ms_todo_synced: bool = False
    outlook_synced: bool = False


class ApprovalOut(BaseModel):
    id: str
    type: str
    status: str
    payload: dict[str, Any]
    reason: str | None
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None

    model_config = {"from_attributes": True}


class ApprovalResolve(BaseModel):
    decision: Literal["approve", "reject"]
    reason: str | None = None


class Horizon(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime


class Objectives(BaseModel):
    minimize_changes: float = 1.0
    minimize_lateness: float = 1.0
    maximize_deep_work: float = 0.7
    minimize_context_switch: float = 0.5


class Constraints(BaseModel):
    slot_minutes: int = Field(default=30, ge=15, le=60)
    split_allowed: bool = False
    max_proposals: int = Field(default=3, ge=1, le=5)


class SchedulingProposalRequest(BaseModel):
    horizon: Horizon
    task_ids: list[str] | None = None
    objectives: Objectives = Objectives()
    constraints: Constraints = Constraints()


class ScheduleChangeOut(BaseModel):
    id: str
    kind: str
    payload: dict[str, Any]

    model_config = {"from_attributes": True}


class ScheduleProposalOut(BaseModel):
    id: str
    summary: str
    explanation: dict[str, Any]
    score: dict[str, Any]
    status: str
    horizon_from: datetime | None
    horizon_to: datetime | None
    changes: list[ScheduleChangeOut]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ApplyProposalRequest(BaseModel):
    approved: bool = False


class DailyBriefTask(BaseModel):
    task_id: str
    title: str
    reason: str
    recommended_block: dict[str, datetime] | None = None


class DailyBriefingOut(BaseModel):
    date: date
    top_tasks: list[DailyBriefTask]
    risks: list[str]
    reminders: list[str]
    snapshot: dict[str, int]


class SyncStatusOut(BaseModel):
    graph_connected: bool
    last_delta_sync_at: datetime | None
    webhook: dict[str, Any]
    throttling: dict[str, Any]


class NLIRequest(BaseModel):
    text: str = Field(min_length=1, max_length=500)


class NLIResponse(BaseModel):
    intent: str
    extracted: dict[str, Any]
    note: str
