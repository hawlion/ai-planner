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
    onboarding_version: int = 0
    learning: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProjectCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str | None = None
    kpi: str | None = None
    target_kpi: str | None = None
    category: str | None = None
    importance: Literal["low", "medium", "high", "critical"] = "medium"
    is_recurring: bool = False
    cadence: str | None = None
    cadence_payload: dict[str, Any] | None = None
    estimated_effort_minutes: int = Field(default=120, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] = "medium"
    milestones: list["MilestoneCreate"] = Field(default_factory=list)


class ProjectPatch(BaseModel):
    title: str | None = None
    description: str | None = None
    kpi: str | None = None
    target_kpi: str | None = None
    category: str | None = None
    importance: Literal["low", "medium", "high", "critical"] | None = None
    is_recurring: bool | None = None
    cadence: str | None = None
    cadence_payload: dict[str, Any] | None = None
    estimated_effort_minutes: int | None = Field(default=None, ge=15, le=8 * 60)
    priority: Literal["low", "medium", "high", "critical"] | None = None
    milestones: list["MilestoneCreate"] | None = None
    version: int | None = None


class MilestoneCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    due: datetime
    description: str | None = None


class MilestoneOut(BaseModel):
    id: str
    title: str
    due: datetime
    description: str | None
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProjectOut(BaseModel):
    id: str
    title: str
    description: str | None
    kpi: str | None
    target_kpi: str | None
    category: str | None
    importance: str
    is_recurring: bool
    cadence: str | None
    cadence_payload: dict[str, Any] | None
    estimated_effort_minutes: int
    priority: str
    milestones: list[MilestoneOut] = Field(default_factory=list)
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
    task_title: str | None = None
    task_due: datetime | None = None
    event_title: str | None = None
    event_start: datetime | None = None
    event_end: datetime | None = None


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
    workload_suggestions: list["WorkloadSuggestionOut"] = Field(default_factory=list)


class WorkloadSuggestionOut(BaseModel):
    suggestion_id: str
    suggestion_type: Literal["propose", "apply", "preview"] = "propose"
    action_type: Literal[
        "move_events",
        "move_events_after_hour",
        "reduce_meeting_load",
        "split_focus_blocks",
        "shift_workload",
        "apply_roadmap",
    ]
    title: str
    rationale: str
    can_auto_apply: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)


class WeeklyRoadmapItemOut(BaseModel):
    id: str
    title: str
    project_id: str | None
    task_id: str | None
    week_start: date
    planned_minutes: int
    remaining_minutes: int
    confidence: float
    rationale: str
    status: Literal["queued", "allocated", "over_capacity", "not_scheduled"] = "queued"
    source: Literal["task", "project", "constraint"] = "task"


class WeeklyRoadmapRequest(BaseModel):
    weeks: int = Field(default=8, ge=1, le=24)
    horizon_start: date | None = None


class WeeklyRoadmapApplyRequest(BaseModel):
    roadmap_id: str


class RoadmapDiffItem(BaseModel):
    key: str
    old_value: Any | None = None
    new_value: Any | None = None


class WeeklyRoadmapDiffOut(BaseModel):
    roadmap_id: str
    previous_roadmap_id: str | None = None
    changed_fields: list[str] = Field(default_factory=list)
    diffs: list[RoadmapDiffItem] = Field(default_factory=list)


class WeeklyRoadmapOut(BaseModel):
    id: str
    profile_id: str
    period_type: str
    week_start: datetime
    work_capacity_minutes: int
    workload_snapshot: dict[str, Any]
    plan_items: list[WeeklyRoadmapItemOut]
    blocked_reasons: list[str]
    actionable_suggestions: list[WorkloadSuggestionOut]
    constraints: dict[str, Any]
    notes: str
    confidence: float
    built_by: str
    version: int
    status: str
    last_built_at: datetime | None
    created_at: datetime
    updated_at: datetime


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


class GraphAuthUrlOut(BaseModel):
    configured: bool
    auth_url: str | None = None
    missing_settings: list[str] = Field(default_factory=list)
    redirect_uri: str | None = None


class GraphStatusOut(BaseModel):
    configured: bool
    connected: bool
    username: str | None = None
    tenant_id: str | None = None
    scopes: list[str] = Field(default_factory=list)
    missing_settings: list[str] = Field(default_factory=list)
    redirect_uri: str | None = None


class GraphCalendarEventCreate(BaseModel):
    subject: str = Field(min_length=1, max_length=200)
    start: datetime
    end: datetime
    body: str | None = None
    location: str | None = None


class GraphTodoTaskCreate(BaseModel):
    title: str = Field(min_length=1, max_length=250)
    due: datetime | None = None
    body: str | None = None


class GraphTodoTaskPatch(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=250)
    due: datetime | None = None
    body: str | None = None
    status: Literal["notStarted", "inProgress", "completed", "waitingOnOthers", "deferred"] | None = None
    importance: Literal["low", "normal", "high"] | None = None


class AssistantChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    text: str = Field(min_length=1, max_length=2000)


class AssistantChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    history: list[AssistantChatTurn] = Field(default_factory=list, max_length=20)


class AssistantActionOut(BaseModel):
    type: str
    detail: dict[str, Any] = Field(default_factory=dict)


class AssistantChatResponse(BaseModel):
    reply: str
    actions: list[AssistantActionOut] = Field(default_factory=list)
    refresh: list[str] = Field(default_factory=list)
