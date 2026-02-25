from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.db import Base


PRIORITIES = ("low", "medium", "high", "critical")
TASK_STATUSES = ("todo", "in_progress", "done", "blocked", "canceled")
AUTONOMY_LEVELS = ("L0", "L1", "L2", "L3", "L4")


def uuid_str() -> str:
    return str(uuid4())


def default_working_hours() -> dict:
    return {
        "days": [
            {"day": "mon", "start": "09:00", "end": "18:00"},
            {"day": "tue", "start": "09:00", "end": "18:00"},
            {"day": "wed", "start": "09:00", "end": "18:00"},
            {"day": "thu", "start": "09:00", "end": "18:00"},
            {"day": "fri", "start": "09:00", "end": "18:00"},
        ],
        "lunch": {"start": "12:00", "end": "13:00"},
    }


def default_preferences() -> dict:
    return {
        "deep_work_windows": [{"day": "tue", "start": "10:00", "end": "12:00", "weight": 0.8}],
        "meeting_preferences": {
            "prefer_morning": False,
            "avoid_late_afternoon": False,
            "max_back_to_back_minutes": 90,
        },
        "learning": {
            "enabled": True,
            "started_at": None,
            "window_days": 14,
            "last_adjusted_at": None,
            "signals": {
                "event_start_hours": {},
                "task_due_hours": {},
                "total_events": 0,
                "total_task_due": 0,
            },
            "applied": {
                "prefer_morning": False,
                "avoid_late_afternoon": False,
                "last_total_events": 0,
                "last_total_task_due": 0,
            },
        },
        "buffers": {"before_meeting_minutes": 5, "after_meeting_minutes": 5, "travel_minutes_default": 0},
        "notification_preferences": {"block_start_reminder_minutes": 10, "due_reminders": [2, 1, 0]},
    }


class TimestampedMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class UserProfile(Base, TimestampedMixin):
    __tablename__ = "user_profiles"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Seoul", nullable=False)
    autonomy_level: Mapped[str] = mapped_column(String(2), default="L2", nullable=False)
    working_hours: Mapped[dict] = mapped_column(JSON, default=default_working_hours, nullable=False)
    preferences: Mapped[dict] = mapped_column(JSON, default=default_preferences, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class Project(Base, TimestampedMixin):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    kpi: Mapped[str | None] = mapped_column(Text)
    target_kpi: Mapped[str | None] = mapped_column(String(200))
    category: Mapped[str | None] = mapped_column(String(120))
    importance: Mapped[str] = mapped_column(String(12), default="medium", nullable=False)
    is_recurring: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    cadence: Mapped[str | None] = mapped_column(String(24))
    cadence_payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    estimated_effort_minutes: Mapped[int] = mapped_column(Integer, default=120, nullable=False)
    priority: Mapped[str] = mapped_column(String(12), default="medium", nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    tasks: Mapped[list[Task]] = relationship(back_populates="project")
    milestones: Mapped[list[ProjectMilestone]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )


class ProjectMilestone(Base, TimestampedMixin):
    __tablename__ = "project_milestones"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    due: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    project: Mapped[Project] = relationship(back_populates="milestones")


class Task(Base, TimestampedMixin):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    title: Mapped[str] = mapped_column(String(250), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="todo", nullable=False)
    priority: Mapped[str] = mapped_column(String(12), default="medium", nullable=False)
    due: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    effort_minutes: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    project_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("projects.id", ondelete="SET NULL"), index=True)
    source: Mapped[str] = mapped_column(String(16), default="manual", nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(120))
    ms_todo_task_id: Mapped[str | None] = mapped_column(String(120), index=True)
    ms_todo_list_id: Mapped[str | None] = mapped_column(String(120))
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    project: Mapped[Project | None] = relationship(back_populates="tasks")
    blocks: Mapped[list[CalendarBlock]] = relationship(back_populates="task")


class CalendarBlock(Base, TimestampedMixin):
    __tablename__ = "calendar_blocks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    type: Mapped[str] = mapped_column(String(20), default="task_block", nullable=False)
    title: Mapped[str] = mapped_column(String(250), default="Focused Work", nullable=False)
    start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    task_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("tasks.id", ondelete="SET NULL"), index=True)
    locked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    source: Mapped[str] = mapped_column(String(16), default="aawo", nullable=False)
    outlook_event_id: Mapped[str | None] = mapped_column(String(120), index=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    task: Mapped[Task | None] = relationship(back_populates="blocks")


class Meeting(Base, TimestampedMixin):
    __tablename__ = "meetings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    meeting_id: Mapped[str] = mapped_column(String(120), unique=True, nullable=False, default=uuid_str)
    title: Mapped[str | None] = mapped_column(String(250))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    summary: Mapped[str | None] = mapped_column(Text)
    transcript: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    extraction_status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)

    candidates: Mapped[list[ActionItemCandidate]] = relationship(back_populates="meeting", cascade="all, delete-orphan")


class ActionItemCandidate(Base, TimestampedMixin):
    __tablename__ = "action_item_candidates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    meeting_id: Mapped[str] = mapped_column(String(36), ForeignKey("meetings.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(250), nullable=False)
    assignee_name: Mapped[str | None] = mapped_column(String(120))
    assignee_email: Mapped[str | None] = mapped_column(String(200))
    due: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    effort_minutes: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.5, nullable=False)
    rationale: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    linked_task_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("tasks.id", ondelete="SET NULL"), index=True)

    meeting: Mapped[Meeting] = relationship(back_populates="candidates")


class ApprovalRequest(Base, TimestampedMixin):
    __tablename__ = "approval_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    type: Mapped[str] = mapped_column(String(20), default="action_item", nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str | None] = mapped_column(Text)


class SchedulingProposal(Base, TimestampedMixin):
    __tablename__ = "scheduling_proposals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    summary: Mapped[str] = mapped_column(String(250), nullable=False)
    explanation: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    score: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="draft", nullable=False)
    horizon_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    horizon_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    changes: Mapped[list[SchedulingChange]] = relationship(back_populates="proposal", cascade="all, delete-orphan")


class SchedulingChange(Base, TimestampedMixin):
    __tablename__ = "scheduling_changes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    proposal_id: Mapped[str] = mapped_column(String(36), ForeignKey("scheduling_proposals.id", ondelete="CASCADE"), index=True)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    proposal: Mapped[SchedulingProposal] = relationship(back_populates="changes")


class WeeklyRoadmap(Base, TimestampedMixin):
    __tablename__ = "weekly_roadmaps"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    profile_id: Mapped[str] = mapped_column(String(36), index=True)
    period_type: Mapped[str] = mapped_column(String(16), default="weekly", nullable=False)
    week_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    work_capacity_minutes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    workload_snapshot: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    plan_items: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    blocked_reasons: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    actionable_suggestions: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    constraints: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    notes: Mapped[str] = mapped_column(Text, default="", nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    built_by: Mapped[str] = mapped_column(String(24), default="scheduler", nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False)
    last_built_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuditLog(Base, TimestampedMixin):
    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    action: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    actor: Mapped[str] = mapped_column(String(40), default="user", nullable=False)
    object_ref: Mapped[str | None] = mapped_column(String(120), index=True)
    meta: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class SyncStatus(Base):
    __tablename__ = "sync_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    graph_connected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_delta_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_webhook_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_429_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recent_429_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class GraphConnection(Base, TimestampedMixin):
    __tablename__ = "graph_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    connected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    username: Mapped[str | None] = mapped_column(String(240))
    tenant_id: Mapped[str | None] = mapped_column(String(120))
    home_account_id: Mapped[str | None] = mapped_column(String(240))
    token_cache: Mapped[str] = mapped_column(Text, default="", nullable=False)
    pending_state: Mapped[str | None] = mapped_column(String(120))
    scopes: Mapped[str | None] = mapped_column(Text)


class EmailTriage(Base, TimestampedMixin):
    __tablename__ = "email_triage"
    __table_args__ = (UniqueConstraint("ms_message_id", name="uq_email_triage_message"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    ms_message_id: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    internet_message_id: Mapped[str | None] = mapped_column(String(320), index=True)
    subject: Mapped[str | None] = mapped_column(String(320))
    sender: Mapped[str | None] = mapped_column(String(240))
    preview: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    classification: Mapped[str] = mapped_column(String(32), default="no_action", nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="ignored", nullable=False, index=True)
    approval_id: Mapped[str | None] = mapped_column(String(36), index=True)
    created_task_id: Mapped[str | None] = mapped_column(String(36), index=True)
    created_block_id: Mapped[str | None] = mapped_column(String(36), index=True)


class GraphDeltaState(Base, TimestampedMixin):
    __tablename__ = "graph_delta_states"
    __table_args__ = (UniqueConstraint("resource_type", "resource_key", name="uq_graph_delta_resource"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    resource_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    resource_key: Mapped[str] = mapped_column(String(160), default="default", nullable=False, index=True)
    delta_link: Mapped[str | None] = mapped_column(Text)
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class GraphSubscription(Base, TimestampedMixin):
    __tablename__ = "graph_subscriptions"
    __table_args__ = (UniqueConstraint("subscription_id", name="uq_graph_subscription_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    resource: Mapped[str] = mapped_column(String(160), nullable=False, default="/me/events")
    change_type: Mapped[str] = mapped_column(String(80), nullable=False, default="created,updated,deleted")
    notification_url: Mapped[str | None] = mapped_column(String(600))
    lifecycle_url: Mapped[str | None] = mapped_column(String(600))
    subscription_id: Mapped[str | None] = mapped_column(String(160), index=True)
    client_state: Mapped[str | None] = mapped_column(String(160))
    expiration_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="inactive")
    last_notification_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class IntegrationOutbox(Base, TimestampedMixin):
    __tablename__ = "integration_outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=12, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_error: Mapped[str | None] = mapped_column(Text)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
