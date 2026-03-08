from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    app_name: str = "AAWO - AI Autonomous Work Orchestrator"
    api_prefix: str = "/api"
    db_url: str = os.getenv("DATABASE_URL", "sqlite:///./aawo.db")
    timezone: str = os.getenv("AAWO_TIMEZONE", "Asia/Seoul")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    openai_assistant_model: str = os.getenv("OPENAI_ASSISTANT_MODEL", "")
    openai_extraction_model: str = os.getenv("OPENAI_EXTRACTION_MODEL", "")
    openai_nli_model: str = os.getenv("OPENAI_NLI_MODEL", "")
    openai_fallback_model: str = os.getenv("OPENAI_FALLBACK_MODEL", "gpt-5-mini")
    openai_temperature: float = float(os.getenv("OPENAI_TEMPERATURE", "0.1"))
    openai_assistant_temperature: float = float(os.getenv("OPENAI_ASSISTANT_TEMPERATURE", "0.05"))
    openai_assistant_timeout_seconds: float = float(os.getenv("OPENAI_ASSISTANT_TIMEOUT_SECONDS", "14"))
    openai_timeout_seconds: float = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "12"))
    assistant_llm_only: bool = os.getenv("ASSISTANT_LLM_ONLY", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    ms_tenant_id: str = os.getenv("MS_TENANT_ID", "common")
    ms_client_id: str = os.getenv("MS_CLIENT_ID", "")
    ms_client_secret: str = os.getenv("MS_CLIENT_SECRET", "")
    ms_redirect_uri: str = os.getenv("MS_REDIRECT_URI", "http://localhost:8000/api/graph/auth/callback")
    ms_scopes: str = os.getenv(
        "MS_SCOPES",
        "User.Read offline_access Calendars.ReadWrite Tasks.ReadWrite Mail.Read",
    )
    ms_webhook_notification_url: str = os.getenv("MS_WEBHOOK_NOTIFICATION_URL", "")
    ms_webhook_lifecycle_url: str = os.getenv("MS_WEBHOOK_LIFECYCLE_URL", "")
    ms_webhook_client_state: str = os.getenv("MS_WEBHOOK_CLIENT_STATE", "aawo-webhook-state")
    ms_webhook_resource: str = os.getenv("MS_WEBHOOK_RESOURCE", "/me/events")
    ms_webhook_change_type: str = os.getenv("MS_WEBHOOK_CHANGE_TYPE", "created,updated,deleted")
    ms_subscription_ttl_minutes: int = int(os.getenv("MS_SUBSCRIPTION_TTL_MINUTES", "120"))
    sync_worker_enabled: bool = os.getenv("SYNC_WORKER_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    sync_worker_poll_seconds: int = int(os.getenv("SYNC_WORKER_POLL_SECONDS", "5"))
    sync_worker_outbox_interval_seconds: int = int(os.getenv("SYNC_WORKER_OUTBOX_INTERVAL_SECONDS", "15"))
    sync_worker_outbox_batch_size: int = int(os.getenv("SYNC_WORKER_OUTBOX_BATCH_SIZE", "20"))
    sync_worker_calendar_delta_interval_seconds: int = int(
        os.getenv("SYNC_WORKER_CALENDAR_DELTA_INTERVAL_SECONDS", "60")
    )
    sync_worker_renew_check_seconds: int = int(os.getenv("SYNC_WORKER_RENEW_CHECK_SECONDS", "60"))
    sync_worker_renew_margin_minutes: int = int(os.getenv("SYNC_WORKER_RENEW_MARGIN_MINUTES", "20"))
    sync_worker_mail_delta_interval_seconds: int = int(os.getenv("SYNC_WORKER_MAIL_DELTA_INTERVAL_SECONDS", "90"))
    sync_worker_mail_unread_only: bool = os.getenv("SYNC_WORKER_MAIL_UNREAD_ONLY", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    scheduler_cpsat_enabled: bool = os.getenv("SCHEDULER_CPSAT_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    scheduler_cpsat_timeout_seconds: float = float(os.getenv("SCHEDULER_CPSAT_TIMEOUT_SECONDS", "3.0"))
    scheduler_cpsat_max_candidates_per_task: int = int(os.getenv("SCHEDULER_CPSAT_MAX_CANDIDATES_PER_TASK", "80"))


settings = Settings()
