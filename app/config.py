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
    openai_timeout_seconds: float = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "25"))
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
        "User.Read offline_access Calendars.ReadWrite Tasks.ReadWrite",
    )


settings = Settings()
