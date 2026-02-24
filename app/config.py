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
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    openai_timeout_seconds: float = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "25"))


settings = Settings()
