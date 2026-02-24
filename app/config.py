from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "AAWO - AI Autonomous Work Orchestrator"
    api_prefix: str = "/api"
    db_url: str = os.getenv("DATABASE_URL", "sqlite:///./aawo.db")
    timezone: str = os.getenv("AAWO_TIMEZONE", "Asia/Seoul")


settings = Settings()
