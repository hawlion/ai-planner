from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import Base, engine
from app.routers import approvals, briefing, calendar, health, meetings, nli, profile, projects, scheduling, sync, tasks
from app.services.core import ensure_profile
from app.db import SessionLocal

app = FastAPI(title=settings.app_name, version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_prefix = settings.api_prefix
app.include_router(health.router, prefix=api_prefix)
app.include_router(profile.router, prefix=api_prefix)
app.include_router(projects.router, prefix=api_prefix)
app.include_router(tasks.router, prefix=api_prefix)
app.include_router(calendar.router, prefix=api_prefix)
app.include_router(meetings.router, prefix=api_prefix)
app.include_router(approvals.router, prefix=api_prefix)
app.include_router(scheduling.router, prefix=api_prefix)
app.include_router(briefing.router, prefix=api_prefix)
app.include_router(sync.router, prefix=api_prefix)
app.include_router(nli.router, prefix=api_prefix)

static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        ensure_profile(db)


@app.get("/")
def root() -> FileResponse:
    return FileResponse(static_dir / "index.html")
