from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime, timedelta

from app.config import settings
from app.db import SessionLocal
from app.services.graph_service import (
    GraphApiError,
    GraphAuthError,
    GraphConfigError,
    create_or_renew_event_subscription,
    is_graph_connected,
    process_outbox,
    sync_mail_delta_to_local,
    webhook_status_payload,
)

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_stop_event = threading.Event()
_thread: threading.Thread | None = None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ensure_subscription_if_needed() -> None:
    margin_minutes = max(5, int(settings.sync_worker_renew_margin_minutes))
    with SessionLocal() as db:
        status = webhook_status_payload(db)
        if not status.get("configured"):
            return

        expires_at = _parse_iso_datetime(status.get("expiration_at"))
        now = datetime.now(UTC)
        should_renew = False
        if not status.get("subscription_id"):
            should_renew = True
        elif expires_at is None:
            should_renew = True
        elif expires_at <= now + timedelta(minutes=margin_minutes):
            should_renew = True

        if should_renew:
            create_or_renew_event_subscription(db, force_new=False)


def _run_once() -> None:
    last_outbox = 0.0
    last_renew = 0.0
    last_mail = 0.0
    poll = max(1, int(settings.sync_worker_poll_seconds))
    outbox_interval = max(1, int(settings.sync_worker_outbox_interval_seconds))
    renew_interval = max(5, int(settings.sync_worker_renew_check_seconds))
    mail_interval = max(15, int(settings.sync_worker_mail_delta_interval_seconds))
    outbox_batch = max(1, min(int(settings.sync_worker_outbox_batch_size), 200))

    while not _stop_event.is_set():
        now_mono = time.monotonic()

        if now_mono - last_outbox >= outbox_interval:
            last_outbox = now_mono
            try:
                with SessionLocal() as db:
                    process_outbox(db, limit=outbox_batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning("sync worker outbox processing failed: %s", exc)

        if now_mono - last_renew >= renew_interval:
            last_renew = now_mono
            try:
                _ensure_subscription_if_needed()
            except (GraphAuthError, GraphApiError, GraphConfigError) as exc:
                logger.warning("sync worker webhook renew skipped: %s", exc)
            except Exception as exc:  # noqa: BLE001
                logger.warning("sync worker webhook renew failed: %s", exc)

        if now_mono - last_mail >= mail_interval:
            last_mail = now_mono
            try:
                with SessionLocal() as db:
                    if is_graph_connected(db) and "mail.read" in (settings.ms_scopes or "").lower():
                        sync_mail_delta_to_local(
                            db,
                            reset=False,
                            unread_only=bool(settings.sync_worker_mail_unread_only),
                        )
            except (GraphAuthError, GraphApiError, GraphConfigError) as exc:
                logger.warning("sync worker mail delta skipped: %s", exc)
            except Exception as exc:  # noqa: BLE001
                logger.warning("sync worker mail delta failed: %s", exc)

        _stop_event.wait(poll)


def start_sync_worker() -> None:
    global _thread
    if not settings.sync_worker_enabled:
        return

    with _lock:
        if _thread is not None and _thread.is_alive():
            return
        _stop_event.clear()
        _thread = threading.Thread(target=_run_once, name="aawo-sync-worker", daemon=True)
        _thread.start()


def stop_sync_worker() -> None:
    global _thread
    with _lock:
        if _thread is None:
            return
        _stop_event.set()
        _thread.join(timeout=5.0)
        _thread = None
