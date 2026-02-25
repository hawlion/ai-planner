from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

from app.config import settings
from app.models import UserProfile, default_preferences

LEARNING_WINDOW_DAYS_DEFAULT = 14
LEARNING_MIN_TOTAL_SAMPLES = 8
LEARNING_MIN_EVENT_SAMPLES = 4
LEARNING_MIN_TASK_SAMPLES = 4
PREFER_MORNING_THRESHOLD = 0.55
AVOID_LATE_THRESHOLD = 0.45
EVENT_PREFER_MORNING_HOURS = {9, 10, 11, 12}
EVENT_AVOID_LATE_START_HOUR = 16


def _coerce_timezone(tz_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name or settings.timezone)
    except Exception:
        return ZoneInfo("Asia/Seoul")


def _coerce_datetime(value: Any, default_tz: str | None = None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_coerce_timezone(default_tz))
    return dt.astimezone(timezone.utc)


def _coerce_profile_timezone(profile: UserProfile) -> ZoneInfo:
    return _coerce_timezone(profile.timezone)


def _touch_profile_preferences(profile: UserProfile) -> None:
    if isinstance(profile.preferences, dict):
        profile.preferences = deepcopy(profile.preferences)


def _deep_merge_with_defaults(current: dict[str, Any], defaults: dict[str, Any]) -> bool:
    changed = False
    for key, default_value in defaults.items():
        if key not in current:
            current[key] = deepcopy(default_value)
            changed = True
            continue

        value = current[key]
        if isinstance(default_value, dict):
            if not isinstance(value, dict):
                current[key] = deepcopy(default_value)
                changed = True
                continue
            if _deep_merge_dict(value, default_value):
                changed = True
        elif value is None:
            current[key] = deepcopy(default_value)
            changed = True
    return changed


def _deep_merge_dict(current: dict[str, Any], defaults: dict[str, Any]) -> bool:
    changed = False
    for key, default_value in defaults.items():
        if key not in current or current[key] is None:
            current[key] = deepcopy(default_value)
            changed = True
            continue
        if isinstance(default_value, dict):
            value = current[key]
            if not isinstance(value, dict):
                current[key] = deepcopy(default_value)
                changed = True
                continue
            if _deep_merge_dict(value, default_value):
                changed = True
            continue
        if key == "window_days" and isinstance(current.get(key), bool):
            current[key] = LEARNING_WINDOW_DAYS_DEFAULT
            changed = True
    return changed


def _extract_hour_from_sample(sample: Any, tz: ZoneInfo) -> int | None:
    if not isinstance(sample, dict):
        return None
    hour = sample.get("hour")
    if isinstance(hour, int) and 0 <= hour <= 23:
        return hour
    if isinstance(hour, str):
        try:
            parsed = int(hour)
        except ValueError:
            return None
        if 0 <= parsed <= 23:
            return parsed

    dt = _coerce_datetime(sample.get("ts"), tz.zone)
    if dt is None:
        return None
    return dt.astimezone(tz).hour


def _normalize_learning_samples(profile: UserProfile) -> bool:
    prefs = profile.preferences if isinstance(profile.preferences, dict) else {}
    if not isinstance(prefs, dict):
        profile.preferences = deepcopy(default_preferences())
        return True

    defaults = default_preferences()
    changed = _deep_merge_with_defaults(prefs, defaults)

    learning = prefs.get("learning")
    if not isinstance(learning, dict):
        learning = deepcopy(defaults["learning"])
        prefs["learning"] = learning
        changed = True

    signals = learning.get("signals")
    if not isinstance(signals, dict):
        signals = deepcopy(defaults["learning"]["signals"])
        learning["signals"] = signals
        changed = True

    if not isinstance(signals.get("event_start_hours"), dict):
        signals["event_start_hours"] = {}
        changed = True
    if not isinstance(signals.get("task_due_hours"), dict):
        signals["task_due_hours"] = {}
        changed = True

    if "total_events" not in signals:
        signals["total_events"] = 0
        changed = True
    if "total_task_due" not in signals:
        signals["total_task_due"] = 0
        changed = True

    if not isinstance(learning.get("applied"), dict):
        learning["applied"] = deepcopy(defaults["learning"]["applied"])
        changed = True

    if "started_at" not in learning:
        learning["started_at"] = None
        changed = True
    if "last_adjusted_at" not in learning:
        learning["last_adjusted_at"] = None
        changed = True
    if not isinstance(learning.get("window_days"), int) or learning.get("window_days", 0) <= 0:
        learning["window_days"] = LEARNING_WINDOW_DAYS_DEFAULT
        changed = True
    if not isinstance(learning.get("enabled"), bool):
        learning["enabled"] = True
        changed = True

    # Internal helper keys for cleanup/replay; no issue to keep for compatibility.
    samples = signals.get("event_start_samples")
    if not isinstance(samples, list):
        signals["event_start_samples"] = []
        changed = True
    samples = signals.get("task_due_samples")
    if not isinstance(samples, list):
        signals["task_due_samples"] = []
        changed = True

    return changed


def normalize_profile(profile: UserProfile) -> bool:
    """Normalize legacy profile objects and return whether update is required."""
    changed = _normalize_learning_samples(profile)
    if changed:
        _touch_profile_preferences(profile)
    return changed


def _get_learning_block(profile: UserProfile) -> dict[str, Any]:
    prefs = profile.preferences
    if not isinstance(prefs, dict):
        profile.preferences = deepcopy(default_preferences())
        prefs = profile.preferences
    if not isinstance(prefs.get("learning"), dict):
        prefs["learning"] = deepcopy(default_preferences()["learning"])
    if not isinstance(prefs.get("meeting_preferences"), dict):
        prefs["meeting_preferences"] = deepcopy(default_preferences()["meeting_preferences"])
    return prefs["learning"]


def _cleanup_signal_samples(profile: UserProfile, now: datetime) -> bool:
    """Remove samples older than window and rebuild hour buckets. Returns whether data changed."""
    prefs = profile.preferences
    learning = _get_learning_block(profile)
    signals = learning.setdefault("signals", {})
    if not isinstance(signals, dict):
        signals = {}
        learning["signals"] = signals
    if not isinstance(signals.get("event_start_samples"), list):
        signals["event_start_samples"] = []
    if not isinstance(signals.get("task_due_samples"), list):
        signals["task_due_samples"] = []

    window_days = max(1, int(learning.get("window_days", LEARNING_WINDOW_DAYS_DEFAULT)))
    cutoff = now - timedelta(days=window_days)
    profile_tz = _coerce_profile_timezone(profile)

    changed = False
    bucket_map = {"event_start_samples": "event_start_hours", "task_due_samples": "task_due_hours"}
    count_map = {"event_start_samples": "total_events", "task_due_samples": "total_task_due"}
    for sample_key in ("event_start_samples", "task_due_samples"):
        total_count = 0
        hour_buckets: dict[str, int] = {}
        samples = signals.get(sample_key)
        if not isinstance(samples, list):
            samples = []
            signals[sample_key] = samples
            changed = True

        cleaned = []
        for item in samples:
            if not isinstance(item, dict):
                changed = True
                continue
            ts = _coerce_datetime(item.get("ts"), profile.timezone)
            if ts is None or ts < cutoff:
                changed = True
                continue
            hour = _extract_hour_from_sample(item, profile_tz)
            if hour is None:
                changed = True
                continue
            hour_key = str(hour)
            hour_buckets[hour_key] = hour_buckets.get(hour_key, 0) + 1
            cleaned.append({"ts": ts.astimezone(timezone.utc).isoformat(), "hour": hour})
            total_count += 1

        count_key = count_map[sample_key]
        hour_key = bucket_map[sample_key]
        signals[hour_key] = hour_buckets
        if signals.get(count_key) != total_count:
            signals[count_key] = total_count
            changed = True

        if len(cleaned) != len(samples):
            signals[sample_key] = cleaned
            changed = True

    return changed


def _increment_learning_signal(profile: UserProfile, signal_type: str, occurred_at: datetime) -> bool:
    """Append one signal, return whether raw sample was recorded."""
    learning = _get_learning_block(profile)
    signals = learning.setdefault("signals", {})
    if not isinstance(signals, dict):
        signals = {}
        learning["signals"] = signals

    tz = _coerce_profile_timezone(profile)
    dt = _coerce_datetime(occurred_at, profile.timezone)
    if dt is None:
        return False

    local_dt = dt.astimezone(tz)
    hour_key = str(local_dt.hour)
    sample_key = f"{signal_type}_samples"
    bucket_key = f"{signal_type}_hours"
    if signal_type == "event_start":
        total_key = "total_events"
    elif signal_type == "task_due":
        total_key = "total_task_due"
    else:
        total_key = f"total_{signal_type}"

    buckets = signals.setdefault(bucket_key, {})
    if not isinstance(buckets, dict):
        buckets = {}
        signals[bucket_key] = buckets
    total = signals.get(total_key, 0)
    try:
        total = int(total)
    except Exception:
        total = 0

    buckets[hour_key] = int(buckets.get(hour_key, 0)) + 1
    total += 1
    signals[bucket_key] = buckets
    signals[total_key] = total

    samples = signals.setdefault(sample_key, [])
    if not isinstance(samples, list):
        samples = []
        signals[sample_key] = samples
    samples.append({"ts": local_dt.astimezone(timezone.utc).isoformat(), "hour": local_dt.hour})

    return True


def record_event_start_signal(profile: UserProfile, start_dt: datetime) -> None:
    learning = _get_learning_block(profile)
    if not bool(learning.get("enabled", True)):
        return
    if _increment_learning_signal(profile, "event_start", start_dt):
        _touch_profile_preferences(profile)


def record_task_due_signal(profile: UserProfile, due_dt: datetime) -> None:
    learning = _get_learning_block(profile)
    if not bool(learning.get("enabled", True)):
        return
    if _increment_learning_signal(profile, "task_due", due_dt):
        _touch_profile_preferences(profile)


def _evaluate_learning_preferences(profile: UserProfile) -> tuple[bool, bool]:
    learning = _get_learning_block(profile)
    signals = learning.setdefault("signals", {})
    if not isinstance(signals, dict):
        signals = {}
        learning["signals"] = signals

    event_total = int(signals.get("total_events", 0) or 0)
    task_total = int(signals.get("total_task_due", 0) or 0)
    event_hours = signals.get("event_start_hours", {})
    task_hours = signals.get("task_due_hours", {})
    if not isinstance(event_hours, dict):
        event_hours = {}
    if not isinstance(task_hours, dict):
        task_hours = {}

    if event_total >= LEARNING_MIN_EVENT_SAMPLES:
        morning_count = sum(int(event_hours.get(str(hour), 0) or 0) for hour in EVENT_PREFER_MORNING_HOURS)
        prefer_morning = event_total > 0 and (morning_count / event_total) >= PREFER_MORNING_THRESHOLD
    else:
        prefer_morning = False

    if task_total >= LEARNING_MIN_TASK_SAMPLES:
        late_total = 0
        for hour in range(EVENT_AVOID_LATE_START_HOUR, 24):
            late_total += int(task_hours.get(str(hour), 0) or 0)
        avoid_late_afternoon = task_total > 0 and (late_total / task_total) >= AVOID_LATE_THRESHOLD
    else:
        avoid_late_afternoon = False

    return prefer_morning, avoid_late_afternoon


def apply_learning_if_due(profile: UserProfile, now: datetime | None = None) -> dict[str, Any]:
    """Apply learning preferences from recent signals and return a compact status payload."""
    learning = _get_learning_block(profile)
    if not bool(learning.get("enabled", True)):
        return {"applied": False, "updated": False, "reason": "learning_disabled"}

    now_utc = _coerce_datetime(now, settings.timezone) or datetime.now(tz=timezone.utc)
    mutated = bool(_cleanup_signal_samples(profile, now_utc))

    if not learning.get("started_at"):
        learning["started_at"] = now_utc.isoformat()
        mutated = True

    signals = learning.setdefault("signals", {})
    if not isinstance(signals, dict):
        signals = {}
        learning["signals"] = signals
    total_events = int(signals.get("total_events", 0) or 0)
    total_tasks = int(signals.get("total_task_due", 0) or 0)
    if total_events + total_tasks < LEARNING_MIN_TOTAL_SAMPLES:
        if mutated:
            _touch_profile_preferences(profile)
        return {
            "applied": False,
            "updated": False,
            "reason": "insufficient_samples",
            "totals": {"events": total_events, "tasks": total_tasks},
        }

    desired_prefer_morning, desired_avoid_late = _evaluate_learning_preferences(profile)

    applied = learning.setdefault("applied", {})
    if not isinstance(applied, dict):
        applied = {}
        learning["applied"] = applied

    prev_prefer = bool(applied.get("prefer_morning", False))
    prev_avoid = bool(applied.get("avoid_late_afternoon", False))
    updated = False

    if bool(prev_prefer) != bool(desired_prefer_morning):
        applied["prefer_morning"] = bool(desired_prefer_morning)
        updated = True
    if bool(prev_avoid) != bool(desired_avoid_late):
        applied["avoid_late_afternoon"] = bool(desired_avoid_late)
        updated = True

    last_total_events = int(applied.get("last_total_events", -1) or -1)
    last_total_tasks = int(applied.get("last_total_task_due", -1) or -1)
    if last_total_events != total_events or last_total_tasks != total_tasks:
        applied["last_total_events"] = total_events
        applied["last_total_task_due"] = total_tasks
        updated = True

    prefs = profile.preferences
    if not isinstance(prefs, dict):
        profile.preferences = deepcopy(default_preferences())
        prefs = profile.preferences
    mp = prefs.setdefault("meeting_preferences", {})
    if not isinstance(mp, dict):
        mp = deepcopy(default_preferences()["meeting_preferences"])
        prefs["meeting_preferences"] = mp

    if bool(mp.get("prefer_morning", False)) != bool(desired_prefer_morning):
        mp["prefer_morning"] = bool(desired_prefer_morning)
        updated = True
    if bool(mp.get("avoid_late_afternoon", False)) != bool(desired_avoid_late):
        mp["avoid_late_afternoon"] = bool(desired_avoid_late)
        updated = True

    if updated:
        learning["last_adjusted_at"] = now_utc.isoformat()
        mutated = True

    if mutated:
        _touch_profile_preferences(profile)
    return {
        "applied": bool(updated),
        "updated": bool(updated),
        "reason": "applied" if updated else "no_change",
        "totals": {"events": total_events, "tasks": total_tasks},
        "meeting_preferences": {
            "prefer_morning": bool(mp.get("prefer_morning", False)),
            "avoid_late_afternoon": bool(mp.get("avoid_late_afternoon", False)),
            "applied": {
                "prefer_morning": bool(applied.get("prefer_morning", False)),
                "avoid_late_afternoon": bool(applied.get("avoid_late_afternoon", False)),
                "last_total_events": int(applied.get("last_total_events", 0) or 0),
                "last_total_task_due": int(applied.get("last_total_task_due", 0) or 0),
            },
        },
    }


def maybe_get_learning_snapshot(profile: UserProfile) -> dict[str, Any]:
    learning = _get_learning_block(profile)
    started_at = _coerce_datetime(learning.get("started_at"), profile.timezone)
    last_adjusted_at = _coerce_datetime(learning.get("last_adjusted_at"), profile.timezone)

    signals = learning.setdefault("signals", {})
    if not isinstance(signals, dict):
        signals = {}
        learning["signals"] = signals
    return {
        "enabled": bool(learning.get("enabled", True)),
        "window_days": int(learning.get("window_days", LEARNING_WINDOW_DAYS_DEFAULT) or LEARNING_WINDOW_DAYS_DEFAULT),
        "started_at": started_at.isoformat() if started_at else None,
        "last_adjusted_at": last_adjusted_at.isoformat() if last_adjusted_at else None,
        "signals": {
            "total_events": int(signals.get("total_events", 0) or 0),
            "total_task_due": int(signals.get("total_task_due", 0) or 0),
            "event_start_hours": dict(signals.get("event_start_hours") or {}),
            "task_due_hours": dict(signals.get("task_due_hours") or {}),
        },
        "applied": dict(learning.get("applied") or {}),
    }
