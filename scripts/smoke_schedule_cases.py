from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.routers.assistant import _fallback_classify, _quick_plan_actions


CASES = [
    ("내일 오전 10시에 고객 미팅 일정 추가", "create_event"),
    ("이번주 목요일 오후 3시에 공인알림 미팅 잡아줘", "create_event"),
    ("오늘 미팅 일정 내일 오후 4시로 변경", "move_event"),
    ("내일 고객 미팅 금요일 오전 11시로 옮겨줘", "move_event"),
    ("오늘 미팅 일정 30분 늦춰줘", "move_event"),
    ("금요일 회의 30분 당겨줘", "move_event"),
    ("오늘 미팅 일정 30분 연장해줘", "update_event"),
    ("고객 미팅을 45분으로 변경", "update_event"),
    ("오늘 미팅 일정 제목을 주간 싱크로 변경", "update_event"),
    ("중복된 미팅 삭제", "delete_duplicate_events"),
    ("중복 일정 정리", "delete_duplicate_events"),
    ("오늘 미팅 일정 삭제", "delete_event"),
    ("내일 고객 미팅 취소해줘", "delete_event"),
    ("오늘 일정 보여줘", "list_events"),
    ("이번주 일정 알려줘", "list_events"),
    ("오늘 1시간 비는 시간 찾아줘", "find_free_time"),
    ("이번주 언제 비어?", "find_free_time"),
    ("오후 6시 이후 일정들 모두 재배치해줘", "reschedule_after_hour"),
]


def main() -> int:
    failures: list[str] = []
    for text, expected in CASES:
        quick = _quick_plan_actions(text)
        parsed = quick[0] if quick else _fallback_classify(text, allow_openai_nli=False)
        actual = str(parsed.get("intent") or "unknown")
        if actual != expected:
            failures.append(f"{text!r}: expected={expected} actual={actual} payload={parsed}")

    if failures:
        print("FAILED")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(f"OK {len(CASES)} cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
