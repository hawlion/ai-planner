# AAWO (AI 업무 플래너 MVP)

요구사항 정의서 기반으로 구현한 실행 가능한 웹 앱입니다.

## 포함된 핵심 기능
- 온보딩/프로필: 타임존, 자율성 레벨(L0~L4) 설정
- Task 관리: 생성/조회/수정/삭제
- Calendar block 관리: 생성/조회/수정/삭제 + 충돌 방지
- Meeting ingest (비동기 202): 회의록 처리 후 Action item 후보 자동 추출
- Action item 승인/거절: 승인 시 Task + Time-block 자동 생성
- Approval queue: 범용 승인 리소스 처리(action_item, reschedule)
- Scheduling proposal/apply 분리: 제약 기반 제안 생성 후 적용
- Daily briefing: Top tasks, 리스크, 가용 시간 스냅샷
- NLI command: 자연어 기반 간단한 작업 생성/의도 파싱
- Graph sync 상태(스텁): 429 백오프 정책 시뮬레이션

## 기술 스택
- Backend: FastAPI + SQLAlchemy + SQLite
- Frontend: FastAPI static (Vanilla JS + CSS)
- 기타: dateparser(자연어 날짜 파싱)

## 실행 방법
```bash
cd "/Users/1110025/AI Planner"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

브라우저에서 아래 주소를 엽니다.
- http://127.0.0.1:8000

## 주요 API
- `GET/PATCH /api/profile`
- `GET/POST /api/tasks`
- `GET/POST /api/calendar/blocks`
- `POST /api/meetings` (202 Accepted)
- `GET /api/meetings/{id}/action-items`
- `POST /api/action-items/{id}/approve`
- `GET /api/approvals`
- `POST /api/approvals/{id}/resolve`
- `POST /api/scheduling/proposals`
- `POST /api/scheduling/proposals/{id}/apply`
- `GET /api/briefings/daily`
- `POST /api/nli/command`

## 참고
- Microsoft Graph 실제 연동은 스텁으로 구현되어 있으며, `app/services/graph_connector.py`를 실제 SDK 호출로 교체하면 됩니다.
- 현재 DB는 로컬 `aawo.db` 파일을 사용합니다.
