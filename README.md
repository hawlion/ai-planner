# AAWO (AI 업무 플래너 MVP)

요구사항 정의서 기반으로 구현한 실행 가능한 웹 앱입니다.

## 포함된 핵심 기능
- Outlook 스타일 단일 화면 UI: 주간 캘린더 뷰 + 일정 리스트 + To-do 패널 + AI 채팅창
- 온보딩/프로필: 타임존, 자율성 레벨(L0~L4) 설정
- Task 관리: 생성/조회/수정/삭제
- Calendar block 관리: 생성/조회/수정/삭제 + 충돌 방지
- Meeting ingest (비동기 202): 회의록 처리 후 Action item 후보 자동 추출
- Action item 승인/거절: 승인 시 Task + Time-block 자동 생성
- Approval queue: 범용 승인 리소스 처리(action_item, reschedule)
- Scheduling proposal/apply 분리: 제약 기반 제안 생성 후 적용
- Daily briefing: Top tasks, 리스크, 가용 시간 스냅샷
- NLI command: 자연어 기반 간단한 작업 생성/의도 파싱
- Microsoft Graph OAuth: Outlook Calendar / Microsoft To Do 실연동
- Outlook 캘린더 양방향 반영: 가져오기(import) + 로컬 블록 내보내기(export)
- AI Assistant Chat: 회의록 등록/할일 조정/일정 재배치를 대화로 실행

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
# 환경변수 설정(.env 사용 가능)
cp .env.example .env
# .env 파일에 OPENAI_API_KEY, MS_CLIENT_ID, MS_CLIENT_SECRET 입력
uvicorn app.main:app --reload --port 8000
```

브라우저에서 아래 주소를 엽니다.
- http://localhost:8000

## Microsoft Graph 설정
1. Azure Portal > App registrations > New registration
2. Redirect URI(Web): `http://localhost:8000/api/graph/auth/callback`
3. API permissions(Delegated): `User.Read`, `offline_access`, `Calendars.ReadWrite`, `Tasks.ReadWrite`
4. Certificates & secrets에서 Client secret 생성
5. `.env`에 `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_TENANT_ID` 입력

## 주요 API
- `GET/PATCH /api/profile`
- `POST /api/assistant/chat`
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
- `GET /api/graph/auth/url`
- `GET /api/graph/auth/callback`
- `GET /api/graph/status`
- `GET /api/graph/calendar/events`
- `POST /api/graph/calendar/import`
- `POST /api/graph/calendar/export`
- `GET /api/graph/todo/lists`
- `POST /api/graph/todo/lists/{list_id}/import`

## 참고
- 현재 DB는 로컬 `aawo.db` 파일을 사용합니다.
- `OPENAI_API_KEY`가 설정되면 회의 Action Item 추출과 NLI 의도 파싱에 OpenAI API를 우선 사용합니다.
- 키가 없거나 OpenAI 호출이 실패하면 기존 규칙 기반 파서로 자동 폴백합니다.
- Microsoft OAuth Redirect URI는 `http://localhost:8000/api/graph/auth/callback`로 Azure App Registration에 등록해야 합니다.
- 필요 권한(scope): `User.Read`, `offline_access`, `Calendars.ReadWrite`, `Tasks.ReadWrite`
