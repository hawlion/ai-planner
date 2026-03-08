const API = "/api";
const HOUR_START = 7;
const HOUR_END = 22;
const HOUR_HEIGHT = 60;
const DRAG_STEP_MINUTES = 15;
const MIN_EVENT_MINUTES = 15;
const MIN_EVENT_HEIGHT = 18;
const DEFAULT_BLOCK_MINUTES = 60;
const CALENDAR_WEEK_CACHE_TTL_MS = 45_000;
const STATIC_PROMPT_CHIPS = [
  { label: "회의록 등록", prompt: "회의록: PM: 금요일까지 제안서 초안 작성. 디자이너: 목요일 오전 시안 공유" },
  { label: "일정 재배치", prompt: "이번 주 일정 재배치해줘" },
  { label: "우선순위 변경", prompt: "보고서 작업 우선순위 높음으로 변경해줘" },
];

const state = {
  profile: null,
  graph: null,
  graphAuthError: null,
  graphAuthWarned: false,
  liveBriefingTimer: null,
  liveBriefingInFlight: false,
  syncStatus: null,
  dailyBriefing: null,
  tasks: [],
  approvals: [],
  approvalPromptedIds: new Set(),
  approvalPromptInFlight: false,
  approvalPromptTimer: null,
  previewedApprovals: new Set(),
  syncInProgress: false,
  llmErrorReason: "",
  llmErrorAt: null,
  systemNotice: "",
  systemNoticeIsError: false,
  chatHistory: [],
  localBlocks: [],
  remoteEvents: [],
  calendarDragState: null,
  weekStart: startOfWeek(new Date()),
  selectedDate: startOfDay(new Date()),
  miniMonth: new Date(new Date().getFullYear(), new Date().getMonth(), 1),
  calendarCache: Object.create(null),
  calendarLoadRequests: Object.create(null),
  calendarRenderScheduled: false,
  mergedEventsCache: null,
  taskTitleMap: new Map(),
};

const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function startOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function startOfWeek(date) {
  const day = date.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  const monday = new Date(date);
  monday.setDate(monday.getDate() + diff);
  return startOfDay(monday);
}

function toDateKey(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function getWeekCacheKey(weekStart) {
  const start = weekStart instanceof Date ? startOfWeek(weekStart) : startOfWeek(new Date());
  return toDateKey(start);
}

function isSameDay(a, b) {
  return (
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
  );
}

function fmtDate(value) {
  return value.toLocaleDateString("ko-KR", { month: "short", day: "numeric", weekday: "short" });
}

function fmtDateTime(value) {
  const date = value instanceof Date ? value : new Date(value);
  return date.toLocaleString("ko-KR", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function fmtTime(date) {
  return date.toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit" });
}

function toDateTimeInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function isoToLocalDatetimeValue(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function toLocalPayloadDatetime(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(
    date.getMinutes(),
  )}:${pad(date.getSeconds())}`;
}

function localInputToIso(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return toLocalPayloadDatetime(date);
}

function renderSystemStatus() {
  const el = $("status-message");
  if (!el) return;

  let graphPart = "Graph: 미설정";
  if (state.graphAuthError) {
    graphPart = "Graph: 오류(재연결 필요)";
  } else if (state.graph?.connected) {
    graphPart = `Graph: 연결됨${state.graph.username ? `(${state.graph.username})` : ""}`;
  } else if (state.graph?.configured) {
    graphPart = "Graph: 미연결";
  }

  const syncAt = state.syncStatus?.last_delta_sync_at ? fmtDateTime(state.syncStatus.last_delta_sync_at) : "이력 없음";
  const syncPart = state.syncInProgress ? "Sync: 진행 중" : `Sync: ${syncAt}`;
  const llmPart = state.llmErrorReason ? `LLM: 오류(${clipText(state.llmErrorReason, 80)})` : "LLM: 정상";
  const noticePart = state.systemNotice ? `알림: ${clipText(state.systemNotice, 120)}` : "";

  el.textContent = [graphPart, syncPart, llmPart, noticePart].filter(Boolean).join(" | ");
  const severity = state.systemNoticeIsError || state.graphAuthError || state.llmErrorReason ? "error" : "ok";
  el.className = `status-line ${severity}`;
}

function notify(message, isError = false) {
  state.systemNotice = String(message || "").trim();
  state.systemNoticeIsError = Boolean(isError);
  renderSystemStatus();
}

function invalidateCalendarDerivedState() {
  state.mergedEventsCache = null;
}

function applyCalendarPayload(payload) {
  state.localBlocks = payload?.localBlocks || [];
  state.remoteEvents = payload?.remoteEvents || [];
  invalidateCalendarDerivedState();
}

async function api(path, options = {}) {
  const response = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  const isJson = response.headers.get("content-type")?.includes("application/json");
  const body = isJson ? await response.json() : await response.text();

  if (!response.ok) {
    const detail = typeof body === "object" ? body.detail || JSON.stringify(body) : body;
    throw new Error(detail || `HTTP ${response.status}`);
  }

  return body;
}

async function loadProfile() {
  state.profile = await api("/profile");
  $("autonomy").value = state.profile.autonomy_level;
  $("timezone").value = state.profile.timezone;
}

async function saveProfile() {
  await api("/profile", {
    method: "PATCH",
    body: JSON.stringify({
      autonomy_level: $("autonomy").value,
      timezone: $("timezone").value.trim(),
      version: state.profile?.version ?? null,
    }),
  });
  await loadProfile();
  notify("프로필 설정을 저장했습니다.");
}

async function loadGraphStatus() {
  state.graph = await api("/graph/status");
  renderGraphStatus();
}

async function loadSyncStatus() {
  state.syncStatus = await api("/sync/status");
  renderSystemStatus();
}

function selectedDateParam() {
  const date = state.selectedDate instanceof Date ? state.selectedDate : new Date();
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

async function loadDailyBriefing() {
  const targetDate = selectedDateParam();
  state.dailyBriefing = await api(`/briefings/daily?target_date=${encodeURIComponent(targetDate)}`);
}

function renderGraphStatus() {
  const label = $("graph-status-label");
  if (label && state.graph) {
    if (state.graphAuthError) {
      label.className = "status-pill warn";
      label.textContent = "Graph 재연결 필요";
    } else if (state.graph.connected) {
      label.className = "status-pill connected";
      label.textContent = `연결됨: ${state.graph.username || "Microsoft"}`;
    } else if (!state.graph.configured) {
      label.className = "status-pill warn";
      label.textContent = "Graph 설정 누락";
    } else {
      label.className = "status-pill";
      label.textContent = "Graph 미연결";
    }
  }
  renderSystemStatus();
}

function renderDailyBriefing() {
  const briefing = state.dailyBriefing;
  const sync = state.syncStatus;
  const topList = $("today-briefing-top");
  const risksList = $("today-briefing-risks");
  const remindersList = $("today-briefing-reminders");
  const snapshotBox = $("today-briefing-snapshot");

  if (!briefing || !topList || !risksList || !remindersList || !snapshotBox) return;

  const dateText = briefing.date ? new Date(briefing.date).toLocaleDateString("ko-KR", { month: "short", day: "numeric", weekday: "short" }) : fmtDate(new Date());
  $("today-briefing-date").textContent = `${dateText} 기준`;

  const lastSync = sync?.last_delta_sync_at ? fmtDateTime(sync.last_delta_sync_at) : "동기화 이력 없음";
  const throttled = Number(sync?.throttling?.recent_429_count || 0);
  const syncNote = throttled > 0 ? `동기화 지연 가능(429 ${throttled}회)` : "동기화 정상";
  $("today-briefing-sync").textContent = `마지막 동기화: ${lastSync} · ${syncNote}`;

  const meeting = Number(briefing.snapshot?.meeting_minutes || 0);
  const focus = Number(briefing.snapshot?.focus_minutes || 0);
  const free = Number(briefing.snapshot?.free_minutes || 0);
  snapshotBox.innerHTML = `
    <div class="snapshot-item"><span>회의</span><strong>${meeting}분</strong></div>
    <div class="snapshot-item"><span>집중</span><strong>${focus}분</strong></div>
    <div class="snapshot-item"><span>가용</span><strong>${free}분</strong></div>
  `;

  const topTasks = briefing.top_tasks || [];
  if (!topTasks.length) {
    topList.innerHTML = `<div class="briefing-item"><div class="todo-meta">추천 작업이 없습니다.</div></div>`;
  } else {
    topList.innerHTML = topTasks
      .map((task) => {
        const block = task.recommended_block
          ? `${fmtTime(new Date(task.recommended_block.start))} - ${fmtTime(new Date(task.recommended_block.end))}`
          : "추천 시간 없음";
        return `
          <div class="briefing-item">
            <div class="briefing-title">${escapeHtml(task.title)}</div>
            <div class="todo-meta">${escapeHtml(task.reason || "")}</div>
            <div class="todo-meta">권장: ${escapeHtml(block)}</div>
          </div>
        `;
      })
      .join("");
  }

  const risks = briefing.risks || [];
  risksList.innerHTML = risks.length
    ? risks.map((risk) => `<div class="briefing-item risk">${escapeHtml(risk)}</div>`).join("")
    : `<div class="briefing-item"><div class="todo-meta">특이 리스크 없음</div></div>`;

  const reminders = briefing.reminders || [];
  remindersList.innerHTML = reminders.length
    ? reminders.map((item) => `<div class="briefing-item">${escapeHtml(item)}</div>`).join("")
    : `<div class="briefing-item"><div class="todo-meta">리마인드 없음</div></div>`;
}

async function loadTasks() {
  state.tasks = await api("/tasks");
  state.taskTitleMap = new Map((state.tasks || []).map((item) => [item.id, item.title || ""]));
}

async function loadApprovals() {
  state.approvals = await api("/approvals?status=pending");
}

function parseGraphDateTime(value) {
  if (!value) return null;
  if (typeof value === "string") return new Date(value);
  if (value.dateTime) return new Date(value.dateTime);
  return null;
}

function normalizeLocalBlocks(blocks) {
  return blocks.map((block) => ({
    id: `local-${block.id}`,
    title: block.title || "일정",
    start: new Date(block.start),
    end: new Date(block.end),
    kind: block.source === "external" ? "outlook" : block.outlook_event_id ? "mixed" : "local",
    source: block.source,
    taskId: block.task_id || null,
    outlookId: block.outlook_event_id || null,
    locked: Boolean(block.locked),
    version: typeof block.version === "number" ? block.version : null,
  }));
}

function normalizeRemoteEvents(events) {
  return events.map((event) => ({
    id: `outlook-${event.id}`,
    title: event.subject || "Outlook Event",
    start: parseGraphDateTime(event.start),
    end: parseGraphDateTime(event.end),
    kind: "outlook",
    source: "outlook",
    outlookId: event.id,
  }));
}

function syncLocalBlockFromServer(payload) {
  if (!payload?.id) return;
  const blockId = String(payload.id);
  const start = payload.start instanceof Date ? payload.start : new Date(payload.start);
  const end = payload.end instanceof Date ? payload.end : new Date(payload.end);
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime())) return;

  const normalized = {
    ...payload,
    id: blockId,
    start,
    end,
  };
  const idx = state.localBlocks.findIndex((item) => item.id === blockId);
  if (idx >= 0) {
    state.localBlocks[idx] = normalized;
  } else {
    state.localBlocks.push(normalized);
  }
  invalidateCalendarDerivedState();
}

function mergedEvents() {
  if (Array.isArray(state.mergedEventsCache)) {
    return state.mergedEventsCache;
  }

  const local = normalizeLocalBlocks(state.localBlocks);
  const known = new Set(local.map((item) => item.outlookId).filter(Boolean));
  const remote = normalizeRemoteEvents(state.remoteEvents).filter((item) => !known.has(item.outlookId));

  state.mergedEventsCache = [...local, ...remote]
    .filter((item) => item.start instanceof Date && item.end instanceof Date)
    .sort((a, b) => a.start.getTime() - b.start.getTime());
  return state.mergedEventsCache;
}

function findCalendarEvent(rawEventId) {
  if (!rawEventId) return null;
  return mergedEvents().find((item) => item.id === rawEventId) || null;
}

function clampToRange(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function roundToDragStep(minutes) {
  return Math.round(minutes / DRAG_STEP_MINUTES) * DRAG_STEP_MINUTES;
}

function getDragMinutesFromY(y, columnRect) {
  if (!columnRect) return 0;
  const minutesPerPixel = HOUR_HEIGHT / 60;
  return (y - columnRect.top) / minutesPerPixel;
}

function startMinutesToY(minutes) {
  return (minutes / 60) * HOUR_HEIGHT;
}

function minutesToDate(dayStartMs, minuteOffset) {
  const base = new Date(dayStartMs);
  const totalMinutes = HOUR_START * 60 + clampToRange(Math.round(minuteOffset), 0, (HOUR_END - HOUR_START) * 60);
  base.setHours(0, 0, 0, 0);
  base.setMinutes(totalMinutes);
  return base;
}

function hasCalendarConflict(start, end, rawEventId) {
  return mergedEvents()
    .filter((item) => item.id.startsWith("local-"))
    .filter((item) => item.id !== rawEventId)
    .some((item) => item.start < end && item.end > start);
}

function setSuppressEventOpen(rawEventId) {
  const target = document.querySelector(`.calendar-event[data-event-id="${CSS.escape(rawEventId)}"]`);
  if (target) target.dataset.suppressOpen = "1";
}

function normalizedTaskKey(title) {
  return String(title || "")
    .toLowerCase()
    .replace(/[^a-z0-9가-힣\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildTaskDuplicateCounts() {
  const counts = new Map();
  for (const task of state.tasks || []) {
    if (!task || task.status === "canceled") continue;
    const key = normalizedTaskKey(task.title);
    if (key.length < 2) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

function eventIsConflicted(target, all) {
  return all.some(
    (other) =>
      other.id !== target.id &&
      other.start < target.end &&
      other.end > target.start,
  );
}

function countEventConflicts(events) {
  const conflictedIds = new Set();
  for (let i = 0; i < events.length; i += 1) {
    for (let j = i + 1; j < events.length; j += 1) {
      if (events[j].start >= events[i].end) break;
      if (events[i].start < events[j].end && events[j].start < events[i].end) {
        conflictedIds.add(events[i].id);
        conflictedIds.add(events[j].id);
      }
    }
  }
  return conflictedIds.size;
}

function taskTitleById(taskId) {
  if (!taskId) return "";
  return state.taskTitleMap.get(taskId) || "";
}

function linkedBlockCount(taskId) {
  if (!taskId) return 0;
  return (state.localBlocks || []).filter((block) => block.task_id === taskId).length;
}

async function loadCalendarData({
  force = false,
  useCache = true,
  staleWhileRevalidate = true,
  onUpdated,
  weekStartDate = null,
} = {}) {
  const targetWeekStart = startOfWeek(weekStartDate || state.weekStart);
  const start = addDays(targetWeekStart, -1);
  const end = addDays(targetWeekStart, 8);
  const localQuery = `start=${encodeURIComponent(toLocalPayloadDatetime(start))}&end=${encodeURIComponent(toLocalPayloadDatetime(end))}`;
  const graphQuery = `start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`;
  const weekKey = getWeekCacheKey(targetWeekStart);
  const applyIfCurrent = (payload, source = "network") => {
    const currentKey = getWeekCacheKey(state.weekStart);
    if (currentKey !== weekKey) return;
    applyCalendarPayload(payload);
    if (typeof onUpdated === "function") {
      onUpdated(payload, source);
    }
  };

  const cached = useCache ? state.calendarCache[weekKey] : null;
  const now = Date.now();
  const isFresh = cached && now - cached.loadedAt <= CALENDAR_WEEK_CACHE_TTL_MS;
  if (cached && useCache && (isFresh || staleWhileRevalidate)) {
    applyCalendarPayload(cached);
    if (typeof onUpdated === "function") {
      onUpdated(cached, isFresh ? "cache-fresh" : "cache-stale");
    }
    if (!isFresh && staleWhileRevalidate) {
      void loadCalendarData({
        force: true,
        useCache: false,
        staleWhileRevalidate: false,
        onUpdated: (payload, source) => applyIfCurrent(payload, source),
        weekStartDate: targetWeekStart,
      });
    }
    return { source: "cache", stale: !isFresh, key: weekKey };
  }

  const existing = force ? null : state.calendarLoadRequests[weekKey];
  if (existing) {
    const payload = await existing;
    if (payload && payload.localBlocks && payload.remoteEvents) {
      applyIfCurrent(payload);
    }
    return { source: "inflight", stale: false, key: weekKey };
  }

  const request = (async () => {
    const localPromise = api(`/calendar/blocks?${localQuery}`);
    const remotePromise = state.graph?.connected
      ? api(`/graph/calendar/events?${graphQuery}`)
          .then((remote) => {
            if (state.graphAuthError) {
              state.graphAuthError = null;
              state.graphAuthWarned = false;
              renderGraphStatus();
            }
            return remote;
          })
          .catch((error) => {
            const nextMessage = error.message || "Graph auth error";
            const changed = state.graphAuthError !== nextMessage;
            state.graphAuthError = nextMessage;
            renderGraphStatus();
            if (changed || !state.graphAuthWarned) {
              notify("Outlook 일정 조회 실패: 재연결 후 다시 시도하세요. 로컬 일정만 표시합니다.", true);
              state.graphAuthWarned = true;
            }
            return [];
          })
      : Promise.resolve([]);

    if (!state.graph?.connected) {
      state.graphAuthError = null;
      state.graphAuthWarned = false;
    }

    const local = await localPromise;
    applyIfCurrent(
      {
        localBlocks: local,
        remoteEvents: [],
        loadedAt: Date.now(),
        key: weekKey,
      },
      "local-first",
    );
    const remote = await remotePromise;

    return {
      localBlocks: local,
      remoteEvents: remote,
      loadedAt: Date.now(),
      key: weekKey,
    };
  })();
  state.calendarLoadRequests[weekKey] = request;

  try {
    const payload = await request;
    state.calendarCache[weekKey] = payload;
    applyIfCurrent(payload, "network");
    return { source: "network", stale: false, key: weekKey };
  } finally {
    if (state.calendarLoadRequests[weekKey] === request) {
      delete state.calendarLoadRequests[weekKey];
    }
  }
}

function scheduleCalendarViewportRender() {
  if (state.calendarRenderScheduled) return;
  state.calendarRenderScheduled = true;
  window.requestAnimationFrame(() => {
    state.calendarRenderScheduled = false;
    renderCalendarViewport();
  });
}

function prefetchAdjacentWeeks() {
  const currentWeek = startOfWeek(state.weekStart);
  [ -7, 7 ].forEach((offset) => {
    const target = addDays(currentWeek, offset);
    const cached = state.calendarCache[getWeekCacheKey(target)];
    const now = Date.now();
    const isFresh = cached && now - cached.loadedAt <= CALENDAR_WEEK_CACHE_TTL_MS;
    if (isFresh) return;
    void loadCalendarData({
      force: true,
      useCache: false,
      staleWhileRevalidate: false,
      weekStartDate: target,
      onUpdated: null,
    });
  });
}

async function deleteCalendarBlock(id, isOutlook) {
  const rawId = String(id || "");
  const isLocal = rawId.startsWith("local-");
  const isRemoteOutlook = rawId.startsWith("outlook-");
  const localBlock = isLocal
    ? state.localBlocks.find((item) => String(item.id) === rawId.replace("local-", ""))
    : null;
  const localVersion = localBlock?.version;

  try {
    notify("일정 삭제 중...");

    if (isLocal) {
      const qs = typeof localVersion === "number" ? `?version=${encodeURIComponent(localVersion)}` : "";
      await api(`/calendar/blocks/${rawId.replace("local-", "")}${qs}`, { method: "DELETE" });
    } else if (isRemoteOutlook) {
      await api(`/graph/calendar/events/${encodeURIComponent(rawId.replace("outlook-", ""))}`, { method: "DELETE" });
    } else if (isOutlook) {
      await api(`/graph/calendar/events/${encodeURIComponent(rawId)}`, { method: "DELETE" });
    } else {
      await api(`/calendar/blocks/${rawId}`, { method: "DELETE" });
    }

    notify("일정이 삭제되었습니다.");
    closeEventModal();
    await refreshCalendarOnly();
  } catch (error) {
    notify(`삭제 실패: ${error.message}`, true);
  }
}

function bindEventDeleteButtons(container) {
  if (!container) return;
  container.querySelectorAll(".event-delete[data-event-id]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const encodedId = button.dataset.eventId || "";
      const rawId = encodedId ? decodeURIComponent(encodedId) : "";
      const isOutlook = (button.dataset.eventOutlook || "0") === "1";
      if (!rawId) return;
      deleteCalendarBlock(rawId, isOutlook);
    });
  });
}

function bindEventOpenTargets(container) {
  if (!container) return;
  container.querySelectorAll("[data-open-event-id]").forEach((target) => {
    target.addEventListener("click", (evt) => {
      if (evt.target.closest(".event-resize-handle") || evt.target.closest(".event-delete")) return;
      const eventEl = target.closest(".calendar-event");
      if (eventEl?.dataset?.suppressOpen === "1") {
        eventEl.dataset.suppressOpen = "0";
        return;
      }
      const encodedId = target.dataset.openEventId || "";
      if (!encodedId) return;
      openEventModal(decodeURIComponent(encodedId));
    });
  });
}

function bindEventInteractions(container) {
  if (!container) return;
  container.querySelectorAll(".calendar-event").forEach((eventEl) => {
    eventEl.addEventListener("pointerdown", (event) => {
      if (event.target.closest(".event-delete")) return;
      if (event.target.closest(".event-resize-handle")) return;
      const rawId = eventEl.dataset.eventId ? decodeURIComponent(eventEl.dataset.eventId) : "";
      if (!rawId.startsWith("local-")) return;
      startDragFromPointer(event, eventEl, false);
    });

    const resizeHandle = eventEl.querySelector(".event-resize-handle");
    if (!resizeHandle) return;
    resizeHandle.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const rawId = eventEl.dataset.eventId ? decodeURIComponent(eventEl.dataset.eventId) : "";
      if (!rawId.startsWith("local-")) return;
      startDragFromPointer(event, eventEl, true);
    });
  });
}

function toLocalDatetimeValue(date) {
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function openEventModal(eventId) {
  const allEvents = mergedEvents();
  const ev = allEvents.find(e => e.id === eventId);
  if (!ev) return;
  if (ev.kind === 'outlook') {
    notify('Outlook 일정은 앱에서 직접 수정할 수 없습니다.', true);
    return;
  }
  document.getElementById('edit-event-id').value = ev.id;
  document.getElementById('edit-event-version').value = typeof ev.version === "number" ? String(ev.version) : "";
  document.getElementById('edit-event-title').value = ev.title;
  document.getElementById('edit-event-start').value = toLocalDatetimeValue(ev.start);
  document.getElementById('edit-event-end').value = toLocalDatetimeValue(ev.end);
  document.getElementById('event-modal-overlay').classList.remove('hidden');
}

function closeEventModal() {
  document.getElementById('event-modal-overlay').classList.add('hidden');
}

async function saveEventChanges() {
  const rawId = document.getElementById('edit-event-id').value;
  const versionRaw = document.getElementById('edit-event-version').value;
  const version = Number.parseInt(versionRaw, 10);
  const title = document.getElementById('edit-event-title').value.trim();
  const start = document.getElementById('edit-event-start').value;
  const end = document.getElementById('edit-event-end').value;
  if (!title || !start || !end) { notify('모든 필드를 입력해주세요.', true); return; }
  const startDt = new Date(start);
  const endDt = new Date(end);
  if (endDt <= startDt) { notify('종료 시간은 시작 시간 이후여야 합니다.', true); return; }
  const blockId = rawId.replace('local-', '');
  try {
    await api(`/calendar/blocks/${blockId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        start: toLocalPayloadDatetime(startDt),
        end: toLocalPayloadDatetime(endDt),
        version: Number.isFinite(version) ? version : null,
      }),
    });
    notify('일정이 수정되었습니다.');
    closeEventModal();
    await refreshCalendarOnly();
  } catch (error) {
    notify(`수정 실패: ${error.message}`, true);
  }
}

function getCalendarColumns() {
  const columns = Array.from(document.querySelectorAll("#week-columns .day-column"));
  return columns
    .map((column, index) => {
      const rect = column.getBoundingClientRect();
      const dayStartMs = Number(column.dataset.dayStart);
      if (!Number.isFinite(dayStartMs)) return null;
      return {
        index,
        element: column,
        rect,
        dayStartMs,
      };
    })
    .filter(Boolean);
}

function getCalendarColumnFromPoint(columns, clientX, clientY) {
  if (!columns.length) return null;

  const weekColumns = document.querySelector("#week-columns");
  const weekRect = weekColumns?.getBoundingClientRect?.();
  if (Number.isFinite(weekRect?.left) && Number.isFinite(weekRect?.right) && Number.isFinite(weekRect?.width)) {
    if (clientX < weekRect.left || clientX > weekRect.right) return null;
  }

  const inBounds = columns.find((column) => {
    const rect = column.rect;
    if (!rect) return false;
    const horizontalMatch = clientX >= rect.left && clientX <= rect.right;
    if (typeof clientY !== "number" || Number.isNaN(clientY)) return horizontalMatch;
    const verticalMatch = clientY >= rect.top && clientY <= rect.bottom;
    return horizontalMatch && verticalMatch;
  });
  if (inBounds) return inBounds;

  const targetElement = document.elementFromPoint(clientX, clientY);
  const targetColumn = targetElement?.closest?.(".day-column") || null;
  if (targetColumn) {
    const found = columns.find((column) => column.element === targetColumn);
    if (found) return found;
  }

  const week = document.querySelector("#week-columns");
  const weekRectFallback = week?.getBoundingClientRect?.();
  const first = columns[0];
  if (weekRectFallback && Number.isFinite(weekRectFallback.width) && Number.isFinite(weekRectFallback.left) && weekRectFallback.width > 0) {
    const estimate = Math.floor((clientX - weekRectFallback.left) / (weekRectFallback.width / columns.length));
    const index = clampToRange(estimate, 0, columns.length - 1);
    return columns[index];
  }
  if (!first?.rect) return columns[0] || null;
  const estimate = Math.floor((clientX - first.rect.left) / first.rect.width);
  const index = clampToRange(estimate, 0, columns.length - 1);
  return columns[index];
}

function isEventEditable(event) {
  return event && event.kind !== "outlook" && !event.locked;
}

function startDragFromPointer(event, eventEl, isResize) {
  if (event.button !== undefined && event.button !== 0 && event.pointerType === "mouse") return;
  event.preventDefault();
  event.stopPropagation();

  const rawId = decodeURIComponent(eventEl.dataset.eventId || "");
  const calendarEvent = findCalendarEvent(rawId);
  if (!isEventEditable(calendarEvent)) return;

  const columns = getCalendarColumns();
  const startColumn = eventEl.closest(".day-column");
  if (!startColumn) return;

  const startEventRect = eventEl.getBoundingClientRect();
  const startColumnData = columns.find((column) => column.element === startColumn);
  if (!startColumnData) return;

  const eventStart = calendarEvent.start instanceof Date ? calendarEvent.start : new Date(calendarEvent.start);
  const eventEnd = calendarEvent.end instanceof Date ? calendarEvent.end : new Date(calendarEvent.end);
  if (Number.isNaN(eventStart.getTime()) || Number.isNaN(eventEnd.getTime())) return;

  const durationMinutes = Math.max(MIN_EVENT_MINUTES, (eventEnd.getTime() - eventStart.getTime()) / 60000);
  const daySpanMinutes = (HOUR_END - HOUR_START) * 60;
  const initialStartMinutes = clampToRange(
    (eventStart.getHours() - HOUR_START) * 60 + eventStart.getMinutes() + eventStart.getSeconds() / 60,
    0,
    daySpanMinutes - durationMinutes,
  );

  const stateValue = {
    mode: isResize ? "resize" : "move",
    pointerId: event.pointerId,
    eventEl,
    eventId: rawId,
    localId: rawId.replace("local-", ""),
    version: calendarEvent.version,
    columns,
    dayStartMs: startColumnData.dayStartMs,
    currentColumnIndex: startColumnData.index,
    startColumnIndex: startColumnData.index,
    columnWidth: startColumnData.rect.width || 0,
    startColumnLeft: startColumnData.rect.left || 0,
    daySpanMinutes,
    durationMinutes,
    previewStartMinutes: initialStartMinutes,
    previewEndMinutes: initialStartMinutes + durationMinutes,
    grabOffsetY: isResize ? startEventRect.bottom - event.clientY : event.clientY - startEventRect.top,
    moved: false,
    startClientX: event.clientX,
    startClientY: event.clientY,
    originalLeft: eventEl.style.left,
    originalWidth: eventEl.style.width,
    originalVersion: calendarEvent.version,
  };

  state.calendarDragState = stateValue;
  eventEl.classList.add("dragging");
  eventEl.setPointerCapture?.(event.pointerId);

  const onPointerMove = (moveEvent) => {
    const current = state.calendarDragState;
    if (!current || moveEvent.pointerId !== current.pointerId) return;
    moveEvent.preventDefault();
    moveEvent.stopPropagation();

    const dx = moveEvent.clientX - current.startClientX;
    const dy = moveEvent.clientY - current.startClientY;
    if (!current.moved && Math.abs(dx) + Math.abs(dy) > 4) {
      current.moved = true;
      current.eventEl.dataset.suppressOpen = "1";
    }

    let targetColumn = getCalendarColumnFromPoint(current.columns, moveEvent.clientX, moveEvent.clientY) || current.columns[current.currentColumnIndex];
    if (!targetColumn) return;

    if (current.mode !== "resize" && targetColumn.index !== current.currentColumnIndex) {
      const dayShift = targetColumn.index - current.startColumnIndex;
      current.dayStartMs = targetColumn.dayStartMs;
      current.currentColumnIndex = targetColumn.index;
      const deltaX = targetColumn.rect.left - current.startColumnLeft;
      current.eventEl.style.transform = dayShift !== 0 ? `translateX(${deltaX}px)` : "none";
    } else if (current.mode !== "resize") {
      current.eventEl.style.transform = "none";
    } else if (current.mode === "resize") {
      current.eventEl.style.transform = "none";
    }

    const cursorMinute = getDragMinutesFromY(moveEvent.clientY, targetColumn.rect);
    if (current.mode === "move") {
      const nextStart = roundToDragStep(cursorMinute - current.grabOffsetY / (HOUR_HEIGHT / 60));
      const clampedStart = clampToRange(nextStart, 0, Math.max(0, current.daySpanMinutes - current.durationMinutes));
      const nextEnd = clampedStart + current.durationMinutes;
      current.previewStartMinutes = clampedStart;
      current.previewEndMinutes = nextEnd;
    } else {
      const nextEnd = roundToDragStep(cursorMinute + current.grabOffsetY / (HOUR_HEIGHT / 60));
      const clampedEnd = clampToRange(nextEnd, current.previewStartMinutes + MIN_EVENT_MINUTES, current.daySpanMinutes);
      current.previewEndMinutes = clampedEnd;
      const baseMinutes = (eventStart.getHours() - HOUR_START) * 60 + eventStart.getMinutes() + eventStart.getSeconds() / 60;
      current.previewStartMinutes = clampToRange(baseMinutes, 0, Math.max(0, current.daySpanMinutes - MIN_EVENT_MINUTES));
    }

    const nextStartDate = minutesToDate(current.dayStartMs, current.previewStartMinutes);
    const nextEndDate = minutesToDate(current.dayStartMs, current.previewEndMinutes);
    const hasConflict = hasCalendarConflict(nextStartDate, nextEndDate, current.eventId);
    current.eventEl.style.top = `${startMinutesToY(current.previewStartMinutes)}px`;
    current.eventEl.style.height = `${Math.max(MIN_EVENT_HEIGHT, ((current.previewEndMinutes - current.previewStartMinutes) / 60) * HOUR_HEIGHT)}px`;
    current.eventEl.classList.toggle("conflict-preview", hasConflict);
    const timeNode = current.eventEl.querySelector(".event-time");
    if (timeNode) {
      timeNode.textContent = `${fmtTime(nextStartDate)} - ${fmtTime(nextEndDate)}`;
    }
  };

  const stopInteraction = async (upEvent) => {
    const current = state.calendarDragState;
    if (!current || upEvent.pointerId !== current.pointerId) return;
    current.eventEl.releasePointerCapture?.(current.pointerId);

    window.removeEventListener("pointermove", onPointerMove);
    window.removeEventListener("pointerup", stopInteraction);
    window.removeEventListener("pointercancel", stopInteraction);

    current.eventEl.classList.remove("dragging");
    current.eventEl.classList.remove("conflict-preview");
    current.eventEl.style.left = current.originalLeft;
    current.eventEl.style.width = current.originalWidth;
    current.eventEl.style.transform = "none";

    if (!current.moved) {
      state.calendarDragState = null;
      return;
    }

    const nextStartDate = minutesToDate(current.dayStartMs, current.previewStartMinutes);
    const nextEndDate = minutesToDate(current.dayStartMs, current.previewEndMinutes);
    const hasConflict = hasCalendarConflict(nextStartDate, nextEndDate, current.eventId);
    if (hasConflict) {
      notify("겹치는 시간대로는 이동/조정할 수 없습니다.", true);
      state.calendarDragState = null;
      await refreshCalendarOnly();
      return;
    }

    const currentVersion = Number.isFinite(current.version) ? current.version : current.originalVersion;
    if (!Number.isFinite(currentVersion)) {
      notify("해당 일정은 버전 정보가 없어 수정할 수 없습니다.", true);
      state.calendarDragState = null;
      await refreshCalendarOnly();
      return;
    }

    try {
      const updated = await api(`/calendar/blocks/${current.localId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          start: toLocalPayloadDatetime(nextStartDate),
          end: toLocalPayloadDatetime(nextEndDate),
          version: currentVersion,
        }),
      });
      syncLocalBlockFromServer(updated);
      notify("일정이 수정되었습니다.");
      setSuppressEventOpen(current.eventId);
      await refreshCalendarOnly();

      const shouldExist = state.localBlocks.some((item) => String(item.id) === String(current.localId));
      if (!shouldExist) {
        syncLocalBlockFromServer({
          id: current.localId,
          source: "aawo",
          title: current.eventEl?.querySelector(".event-title")?.textContent || "일정",
          start: nextStartDate,
          end: nextEndDate,
          task_id: null,
          outlook_event_id: null,
          version: updated.version || Number(current.version || 0) + 1,
          locked: false,
          type: "task_block",
          kind: "local",
        });
        await refreshCalendarOnly();
      }
    } catch (error) {
      notify(`일정 수정 실패: ${error.message}`, true);
      await refreshCalendarOnly();
    } finally {
      state.calendarDragState = null;
    }
  };

  window.addEventListener("pointermove", onPointerMove);
  window.addEventListener("pointerup", stopInteraction);
  window.addEventListener("pointercancel", stopInteraction);
}

function deleteFromModal() {
  const rawId = document.getElementById('edit-event-id').value;
  const isOutlook = rawId.startsWith('outlook-');
  deleteCalendarBlock(rawId, isOutlook);
}

function renderHeaderRange() {
  const start = state.weekStart;
  const end = addDays(state.weekStart, 6);
  $("range-title").textContent = `${start.getFullYear()}년 ${start.getMonth() + 1}월 ${start.getDate()}일 - ${end.getMonth() + 1}월 ${end.getDate()}일`;
}

function getWeekDays() {
  return Array.from({ length: 7 }, (_, idx) => addDays(state.weekStart, idx));
}

function renderWeekHeader() {
  const container = $("week-days-header");
  const weekDays = getWeekDays();
  const today = startOfDay(new Date());

  const headerHtml = [
    '<div class="time-head-empty"></div>',
    ...weekDays.map((day) => {
      const classes = ["week-day-head"];
      if (isSameDay(day, state.selectedDate)) classes.push("selected");
      if (isSameDay(day, today)) classes.push("today");
      return `
        <div class="${classes.join(" ")}" data-select-day="${day.toISOString()}">
          <div class="dow">${day.toLocaleDateString("ko-KR", { weekday: "short" })}</div>
          <div class="dom">${day.getDate()}</div>
        </div>
      `;
    }),
  ];

  container.innerHTML = headerHtml.join("");
  container.querySelectorAll("[data-select-day]").forEach((el) => {
    el.addEventListener("click", async () => {
      state.selectedDate = startOfDay(new Date(el.dataset.selectDay));
      state.miniMonth = new Date(state.selectedDate.getFullYear(), state.selectedDate.getMonth(), 1);
      renderWeekHeader();
      renderAgenda();
      renderMiniMonth();
      ensureLiveBriefingTicker();
    });
  });
}

function renderTimeLabels() {
  const labels = [];
  for (let hour = HOUR_START; hour <= HOUR_END; hour += 1) {
    const label = hour === 12 ? "오후 12:00" : hour > 12 ? `오후 ${hour - 12}:00` : `오전 ${hour}:00`;
    labels.push(`<div class="time-slot-label">${label}</div>`);
  }
  $("time-labels").innerHTML = labels.join("");
}

function eventIntersectionWithDay(event, dayStart, dayEnd) {
  if (event.end <= dayStart || event.start >= dayEnd) return null;
  const start = new Date(Math.max(event.start.getTime(), dayStart.getTime()));
  const end = new Date(Math.min(event.end.getTime(), dayEnd.getTime()));
  return { ...event, start, end };
}

function layoutDayEvents(events) {
  if (!events.length) return [];

  const sorted = [...events].sort((a, b) => a.start.getTime() - b.start.getTime() || a.end.getTime() - b.end.getTime());
  const clusters = [];
  let cluster = [];
  let active = [];

  for (const event of sorted) {
    active = active.filter((item) => item.end.getTime() > event.start.getTime());
    if (!active.length && cluster.length) {
      clusters.push(cluster);
      cluster = [];
    }
    cluster.push({ ...event });
    active.push(event);
  }
  if (cluster.length) clusters.push(cluster);

  const positioned = [];
  for (const items of clusters) {
    const laneEnds = [];
    for (const item of items) {
      let lane = laneEnds.findIndex((end) => end.getTime() <= item.start.getTime());
      if (lane === -1) {
        lane = laneEnds.length;
        laneEnds.push(item.end);
      } else {
        laneEnds[lane] = item.end;
      }
      item.lane = lane;
    }
    const lanes = Math.max(1, laneEnds.length);
    for (const item of items) {
      item.lanes = lanes;
      positioned.push(item);
    }
  }
  return positioned;
}

function renderWeekGrid() {
  const container = $("week-columns");
  const weekDays = getWeekDays();
  const allEvents = mergedEvents();
  const gridHeight = (HOUR_END - HOUR_START) * HOUR_HEIGHT;

  const dayColumns = weekDays.map((day) => {
    const dayStart = startOfDay(day);
    const dayEnd = addDays(dayStart, 1);
    const visibleStart = new Date(dayStart);
    visibleStart.setHours(HOUR_START, 0, 0, 0);
    const visibleEnd = new Date(dayStart);
    visibleEnd.setHours(HOUR_END, 0, 0, 0);

    const eventsForDay = allEvents
      .map((event) => eventIntersectionWithDay(event, visibleStart, visibleEnd))
      .filter(Boolean);
    const positioned = layoutDayEvents(eventsForDay);

    const eventsHtml = positioned
      .map((event) => {
        const startMinutes = (event.start.getHours() - HOUR_START) * 60 + event.start.getMinutes();
        const durationMinutes = Math.max(15, (event.end.getTime() - event.start.getTime()) / 60000);
        const top = (startMinutes / 60) * HOUR_HEIGHT;
        const height = Math.max(MIN_EVENT_HEIGHT, (durationMinutes / 60) * HOUR_HEIGHT);
        const left = (event.lane / event.lanes) * 100;
        const width = 100 / event.lanes;
        const hasConflict = event.lanes > 1;
        const linkedTaskTitle = taskTitleById(event.taskId);
        const linkedTaskHtml = linkedTaskTitle ? `<div class="event-link">🔗 ${escapeHtml(linkedTaskTitle)}</div>` : "";

        const isEditable = event.kind !== "outlook" && !event.locked;
        const resizeHandle = isEditable ? '<span class="event-resize-handle" title="시간 길이 조정">⋮⋮</span>' : "";

        const isOutlook = event.kind === "outlook" || event.kind === "mixed";
        const encodedId = encodeURIComponent(event.id);
        return `
          <div class="calendar-event ${event.kind}${hasConflict ? " conflict" : ""}${isEditable ? " editable" : ""}" data-event-id="${encodedId}" data-event-date="${event.start.toISOString()}" data-open-event-id="${encodedId}" data-day-start="${dayStart.getTime()}" style="top:${top}px;height:${height}px;left:calc(${left}% + 2px);width:calc(${width}% - 4px);">
            <button class="event-delete" data-event-id="${encodedId}" data-event-outlook="${isOutlook ? "1" : "0"}" title="일정 삭제">×</button>
            ${hasConflict ? `<span class="event-conflict-badge">중복</span>` : ""}
            <div class="event-time">${fmtTime(event.start)}</div>
            <div class="event-title">${escapeHtml(event.title)}</div>
            ${linkedTaskHtml}
            ${resizeHandle}
          </div>
        `;
      })
      .join("");

    let timeIndicatorHtml = "";
    if (isSameDay(dayStart, new Date())) {
      const now = new Date();
      const currentMinutes = (now.getHours() - HOUR_START) * 60 + now.getMinutes();
      if (currentMinutes >= 0 && currentMinutes < (HOUR_END - HOUR_START) * 60) {
        const top = (currentMinutes / 60) * HOUR_HEIGHT;
        timeIndicatorHtml = `
          <div class="current-time-indicator" style="top:${top}px;">
            <div class="current-time-ball"></div>
          </div>
        `;
      }
    }

    return `<div class="day-column" data-day-column="${dayStart.toISOString()}" data-day-start="${dayStart.getTime()}" style="height:${gridHeight}px">
      ${eventsHtml}
      ${timeIndicatorHtml}
    </div>`;
  });

  container.innerHTML = dayColumns.join("");
  bindEventInteractions(container);
  bindEventOpenTargets(container);
  bindEventDeleteButtons(container);
  container.querySelectorAll("[data-day-column]").forEach((el) => {
    el.addEventListener("click", () => {
      state.selectedDate = startOfDay(new Date(el.dataset.dayColumn));
      renderWeekHeader();
      renderAgenda();
      renderMiniMonth();
      ensureLiveBriefingTicker();
    });
  });
}

function renderAgenda() {
  const dayStart = startOfDay(state.selectedDate);
  const dayEnd = addDays(dayStart, 1);
  $("agenda-date").textContent = fmtDate(dayStart);

  const events = mergedEvents()
    .filter((event) => event.end > dayStart && event.start < dayEnd)
    .sort((a, b) => a.start.getTime() - b.start.getTime());

  if (!events.length) {
    $("agenda-list").innerHTML = `<div class="agenda-item"><div class="agenda-meta">선택한 날짜에 일정이 없습니다.</div></div>`;
    return;
  }

  $("agenda-list").innerHTML = events
    .map((event) => {
      const tags = [`<span class="tag">${event.kind === "outlook" ? "Outlook" : "Local"}</span>`];
      if (event.kind === "mixed") tags.push('<span class="tag outlook">Synced</span>');
      if (eventIsConflicted(event, events)) tags.push('<span class="tag conflict">중복</span>');
      if (event.taskId) {
        const linkedTask = taskTitleById(event.taskId);
        if (linkedTask) tags.push(`<span class="tag linked">할일:${escapeHtml(linkedTask)}</span>`);
      }
      const isOutlook = event.kind === "outlook" || event.kind === "mixed";
      const encodedId = encodeURIComponent(event.id);
      return `
        <div class="agenda-item" data-open-event-id="${encodedId}">
          <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div class="agenda-title">${escapeHtml(event.title)}</div>
            <button class="event-delete" style="position: relative; opacity: 1; opacity: 0.7;" data-event-id="${encodedId}" data-event-outlook="${isOutlook ? "1" : "0"}" title="일정 삭제">×</button>
          </div>
          <div class="agenda-meta">${fmtDateTime(event.start)} - ${fmtTime(event.end)}</div>
          <div class="meta-row">${tags.join("")}</div>
        </div>
      `;
    })
    .join("");

  const agenda = $("agenda-list");
  bindEventOpenTargets(agenda);
  bindEventDeleteButtons(agenda);
}

function renderMiniMonth() {
  const container = $("mini-month");
  const title = $("mini-title");
  const base = state.miniMonth;
  const year = base.getFullYear();
  const month = base.getMonth();
  title.textContent = `${year}년 ${month + 1}월`;

  const first = new Date(year, month, 1);
  const offset = (first.getDay() + 6) % 7;
  const start = addDays(first, -offset);
  const today = startOfDay(new Date());
  const weekStart = state.weekStart;
  const weekEnd = addDays(weekStart, 7);

  const cells = [];
  for (let i = 0; i < 42; i += 1) {
    const day = addDays(start, i);
    const classes = ["mini-day"];
    if (day.getMonth() !== month) classes.push("outside");
    if (isSameDay(day, today)) classes.push("today");
    if (isSameDay(day, state.selectedDate)) classes.push("selected");
    if (day >= weekStart && day < weekEnd) classes.push("in-week");

    cells.push(
      `<button class="${classes.join(" ")}" type="button" data-mini-day="${day.toISOString()}">${day.getDate()}</button>`,
    );
  }
  container.innerHTML = cells.join("");

  container.querySelectorAll("[data-mini-day]").forEach((button) => {
    button.addEventListener("click", async () => {
      const day = startOfDay(new Date(button.dataset.miniDay));
      const nextWeek = startOfWeek(day);
      state.selectedDate = day;
      state.weekStart = nextWeek;
      state.miniMonth = new Date(day.getFullYear(), day.getMonth(), 1);
      await refreshCalendarWeekQuick({ force: false });
      const chatLog = document.getElementById("chat-log");
      const match = chatLog.innerHTML.match(/<div class="chat-msg assistant">([^<]*)<\/div>/g);
      if (match) {
        let lastMsg = match[match.length - 1].replace(/<[^>]+>/g, "");
        if (lastMsg) speak(lastMsg);
      }
    });
  });
}

function initChatToggle() {
  const fab = document.getElementById("chat-fab");
  const chatWindow = document.getElementById("floating-chat");
  const closeBtn = document.getElementById("close-chat");

  if (fab && chatWindow && closeBtn) {
    fab.addEventListener("click", openChatWindow);

    closeBtn.addEventListener("click", () => {
      chatWindow.classList.add("hidden");
      chatWindow.classList.remove("show");
      fab.style.display = "flex";
    });
  }
}

function openChatWindow() {
  const fab = document.getElementById("chat-fab");
  const chatWindow = document.getElementById("floating-chat");
  if (!fab || !chatWindow) return;
  chatWindow.classList.remove("hidden");
  chatWindow.classList.add("show");
  fab.style.display = "none";
}

function roundToHalfHour(date) {
  const d = new Date(date);
  d.setSeconds(0, 0);
  const minutes = d.getMinutes();
  if (minutes === 0 || minutes === 30) return d;
  if (minutes < 30) d.setMinutes(30);
  else {
    d.setHours(d.getHours() + 1);
    d.setMinutes(0);
  }
  return d;
}

function withinVisibleWorkingHours(start, end) {
  if (!isSameDay(start, end)) return false;
  const startHour = start.getHours() + start.getMinutes() / 60;
  const endHour = end.getHours() + end.getMinutes() / 60;
  return startHour >= HOUR_START && endHour <= HOUR_END;
}

function hasEventConflict(start, end, events) {
  return events.some((event) => event.start < end && event.end > start);
}

function buildTaskBlockCandidates(task, durationMinutes) {
  const now = roundToHalfHour(new Date());
  const candidates = [];
  const seen = new Set();

  const pushCandidate = (date) => {
    const key = date.toISOString();
    if (seen.has(key)) return;
    seen.add(key);
    candidates.push(date);
  };

  if (task?.due) {
    const due = new Date(task.due);
    if (!Number.isNaN(due.getTime())) {
      pushCandidate(new Date(due.getTime() - durationMinutes * 60000));
      pushCandidate(new Date(due.getTime() - (durationMinutes + 60) * 60000));
    }
  }

  pushCandidate(new Date(now));
  const base = new Date(now);
  for (let day = 0; day < 7; day += 1) {
    for (let hour = Math.max(9, HOUR_START); hour <= Math.min(18, HOUR_END - 1); hour += 1) {
      for (const minute of [0, 30]) {
        const slot = new Date(base);
        slot.setDate(base.getDate() + day);
        slot.setHours(hour, minute, 0, 0);
        pushCandidate(slot);
      }
    }
  }
  return candidates;
}

async function createCalendarBlockForTask(taskId) {
  const task = (state.tasks || []).find((item) => item.id === taskId);
  if (!task) throw new Error("대상 할일을 찾지 못했습니다.");

  const duration = Math.max(30, Math.min(180, Number(task.effort_minutes || DEFAULT_BLOCK_MINUTES)));
  const events = mergedEvents();
  const candidates = buildTaskBlockCandidates(task, duration);

  for (const start of candidates) {
    const end = new Date(start.getTime() + duration * 60000);
    if (end <= new Date()) continue;
    if (!withinVisibleWorkingHours(start, end)) continue;
    if (hasEventConflict(start, end, events)) continue;

    try {
      await api("/calendar/blocks", {
        method: "POST",
        body: JSON.stringify({
          type: "task_block",
          title: task.title,
          start: toLocalPayloadDatetime(start),
          end: toLocalPayloadDatetime(end),
          task_id: task.id,
          locked: false,
        }),
      });
      await refreshAll();
      notify(`할일을 캘린더에 배치했습니다: ${task.title}`);
      return;
    } catch (error) {
      const msg = String(error?.message || "");
      if (msg.includes("Calendar conflict")) continue;
      throw error;
    }
  }

  throw new Error("가용 슬롯을 찾지 못했습니다. 일정 재배치를 먼저 실행해 주세요.");
}

function renderTasks() {
  const duplicateCounts = buildTaskDuplicateCounts();
  const activeTasks = state.tasks.filter(t => t.status !== "canceled" && t.status !== "done").sort((a, b) => {
    const aDue = a.due ? new Date(a.due).getTime() : Number.MAX_SAFE_INTEGER;
    const bDue = b.due ? new Date(b.due).getTime() : Number.MAX_SAFE_INTEGER;
    return aDue - bDue;
  });

  const doneTasks = state.tasks.filter(t => t.status === "done").sort((a, b) => {
    const aDue = a.due ? new Date(a.due).getTime() : Number.MAX_SAFE_INTEGER;
    const bDue = b.due ? new Date(b.due).getTime() : Number.MAX_SAFE_INTEGER;
    return aDue - bDue;
  });

  if (!activeTasks.length && !doneTasks.length) {
    $("todo-list").innerHTML = `<div class="todo-item"><div class="todo-meta">할일이 없습니다.</div></div>`;
    return;
  }

  const renderItem = (task) => {
    const dupKey = normalizedTaskKey(task.title);
    const dupCount = duplicateCounts.get(dupKey) || 0;
    const linkedCount = linkedBlockCount(task.id);
    return `
    <div class="todo-item ${dupCount > 1 ? "duplicate" : ""}">
      <div class="todo-title">${escapeHtml(task.title)}${dupCount > 1 ? ` <span class="task-dup-badge">중복 x${dupCount}</span>` : ""}</div>
      <div class="todo-meta">${task.due ? fmtDateTime(task.due) : "마감 없음"} · ${escapeHtml(task.priority)} · 캘린더 ${linkedCount}건</div>
      <div class="meta-row">
        <span class="tag ${task.status === "done" ? "done" : ""}">${escapeHtml(task.status)}</span>
        <button class="btn btn-ghost btn-mini" type="button" data-task-schedule="${task.id}">캘린더로 배치</button>
        <button class="btn btn-ghost btn-mini" type="button" data-task-progress="${task.id}" data-task-version="${task.version}">진행중</button>
        <button class="btn btn-ghost btn-mini" type="button" data-task-done="${task.id}" data-task-version="${task.version}">완료</button>
      </div>
    </div>
  `;
  };

  let html = activeTasks.map(renderItem).join("");

  if (doneTasks.length > 0) {
    html += `
      <details class="done-tasks-folder" style="margin-top: 12px; font-size: 13px;">
        <summary style="cursor: pointer; color: var(--muted); font-weight: 500;">완료된 작업 (${doneTasks.length})</summary>
        <div style="margin-top: 8px;">
          ${doneTasks.map(renderItem).join("")}
        </div>
      </details>
    `;
  }

  $("todo-list").innerHTML = html;

  document.querySelectorAll("[data-task-progress]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const version = Number.parseInt(button.dataset.taskVersion || "", 10);
        await api(`/tasks/${button.dataset.taskProgress}`, {
          method: "PATCH",
          body: JSON.stringify({ status: "in_progress", version: Number.isFinite(version) ? version : null }),
        });
        await refreshTasksOnly();
        notify("할일 상태를 진행중으로 변경했습니다.");
      } catch (error) {
        notify(error.message, true);
      }
    });
  });

  document.querySelectorAll("[data-task-done]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const version = Number.parseInt(button.dataset.taskVersion || "", 10);
        await api(`/tasks/${button.dataset.taskDone}`, {
          method: "PATCH",
          body: JSON.stringify({ status: "done", version: Number.isFinite(version) ? version : null }),
        });
        await refreshTasksOnly();
        notify("할일 상태를 완료로 변경했습니다.");
      } catch (error) {
        notify(error.message, true);
      }
    });
  });

  document.querySelectorAll("[data-task-schedule]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await createCalendarBlockForTask(button.dataset.taskSchedule);
      } catch (error) {
        notify(error.message, true);
      }
    });
  });
}

function approvalSummary(approval) {
  const payload = approval.payload || {};
  if (approval.type === "reschedule") {
    return payload.summary || "일정 재배치 승인 요청";
  }
  if (approval.type === "action_item") {
    return payload.reason ? `사유: ${payload.reason}` : "회의 액션아이템 승인 요청";
  }
  if (approval.type === "email_intake") {
    const classification = payload.classification || "unclear";
    const subject = payload.subject || "메일 제목 없음";
    const sender = payload.sender || "보낸사람 미상";
    const reason = payload.reason || "메일 분류 결과";
    const taskLabel = payload.task?.title ? `할일:${payload.task.title}` : "";
    const eventLabel = payload.event?.title ? `일정:${payload.event.title}` : "";
    const actionLabel = [taskLabel, eventLabel].filter(Boolean).join(" / ");
    return `[${classification}] ${subject} · ${sender}${actionLabel ? ` · ${actionLabel}` : ""} · ${reason}`;
  }
  return JSON.stringify(payload);
}

function approvalTypeLabel(type) {
  if (type === "action_item") return "회의 액션아이템";
  if (type === "reschedule") return "일정 재배치";
  if (type === "email_intake") return "신규 메일 분류";
  return type || "승인 요청";
}

function clipText(value, limit = 140) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

function approvalEvidenceText(approval) {
  const payload = approval.payload || {};
  if (approval.type === "email_intake") {
    const reason = String(payload.reason || "").trim();
    return reason || "메일 본문에서 일정/업무 요청 표현을 감지했습니다.";
  }
  if (approval.type === "action_item") {
    const reason = String(payload.reason || "").toLowerCase();
    if (reason.includes("low_confidence") || reason.includes("large_effort")) {
      return "추출 신뢰도 또는 작업 규모 기준으로 수동 확인이 필요합니다.";
    }
    return "회의 내용에서 실행 가능한 액션 아이템이 추출되었습니다.";
  }
  if (approval.type === "reschedule") {
    return "현재 일정/작업 제약을 만족하는 재배치 제안을 생성했습니다.";
  }
  if (approval.type === "chat_pending_action") {
    return "대량 변경이 포함될 수 있어 실행 전 사용자 확인이 필요합니다.";
  }
  return "변경 작업 실행 전 확인이 필요합니다.";
}

function approvalPromptText(approval) {
  const payload = approval.payload || {};
  const evidence = clipText(approvalEvidenceText(approval), 160);
  if (approval.type === "email_intake") {
    const subject = payload.subject || "메일 제목 없음";
    const sender = payload.sender || "보낸사람 미상";
    const eventTitle = payload.event?.title ? `일정: ${payload.event.title}` : null;
    const taskTitle = payload.task?.title ? `할일: ${payload.task.title}` : null;
    const candidates = [eventTitle, taskTitle].filter(Boolean).join(" / ");
    return candidates
      ? `새 메일을 분석했습니다.\n[${subject}] (${sender})\n후보: ${candidates}\n근거: ${evidence}\n등록할까요?`
      : `새 메일을 분석했습니다.\n[${subject}] (${sender})\n근거: ${evidence}\n일정/할일 후보를 등록할까요?`;
  }
  if (approval.type === "reschedule") {
    return `재배치 제안이 있습니다.\n${approvalSummary(approval)}\n근거: ${evidence}\n적용할까요?`;
  }
  if (approval.type === "action_item") {
    return `회의 액션아이템 반영 요청입니다.\n${approvalSummary(approval)}\n근거: ${evidence}\n반영할까요?`;
  }
  if (approval.type === "chat_pending_action") {
    return `실행 전 확인이 필요한 작업입니다.\n${approvalSummary(approval)}\n근거: ${evidence}\n진행할까요?`;
  }
  return `${approvalTypeLabel(approval.type)} 요청이 있습니다.\n${approvalSummary(approval)}\n근거: ${evidence}\n승인할까요?`;
}

function buildPendingApprovalActions(approval) {
  const payload = approval.payload || {};
  return [
    {
      type: "approval_pending",
      detail: {
        approval_id: approval.id,
        type: approval.type,
        proposal_id: payload?.proposal_id || "",
        summary: `${approvalSummary(approval)} · 근거: ${clipText(approvalEvidenceText(approval), 120)}`,
        task_title: payload?.task?.title || "",
        task_due: payload?.task?.due || "",
        event_title: payload?.event?.title || "",
        event_start: payload?.event?.start || "",
        event_end: payload?.event?.end || "",
      },
    },
  ];
}

function prunePromptedApprovalIds() {
  const pendingIds = new Set((state.approvals || []).map((item) => item.id));
  for (const id of state.approvalPromptedIds) {
    if (!pendingIds.has(id)) state.approvalPromptedIds.delete(id);
  }
  for (const id of state.previewedApprovals) {
    if (!pendingIds.has(id)) state.previewedApprovals.delete(id);
  }
}

function promptNextPendingApprovalInChat({ preferType = null } = {}) {
  prunePromptedApprovalIds();
  if (!state.approvals.length) return false;

  const ordered = [...state.approvals].sort((a, b) => {
    const aTime = new Date(a.created_at || 0).getTime();
    const bTime = new Date(b.created_at || 0).getTime();
    return aTime - bTime;
  });
  const prioritized = preferType
    ? [...ordered.filter((item) => item.type === preferType), ...ordered.filter((item) => item.type !== preferType)]
    : ordered;

  const next = prioritized.find((item) => !state.approvalPromptedIds.has(item.id));
  if (!next) return false;

  state.approvalPromptedIds.add(next.id);
  openChatWindow();
  addChatMessage("assistant", approvalPromptText(next), true, buildPendingApprovalActions(next));
  return true;
}

async function pollPendingApprovals() {
  if (state.approvalPromptInFlight || document.hidden) return;
  state.approvalPromptInFlight = true;
  try {
    await loadApprovals();
    promptNextPendingApprovalInChat();
  } catch (error) {
    console.warn("Approval polling skipped:", error);
  } finally {
    state.approvalPromptInFlight = false;
  }
}

function startApprovalPromptPolling() {
  if (state.approvalPromptTimer) return;
  state.approvalPromptTimer = window.setInterval(() => {
    void pollPendingApprovals();
  }, 20000);
}

function buildReschedulePreviewText(proposal) {
  const changes = (proposal?.changes || [])
    .map((row) => row?.payload || {})
    .filter((payload) => payload.kind === "create_block")
    .map((payload) => payload.block || {})
    .filter((block) => block.start && block.end);

  if (!changes.length) {
    return "재배치 미리보기를 생성하지 못했습니다.";
  }

  const lines = changes.slice(0, 8).map((block) => {
    const start = new Date(block.start);
    const end = new Date(block.end);
    const title = block.title || "일정";
    return `- ${fmtDateTime(start)} ~ ${fmtTime(end)} · ${title}`;
  });
  const more = changes.length > 8 ? `\n...외 ${changes.length - 8}건` : "";
  return `재배치 미리보기 (${changes.length}건)\n${lines.join("\n")}${more}`;
}

async function previewRescheduleApproval(approvalId, proposalId) {
  if (!approvalId || !proposalId) {
    throw new Error("재배치 미리보기 정보가 없습니다.");
  }
  const proposal = await api(`/scheduling/proposals/${proposalId}`);
  state.previewedApprovals.add(approvalId);
  addChatMessage("assistant", buildReschedulePreviewText(proposal));
}

async function refreshTasksOnly() {
  await loadTasks();
  renderTasks();
}

async function createTask(event) {
  event.preventDefault();
  const title = $("task-title").value.trim();
  if (!title) return;

  const dueInput = $("task-due").value;
  await api("/tasks", {
    method: "POST",
    body: JSON.stringify({
      title,
      priority: $("task-priority").value,
      effort_minutes: Number($("task-effort").value || 60),
      due: dueInput ? toLocalPayloadDatetime(new Date(dueInput)) : null,
      source: "manual",
    }),
  });

  $("task-form").reset();
  $("task-effort").value = "60";
  await refreshTasksOnly();
  notify("할일을 추가했습니다.");
}

async function connectGraph() {
  const result = await api("/graph/auth/url");
  if (!result.configured || !result.auth_url) {
    throw new Error(`Graph 설정 누락: ${(result.missing_settings || []).join(", ")}`);
  }
  window.location.href = result.auth_url;
}

async function disconnectGraph() {
  await api("/graph/disconnect", { method: "POST" });
  await loadGraphStatus();
  state.remoteEvents = [];
  renderWeekGrid();
  renderAgenda();
  notify("Outlook 연결을 해제했습니다.");
}

async function syncBidirectional(silent = false) {
  if (!state.graph?.connected) {
    throw new Error("Outlook 연결 후 동기화할 수 있습니다.");
  }

  state.syncInProgress = true;
  renderSystemStatus();
  try {
    const exportResult = await api("/graph/calendar/export", { method: "POST" });
    const importResult = await api("/sync/calendar/delta?reset=true&reconcile=true", { method: "POST" });
    let todoExportResult = null;
    let todoDeltaResult = null;
    let mailDeltaResult = null;
    let mailRecoveryUsed = false;
    try {
      todoExportResult = await api("/graph/todo/export", { method: "POST" });
      todoDeltaResult = await api("/sync/todo/delta", { method: "POST" });
    } catch (error) {
      console.warn("To Do export skipped:", error);
    }
    try {
      mailDeltaResult = await api("/sync/mail/delta", { method: "POST" });
      if ((mailDeltaResult?.processed || 0) === 0 && (mailDeltaResult?.skipped_read || 0) > 0) {
        try {
          const recovery = await api("/sync/mail/delta?reset=true&unread_only=false", { method: "POST" });
          mailRecoveryUsed = true;
          mailDeltaResult = {
            processed: Number(mailDeltaResult.processed || 0) + Number(recovery.processed || 0),
            created_approvals: Number(mailDeltaResult.created_approvals || 0) + Number(recovery.created_approvals || 0),
            ignored: Number(mailDeltaResult.ignored || 0) + Number(recovery.ignored || 0),
            skipped_existing: Number(mailDeltaResult.skipped_existing || 0) + Number(recovery.skipped_existing || 0),
            skipped_read: Number(mailDeltaResult.skipped_read || 0) + Number(recovery.skipped_read || 0),
            processed_read_actionable:
              Number(mailDeltaResult.processed_read_actionable || 0) + Number(recovery.processed_read_actionable || 0),
          };
        } catch (recoveryError) {
          console.warn("Mail delta recovery skipped:", recoveryError);
        }
      }
    } catch (error) {
      console.warn("Mail delta skipped:", error);
    }
    const createdApprovalCount = Number(mailDeltaResult?.created_approvals || 0);
    if (createdApprovalCount > 0) {
      await refreshAll({ promptPendingApproval: false });
      promptNextPendingApprovalInChat({ preferType: "email_intake" });
    } else {
      await refreshAll();
    }
    if (!silent) {
      const todoSummary = todoExportResult && todoDeltaResult
        ? ` / To Do 내보내기 ${todoExportResult.created + todoExportResult.updated}건(생성 ${todoExportResult.created}, 업데이트 ${todoExportResult.updated}, 실패 ${todoExportResult.failed}) + delta ${todoDeltaResult.created + todoDeltaResult.updated}건(신규 ${todoDeltaResult.created}, 수정 ${todoDeltaResult.updated}, 삭제 ${todoDeltaResult.deleted})`
        : " / To Do 내보내기 보류";
      const mailSummary = mailDeltaResult
        ? ` / 메일 분류 ${mailDeltaResult.processed}건(승인 요청 ${mailDeltaResult.created_approvals}, 무시 ${mailDeltaResult.ignored}, 기존건너뜀 ${mailDeltaResult.skipped_existing || 0}, 읽음건너뜀 ${mailDeltaResult.skipped_read || 0}, 읽음처리 ${mailDeltaResult.processed_read_actionable || 0}${mailRecoveryUsed ? ", 읽음메일 보강스캔 실행" : ""})`
        : " / 메일 분류 보류";
      const reconcileSummary = importResult.reconciled
        ? ` / 재조정 remote:${importResult.reconciled.remote_events || 0}, 삭제반영:${importResult.reconciled.reconciled_deleted || 0}`
        : "";
      notify(
        `동기화 완료 · 일정 내보내기 ${exportResult.synced}건(생성 ${exportResult.created}, 업데이트 ${exportResult.updated}) / delta 반영 ${importResult.created + importResult.updated}건(신규 ${importResult.created}, 수정 ${importResult.updated}, 삭제 ${importResult.deleted})${reconcileSummary}${todoSummary}${mailSummary}`,
      );
    }
  } finally {
    state.syncInProgress = false;
    renderSystemStatus();
  }
}

async function syncTodoFromGraph() {
  if (!state.graph?.connected) {
    throw new Error("Outlook 연결 후 To Do를 가져올 수 있습니다.");
  }

  const lists = await api("/graph/todo/lists");
  if (!lists.length) {
    throw new Error("가져올 To Do 목록이 없습니다.");
  }

  const first = lists[0];
  const result = await api(`/graph/todo/lists/${first.id}/import`, { method: "POST" });
  await refreshTasksOnly();
  notify(`To Do 가져오기 완료: ${result.imported}/${result.tasks}`);
}

function buildDynamicPromptChips() {
  const chips = [];
  const now = new Date();
  const activeTasks = (state.tasks || []).filter((task) => !["done", "canceled"].includes(task.status));
  const pendingEmailApprovals = (state.approvals || []).filter((row) => row.type === "email_intake").length;
  const conflictCount = countEventConflicts(mergedEvents());
  const overdueCount = activeTasks.filter((task) => task.due && new Date(task.due) < now).length;
  const unscheduledCount = activeTasks.filter((task) => linkedBlockCount(task.id) === 0).length;

  if (pendingEmailApprovals > 0) {
    chips.push({
      label: `메일 승인 ${pendingEmailApprovals}건`,
      prompt: "대기 중인 메일 승인 요청을 최신 순으로 하나씩 처리해줘",
    });
  }
  if (conflictCount > 0) {
    chips.push({
      label: `중복 일정 ${conflictCount}건`,
      prompt: "겹치는 일정들을 충돌 없게 재배치해줘",
    });
  }
  if (overdueCount > 0) {
    chips.push({
      label: `지연 작업 ${overdueCount}건`,
      prompt: "지연된 작업 우선순위와 마감 일정을 정리해줘",
    });
  }
  if (unscheduledCount > 0) {
    chips.push({
      label: `미배치 작업 ${unscheduledCount}건`,
      prompt: "아직 캘린더에 배치되지 않은 작업을 중요도 순으로 배치해줘",
    });
  }
  return chips.slice(0, 4);
}

function renderPromptChips() {
  const container = $("chat-prompts");
  if (!container) return;
  const merged = [...STATIC_PROMPT_CHIPS, ...buildDynamicPromptChips()];
  container.innerHTML = merged
    .map((chip) => `<button class="prompt-chip" type="button" data-prompt="${escapeHtml(chip.prompt)}">${escapeHtml(chip.label)}</button>`)
    .join("");
}

function approvalCardMeta(action) {
  const detail = action?.detail || {};
  const approvalId = detail.approval_id;
  if (!approvalId) return null;
  if (
    action.type !== "approval_requested" &&
    action.type !== "reschedule_approval_requested" &&
    action.type !== "approval_pending"
  ) {
    return null;
  }

  if (detail.type === "email_intake") {
    return {
      approvalId,
      title: "메일 일정/할일 등록 승인",
      summary: detail.summary || "메일에서 추출한 일정/할일 후보를 반영합니다.",
      supportsEdit: true,
      editData: {
        taskTitle: detail.task_title || "",
        taskDue: detail.task_due || "",
        eventTitle: detail.event_title || "",
        eventStart: detail.event_start || "",
        eventEnd: detail.event_end || "",
      },
    };
  }

  if (detail.type === "action_item") {
    return {
      approvalId,
      title: "회의 액션아이템 승인",
      summary: detail.summary || "회의에서 추출한 할일을 반영합니다.",
      supportsEdit: false,
    };
  }
  if (action.type === "reschedule_approval_requested" || detail.type === "reschedule") {
    return {
      approvalId,
      title: "일정 재배치 승인",
      summary: detail.summary || "재배치 제안을 일정에 반영합니다.",
      supportsEdit: false,
      proposalId: detail.proposal_id || "",
      previewRequired: true,
    };
  }
  return {
    approvalId,
    title: "AI 작업 승인",
    summary: detail.summary || "요청 작업을 실행하기 전에 확인이 필요합니다.",
    supportsEdit: false,
  };
}

function buildAssistantActionCards(actions) {
  return (actions || [])
    .map((action) => {
      const meta = approvalCardMeta(action);
      if (!meta) return "";
      const edit = meta.editData || {};
      const editForm = meta.supportsEdit
        ? `
          <div class="chat-approval-edit hidden" data-chat-edit-form="${meta.approvalId}">
            <input type="text" class="chat-edit-input" data-chat-edit-task-title="${meta.approvalId}" placeholder="할일 제목(선택)" value="${escapeHtml(edit.taskTitle || "")}" />
            <input type="text" class="chat-edit-input" data-chat-edit-event-title="${meta.approvalId}" placeholder="일정 제목(선택)" value="${escapeHtml(edit.eventTitle || "")}" />
            <div class="chat-edit-grid">
              <label class="chat-edit-label">일정 시작
                <input type="datetime-local" class="chat-edit-input" data-chat-edit-event-start="${meta.approvalId}" value="${escapeHtml(isoToLocalDatetimeValue(edit.eventStart))}" />
              </label>
              <label class="chat-edit-label">일정 종료
                <input type="datetime-local" class="chat-edit-input" data-chat-edit-event-end="${meta.approvalId}" value="${escapeHtml(isoToLocalDatetimeValue(edit.eventEnd))}" />
              </label>
            </div>
            <label class="chat-edit-label">할일 마감
              <input type="datetime-local" class="chat-edit-input" data-chat-edit-task-due="${meta.approvalId}" value="${escapeHtml(isoToLocalDatetimeValue(edit.taskDue))}" />
            </label>
            <div class="chat-approval-actions">
              <button class="btn btn-primary btn-mini" type="button" data-chat-edit-save="${meta.approvalId}">수정 후 승인</button>
              <button class="btn btn-ghost btn-mini" type="button" data-chat-edit-cancel="${meta.approvalId}">취소</button>
            </div>
          </div>
        `
        : "";
      return `
        <div class="chat-approval-card">
          <div class="chat-approval-title">${escapeHtml(meta.title)}</div>
          <div class="chat-approval-summary">${escapeHtml(meta.summary)}</div>
          <div class="chat-approval-id">ID: ${escapeHtml(meta.approvalId)}</div>
          <div class="chat-approval-actions">
            <button class="btn btn-primary btn-mini" type="button" data-chat-approve="${meta.approvalId}" data-chat-preview-required="${meta.previewRequired ? "1" : "0"}" data-chat-proposal-id="${escapeHtml(meta.proposalId || "")}">승인</button>
            <button class="btn btn-danger btn-mini" type="button" data-chat-reject="${meta.approvalId}">거절</button>
            ${meta.previewRequired ? `<button class="btn btn-ghost btn-mini" type="button" data-chat-preview="${meta.approvalId}" data-chat-proposal-id="${escapeHtml(meta.proposalId || "")}">미리보기</button>` : ""}
            ${meta.supportsEdit ? `<button class="btn btn-ghost btn-mini" type="button" data-chat-edit="${meta.approvalId}">수정 후 승인</button>` : ""}
          </div>
          ${editForm}
        </div>
      `;
    })
    .filter(Boolean)
    .join("");
}

function bindAssistantActionCards(scope) {
  scope.querySelectorAll("[data-chat-approve]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", async () => {
      if (button.dataset.busy === "1") return;
      button.dataset.busy = "1";
      button.disabled = true;
      try {
        const approvalId = button.dataset.chatApprove || "";
        const previewRequired = (button.dataset.chatPreviewRequired || "0") === "1";
        const proposalId = button.dataset.chatProposalId || "";
        if (previewRequired && !state.previewedApprovals.has(approvalId)) {
          await previewRescheduleApproval(approvalId, proposalId);
          addChatMessage("assistant", "미리보기를 확인했습니다. 승인 버튼을 한 번 더 누르면 확정 반영됩니다.");
          return;
        }
        await submitChatMessage(`승인 ${approvalId}`);
        state.previewedApprovals.delete(approvalId);
      } finally {
        button.dataset.busy = "0";
        button.disabled = false;
      }
    });
  });

  scope.querySelectorAll("[data-chat-reject]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", async () => {
      if (button.dataset.busy === "1") return;
      button.dataset.busy = "1";
      button.disabled = true;
      try {
        const approvalId = button.dataset.chatReject || "";
        await submitChatMessage(`취소 ${approvalId}`);
        state.previewedApprovals.delete(approvalId);
      } finally {
        button.dataset.busy = "0";
        button.disabled = false;
      }
    });
  });

  scope.querySelectorAll("[data-chat-preview]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", async () => {
      if (button.dataset.busy === "1") return;
      button.dataset.busy = "1";
      button.disabled = true;
      try {
        const approvalId = button.dataset.chatPreview || "";
        const proposalId = button.dataset.chatProposalId || "";
        await previewRescheduleApproval(approvalId, proposalId);
      } catch (error) {
        notify(error.message, true);
      } finally {
        button.dataset.busy = "0";
        button.disabled = false;
      }
    });
  });

  scope.querySelectorAll("[data-chat-edit]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", () => {
      const approvalId = button.dataset.chatEdit || "";
      if (!approvalId) return;
      const form = scope.querySelector(`[data-chat-edit-form="${approvalId}"]`);
      if (!form) return;
      form.classList.toggle("hidden");
    });
  });

  scope.querySelectorAll("[data-chat-edit-cancel]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", () => {
      const approvalId = button.dataset.chatEditCancel || "";
      if (!approvalId) return;
      const form = scope.querySelector(`[data-chat-edit-form="${approvalId}"]`);
      if (!form) return;
      form.classList.add("hidden");
    });
  });

  scope.querySelectorAll("[data-chat-edit-save]").forEach((button) => {
    if (button.dataset.bound === "1") return;
    button.dataset.bound = "1";
    button.addEventListener("click", async () => {
      const approvalId = button.dataset.chatEditSave || "";
      if (!approvalId) return;
      if (button.dataset.busy === "1") return;
      button.dataset.busy = "1";
      button.disabled = true;
      try {
        const taskTitleInput = scope.querySelector(`[data-chat-edit-task-title="${approvalId}"]`);
        const taskDueInput = scope.querySelector(`[data-chat-edit-task-due="${approvalId}"]`);
        const eventTitleInput = scope.querySelector(`[data-chat-edit-event-title="${approvalId}"]`);
        const eventStartInput = scope.querySelector(`[data-chat-edit-event-start="${approvalId}"]`);
        const eventEndInput = scope.querySelector(`[data-chat-edit-event-end="${approvalId}"]`);

        await api(`/approvals/${approvalId}/resolve`, {
          method: "POST",
          body: JSON.stringify({
            decision: "approve",
            reason: "approved_with_edits_via_chat",
            task_title: (taskTitleInput?.value || "").trim() || null,
            task_due: localInputToIso(taskDueInput?.value || ""),
            event_title: (eventTitleInput?.value || "").trim() || null,
            event_start: localInputToIso(eventStartInput?.value || ""),
            event_end: localInputToIso(eventEndInput?.value || ""),
          }),
        });
        addChatMessage("assistant", `승인 ${approvalId}를 수정 내용으로 반영했습니다.`);
        await refreshAll();
      } catch (error) {
        notify(error.message, true);
      } finally {
        button.dataset.busy = "0";
        button.disabled = false;
      }
    });
  });
}

function addChatMessage(role, text, remember = true, actions = []) {
  const log = $("chat-log");
  const box = document.createElement("div");
  box.className = `chat-msg ${role}`;
  const content = document.createElement("div");
  content.className = "chat-text";
  content.innerHTML = escapeHtml(text).replaceAll("\n", "<br />");
  box.appendChild(content);

  if (role === "assistant" && actions.length) {
    const cardsHtml = buildAssistantActionCards(actions);
    if (cardsHtml) {
      const cards = document.createElement("div");
      cards.className = "chat-action-list";
      cards.innerHTML = cardsHtml;
      box.appendChild(cards);
      bindAssistantActionCards(cards);
    }
  }

  log.appendChild(box);
  log.scrollTop = log.scrollHeight;

  if (!remember) return;
  state.chatHistory.push({ role, text });
  if (state.chatHistory.length > 20) {
    state.chatHistory = state.chatHistory.slice(-20);
  }
}

function composeAssistantReply(result) {
  const actions = result.actions || [];
  const llmError = actions.find((item) => item.type === "llm_error");
  if (llmError) {
    const reason = String(llmError?.detail?.reason || "").trim();
    state.llmErrorReason = reason || "LLM 호출 실패";
    state.llmErrorAt = new Date().toISOString();
    renderSystemStatus();
    if (reason) {
      const brief = reason.length > 180 ? `${reason.slice(0, 180)}...` : reason;
      return `${result.reply}\n\n원인: ${brief}\n\n작업: llm_error`;
    }
    return `${result.reply}\n\n작업: llm_error`;
  }
  state.llmErrorReason = "";
  state.llmErrorAt = null;
  renderSystemStatus();
  const actionSummary = actions.map((item) => item.type).join(", ");
  return actionSummary ? `${result.reply}\n\n작업: ${actionSummary}` : result.reply;
}

function renderCalendarViewport() {
  renderHeaderRange();
  renderWeekHeader();
  renderWeekGrid();
  renderAgenda();
  renderMiniMonth();
  ensureLiveBriefingTicker();
}

function isShortcutInputTarget(element) {
  if (!(element instanceof Element)) return false;
  const tag = element.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || tag === "BUTTON") return true;
  return element.isContentEditable;
}

async function refreshCalendarWeekQuick({ force = false } = {}) {
  const tasks = [
    loadCalendarData({
      force,
      useCache: true,
      staleWhileRevalidate: true,
      onUpdated: scheduleCalendarViewportRender,
    }),
    loadDailyBriefing(),
  ];

  const results = await Promise.allSettled(tasks);
  const failedCount = results.filter((item) => item.status === "rejected").length;
  if (failedCount > 0) {
    notify(`일부 데이터 로드에 실패했습니다. (${failedCount}개)`, true);
  }

  renderDailyBriefing();
  scheduleCalendarViewportRender();
  renderPromptChips();
}

async function applyWeekOffset(days) {
  const nextWeekStart = addDays(state.weekStart, days);
  const nextSelected = addDays(state.selectedDate, days);
  state.weekStart = nextWeekStart;
  state.selectedDate = nextSelected;
  state.miniMonth = new Date(nextSelected.getFullYear(), nextSelected.getMonth(), 1);
  await refreshCalendarWeekQuick({ force: false });
  prefetchAdjacentWeeks();
}

async function goToTodayWeek() {
  const today = startOfDay(new Date());
  state.selectedDate = today;
  state.weekStart = startOfWeek(today);
  state.miniMonth = new Date(today.getFullYear(), today.getMonth(), 1);
  await refreshCalendarWeekQuick({ force: false });
  prefetchAdjacentWeeks();
  scrollCalendarToNow();
}

async function submitChatMessage(message, { echoUser = true } = {}) {
  if (!message) return;
  const history = state.chatHistory.slice(-12);
  if (echoUser) addChatMessage("user", message);

  try {
    const result = await api("/assistant/chat", {
      method: "POST",
      body: JSON.stringify({ message, history }),
    });
    addChatMessage("assistant", composeAssistantReply(result), true, result.actions || []);
    const refreshKeys = Array.isArray(result.refresh) ? result.refresh : [];
    void runPostChatRefresh(refreshKeys);
  } catch (error) {
    addChatMessage("assistant", `오류: ${error.message}`);
    notify(error.message, true);
  }
}

async function runPostChatRefresh(refreshKeys = []) {
  try {
    if (Array.isArray(refreshKeys) && refreshKeys.length > 0) {
      await refreshForChatKeys(refreshKeys);
    } else {
      await refreshAll();
    }
  } catch (error) {
    notify(error.message, true);
  }
}

async function refreshForChatKeys(refreshKeys = []) {
  const keys = new Set(Array.isArray(refreshKeys) ? refreshKeys : []);
  if (!keys.size) {
    await refreshAll();
    return;
  }

  const needsTasks = keys.has("tasks");
  const needsCalendar = keys.has("calendar");
  const needsApprovals = keys.has("approvals");
  const needsBriefing = keys.has("briefing");
  const needsGraph = keys.has("graph");
  const needsSync = keys.has("sync");

  const loads = [];
  if (needsCalendar) loads.push(loadCalendarData({ force: true, useCache: false, staleWhileRevalidate: false, onUpdated: scheduleCalendarViewportRender }));
  if (needsTasks) loads.push(loadTasks());
  if (needsApprovals) loads.push(loadApprovals());
  if (needsBriefing || needsCalendar) loads.push(loadDailyBriefing());
  if (needsSync) loads.push(loadSyncStatus());
  if (needsGraph) loads.push(loadGraphStatus());

  const results = await Promise.allSettled(loads);
  const failedCount = results.filter((item) => item.status === "rejected").length;
  if (failedCount > 0) {
    notify(`일부 데이터 로드에 실패했습니다. (${failedCount}개)`, true);
  }

  if (needsTasks) {
    renderTasks();
  }
  if (needsCalendar) {
    scheduleCalendarViewportRender();
  }
  if (needsBriefing) {
    renderDailyBriefing();
  }
  if (needsTasks || needsCalendar || needsApprovals || needsBriefing) {
    renderPromptChips();
  }
  if (needsApprovals) {
    promptNextPendingApprovalInChat();
  }
}

async function sendChat(event) {
  event.preventDefault();
  const input = $("chat-input");
  const message = input.value.trim();
  if (!message) return;
  input.value = "";
  await submitChatMessage(message);
}

async function refreshCalendarOnly() {
  const results = await Promise.allSettled([
    loadCalendarData({ force: true, useCache: false, staleWhileRevalidate: false, onUpdated: scheduleCalendarViewportRender }),
    loadDailyBriefing(),
    loadSyncStatus(),
  ]);
  const failedCount = results.filter((item) => item.status === "rejected").length;
  if (failedCount > 0) {
    notify(`일부 데이터 로드에 실패했습니다. (${failedCount}개)`, true);
  }
  scheduleCalendarViewportRender();
  renderDailyBriefing();
  renderPromptChips();
}

async function refreshAll({ promptPendingApproval = true, preferApprovalType = null } = {}) {
  const primaryResults = await Promise.allSettled([loadGraphStatus(), loadSyncStatus()]);
  const primaryFailedCount = primaryResults.filter((item) => item.status === "rejected").length;
  if (primaryFailedCount > 0) {
    notify(`초기 상태 조회에 실패했습니다. (${primaryFailedCount}개)`, true);
  }

  const dataResults = await Promise.allSettled([
    loadTasks(),
    loadApprovals(),
    loadCalendarData({ force: true, useCache: false, staleWhileRevalidate: false, onUpdated: scheduleCalendarViewportRender }),
    loadDailyBriefing(),
  ]);
  const dataFailedCount = dataResults.filter((item) => item.status === "rejected").length;
  if (dataFailedCount > 0) {
    notify(`일부 데이터 로드에 실패했습니다. (${dataFailedCount}개)`, true);
  }

  scheduleCalendarViewportRender();
  renderTasks();
  renderDailyBriefing();
  renderPromptChips();
  ensureLiveBriefingTicker();
  if (promptPendingApproval) {
    promptNextPendingApprovalInChat({ preferType: preferApprovalType });
  }
}

function isBriefingLiveTarget() {
  return isSameDay(state.selectedDate, startOfDay(new Date()));
}

async function refreshBriefingLiveTick() {
  if (!isBriefingLiveTarget()) return;
  if (document.hidden) return;
  if (state.liveBriefingInFlight) return;
  state.liveBriefingInFlight = true;
  try {
    await Promise.allSettled([loadDailyBriefing(), loadSyncStatus()]);
    renderDailyBriefing();
  } finally {
    state.liveBriefingInFlight = false;
  }
}

function ensureLiveBriefingTicker() {
  const shouldRun = isBriefingLiveTarget();
  if (!shouldRun) {
    if (state.liveBriefingTimer) {
      window.clearInterval(state.liveBriefingTimer);
      state.liveBriefingTimer = null;
    }
    return;
  }

  if (!state.liveBriefingTimer) {
    state.liveBriefingTimer = window.setInterval(() => {
      void refreshBriefingLiveTick();
    }, 60 * 1000);
  }
}

function scrollCalendarToNow() {
  const wrap = document.querySelector(".time-grid-wrap");
  if (!wrap) return;
  const now = new Date();
  const hoursFromStart = now.getHours() - HOUR_START + now.getMinutes() / 60;
  const target = Math.max(0, (hoursFromStart - 1) * HOUR_HEIGHT);
  wrap.scrollTop = target;
}

function bindEvents() {
  $("save-profile").addEventListener("click", async () => {
    try {
      await saveProfile();
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("graph-connect").addEventListener("click", async () => {
    try {
      await connectGraph();
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("graph-disconnect").addEventListener("click", async () => {
    try {
      await disconnectGraph();
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("sync-bidirectional").addEventListener("click", async () => {
    try {
      await syncBidirectional(false);
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("sync-todo").addEventListener("click", async () => {
    try {
      await syncTodoFromGraph();
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("task-form").addEventListener("submit", async (event) => {
    try {
      await createTask(event);
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("refresh-briefing").addEventListener("click", async () => {
    try {
      await Promise.all([loadDailyBriefing(), loadSyncStatus()]);
      renderDailyBriefing();
      notify("오늘 브리핑을 갱신했습니다.");
    } catch (error) {
      notify(error.message, true);
    }
  });

  $("nav-prev").addEventListener("click", async () => {
    await applyWeekOffset(-7);
  });

  $("nav-next").addEventListener("click", async () => {
    await applyWeekOffset(7);
  });

  $("nav-today").addEventListener("click", async () => {
    await goToTodayWeek();
  });

  $("mini-prev").addEventListener("click", () => {
    state.miniMonth = new Date(state.miniMonth.getFullYear(), state.miniMonth.getMonth() - 1, 1);
    renderMiniMonth();
  });

  $("mini-next").addEventListener("click", () => {
    state.miniMonth = new Date(state.miniMonth.getFullYear(), state.miniMonth.getMonth() + 1, 1);
    renderMiniMonth();
  });

  $("chat-form").addEventListener("submit", sendChat);
  $("chat-input").addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      $("chat-form").requestSubmit();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.isComposing) return;
    if (event.repeat) return;
    if (isShortcutInputTarget(event.target)) return;

    if (event.key === "<" || (event.key === "," && event.shiftKey)) {
      event.preventDefault();
      void applyWeekOffset(-7);
      return;
    }
    if (event.key === ">" || (event.key === "." && event.shiftKey)) {
      event.preventDefault();
      void applyWeekOffset(7);
    }
  });

  $("chat-prompts").addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target.closest(".prompt-chip") : null;
    if (!target) return;
    $("chat-input").value = target.dataset.prompt || "";
    $("chat-input").focus();
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      void refreshBriefingLiveTick();
      void pollPendingApprovals();
    }
  });
}

async function bootstrap() {
  try {
    bindEvents();
    initChatToggle();
    renderTimeLabels();
    renderSystemStatus();

    const due = addDays(new Date(), 1);
    due.setHours(10, 0, 0, 0);
    $("task-due").value = toDateTimeInputValue(due);

    const params = new URLSearchParams(window.location.search);
    if (params.get("graph") === "connected") {
      notify("Microsoft Graph 연결이 완료되었습니다.");
      history.replaceState({}, "", "/");
    }
    if (params.get("graph_error")) {
      notify(`Graph 연결 실패: ${params.get("graph_error")}`, true);
      history.replaceState({}, "", "/");
    }

    await loadProfile();
    await refreshAll({ promptPendingApproval: false });
    scrollCalendarToNow();
    renderPromptChips();

    addChatMessage(
      "assistant",
      "AI Assistant 준비 완료. 회의록 등록, 일정 재배치, 할일 조정을 자연어로 요청하세요.",
      false,
    );
    promptNextPendingApprovalInChat();
    startApprovalPromptPolling();
  } catch (error) {
    notify(error.message, true);
  }
}

window.deleteCalendarBlock = deleteCalendarBlock;
window.openEventModal = openEventModal;
window.closeEventModal = closeEventModal;
window.saveEventChanges = saveEventChanges;
window.deleteFromModal = deleteFromModal;

bootstrap();
