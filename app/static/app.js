const API = "/api";
const HOUR_START = 7;
const HOUR_END = 22;
const HOUR_HEIGHT = 56;
const MIN_EVENT_HEIGHT = 18;

const state = {
  profile: null,
  graph: null,
  syncStatus: null,
  dailyBriefing: null,
  tasks: [],
  approvals: [],
  chatHistory: [],
  localBlocks: [],
  remoteEvents: [],
  weekStart: startOfWeek(new Date()),
  selectedDate: startOfDay(new Date()),
  miniMonth: new Date(new Date().getFullYear(), new Date().getMonth(), 1),
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

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
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

function notify(message, isError = false) {
  const el = $("status-message");
  if (!el) return;
  el.textContent = message || "";
  el.style.color = isError ? "#d93025" : "#5a6788";
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
  if (!label || !state.graph) return;

  if (state.graph.connected) {
    label.className = "status-pill connected";
    label.textContent = `연결됨: ${state.graph.username || "Microsoft"}`;
    return;
  }

  if (!state.graph.configured) {
    label.className = "status-pill warn";
    label.textContent = "Graph 설정 누락";
    return;
  }

  label.className = "status-pill";
  label.textContent = "Graph 미연결";
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
    outlookId: block.outlook_event_id || null,
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

function mergedEvents() {
  const local = normalizeLocalBlocks(state.localBlocks);
  const known = new Set(local.map((item) => item.outlookId).filter(Boolean));
  const remote = normalizeRemoteEvents(state.remoteEvents).filter((item) => !known.has(item.outlookId));

  return [...local, ...remote]
    .filter((item) => item.start instanceof Date && item.end instanceof Date)
    .sort((a, b) => a.start.getTime() - b.start.getTime());
}

async function loadCalendarData() {
  const start = state.weekStart;
  const end = addDays(state.weekStart, 7);
  const query = `start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`;

  const localPromise = api(`/calendar/blocks?${query}`);
  const remotePromise = state.graph?.connected ? api(`/graph/calendar/events?${query}`) : Promise.resolve([]);
  const [local, remote] = await Promise.all([localPromise, remotePromise]);
  state.localBlocks = local;
  state.remoteEvents = remote;
}

async function deleteCalendarBlock(id, isOutlook) {
  const rawId = String(id || "");
  const isLocal = rawId.startsWith("local-");
  const isRemoteOutlook = rawId.startsWith("outlook-");

  try {
    notify("일정 삭제 중...");

    if (isLocal) {
      await api(`/calendar/blocks/${rawId.replace("local-", "")}`, { method: "DELETE" });
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
    target.addEventListener("click", () => {
      const encodedId = target.dataset.openEventId || "";
      if (!encodedId) return;
      openEventModal(decodeURIComponent(encodedId));
    });
  });
}

function toLocalDatetimeValue(date) {
  const d = new Date(date);
  d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
  return d.toISOString().slice(0, 16);
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
      body: JSON.stringify({ title, start: startDt.toISOString(), end: endDt.toISOString() }),
    });
    notify('일정이 수정되었습니다.');
    closeEventModal();
    await refreshCalendarOnly();
  } catch (error) {
    notify(`수정 실패: ${error.message}`, true);
  }
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
      renderWeekGrid();
      renderAgenda();
      renderMiniMonth();
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

        const isOutlook = event.kind === "outlook" || event.kind === "mixed";
        const encodedId = encodeURIComponent(event.id);
        return `
          <div class="calendar-event ${event.kind}" data-open-event-id="${encodedId}" style="top:${top}px;height:${height}px;left:calc(${left}% + 2px);width:calc(${width}% - 4px);">
            <button class="event-delete" data-event-id="${encodedId}" data-event-outlook="${isOutlook ? "1" : "0"}" title="일정 삭제">×</button>
            <div class="event-time">${fmtTime(event.start)}</div>
            <div class="event-title">${escapeHtml(event.title)}</div>
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

    return `<div class="day-column" data-day-column="${dayStart.toISOString()}" style="height:${gridHeight}px">
      ${eventsHtml}
      ${timeIndicatorHtml}
    </div>`;
  });

  container.innerHTML = dayColumns.join("");
  bindEventOpenTargets(container);
  bindEventDeleteButtons(container);
  container.querySelectorAll("[data-day-column]").forEach((el) => {
    el.addEventListener("click", () => {
      state.selectedDate = startOfDay(new Date(el.dataset.dayColumn));
      renderWeekHeader();
      renderAgenda();
      renderMiniMonth();
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
      const weekChanged = nextWeek.getTime() !== state.weekStart.getTime();
      state.selectedDate = day;
      state.weekStart = nextWeek;
      state.miniMonth = new Date(day.getFullYear(), day.getMonth(), 1);

      if (weekChanged) {
        await loadCalendarData();
      }
      renderHeaderRange();
      renderWeekHeader();
      renderWeekGrid();
      renderAgenda();
      renderMiniMonth();
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
    fab.addEventListener("click", () => {
      chatWindow.classList.remove("hidden");
      fab.style.display = "none";
      chatWindow.classList.add("show");
    });

    closeBtn.addEventListener("click", () => {
      chatWindow.classList.add("hidden");
      chatWindow.classList.remove("show");
      fab.style.display = "flex";
    });
  }
}

function renderTasks() {
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

  const renderItem = (task) => `
    <div class="todo-item">
      <div class="todo-title">${escapeHtml(task.title)}</div>
      <div class="todo-meta">${task.due ? fmtDateTime(task.due) : "마감 없음"} · ${escapeHtml(task.priority)}</div>
      <div class="meta-row">
        <span class="tag ${task.status === "done" ? "done" : ""}">${escapeHtml(task.status)}</span>
        <button class="btn btn-ghost btn-mini" type="button" data-task-progress="${task.id}">진행중</button>
        <button class="btn btn-ghost btn-mini" type="button" data-task-done="${task.id}">완료</button>
      </div>
    </div>
  `;

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
        await api(`/tasks/${button.dataset.taskProgress}`, {
          method: "PATCH",
          body: JSON.stringify({ status: "in_progress" }),
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
        await api(`/tasks/${button.dataset.taskDone}`, {
          method: "PATCH",
          body: JSON.stringify({ status: "done" }),
        });
        await refreshTasksOnly();
        notify("할일 상태를 완료로 변경했습니다.");
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
  return JSON.stringify(payload);
}

function renderApprovals() {
  const list = $("approvals-list");
  $("approval-count").textContent = `(${state.approvals.length}건)`;

  if (!state.approvals.length) {
    list.innerHTML = `<div class="approval-item"><div class="approval-meta">대기 중인 승인 요청이 없습니다.</div></div>`;
    return;
  }

  list.innerHTML = state.approvals
    .map(
      (approval) => `
        <div class="approval-item">
          <div class="approval-title">${escapeHtml(approval.type)}</div>
          <div class="approval-meta">${escapeHtml(approvalSummary(approval))}</div>
          <div class="meta-row">
            <button class="btn btn-primary btn-mini" type="button" data-approval-approve="${approval.id}">승인</button>
            <button class="btn btn-danger btn-mini" type="button" data-approval-reject="${approval.id}">거절</button>
          </div>
        </div>
      `,
    )
    .join("");

  list.querySelectorAll("[data-approval-approve]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api(`/approvals/${button.dataset.approvalApprove}/resolve`, {
          method: "POST",
          body: JSON.stringify({ decision: "approve" }),
        });
        await refreshAll();
        notify("승인 요청을 승인했습니다.");
      } catch (error) {
        notify(error.message, true);
      }
    });
  });

  list.querySelectorAll("[data-approval-reject]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api(`/approvals/${button.dataset.approvalReject}/resolve`, {
          method: "POST",
          body: JSON.stringify({ decision: "reject" }),
        });
        await refreshAll();
        notify("승인 요청을 거절했습니다.");
      } catch (error) {
        notify(error.message, true);
      }
    });
  });
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
      due: dueInput ? new Date(dueInput).toISOString() : null,
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

  const exportResult = await api("/graph/calendar/export", { method: "POST" });
  const importResult = await api("/graph/calendar/import", { method: "POST" });
  await refreshAll();
  if (!silent) {
    notify(
      `동기화 완료 · 내보내기 ${exportResult.synced}건(생성 ${exportResult.created}, 업데이트 ${exportResult.updated}) / 가져오기 ${importResult.imported}건`,
    );
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

function approvalCardMeta(action) {
  const detail = action?.detail || {};
  const approvalId = detail.approval_id;
  if (!approvalId) return null;
  if (action.type !== "approval_requested" && action.type !== "reschedule_approval_requested") return null;

  if (detail.type === "action_item") {
    return {
      approvalId,
      title: "회의 액션아이템 승인",
      summary: detail.summary || "회의에서 추출한 할일을 반영합니다.",
    };
  }
  if (action.type === "reschedule_approval_requested" || detail.type === "reschedule") {
    return {
      approvalId,
      title: "일정 재배치 승인",
      summary: detail.summary || "재배치 제안을 일정에 반영합니다.",
    };
  }
  return {
    approvalId,
    title: "AI 작업 승인",
    summary: detail.summary || "요청 작업을 실행하기 전에 확인이 필요합니다.",
  };
}

function buildAssistantActionCards(actions) {
  return (actions || [])
    .map((action) => {
      const meta = approvalCardMeta(action);
      if (!meta) return "";
      return `
        <div class="chat-approval-card">
          <div class="chat-approval-title">${escapeHtml(meta.title)}</div>
          <div class="chat-approval-summary">${escapeHtml(meta.summary)}</div>
          <div class="chat-approval-id">ID: ${escapeHtml(meta.approvalId)}</div>
          <div class="chat-approval-actions">
            <button class="btn btn-primary btn-mini" type="button" data-chat-approve="${meta.approvalId}">승인</button>
            <button class="btn btn-danger btn-mini" type="button" data-chat-reject="${meta.approvalId}">거절</button>
          </div>
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
        await submitChatMessage(`승인 ${button.dataset.chatApprove}`);
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
        await submitChatMessage(`취소 ${button.dataset.chatReject}`);
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
  const actionSummary = actions.map((item) => item.type).join(", ");
  return actionSummary ? `${result.reply}\n\n작업: ${actionSummary}` : result.reply;
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
    await refreshAll();
  } catch (error) {
    addChatMessage("assistant", `오류: ${error.message}`);
    notify(error.message, true);
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
  await Promise.all([loadCalendarData(), loadDailyBriefing(), loadSyncStatus()]);
  renderHeaderRange();
  renderWeekHeader();
  renderWeekGrid();
  renderAgenda();
  renderMiniMonth();
  renderDailyBriefing();
}

async function refreshAll() {
  await Promise.all([loadGraphStatus(), loadSyncStatus()]);
  await Promise.all([loadTasks(), loadApprovals(), loadCalendarData(), loadDailyBriefing()]);
  renderHeaderRange();
  renderWeekHeader();
  renderWeekGrid();
  renderAgenda();
  renderMiniMonth();
  renderTasks();
  renderApprovals();
  renderDailyBriefing();
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

  $("refresh-approvals").addEventListener("click", async () => {
    try {
      await loadApprovals();
      renderApprovals();
      notify("승인 요청 목록을 갱신했습니다.");
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
    state.weekStart = addDays(state.weekStart, -7);
    state.selectedDate = addDays(state.selectedDate, -7);
    state.miniMonth = new Date(state.selectedDate.getFullYear(), state.selectedDate.getMonth(), 1);
    await refreshCalendarOnly();
  });

  $("nav-next").addEventListener("click", async () => {
    state.weekStart = addDays(state.weekStart, 7);
    state.selectedDate = addDays(state.selectedDate, 7);
    state.miniMonth = new Date(state.selectedDate.getFullYear(), state.selectedDate.getMonth(), 1);
    await refreshCalendarOnly();
  });

  $("nav-today").addEventListener("click", async () => {
    state.selectedDate = startOfDay(new Date());
    state.weekStart = startOfWeek(new Date());
    state.miniMonth = new Date(state.selectedDate.getFullYear(), state.selectedDate.getMonth(), 1);
    await refreshCalendarOnly();
    scrollCalendarToNow();
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

  document.querySelectorAll(".prompt-chip").forEach((button) => {
    button.addEventListener("click", () => {
      $("chat-input").value = button.dataset.prompt || "";
      $("chat-input").focus();
    });
  });
}

async function bootstrap() {
  try {
    bindEvents();
    initChatToggle();
    renderTimeLabels();

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
    await refreshAll();
    scrollCalendarToNow();

    addChatMessage(
      "assistant",
      "AI Assistant 준비 완료. 회의록 등록, 일정 재배치, 할일 조정을 자연어로 요청하세요.",
      false,
    );
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
