const API = "/api";

const state = {
  profile: null,
  graph: null,
  tasks: [],
  localBlocks: [],
  remoteEvents: [],
  weekStart: startOfWeek(new Date()),
  selectedDate: startOfDay(new Date()),
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
  const day = date.getDay(); // 0 = Sun, 1 = Mon
  const diff = day === 0 ? -6 : 1 - day;
  const monday = new Date(date);
  monday.setDate(date.getDate() + diff);
  return startOfDay(monday);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function isSameDay(a, b) {
  return (
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
  );
}

function toDateTimeInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function fmtDate(date) {
  return date.toLocaleDateString("ko-KR", { month: "short", day: "numeric", weekday: "short" });
}

function fmtDateTime(value) {
  if (!value) return "-";
  const date = value instanceof Date ? value : new Date(value);
  return date.toLocaleString("ko-KR", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function fmtTimeRange(start, end) {
  const s = start.toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit" });
  const e = end.toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit" });
  return `${s} - ${e}`;
}

function notify(message, isError = false) {
  const el = $("status-message");
  if (!el) return;
  el.textContent = message;
  el.style.color = isError ? "#be2234" : "#516080";
}

async function api(path, options = {}) {
  const res = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  const isJson = res.headers.get("content-type")?.includes("application/json");
  const body = isJson ? await res.json() : await res.text();

  if (!res.ok) {
    const detail = typeof body === "object" ? body.detail || JSON.stringify(body) : body;
    throw new Error(detail || `HTTP ${res.status}`);
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

function renderGraphStatus() {
  const label = $("graph-status-label");
  const note = $("graph-status-note");
  if (!state.graph) {
    label.textContent = "Graph 상태 확인 중";
    label.className = "status-pill";
    note.textContent = "";
    return;
  }

  if (state.graph.connected) {
    label.textContent = "Graph 연결됨";
    label.className = "status-pill connected";
    note.textContent = state.graph.username ? state.graph.username : "Microsoft 계정 연결";
    return;
  }

  if (!state.graph.configured) {
    label.textContent = "Graph 설정 누락";
    label.className = "status-pill warn";
    note.textContent = `누락: ${(state.graph.missing_settings || []).join(", ")}`;
    return;
  }

  label.textContent = "Graph 미연결";
  label.className = "status-pill";
  note.textContent = "Outlook 연결 버튼을 눌러 로그인하세요.";
}

async function loadTasks() {
  state.tasks = await api("/tasks");
}

function parseGraphDateTime(value) {
  if (!value) return null;
  if (typeof value === "string") return new Date(value);
  if (value.dateTime) return new Date(value.dateTime);
  return null;
}

function normalizeLocalBlocks(blocks) {
  return blocks.map((block) => {
    const kind = block.source === "external" ? "outlook" : block.outlook_event_id ? "mixed" : "local";
    return {
      id: `local-${block.id}`,
      title: block.title || "일정",
      start: new Date(block.start),
      end: new Date(block.end),
      kind,
      source: block.source,
      outlookId: block.outlook_event_id || null,
      raw: block,
    };
  });
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
    raw: event,
  }));
}

function mergedEvents() {
  const local = normalizeLocalBlocks(state.localBlocks);
  const knownOutlookIds = new Set(local.map((item) => item.outlookId).filter(Boolean));
  const remote = normalizeRemoteEvents(state.remoteEvents).filter((item) => !knownOutlookIds.has(item.outlookId));

  return [...local, ...remote]
    .filter((item) => item.start && item.end)
    .sort((a, b) => a.start.getTime() - b.start.getTime());
}

async function loadCalendarData() {
  const start = state.weekStart;
  const end = addDays(state.weekStart, 7);
  const qs = `start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`;

  const localPromise = api(`/calendar/blocks?${qs}`);
  const remotePromise = state.graph?.connected ? api(`/graph/calendar/events?${qs}`) : Promise.resolve([]);
  const [local, remote] = await Promise.all([localPromise, remotePromise]);

  state.localBlocks = local;
  state.remoteEvents = remote;
}

function renderWeekRange() {
  const start = state.weekStart;
  const end = addDays(state.weekStart, 6);
  $("week-range").textContent = `${fmtDate(start)} - ${fmtDate(end)}`;
}

function renderCalendar() {
  renderWeekRange();
  const events = mergedEvents();
  const container = $("calendar-grid");

  const cells = Array.from({ length: 7 }, (_, idx) => {
    const day = addDays(state.weekStart, idx);
    const dayEvents = events.filter((event) => isSameDay(event.start, day));
    const activeClass = isSameDay(state.selectedDate, day) ? "active" : "";

    const chips = dayEvents.length
      ? dayEvents
          .slice(0, 4)
          .map(
            (event) =>
              `<div class="event-chip ${event.kind}">${escapeHtml(
                `${event.start.toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit" })} ${event.title}`,
              )}</div>`,
          )
          .join("")
      : `<div class="muted">일정 없음</div>`;

    return `
      <div class="day-cell ${activeClass}" data-day="${day.toISOString()}">
        <div class="day-head">
          <span>${day.toLocaleDateString("ko-KR", { weekday: "short" })}</span>
          <span class="date-num">${day.getDate()}</span>
        </div>
        <div class="day-events">${chips}</div>
      </div>
    `;
  });

  container.innerHTML = cells.join("");
  container.querySelectorAll("[data-day]").forEach((cell) => {
    cell.addEventListener("click", () => {
      state.selectedDate = startOfDay(new Date(cell.dataset.day));
      renderCalendar();
      renderAgenda();
    });
  });
}

function renderAgenda() {
  const events = mergedEvents().filter((event) => isSameDay(event.start, state.selectedDate));
  $("agenda-date").textContent = fmtDate(state.selectedDate);

  if (!events.length) {
    $("agenda-list").innerHTML = `<div class="agenda-item"><div class="agenda-meta">선택한 날짜에 일정이 없습니다.</div></div>`;
    return;
  }

  $("agenda-list").innerHTML = events
    .map((event) => {
      const sourceTag =
        event.kind === "outlook" ? `<span class="tag outlook">Outlook</span>` : `<span class="tag">Local</span>`;
      const syncTag = event.kind === "mixed" ? `<span class="tag">Synced</span>` : "";
      return `
        <div class="agenda-item">
          <div class="agenda-title">${escapeHtml(event.title)}</div>
          <div class="agenda-meta">${fmtTimeRange(event.start, event.end)}</div>
          <div class="meta-row">${sourceTag}${syncTag}</div>
        </div>
      `;
    })
    .join("");
}

function renderTasks() {
  const ordered = [...state.tasks].sort((a, b) => {
    const aDone = a.status === "done" ? 1 : 0;
    const bDone = b.status === "done" ? 1 : 0;
    if (aDone !== bDone) return aDone - bDone;
    const aDue = a.due ? new Date(a.due).getTime() : Number.MAX_SAFE_INTEGER;
    const bDue = b.due ? new Date(b.due).getTime() : Number.MAX_SAFE_INTEGER;
    return aDue - bDue;
  });

  if (!ordered.length) {
    $("todo-list").innerHTML = `<div class="todo-item"><div class="todo-meta">등록된 할일이 없습니다.</div></div>`;
    return;
  }

  $("todo-list").innerHTML = ordered
    .map((task) => {
      const doneClass = task.status === "done" ? "done" : "";
      const dueText = task.due ? fmtDateTime(task.due) : "마감 미정";
      const statusTag = task.status === "done" ? `<span class="tag done">done</span>` : `<span class="tag">${task.status}</span>`;

      return `
        <div class="todo-item ${doneClass}">
          <div class="todo-title">${escapeHtml(task.title)}</div>
          <div class="todo-meta">마감 ${escapeHtml(dueText)} · 우선순위 ${escapeHtml(task.priority)}</div>
          <div class="meta-row">
            ${statusTag}
            <button class="btn btn-ghost btn-mini" data-task-done="${task.id}">완료</button>
            <button class="btn btn-ghost btn-mini" data-task-progress="${task.id}">진행중</button>
          </div>
        </div>
      `;
    })
    .join("");

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
  notify("새 할일을 추가했습니다.");
}

async function connectGraph() {
  const result = await api("/graph/auth/url");
  if (!result.configured || !result.auth_url) {
    const msg = `Graph 설정 누락: ${(result.missing_settings || []).join(", ")}`;
    throw new Error(msg);
  }
  window.location.href = result.auth_url;
}

async function disconnectGraph() {
  await api("/graph/disconnect", { method: "POST" });
  await loadGraphStatus();
  state.remoteEvents = [];
  renderCalendar();
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
      `양방향 동기화 완료 · 내보내기 ${exportResult.synced}건(생성 ${exportResult.created}, 업데이트 ${exportResult.updated}) / 가져오기 ${importResult.imported}건`,
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

  const listId = lists[0].id;
  const result = await api(`/graph/todo/lists/${listId}/import`, { method: "POST" });
  await refreshTasksOnly();
  notify(`To Do 가져오기 완료: ${result.imported}/${result.tasks}`);
}

function addChatMessage(role, text) {
  const log = $("chat-log");
  const item = document.createElement("div");
  item.className = `chat-msg ${role}`;
  item.innerHTML = escapeHtml(text).replaceAll("\n", "<br />");
  log.appendChild(item);
  log.scrollTop = log.scrollHeight;
}

async function sendChat(event) {
  event.preventDefault();
  const input = $("chat-input");
  const message = input.value.trim();
  if (!message) return;

  addChatMessage("user", message);
  input.value = "";

  try {
    const result = await api("/assistant/chat", {
      method: "POST",
      body: JSON.stringify({ message }),
    });

    const actionSummary = (result.actions || []).map((a) => a.type).join(", ");
    addChatMessage("assistant", actionSummary ? `${result.reply}\n\n작업: ${actionSummary}` : result.reply);

    await refreshAll();
  } catch (error) {
    addChatMessage("assistant", `오류: ${error.message}`);
    notify(error.message, true);
  }
}

async function refreshCalendarAndAgenda() {
  await loadCalendarData();
  renderCalendar();
  renderAgenda();
}

async function refreshAll() {
  await loadGraphStatus();
  await Promise.all([loadTasks(), loadCalendarData()]);
  renderCalendar();
  renderAgenda();
  renderTasks();
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

  $("refresh-all").addEventListener("click", async () => {
    try {
      await refreshAll();
      notify("화면 데이터를 새로고침했습니다.");
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

  $("week-prev").addEventListener("click", async () => {
    state.weekStart = addDays(state.weekStart, -7);
    state.selectedDate = addDays(state.selectedDate, -7);
    await refreshCalendarAndAgenda();
  });

  $("week-next").addEventListener("click", async () => {
    state.weekStart = addDays(state.weekStart, 7);
    state.selectedDate = addDays(state.selectedDate, 7);
    await refreshCalendarAndAgenda();
  });

  $("week-today").addEventListener("click", async () => {
    state.weekStart = startOfWeek(new Date());
    state.selectedDate = startOfDay(new Date());
    await refreshCalendarAndAgenda();
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

    const now = new Date();
    const due = addDays(now, 1);
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

    addChatMessage(
      "assistant",
      "AI Assistant 준비 완료. 회의록 등록, 일정 재배치, 할일 완료/우선순위 변경 요청을 바로 처리할 수 있습니다.",
    );
  } catch (error) {
    notify(error.message, true);
  }
}

bootstrap();
