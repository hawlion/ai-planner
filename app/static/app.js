const API = "/api";

const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  return `${d.toLocaleDateString()} ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
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

function notify(message, isError = false) {
  const statusEl = $("meeting-status");
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#b42318" : "#5b6376";
}

function setGraphMessage(message, isError = false) {
  const el = $("graph-message");
  if (!el) return;
  el.textContent = message || "";
  el.style.color = isError ? "#b42318" : "#5b6376";
}

async function loadProfile() {
  const profile = await api("/profile");
  $("autonomy").value = profile.autonomy_level;
  $("timezone").value = profile.timezone;
}

async function saveProfile() {
  const autonomy = $("autonomy").value;
  const timezone = $("timezone").value;
  await api("/profile", {
    method: "PATCH",
    body: JSON.stringify({ autonomy_level: autonomy, timezone }),
  });
  await loadProfile();
  notify("프로필 설정이 저장되었습니다.");
}

async function loadBriefing() {
  const briefing = await api("/briefings/daily");
  const top = briefing.top_tasks
    .map(
      (item) =>
        `<div class="list-item"><strong>${escapeHtml(item.title)}</strong><div>${escapeHtml(item.reason)}</div><div class="muted">추천: ${
          item.recommended_block ? `${fmtDate(item.recommended_block.start)} ~ ${fmtDate(item.recommended_block.end)}` : "없음"
        }</div></div>`,
    )
    .join("");

  const risks = briefing.risks.length
    ? briefing.risks.map((r) => `<span class="badge warn">${escapeHtml(r)}</span>`).join(" ")
    : "<span class=\"badge\">리스크 없음</span>";

  $("briefing").innerHTML = `
    <div class="row">${risks}</div>
    <div class="muted">집중 ${briefing.snapshot.focus_minutes}분 · 여유 ${briefing.snapshot.free_minutes}분</div>
    <div class="list" style="margin-top:8px;">${top}</div>
  `;
}

async function loadTasks() {
  const tasks = await api("/tasks");
  const rows = tasks
    .map(
      (task) => `
        <tr>
          <td>${escapeHtml(task.title)}</td>
          <td>${escapeHtml(task.priority)}</td>
          <td>${escapeHtml(task.status)}</td>
          <td>${fmtDate(task.due)}</td>
          <td>${task.effort_minutes}</td>
        </tr>
      `,
    )
    .join("");
  $("tasks-table").innerHTML = rows || `<tr><td colspan="5">작업이 없습니다.</td></tr>`;
}

async function createTask(event) {
  event.preventDefault();
  const title = $("task-title").value.trim();
  if (!title) return;

  const dueInput = $("task-due").value;
  const payload = {
    title,
    priority: $("task-priority").value,
    effort_minutes: Number($("task-effort").value || 60),
    due: dueInput ? new Date(dueInput).toISOString() : null,
  };

  await api("/tasks", { method: "POST", body: JSON.stringify(payload) });
  $("task-form").reset();
  $("task-effort").value = "60";
  await Promise.all([loadTasks(), loadBriefing()]);
}

async function loadCalendar() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const end = new Date(start.getTime() + 1000 * 60 * 60 * 24 * 3);
  const blocks = await api(`/calendar/blocks?start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`);

  $("calendar-list").innerHTML = blocks.length
    ? blocks
        .map(
          (block) => `<div class="list-item">
      <strong>${escapeHtml(block.title)}</strong>
      <div class="row"><span class="badge">${escapeHtml(block.type)}</span>${
        block.locked ? '<span class="badge warn">locked</span>' : ""
      }</div>
      <div class="muted">${fmtDate(block.start)} ~ ${fmtDate(block.end)}</div>
    </div>`,
        )
        .join("")
    : `<div class="muted">현재 캘린더 블록이 없습니다.</div>`;
}

function parseTranscript(raw) {
  return raw
    .split("\n")
    .map((line, index) => {
      const trimmed = line.trim();
      if (!trimmed) return null;
      const [speaker, ...rest] = trimmed.includes(":") ? trimmed.split(":") : ["참석자", trimmed];
      return {
        ts_ms: index * 20000,
        speaker: speaker.trim() || "참석자",
        text: rest.join(":").trim() || trimmed,
      };
    })
    .filter(Boolean);
}

async function ingestMeeting(event) {
  event.preventDefault();
  const transcript = parseTranscript($("meeting-transcript").value);
  if (!transcript.length) {
    notify("회의록은 최소 1줄 이상 필요합니다.", true);
    return;
  }

  const payload = {
    title: $("meeting-title").value.trim() || null,
    summary: $("meeting-summary").value.trim() || null,
    transcript,
  };

  const res = await api("/meetings", { method: "POST", body: JSON.stringify(payload) });
  notify(`회의 처리 시작: ${res.meeting_id}`);
  await pollMeeting(res.meeting_id);
}

async function pollMeeting(meetingId) {
  const maxTry = 20;
  for (let i = 0; i < maxTry; i += 1) {
    const meeting = await api(`/meetings/${meetingId}`);
    notify(`회의 상태: ${meeting.extraction_status}`);
    if (meeting.extraction_status !== "pending") {
      await loadCandidates(meetingId);
      await loadApprovals();
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1200));
  }
  notify("회의 추출 시간이 길어지고 있습니다. 잠시 후 다시 시도하세요.", true);
}

async function loadCandidates(meetingId) {
  const candidates = await api(`/meetings/${meetingId}/action-items`);
  const html = candidates.length
    ? candidates
        .map(
          (c) => `<div class="list-item">
      <strong>${escapeHtml(c.title)}</strong>
      <div class="row">
        <span class="badge">confidence ${c.confidence.toFixed(2)}</span>
        <span class="badge">effort ${c.effort_minutes}m</span>
        ${c.due ? `<span class="badge">due ${escapeHtml(fmtDate(c.due))}</span>` : ""}
      </div>
      <div class="muted">${escapeHtml(c.rationale || "")}</div>
      <div class="row" style="margin-top:8px;">
        <button class="btn btn-primary" data-approve="${c.id}">승인</button>
        <button class="btn btn-danger" data-reject="${c.id}">거절</button>
      </div>
    </div>`,
        )
        .join("")
    : `<div class="muted">추출된 액션 아이템이 없습니다.</div>`;

  $("meeting-candidates").innerHTML = html;

  document.querySelectorAll("[data-approve]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await api(`/action-items/${btn.dataset.approve}/approve`, {
        method: "POST",
        body: JSON.stringify({ create_time_block: true }),
      });
      notify("액션 아이템을 승인했습니다.");
      await Promise.all([loadTasks(), loadCalendar(), loadApprovals(), loadBriefing()]);
    });
  });

  document.querySelectorAll("[data-reject]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await api(`/action-items/${btn.dataset.reject}/reject`, { method: "POST" });
      notify("액션 아이템을 거절했습니다.");
      await loadApprovals();
    });
  });
}

async function loadApprovals() {
  const approvals = await api("/approvals?status=pending");
  $("approvals").innerHTML = approvals.length
    ? approvals
        .map(
          (a) => `<div class="list-item">
        <strong>${escapeHtml(a.type)}</strong>
        <div class="muted">${escapeHtml(JSON.stringify(a.payload))}</div>
        <div class="row" style="margin-top:8px;">
          <button class="btn btn-primary" data-approve-id="${a.id}">승인</button>
          <button class="btn btn-danger" data-reject-id="${a.id}">거절</button>
        </div>
      </div>`,
        )
        .join("")
    : `<div class="muted">대기 중 승인 요청이 없습니다.</div>`;

  document.querySelectorAll("[data-approve-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await api(`/approvals/${btn.dataset.approveId}/resolve`, {
        method: "POST",
        body: JSON.stringify({ decision: "approve" }),
      });
      await Promise.all([loadApprovals(), loadTasks(), loadCalendar(), loadBriefing()]);
    });
  });

  document.querySelectorAll("[data-reject-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await api(`/approvals/${btn.dataset.rejectId}/resolve`, {
        method: "POST",
        body: JSON.stringify({ decision: "reject" }),
      });
      await loadApprovals();
    });
  });
}

async function createProposals(event) {
  event.preventDefault();
  const fromValue = $("proposal-from").value;
  const toValue = $("proposal-to").value;
  if (!fromValue || !toValue) return;

  const payload = {
    horizon: {
      from: new Date(fromValue).toISOString(),
      to: new Date(toValue).toISOString(),
    },
    constraints: {
      slot_minutes: 30,
      split_allowed: false,
      max_proposals: 3,
    },
  };

  const proposals = await api("/scheduling/proposals", {
    method: "POST",
    body: JSON.stringify(payload),
  });

  renderProposals(proposals);
}

function renderProposals(proposals) {
  $("proposals").innerHTML = proposals.length
    ? proposals
        .map(
          (p) => `<div class="list-item">
      <strong>${escapeHtml(p.summary)}</strong>
      <div class="row">
        <span class="badge">changes ${p.score.changes_count}</span>
        <span class="badge">late ${p.score.lateness_minutes}m</span>
        <span class="badge">deep ${p.score.deep_work_minutes}m</span>
      </div>
      <div class="muted">${escapeHtml((p.explanation.tradeoffs || []).join(" / "))}</div>
      <button class="btn btn-primary" style="margin-top:8px;" data-apply-proposal="${p.id}">이 제안 적용</button>
    </div>`,
        )
        .join("")
    : `<div class="muted">생성된 제안이 없습니다.</div>`;

  document.querySelectorAll("[data-apply-proposal]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const result = await api(`/scheduling/proposals/${btn.dataset.applyProposal}/apply`, {
        method: "POST",
        body: JSON.stringify({ approved: false }),
      });
      if (result.approval_required) {
        notify(`승인 필요: approval ${result.approval_id}`);
        await loadApprovals();
      } else {
        notify("스케줄 제안을 적용했습니다.");
        await Promise.all([loadCalendar(), loadBriefing()]);
      }
    });
  });
}

async function runNLI(event) {
  event.preventDefault();
  const text = $("nli-text").value.trim();
  if (!text) return;
  const result = await api("/nli/command", {
    method: "POST",
    body: JSON.stringify({ text }),
  });
  $("nli-result").textContent = JSON.stringify(result, null, 2);
  await Promise.all([loadTasks(), loadBriefing()]);
}

async function loadSyncStatus() {
  const status = await api("/graph/status");
  $("sync-status").textContent = JSON.stringify(status, null, 2);
  setGraphMessage(
    status.connected
      ? `연결됨: ${status.username || "Microsoft 계정"}`
      : status.configured
        ? "연결되지 않음"
        : `설정 누락: ${(status.missing_settings || []).join(", ")}`,
    !status.connected && !status.configured,
  );
}

async function pingSync() {
  const result = await api("/graph/ping", { method: "POST" });
  $("sync-status").textContent = JSON.stringify(result, null, 2);
}

async function connectGraph() {
  const result = await api("/graph/auth/url");
  if (!result.configured) {
    const missing = (result.missing_settings || []).join(", ");
    const msg = `Microsoft Graph 설정이 필요합니다: ${missing}`;
    setGraphMessage(msg, true);
    throw new Error(msg);
  }
  if (!result.auth_url) {
    const msg = "인증 URL을 생성하지 못했습니다.";
    setGraphMessage(msg, true);
    throw new Error(msg);
  }
  setGraphMessage("Microsoft 로그인 페이지로 이동합니다...");
  window.location.href = result.auth_url;
}

async function disconnectGraph() {
  await api("/graph/disconnect", { method: "POST" });
  setGraphMessage("Microsoft 연결이 해제되었습니다.");
  await loadSyncStatus();
}

async function importOutlookCalendar() {
  const result = await api("/graph/calendar/import", { method: "POST" });
  $("graph-import-result").textContent = `캘린더 반영: ${result.imported}/${result.events}`;
  setGraphMessage("Outlook 캘린더 동기화가 완료되었습니다.");
  await loadCalendar();
}

async function loadTodoLists() {
  const lists = await api("/graph/todo/lists");
  const select = $("todo-list-select");
  select.innerHTML = "";

  if (!lists.length) {
    select.innerHTML = `<option value="">목록 없음</option>`;
    return;
  }

  lists.forEach((list) => {
    const option = document.createElement("option");
    option.value = list.id;
    option.textContent = list.displayName || list.wellknownListName || list.id;
    select.appendChild(option);
  });
}

async function importTodoList() {
  const select = $("todo-list-select");
  const listId = select.value;
  if (!listId) {
    throw new Error("가져올 To Do 목록을 먼저 선택하세요.");
  }

  const result = await api(`/graph/todo/lists/${listId}/import`, { method: "POST" });
  $("graph-import-result").textContent = `To Do 반영: ${result.imported}/${result.tasks}`;
  setGraphMessage("Microsoft To Do 동기화가 완료되었습니다.");
  await loadTasks();
}

function setProposalDefaults() {
  const now = new Date();
  const start = new Date(now.getTime() + 30 * 60 * 1000);
  const end = new Date(now.getTime() + 48 * 60 * 60 * 1000);

  const toInput = (d) => {
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  $("proposal-from").value = toInput(start);
  $("proposal-to").value = toInput(end);
}

async function bootstrap() {
  try {
    setProposalDefaults();
    const params = new URLSearchParams(window.location.search);
    if (params.get("graph") === "connected") {
      notify("Microsoft Graph 연결이 완료되었습니다.");
      history.replaceState({}, "", "/");
    }
    if (params.get("graph_error")) {
      notify(`Graph 연결 실패: ${params.get("graph_error")}`, true);
      history.replaceState({}, "", "/");
    }

    $("save-profile").addEventListener("click", async () => {
      try {
        await saveProfile();
      } catch (error) {
        notify(error.message, true);
      }
    });

    $("refresh-briefing").addEventListener("click", () => loadBriefing());
    $("refresh-tasks").addEventListener("click", () => loadTasks());
    $("refresh-calendar").addEventListener("click", () => loadCalendar());
    $("refresh-approvals").addEventListener("click", () => loadApprovals());
    $("task-form").addEventListener("submit", createTask);
    $("meeting-form").addEventListener("submit", ingestMeeting);
    $("proposal-form").addEventListener("submit", createProposals);
    $("nli-form").addEventListener("submit", runNLI);
    $("ping-sync").addEventListener("click", async () => {
      try {
        await pingSync();
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
    $("import-calendar").addEventListener("click", async () => {
      try {
        await importOutlookCalendar();
      } catch (error) {
        notify(error.message, true);
      }
    });
    $("load-todo-lists").addEventListener("click", async () => {
      try {
        await loadTodoLists();
      } catch (error) {
        notify(error.message, true);
      }
    });
    $("import-todo").addEventListener("click", async () => {
      try {
        await importTodoList();
      } catch (error) {
        notify(error.message, true);
      }
    });

    await Promise.all([loadProfile(), loadBriefing(), loadTasks(), loadCalendar(), loadApprovals(), loadSyncStatus()]);
    await loadTodoLists().catch(() => {});
  } catch (error) {
    notify(error.message, true);
  }
}

bootstrap();
