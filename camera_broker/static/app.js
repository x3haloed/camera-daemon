const state = {
  config: null,
  cameras: [],
  selectedCameraId: null,
  socket: null,
  effective: null,
  frameCount: 0,
  logs: [],
};

const el = {
  brokerSummary: document.querySelector("#brokerSummary"),
  connectionPill: document.querySelector("#connectionPill"),
  activePill: document.querySelector("#activePill"),
  cameraList: document.querySelector("#cameraList"),
  refreshButton: document.querySelector("#refreshButton"),
  selectedCameraName: document.querySelector("#selectedCameraName"),
  selectedCameraMeta: document.querySelector("#selectedCameraMeta"),
  connectButton: document.querySelector("#connectButton"),
  disconnectButton: document.querySelector("#disconnectButton"),
  mediaStage: document.querySelector("#mediaStage"),
  stillFrame: document.querySelector("#stillFrame"),
  videoFrame: document.querySelector("#videoFrame"),
  frameCount: document.querySelector("#frameCount"),
  lastChunk: document.querySelector("#lastChunk"),
  payloadSize: document.querySelector("#payloadSize"),
  motionScore: document.querySelector("#motionScore"),
  modeInput: document.querySelector("#modeInput"),
  fpsInput: document.querySelector("#fpsInput"),
  fpsValue: document.querySelector("#fpsValue"),
  widthInput: document.querySelector("#widthInput"),
  heightInput: document.querySelector("#heightInput"),
  motionGateInput: document.querySelector("#motionGateInput"),
  thresholdInput: document.querySelector("#thresholdInput"),
  cooldownInput: document.querySelector("#cooldownInput"),
  clipInput: document.querySelector("#clipInput"),
  durationInput: document.querySelector("#durationInput"),
  effectiveOutput: document.querySelector("#effectiveOutput"),
  logList: document.querySelector("#logList"),
  logLevelFilter: document.querySelector("#logLevelFilter"),
  clearLogButton: document.querySelector("#clearLogButton"),
};

async function init() {
  wireEvents();
  await loadConfig();
  await refreshStatus();
  setInterval(refreshStatus, 2000);
}

function wireEvents() {
  el.refreshButton.addEventListener("click", refreshStatus);
  el.connectButton.addEventListener("click", connect);
  el.disconnectButton.addEventListener("click", disconnect);
  el.clearLogButton.addEventListener("click", () => {
    state.logs = state.logs.filter((entry) => entry.source === "broker");
    renderLogs();
  });
  el.logLevelFilter.addEventListener("change", renderLogs);
  el.fpsInput.addEventListener("input", () => {
    el.fpsValue.textContent = el.fpsInput.value;
  });
}

async function loadConfig() {
  const response = await fetch("/config");
  const data = await response.json();
  state.config = data.config;
  const defaults = state.config.defaults;
  const limits = state.config.limits;
  el.fpsInput.max = limits.maxFps;
  el.fpsInput.value = defaults.fps;
  el.fpsValue.textContent = defaults.fps;
  el.widthInput.value = defaults.resolution.width;
  el.heightInput.value = defaults.resolution.height;
  el.motionGateInput.checked = defaults.motionGate;
  el.thresholdInput.value = defaults.motionThreshold;
  el.cooldownInput.value = defaults.cooldownSeconds;
  el.clipInput.value = defaults.clipSeconds;
  appendLog("client", "Config loaded", `${state.config.cameras.length} camera definitions`);
}

async function refreshStatus() {
  const [health, cameras, subscriptions] = await Promise.all([
    fetchJson("/health"),
    fetchJson("/cameras"),
    fetchJson("/subscriptions"),
  ]);
  state.cameras = cameras.cameras;
  renderSummary(health, subscriptions);
  renderCameras();
  mergeBrokerLogs(health.health || []);
}

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json();
}

function renderSummary(health, subscriptions) {
  el.brokerSummary.textContent = `${state.cameras.length} configured cameras · ${subscriptions.subscriptions.length} active subscriptions`;
  el.activePill.textContent = `${health.activeCameras} active · ${health.activeSubscriptions} subs`;
  el.activePill.className = health.activeCameras > 0 ? "pill good" : "pill";
}

function renderCameras() {
  if (!state.selectedCameraId && state.cameras.length) {
    state.selectedCameraId = state.cameras[0].id;
  }
  el.cameraList.replaceChildren(...state.cameras.map(cameraButton));
  renderSelectedCamera();
}

function cameraButton(camera) {
  const button = document.createElement("button");
  button.className = `camera-item${camera.id === state.selectedCameraId ? " selected" : ""}`;
  button.type = "button";
  button.innerHTML = `
    <strong>${escapeHtml(camera.nickname || camera.id)}</strong>
    <span>${escapeHtml(camera.id)}</span>
    <div class="camera-badges">
      <span class="badge ${camera.enabled ? "good" : "warn"}">${camera.enabled ? "enabled" : "disabled"}</span>
      <span class="badge ${camera.active ? "good" : ""}">${camera.active ? "capturing" : "idle"}</span>
      <span class="badge ${camera.connected ? "good" : "warn"}">${camera.connected ? "connected" : "offline"}</span>
    </div>
  `;
  button.addEventListener("click", () => {
    state.selectedCameraId = camera.id;
    renderCameras();
  });
  return button;
}

function renderSelectedCamera() {
  const camera = selectedCamera();
  if (!camera) {
    el.selectedCameraName.textContent = "No camera selected";
    el.selectedCameraMeta.textContent = "Select a camera to subscribe.";
    el.connectButton.disabled = true;
    return;
  }
  el.selectedCameraName.textContent = camera.nickname || camera.id;
  el.selectedCameraMeta.textContent = `${camera.id} · ${camera.activeSubscriptions} subscriptions · ${camera.bufferFrames || 0} buffered frames`;
  el.connectButton.disabled = !camera.enabled || Boolean(state.socket);
}

function selectedCamera() {
  return state.cameras.find((camera) => camera.id === state.selectedCameraId);
}

function connect() {
  const camera = selectedCamera();
  if (!camera || state.socket) {
    return;
  }
  const url = websocketUrl();
  const socket = new WebSocket(url);
  state.socket = socket;
  setConnection("Connecting", "");
  resetMedia();

  socket.addEventListener("open", () => {
    socket.send(JSON.stringify(subscriptionPayload(camera.id)));
    appendLog("client", "Subscription requested", `${camera.id} via ${url}`);
  });
  socket.addEventListener("message", (event) => handleSocketMessage(JSON.parse(event.data)));
  socket.addEventListener("close", () => {
    appendLog("client", "Socket closed", camera.id);
    state.socket = null;
    state.effective = null;
    setConnection("Idle", "");
    el.connectButton.disabled = !selectedCamera()?.enabled;
    el.disconnectButton.disabled = true;
  });
  socket.addEventListener("error", () => {
    appendLog("error", "Socket error", camera.id);
  });
  el.connectButton.disabled = true;
  el.disconnectButton.disabled = false;
}

function disconnect() {
  if (state.socket) {
    state.socket.close();
  }
}

function subscriptionPayload(cameraId) {
  const duration = Number(el.durationInput.value);
  const payload = {
    type: "subscribe",
    cameraId,
    mode: el.modeInput.value,
    fps: Number(el.fpsInput.value),
    resolution: {
      width: Number(el.widthInput.value),
      height: Number(el.heightInput.value),
    },
    motionGate: el.motionGateInput.checked,
    motionThreshold: Number(el.thresholdInput.value),
    cooldownSeconds: Number(el.cooldownInput.value),
    clipSeconds: Number(el.clipInput.value),
  };
  if (duration > 0) {
    payload.durationSeconds = duration;
  }
  return payload;
}

function websocketUrl() {
  const wsPort = state.config.server.wsPort;
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${location.hostname}:${wsPort}/`;
}

function handleSocketMessage(message) {
  if (message.type === "ack") {
    state.effective = message.effective;
    el.effectiveOutput.textContent = JSON.stringify(message.effective, null, 2);
    setConnection("Subscribed", "good");
    appendLog("client", "Subscription accepted", `${message.effective.cameraId} · ${message.effective.mode} · ${message.effective.fps} fps`);
    return;
  }
  if (message.type === "error") {
    appendLog("error", "Subscription error", message.message);
    setConnection("Error", "bad");
    return;
  }
  if (message.type === "complete") {
    appendLog("client", "Subscription complete", message.subscriptionId);
    disconnect();
    return;
  }
  if (message.type === "chunk") {
    renderChunk(message);
  }
}

function renderChunk(message) {
  state.frameCount += 1;
  const blob = base64ToBlob(message.dataBase64, message.mediaType);
  const url = URL.createObjectURL(blob);

  document.querySelector(".empty-state").hidden = true;
  if (message.mediaType.startsWith("image/")) {
    if (el.stillFrame.src) {
      URL.revokeObjectURL(el.stillFrame.src);
    }
    el.videoFrame.hidden = true;
    el.stillFrame.hidden = false;
    el.stillFrame.src = url;
  } else {
    if (el.videoFrame.src) {
      URL.revokeObjectURL(el.videoFrame.src);
    }
    el.stillFrame.hidden = true;
    el.videoFrame.hidden = false;
    el.videoFrame.src = url;
    el.videoFrame.play().catch(() => {});
  }

  el.frameCount.textContent = String(state.frameCount);
  el.lastChunk.textContent = message.capturedAt || "-";
  el.payloadSize.textContent = formatBytes(message.sizeBytes);
  el.motionScore.textContent = String(message.metadata?.motionScore ?? "-");
}

function resetMedia() {
  state.frameCount = 0;
  el.frameCount.textContent = "0";
  el.lastChunk.textContent = "-";
  el.payloadSize.textContent = "-";
  el.motionScore.textContent = "-";
  el.effectiveOutput.textContent = "{}";
  document.querySelector(".empty-state").hidden = false;
  el.stillFrame.hidden = true;
  el.videoFrame.hidden = true;
}

function base64ToBlob(value, mediaType) {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new Blob([bytes], { type: mediaType });
}

function setConnection(label, tone) {
  el.connectionPill.textContent = label;
  el.connectionPill.className = `pill ${tone}`.trim();
}

function mergeBrokerLogs(events) {
  const existing = new Set(state.logs.filter((entry) => entry.source === "broker").map((entry) => entry.key));
  for (const event of events) {
    const key = `${event.timestamp}-${event.cameraId}-${event.message}`;
    if (existing.has(key)) {
      continue;
    }
    state.logs.push({
      key,
      source: "broker",
      level: event.level,
      cameraId: event.cameraId || "daemon",
      message: event.message,
      details: JSON.stringify(event.details || {}),
      timestamp: event.timestamp,
    });
  }
  state.logs.sort((a, b) => b.timestamp - a.timestamp);
  state.logs = state.logs.slice(0, 200);
  renderLogs();
}

function appendLog(level, message, details) {
  state.logs.unshift({
    key: `${Date.now()}-${Math.random()}`,
    source: "client",
    level,
    cameraId: "ui",
    message,
    details,
    timestamp: Date.now() / 1000,
  });
  state.logs = state.logs.slice(0, 200);
  renderLogs();
}

function renderLogs() {
  const filter = el.logLevelFilter.value;
  const entries = state.logs.filter((entry) => filter === "all" || entry.level === filter || entry.source === filter);
  el.logList.replaceChildren(...entries.map(logRow));
}

function logRow(entry) {
  const row = document.createElement("div");
  row.className = `log-row ${entry.level}`;
  row.innerHTML = `
    <time>${new Date(entry.timestamp * 1000).toLocaleTimeString()}</time>
    <strong>${escapeHtml(entry.level)}</strong>
    <span>${escapeHtml(entry.cameraId)} · ${escapeHtml(entry.message)} ${entry.details ? "· " + escapeHtml(entry.details) : ""}</span>
  `;
  return row;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) {
    return "-";
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

init().catch((error) => {
  appendLog("error", "Dashboard failed to initialize", error.message);
});
