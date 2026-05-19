import { UI_FLUSH_MS } from "./config.js";
import { fetchHealth, fetchInitialDashboardData } from "./api.js";
import { initCharts, updateCharts } from "./charts.js";
import { connectMarketSocket } from "./socket.js";
import { addAlertsPayload, setError, setHealth, setLoading, upsertMetricsPayload } from "./state.js";
import { initUi, renderUi } from "./ui.js";

let rafId = null;
let lastFlush = 0;

document.addEventListener("DOMContentLoaded", async () => {
  initUi({ onFiltersChanged: scheduleRender });
  initCharts();
  renderUi();

  await loadInitialData();
  connectMarketSocket({
    onChange: scheduleRender,
    onError: (error) => setError(error)
  });

  window.setInterval(refreshHealth, 10_000);
});

async function loadInitialData() {
  setLoading(true);
  scheduleRender(true);

  try {
    const results = await fetchInitialDashboardData();

    handleSettled(results.health, setHealth);
    handleSettled(results.historyMetrics, upsertMetricsPayload);
    handleSettled(results.latestMetrics, upsertMetricsPayload);
    handleSettled(results.latestAlerts, addAlertsPayload);

    const rejected = Object.values(results).find((result) => result.status === "rejected");
    setError(rejected?.reason || null);
  } catch (error) {
    setError(error);
  } finally {
    setLoading(false);
    scheduleRender(true);
  }
}

async function refreshHealth() {
  try {
    setHealth(await fetchHealth());
    setError(null);
  } catch (error) {
    setError(error);
  }

  scheduleRender();
}

function handleSettled(result, handler) {
  if (result.status === "fulfilled") {
    handler(result.value);
  }
}

function scheduleRender(force = false) {
  const now = performance.now();

  if (!force && now - lastFlush < UI_FLUSH_MS) {
    if (!rafId) {
      rafId = requestAnimationFrame(() => scheduleRender(true));
    }
    return;
  }

  if (rafId) {
    cancelAnimationFrame(rafId);
    rafId = null;
  }

  rafId = requestAnimationFrame(() => {
    lastFlush = performance.now();
    renderUi();
    updateCharts();
    rafId = null;
  });
}
