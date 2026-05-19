import { API_BASE_URL, API_ENDPOINTS } from "./config.js";

async function requestJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { Accept: "application/json" }
  });

  if (!response.ok) {
    throw new Error(`${path} returned HTTP ${response.status}`);
  }

  return response.json();
}

export function fetchHealth() {
  return requestJson(API_ENDPOINTS.health);
}

export function fetchLatestMetrics() {
  return requestJson(API_ENDPOINTS.latestMetrics);
}

export function fetchLatestAlerts() {
  return requestJson(API_ENDPOINTS.latestAlerts);
}

export function fetchMetricsHistory() {
  return requestJson(API_ENDPOINTS.historyMetrics);
}

export async function fetchInitialDashboardData() {
  const [health, latestMetrics, latestAlerts, historyMetrics] = await Promise.allSettled([
    fetchHealth(),
    fetchLatestMetrics(),
    fetchLatestAlerts(),
    fetchMetricsHistory()
  ]);

  return {
    health,
    latestMetrics,
    latestAlerts,
    historyMetrics
  };
}
