import { ALERT_LIMIT, CHART_POINT_LIMIT, DEFAULT_FILTERS } from "./config.js";
import { buildMetricKey, getAlertTimestamp, getMetricTimestamp, normalizeMarket, normalizeSource } from "./formatters.js";

export const state = {
  filters: { ...DEFAULT_FILTERS },
  metrics: new Map(),
  alerts: [],
  chartSeries: new Map(),
  health: null,
  socketConnected: false,
  isLoading: true,
  error: null,
  stats: {
    lastMetricAt: null,
    lastAlertAt: null,
    lastUpdatedAt: null
  }
};

export function setFilters(nextFilters) {
  state.filters = { ...state.filters, ...nextFilters };
}

export function setHealth(health) {
  state.health = health;
  state.stats.lastUpdatedAt = Date.now();
}

export function setSocketConnected(isConnected) {
  state.socketConnected = isConnected;
  state.stats.lastUpdatedAt = Date.now();
}

export function setLoading(isLoading) {
  state.isLoading = isLoading;
}

export function setError(error) {
  state.error = error ? String(error.message || error) : null;
}

export function upsertMetric(metric, explicitKey) {
  if (!metric) return null;

  const normalized = {
    ...metric,
    source: normalizeSource(metric.source),
    market: normalizeMarket(metric)
  };
  const key = buildMetricKey(normalized);
  const timestamp = getMetricTimestamp(normalized);

  state.metrics.set(key, normalized);
  state.stats.lastMetricAt = timestamp;
  state.stats.lastUpdatedAt = Date.now();

  appendMetricToSeries(key, normalized, timestamp);
  return key;
}

export function upsertMetricsPayload(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload) ? payload : [];

  rows.forEach((row) => {
    if (row?.metric) {
      upsertMetric(row.metric, row.key);
    } else {
      upsertMetric(row);
    }
  });
}

export function addAlert(alert) {
  if (!alert) return;

  const normalized = {
    ...alert,
    source: normalizeSource(alert.source),
    market: normalizeMarket(alert)
  };

  state.alerts.unshift(normalized);
  state.alerts = dedupeAlerts(state.alerts).slice(0, ALERT_LIMIT);
  state.stats.lastAlertAt = getAlertTimestamp(normalized);
  state.stats.lastUpdatedAt = Date.now();
}

export function addAlertsPayload(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload) ? payload : [];
  rows.slice().reverse().forEach(addAlert);
}

export function resetChartSeries() {
  state.chartSeries.clear();
}

export function filteredMetrics() {
  return Array.from(state.metrics.entries()).filter(([, metric]) => {
    const marketOk = metric.market === state.filters.market;
    const sourceOk = state.filters.source === "compare" || metric.source === state.filters.source;
    return marketOk && sourceOk;
  });
}

export function filteredAlerts() {
  return state.alerts.filter((alert) => {
    if (state.filters.alert === "all") return true;
    if (state.filters.alert === "EXCHANGE_PRICE_DIVERGENCE") return alert.type === "EXCHANGE_PRICE_DIVERGENCE";
    return alert.severity === state.filters.alert;
  });
}

export function getWindowMetrics(metric, windowKey = state.filters.window) {
  return metric?.windows?.[windowKey] || metric?.windows?.["60s"] || null;
}

function appendMetricToSeries(key, metric, timestamp) {
  const windowEntries = Object.entries(metric.windows || {});

  windowEntries.forEach(([windowKey, values]) => {
    if (!values) return;

    const seriesKey = `${key}:${windowKey}`;
    const points = state.chartSeries.get(seriesKey) || [];
    points.push({
      ts: timestamp,
      source: metric.source,
      market: metric.market,
      values
    });

    if (points.length > CHART_POINT_LIMIT) {
      points.splice(0, points.length - CHART_POINT_LIMIT);
    }

    state.chartSeries.set(seriesKey, points);
  });
}

function dedupeAlerts(alerts) {
  const seen = new Set();

  return alerts.filter((alert) => {
    const key = `${alert.type}:${alert.source}:${alert.market}:${alert.trade_id || ""}:${alert.created_ts || alert.created_time || alert.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
