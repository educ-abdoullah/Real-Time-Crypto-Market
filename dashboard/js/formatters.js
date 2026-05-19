export function formatCurrency(value) {
  if (!Number.isFinite(Number(value))) return "--";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: Number(value) >= 1000 ? 2 : 4
  }).format(Number(value));
}

export function formatNumber(value, digits = 2) {
  if (!Number.isFinite(Number(value))) return "--";

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits
  }).format(Number(value));
}

export function formatPercent(value) {
  if (!Number.isFinite(Number(value))) return "--";

  return `${Number(value).toFixed(2)}%`;
}

export function formatDateTime(value) {
  const ts = normalizeTimestamp(value);
  if (!ts) return "--";

  return new Intl.DateTimeFormat("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(ts));
}

export function formatFullDateTime(value) {
  const ts = normalizeTimestamp(value);
  if (!ts) return "--";

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(ts));
}

export function normalizeTimestamp(value) {
  if (!value) return null;
  if (value instanceof Date) return value.getTime();

  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric < 10_000_000_000 ? numeric * 1000 : numeric;
  }

  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

export function normalizeSource(value) {
  const source = String(value || "unknown").toLowerCase().trim();

  if (source.includes("coinbase")) return "coinbase";
  if (source.includes("binance")) return "binance";
  if (source.includes("multi")) return "multi_exchange";

  return source;
}

export function normalizeMarket(metricOrValue) {
  const raw = typeof metricOrValue === "string" ? metricOrValue : metricOrValue?.market || metricOrValue?.symbol || "unknown";
  const market = String(raw || "unknown").toUpperCase().replace("/", "-");

  if (market === "BTCUSDT" || market === "BTC-USD" || market === "BTCUSD") return "BTC-USD";
  if (market === "ETHUSDT" || market === "ETH-USD" || market === "ETHUSD") return "ETH-USD";
  return market;
}

export function buildMetricKey(metric) {
  return `${normalizeSource(metric?.source)}:${normalizeMarket(metric)}`;
}

export function getMetricTimestamp(metric) {
  return normalizeTimestamp(metric?.computed_ts || metric?.computed_time || metric?.api_received_ts || Date.now());
}

export function getAlertTimestamp(alert) {
  return normalizeTimestamp(alert?.created_ts || alert?.created_time || alert?.exchange_ts || alert?.api_received_ts || Date.now());
}

export function labelForSeries(metric) {
  return `${normalizeSource(metric?.source).toUpperCase()} ${normalizeMarket(metric)}`;
}

export function compactDetails(details) {
  if (!details || typeof details !== "object") return "";

  return Object.entries(details)
    .slice(0, 4)
    .map(([key, value]) => `${key}: ${typeof value === "number" ? formatNumber(value, 4) : String(value)}`)
    .join(" | ");
}
