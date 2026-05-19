export const API_BASE_URL = "";

export const API_ENDPOINTS = {
  health: "/api/health",
  latestMetrics: "/api/metrics/latest",
  latestAlerts: "/api/alerts/latest?limit=60",
  historyMetrics: "/api/history/metrics?limit=500"
};

export const MARKETS = ["BTC-USD", "ETH-USD"];
export const SOURCES = ["binance", "coinbase"];
export const WINDOWS = ["60s", "180s", "240s", "300s", "900s"];

export const DEFAULT_FILTERS = {
  market: "BTC-USD",
  source: "compare",
  window: "60s",
  metric: "last_price",
  chartMode: "absolute",
  alert: "all"
};

export const CHART_POINT_LIMIT = 160;
export const ALERT_LIMIT = 80;
export const UI_FLUSH_MS = 180;

export const SOURCE_COLORS = {
  binance: "#2563eb",
  coinbase: "#16a34a",
  multi_exchange: "#dc2626",
  unknown: "#64748b"
};

export const MARKET_COLORS = {
  "BTC-USD": "#0f766e",
  "ETH-USD": "#7c3aed",
  unknown: "#64748b"
};

export const METRIC_UNITS = {
  last_price: "$",
  avg_price: "$",
  vwap: "$",
  volume: "volume",
  price_change_pct: "%",
  trades_per_second: "trades/s",
  price_volatility: "volatility",
  buy_volume_ratio: "%",
  sell_volume_ratio: "%",
  avg_latency_ms: "ms"
};
