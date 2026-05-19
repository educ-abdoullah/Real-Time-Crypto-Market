import { MARKETS, SOURCES } from "./config.js";
import { filteredAlerts, getWindowMetrics, setFilters, state } from "./state.js";
import { compactDetails, formatCurrency, formatDateTime, formatFullDateTime, formatNumber, formatPercent, getAlertTimestamp } from "./formatters.js";

const elements = {};

export function initUi({ onFiltersChanged }) {
  cacheElements();
  initTheme();
  bindFilters(onFiltersChanged);
  renderSkeletonCards();
}

export function bindChartActions({ onResetZoom, onThemeChanged }) {
  document.querySelectorAll("[data-reset-chart]").forEach((button) => {
    button.addEventListener("click", () => onResetZoom?.(button.dataset.resetChart));
  });

  elements.themeToggle?.addEventListener("click", () => {
    const nextTheme = state.theme === "dark" ? "light" : "dark";
    applyTheme(nextTheme);
    onThemeChanged?.();
    renderTopbar();
  });
}

export function renderUi() {
  renderTopbar();
  renderStates();
  renderKpiCards();
  renderComparison();
  renderDetails();
  renderAlerts();
  renderSystemStatus();
  renderChartCaption();
}

function cacheElements() {
  [
    "socketStatus",
    "apiStatus",
    "lastUpdated",
    "themeToggle",
    "marketFilter",
    "sourceFilter",
    "windowFilter",
    "metricFilter",
    "chartModeFilter",
    "alertFilter",
    "loadingState",
    "errorState",
    "kpiGrid",
    "comparisonGrid",
    "comparisonMarketTag",
    "detailsGrid",
    "alertsList",
    "alertCount",
    "systemStatus",
    "mainChartTitle",
    "mainChartCaption"
  ].forEach((id) => {
    elements[id] = document.getElementById(id);
  });
}

function initTheme() {
  const stored = localStorage.getItem("crypto-dashboard-theme");
  applyTheme(stored === "dark" ? "dark" : "light");
}

function applyTheme(theme) {
  state.theme = theme;
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("crypto-dashboard-theme", theme);

  if (elements.themeToggle) {
    elements.themeToggle.textContent = theme === "dark" ? "Mode clair" : "Mode sombre";
    elements.themeToggle.setAttribute("aria-pressed", String(theme === "dark"));
  }
}

function bindFilters(onFiltersChanged) {
  const bindings = [
    ["marketFilter", "market"],
    ["sourceFilter", "source"],
    ["windowFilter", "window"],
    ["metricFilter", "metric"],
    ["chartModeFilter", "chartMode"],
    ["alertFilter", "alert"]
  ];

  bindings.forEach(([elementKey, filterKey]) => {
    if (!elements[elementKey]) return;

    elements[elementKey].value = state.filters[filterKey];
    elements[elementKey].addEventListener("change", (event) => {
      setFilters({ [filterKey]: event.target.value });
      onFiltersChanged?.();
    });
  });
}

function renderTopbar() {
  elements.socketStatus.className = `status-pill ${state.socketConnected ? "status-connected" : "status-disconnected"}`;
  elements.socketStatus.textContent = `Socket ${state.socketConnected ? "connected" : "disconnected"}`;

  const healthOk = state.health?.status === "ok";
  elements.apiStatus.className = `status-pill ${healthOk ? "status-connected" : state.error ? "status-error" : "status-loading"}`;
  elements.apiStatus.textContent = healthOk ? "API ok" : state.error ? "API error" : "API loading";
  elements.lastUpdated.textContent = `Last update: ${formatDateTime(state.stats.lastUpdatedAt)}`;
}

function renderStates() {
  elements.loadingState.classList.toggle("hidden", !state.isLoading);

  if (state.error) {
    elements.errorState.textContent = state.socketConnected ? state.error : `${state.error} - live connection may be degraded`;
    elements.errorState.classList.remove("hidden");
  } else {
    elements.errorState.classList.add("hidden");
  }
}

function renderSkeletonCards() {
  elements.kpiGrid.innerHTML = SOURCES.flatMap((source) => MARKETS.map((market) => renderKpiCard(source, market, null))).join("");
}

function renderKpiCards() {
  elements.kpiGrid.innerHTML = SOURCES.flatMap((source) => {
    return MARKETS.map((market) => renderKpiCard(source, market, state.metrics.get(`${source}:${market}`)));
  }).join("");
}

function renderKpiCard(source, market, metric) {
  const windowData = getWindowMetrics(metric);
  const change = Number(windowData?.price_change_pct);
  const changeClass = change >= 0 ? "change-positive" : "change-negative";
  const active = Boolean(windowData);

  return `
    <article class="kpi-card source-${source}">
      <div class="kpi-header">
        <div class="kpi-title">
          <div class="badge-row">
            <span class="badge badge-source ${source === "coinbase" ? "coinbase" : ""}">${sourceLabel(source)}</span>
            <span class="badge badge-market">${market}</span>
          </div>
        </div>
        <span class="kpi-status ${active ? "active" : ""}">${active ? "active" : "inactive"}</span>
      </div>
      <div class="price">${formatCurrency(windowData?.last_price)}</div>
      <div class="${Number.isFinite(change) ? changeClass : ""}">${formatPercent(change)}</div>
      <div class="metric-list">
        ${row("Volume", formatNumber(windowData?.volume, 6))}
        ${row("Trades / sec", formatNumber(windowData?.trades_per_second, 4))}
        ${row("Updated", formatDateTime(metric?.computed_ts || metric?.api_received_ts))}
      </div>
    </article>
  `;
}

function renderComparison() {
  const market = state.filters.market;
  const binance = getWindowMetrics(state.metrics.get(`binance:${market}`));
  const coinbase = getWindowMetrics(state.metrics.get(`coinbase:${market}`));

  if (elements.comparisonMarketTag) {
    elements.comparisonMarketTag.textContent = market;
  }

  if (!binance || !coinbase) {
    elements.comparisonGrid.innerHTML = `<div class="empty">waiting for both sources on ${market}</div>`;
    return;
  }

  const binancePrice = Number(binance.last_price);
  const coinbasePrice = Number(coinbase.last_price);
  const spread = Math.abs(binancePrice - coinbasePrice);
  const pct = spread / ((binancePrice + coinbasePrice) / 2) * 100;
  const spreadClass = pct >= 0.15 ? "spread-critical" : pct >= 0.05 ? "spread-warning" : "";
  const spreadLabel = pct >= 0.15 ? "High spread" : pct >= 0.05 ? "Medium spread" : "Low spread";
  const activeSource = Number(binance.trades_per_second || 0) >= Number(coinbase.trades_per_second || 0) ? "Binance" : "Coinbase";
  const bestPriceSource = binancePrice <= coinbasePrice ? "Binance" : "Coinbase";

  elements.comparisonGrid.innerHTML = `
    <div class="comparison-card">
      <h3>${market}</h3>
      ${comparisonRow("Binance", formatCurrency(binancePrice))}
      ${comparisonRow("Coinbase", formatCurrency(coinbasePrice))}
      ${comparisonRow("Absolute spread", formatCurrency(spread), spreadClass)}
      ${comparisonRow("Spread %", formatPercent(pct), spreadClass)}
      ${comparisonRow("Spread status", spreadLabel, pct >= 0.15 ? "spread-critical" : pct >= 0.05 ? "spread-warning" : "spread-low")}
      ${comparisonRow("Binance volume", formatNumber(binance.volume, 6))}
      ${comparisonRow("Coinbase volume", formatNumber(coinbase.volume, 6))}
      ${comparisonRow("Binance trades/sec", formatNumber(binance.trades_per_second, 4))}
      ${comparisonRow("Coinbase trades/sec", formatNumber(coinbase.trades_per_second, 4))}
      ${comparisonRow("Binance latency", `${formatNumber(binance.avg_latency_ms, 1)} ms`)}
      ${comparisonRow("Coinbase latency", `${formatNumber(coinbase.avg_latency_ms, 1)} ms`)}
      ${comparisonRow("Best displayed price", bestPriceSource)}
      ${comparisonRow("Most active source", activeSource)}
    </div>
  `;
}

function renderDetails() {
  const market = state.filters.market;
  const sources = state.filters.source === "compare" ? SOURCES : [state.filters.source];
  const signalHtml = renderTraderSignals(market);

  const sourceCards = sources.map((source) => {
    const metric = state.metrics.get(`${source}:${market}`);
    const data = getWindowMetrics(metric);

    if (!data) {
      return `<div class="detail-card"><h3>${sourceLabel(source)}</h3><div class="empty">No ${market} metrics yet.</div></div>`;
    }

    return `
      <div class="detail-card">
        <h3>${sourceLabel(source)} ${market}</h3>
        ${detailRow("VWAP", formatCurrency(data.vwap))}
        ${detailRow("Average price", formatCurrency(data.avg_price))}
        ${detailRow("High / Low", `${formatCurrency(data.high_price)} / ${formatCurrency(data.low_price)}`)}
        ${detailRow("Volatility", formatNumber(data.price_volatility, 6))}
        ${detailRow("Buy ratio", formatPercent(toPercent(data.buy_volume_ratio)))}
        ${detailRow("Sell ratio", formatPercent(toPercent(data.sell_volume_ratio)))}
        ${detailRow("Avg latency", `${formatNumber(data.avg_latency_ms, 1)} ms`)}
      </div>
    `;
  }).join("");

  elements.detailsGrid.innerHTML = `${signalHtml}${sourceCards}`;
}

function renderAlerts() {
  const alerts = filteredAlerts();
  elements.alertCount.textContent = `${alerts.length} shown`;

  if (!alerts.length) {
    elements.alertsList.innerHTML = `<div class="empty">No alerts for this filter.</div>`;
    return;
  }

  elements.alertsList.innerHTML = alerts.map((alert) => {
    const severity = String(alert.severity || "LOW").toLowerCase();
    const isDivergence = alert.type === "EXCHANGE_PRICE_DIVERGENCE";
    const typeClass = `alert-${String(alert.type || "").toLowerCase()}`;
    const details = compactDetails(alert.details);

    return `
      <article class="alert-card alert-${severity} ${typeClass} ${isDivergence ? "alert-divergence" : ""}">
        <div class="alert-meta">
          <span class="badge">${alert.severity || "LOW"}</span>
          <span class="badge">${alert.type || "ALERT"}</span>
          <span class="badge badge-source ${alert.source === "coinbase" ? "coinbase" : ""}">${alert.source || "--"}</span>
          <span class="badge badge-market">${alert.market || "--"}</span>
        </div>
        <p>${escapeHtml(alert.message || "Alert received")}</p>
        <div class="alert-details">${formatFullDateTime(getAlertTimestamp(alert))}${details ? ` | ${escapeHtml(details)}` : ""}</div>
      </article>
    `;
  }).join("");
}

function renderSystemStatus() {
  const health = state.health || {};
  const kafka = health.kafka || {};

  elements.systemStatus.innerHTML = [
    ["Socket.IO", state.socketConnected ? "connected" : "disconnected"],
    ["API status", health.status || "--"],
    ["Kafka metrics", kafka.metricsConsumer || kafka.metrics || "--"],
    ["Kafka alerts", kafka.alertsConsumer || kafka.alerts || "--"],
    ["MongoDB", health.mongo || "--"],
    ["Last metric", formatDateTime(state.stats.lastMetricAt)],
    ["Last alert", formatDateTime(state.stats.lastAlertAt)]
  ].map(([label, value]) => `<div><dt>${label}</dt><dd>${value}</dd></div>`).join("");
}

function renderChartCaption() {
  const sourceText = state.filters.source === "compare" ? "Binance vs Coinbase" : sourceLabel(state.filters.source);
  elements.mainChartTitle.textContent = `${state.filters.market} ${state.filters.metric}`;
  elements.mainChartCaption.textContent = `${sourceText} | ${state.filters.window} window | ${state.filters.chartMode} scale | live dataset capped for browser performance`;
}

function renderTraderSignals(market) {
  const rows = SOURCES.map((source) => getWindowMetrics(state.metrics.get(`${source}:${market}`))).filter(Boolean);
  if (!rows.length) return `<div class="empty">Waiting for ${market} trader signals.</div>`;

  const avgChange = average(rows.map((row) => Number(row.price_change_pct)));
  const totalVolume = sum(rows.map((row) => Number(row.volume)));
  const totalTps = sum(rows.map((row) => Number(row.trades_per_second)));
  const avgBuyRatio = average(rows.map((row) => toPercent(row.buy_volume_ratio)));
  const avgSellRatio = average(rows.map((row) => toPercent(row.sell_volume_ratio)));
  const avgVolatility = average(rows.map((row) => Number(row.price_volatility)));
  const avgLatency = average(rows.map((row) => Number(row.avg_latency_ms)));

  return `
    <div class="signal-grid">
      ${signalCard("Market Momentum", momentumLabel(avgChange), Math.abs(avgChange) >= 0.2 ? "high" : Math.abs(avgChange) >= 0.05 ? "medium" : "low", formatPercent(avgChange))}
      ${signalCard("Liquidity Pressure", liquidityLabel(totalVolume, totalTps), totalTps >= 8 ? "high" : totalTps >= 2 ? "medium" : "low", `${formatNumber(totalVolume, 4)} vol / ${formatNumber(totalTps, 2)} tps`)}
      ${signalCard("Buy/Sell Pressure", buySellLabel(avgBuyRatio, avgSellRatio), Math.abs(avgBuyRatio - avgSellRatio) >= 20 ? "high" : Math.abs(avgBuyRatio - avgSellRatio) >= 8 ? "medium" : "low", `${formatPercent(avgBuyRatio)} buy`)}
      ${signalCard("Volatility Level", volatilityLabel(avgVolatility), avgVolatility >= 0.002 ? "high" : avgVolatility >= 0.0005 ? "medium" : "low", formatNumber(avgVolatility, 6))}
      ${signalCard("Latency Status", latencyLabel(avgLatency), avgLatency >= 1000 ? "high" : avgLatency >= 350 ? "medium" : "low", `${formatNumber(avgLatency, 1)} ms`)}
    </div>
  `;
}

function signalCard(label, value, level, hint) {
  return `<div class="signal-card signal-${level}"><span>${label}</span><strong>${value}</strong><small>${hint}</small></div>`;
}

function momentumLabel(value) {
  if (value > 0.05) return "Bullish";
  if (value < -0.05) return "Bearish";
  return "Neutral";
}

function liquidityLabel(volume, tps) {
  if (tps >= 8 || volume >= 10) return "High";
  if (tps >= 2 || volume >= 2) return "Moderate";
  return "Calm";
}

function buySellLabel(buyRatio, sellRatio) {
  if (buyRatio - sellRatio > 8) return "Buy pressure";
  if (sellRatio - buyRatio > 8) return "Sell pressure";
  return "Balanced";
}

function volatilityLabel(value) {
  if (value >= 0.002) return "HIGH";
  if (value >= 0.0005) return "MEDIUM";
  return "LOW";
}

function latencyLabel(value) {
  if (value >= 1000) return "HIGH";
  if (value >= 350) return "WARNING";
  return "OK";
}

function toPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return NaN;
  return numeric <= 1 ? numeric * 100 : numeric;
}

function average(values) {
  const clean = values.filter(Number.isFinite);
  if (!clean.length) return NaN;
  return sum(clean) / clean.length;
}

function sum(values) {
  return values.filter(Number.isFinite).reduce((total, value) => total + value, 0);
}

function row(label, value) {
  return `<div class="kpi-row"><span>${label}</span><strong>${value}</strong></div>`;
}

function comparisonRow(label, value, className = "") {
  return `<div class="comparison-row"><span>${label}</span><strong class="${className}">${value}</strong></div>`;
}

function detailRow(label, value) {
  return `<div class="detail-row"><span>${label}</span><strong>${value}</strong></div>`;
}

function sourceLabel(source) {
  return source === "binance" ? "Binance" : source === "coinbase" ? "Coinbase" : source;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;"
  })[char]);
}
