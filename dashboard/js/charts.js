import { CHART_POINT_LIMIT, METRIC_UNITS, SOURCE_COLORS } from "./config.js";
import { getWindowMetrics, state } from "./state.js";
import { formatCurrency, formatDateTime, formatNumber, formatPercent } from "./formatters.js";

const charts = {};
let zoomInteractionActive = false;

const baseScales = {
  x: {
    type: "linear",
    ticks: {
      color: "#64748b",
      maxRotation: 0,
      autoSkip: true,
      maxTicksLimit: 8,
      callback: (value) => formatDateTime(value)
    },
    grid: { color: "#eef2f7" },
    border: { color: "#d8e2ec" }
  },
  y: {
    ticks: { color: "#64748b" },
    grid: { color: "#eef2f7" },
    border: { color: "#d8e2ec" }
  }
};

export function initCharts() {
  charts.main = createChart("mainMetricChart", "line");
  charts.volume = createChart("volumeChart", "bar");
  charts.spread = createChart("spreadChart", "line", true);
  charts.activity = createChart("activityChart", "line");
  charts.volatility = createChart("volatilityChart", "line");
}

export function updateCharts() {
  if (!charts.main) return;

  updateMainMetricChart();
  updateVolumeChart();
  updateSpreadChart();
  updateActivityChart();
  updateVolatilityChart();
}

export function resetChartZoom(chartId) {
  if (chartId === "all") {
    Object.values(charts).forEach((chart) => chart?.resetZoom?.());
    zoomInteractionActive = false;
    return;
  }

  charts[chartId]?.resetZoom?.();
  zoomInteractionActive = false;
}

export function updateChartsTheme() {
  Object.values(charts).forEach((chart) => {
    if (!chart) return;
    applyThemeToChart(chart);
    chart.update("none");
  });
}

function createChart(canvasId, type, dualAxis = false) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;

  const options = createBaseOptions();
  if (dualAxis) {
    options.scales.y1 = {
      position: "right",
      ticks: { color: "#64748b" },
      grid: { drawOnChartArea: false },
      border: { color: "#d8e2ec" }
    };
  }

  return new Chart(canvas, {
    type,
    data: { datasets: [] },
    options: {
      ...options,
      elements: {
        point: { radius: 0, hoverRadius: 4 },
        line: { borderWidth: 2.3, tension: 0.22 }
      }
    }
  });
}

function createBaseOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    parsing: false,
    normalized: true,
    interaction: {
      mode: "nearest",
      intersect: false
    },
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          color: "#475569",
          boxWidth: 12,
          boxHeight: 12,
          usePointStyle: true,
          padding: 16
        }
      },
      tooltip: {
        backgroundColor: "#172033",
        titleColor: "#ffffff",
        bodyColor: "#ffffff",
        callbacks: {
          title: (items) => items.length ? formatDateTime(items[0].parsed.x) : "",
          label: (context) => `${context.dataset.label}: ${formatByMetric(context.parsed.y, context.dataset.metricName)}`
        }
      },
      zoom: {
        pan: {
          enabled: true,
          mode: "x",
          modifierKey: "shift",
          onPanStart: () => {
            zoomInteractionActive = true;
          }
        },
        zoom: {
          wheel: {
            enabled: true,
            modifierKey: "ctrl"
          },
          pinch: {
            enabled: true
          },
          drag: {
            enabled: true,
            backgroundColor: "rgba(37, 99, 235, 0.12)",
            borderColor: "rgba(37, 99, 235, 0.45)",
            borderWidth: 1
          },
          mode: "x",
          onZoomStart: () => {
            zoomInteractionActive = true;
          }
        },
        limits: {
          x: { min: "original", max: "original" }
        }
      }
    },
    scales: {
      x: { ...baseScales.x, ticks: { ...baseScales.x.ticks }, grid: { ...baseScales.x.grid }, border: { ...baseScales.x.border } },
      y: { ...baseScales.y, ticks: { ...baseScales.y.ticks }, grid: { ...baseScales.y.grid }, border: { ...baseScales.y.border } }
    }
  };
}

function updateMainMetricChart() {
  const metricName = state.filters.metric;
  const datasets = activeSources().map((source) => buildMetricDataset(source, metricName, state.filters.chartMode)).filter(Boolean);
  applyDatasets(charts.main, datasets, displayMetricName(metricName), { smartScale: true });
}

function updateVolumeChart() {
  const datasets = activeSources().map((source) => buildMetricDataset(source, "volume", "absolute", { bar: true })).filter(Boolean);
  applyDatasets(charts.volume, datasets, "volume", { beginAtZero: true });
}

function updateActivityChart() {
  const datasets = activeSources().map((source) => buildMetricDataset(source, "trades_per_second", "absolute", { fill: true })).filter(Boolean);
  applyDatasets(charts.activity, datasets, "trades_per_second", { beginAtZero: true });
}

function updateVolatilityChart() {
  const datasets = activeSources().map((source) => buildMetricDataset(source, "price_volatility", "absolute", { fill: true })).filter(Boolean);
  applyDatasets(charts.volatility, datasets, "price_volatility", { beginAtZero: true });
}

function updateSpreadChart() {
  const datasets = buildSpreadDatasets(state.filters.market);
  applyDatasets(charts.spread, datasets, "last_price", { smartScale: false, dualAxis: true });
}

function buildMetricDataset(source, metricName, mode, options = {}) {
  const market = state.filters.market;
  const key = `${source}:${market}`;
  const metric = state.metrics.get(key);
  if (!metric) return null;

  const raw = getSeriesPoints(key, metric)
    .map((point) => ({ ts: point.ts, value: normalizeMetricValue(metricName, point.values?.[metricName]) }))
    .filter((point) => Number.isFinite(point.value))
    .slice(-CHART_POINT_LIMIT);

  if (!raw.length) return null;

  const transformed = transformValues(raw, mode);
  const color = sourceColor(source);

  return {
    type: options.bar ? "bar" : "line",
    label: `${sourceLabel(source)} ${market}`,
    metricName: displayMetricName(metricName, mode),
    data: transformed.map((point) => ({ x: point.ts, y: point.value })),
    borderColor: color,
    backgroundColor: sourceColor(source, options.bar ? 0.58 : options.fill ? 0.14 : 0.08),
    fill: Boolean(options.fill),
    borderWidth: options.bar ? 0 : source === "coinbase" ? 2.8 : 2.4,
    borderDash: source === "coinbase" && !options.bar ? [7, 4] : [],
    pointRadius: options.bar ? 0 : source === "coinbase" ? 2 : 1.5,
    pointHoverRadius: 5,
    order: source === "coinbase" ? 1 : 2,
    spanGaps: true,
    barThickness: options.bar ? 10 : undefined,
    maxBarThickness: options.bar ? 14 : undefined,
    barPercentage: 0.72,
    categoryPercentage: 0.72
  };
}

function buildSpreadDatasets(market) {
  const binance = state.metrics.get(`binance:${market}`);
  const coinbase = state.metrics.get(`coinbase:${market}`);
  if (!binance || !coinbase) return [];

  const bPoints = getSeriesPoints(`binance:${market}`, binance);
  const cPoints = getSeriesPoints(`coinbase:${market}`, coinbase);
  const pairs = alignSeriesByNearestTimestamp(bPoints, cPoints).slice(-CHART_POINT_LIMIT);
  const absData = [];
  const pctData = [];

  pairs.forEach(({ b, c }) => {
    const bPrice = Number(b?.values?.last_price);
    const cPrice = Number(c?.values?.last_price);

    if (Number.isFinite(bPrice) && Number.isFinite(cPrice)) {
      const ts = Math.max(b.ts, c.ts);
      const spread = Math.abs(bPrice - cPrice);
      const pct = spread / ((bPrice + cPrice) / 2) * 100;
      absData.push({ x: ts, y: spread });
      pctData.push({ x: ts, y: pct });
    }
  });

  return [
    {
      label: "Absolute spread ($)",
      metricName: "last_price",
      data: absData,
      borderColor: "#dc2626",
      backgroundColor: "rgba(220, 38, 38, 0.12)",
      fill: true,
      yAxisID: "y"
    },
    {
      label: "Spread (%)",
      metricName: "price_change_pct",
      data: pctData,
      borderColor: "#d97706",
      backgroundColor: "rgba(217, 119, 6, 0.08)",
      borderDash: [5, 4],
      fill: false,
      yAxisID: "y1"
    }
  ];
}

function alignSeriesByNearestTimestamp(binancePoints, coinbasePoints) {
  if (!binancePoints.length || !coinbasePoints.length) return [];

  const sortedCoinbase = [...coinbasePoints].sort((a, b) => a.ts - b.ts);
  return [...binancePoints]
    .sort((a, b) => a.ts - b.ts)
    .map((bPoint) => {
      const cPoint = nearestPoint(sortedCoinbase, bPoint.ts);
      return cPoint ? { b: bPoint, c: cPoint } : null;
    })
    .filter(Boolean);
}

function nearestPoint(points, timestamp) {
  let best = null;
  let bestDelta = Infinity;

  for (const point of points) {
    const delta = Math.abs(point.ts - timestamp);
    if (delta < bestDelta) {
      best = point;
      bestDelta = delta;
    }

    if (point.ts > timestamp && delta > bestDelta) {
      break;
    }
  }

  return bestDelta <= 120_000 ? best : null;
}

function getSeriesPoints(key, metric) {
  const seriesKey = `${key}:${state.filters.window}`;
  const existing = state.chartSeries.get(seriesKey);

  if (existing?.length) return existing;

  const values = getWindowMetrics(metric);
  return values ? [{ ts: Date.now(), source: metric.source, market: metric.market, values }] : [];
}

function transformValues(points, mode) {
  if (mode === "absolute") return points;

  const first = points.find((point) => Number.isFinite(point.value) && point.value !== 0)?.value;
  if (!Number.isFinite(first) || first === 0) return points;

  return points.map((point) => ({
    ts: point.ts,
    value: mode === "indexed" ? point.value / first * 100 : (point.value - first) / first * 100
  }));
}

function normalizeMetricValue(metricName, value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return NaN;

  if ((metricName === "buy_volume_ratio" || metricName === "sell_volume_ratio") && numeric <= 1) {
    return numeric * 100;
  }

  return numeric;
}

function activeSources() {
  if (state.filters.source === "compare") return ["binance", "coinbase"];
  return [state.filters.source];
}

function applyDatasets(chart, datasets, metricName, options = {}) {
  if (!chart) return;

  const xRange = captureXScale(chart);
  chart.data.datasets = datasets;
  applyThemeToChart(chart);
  chart.options.scales.y.ticks.callback = (value) => formatAxisValue(value, metricName);
  chart.options.scales.y.beginAtZero = Boolean(options.beginAtZero);

  if (options.dualAxis && chart.options.scales.y1) {
    chart.options.scales.y1.ticks.callback = (value) => formatPercent(value);
  }

  const primaryDatasets = datasets.filter((dataset) => dataset.yAxisID !== "y1");
  const bounds = options.smartScale ? smartBounds(primaryDatasets, metricName) : defaultBounds(primaryDatasets, metricName, options.beginAtZero);
  chart.options.scales.y.min = bounds.min;
  chart.options.scales.y.max = bounds.max;

  if (options.dualAxis && chart.options.scales.y1) {
    const secondaryBounds = smartBounds(datasets.filter((dataset) => dataset.yAxisID === "y1"), "price_change_pct");
    chart.options.scales.y1.min = secondaryBounds.min;
    chart.options.scales.y1.max = secondaryBounds.max;
  }

  chart.update("none");
  if (zoomInteractionActive && xRange) {
    restoreXScale(chart, xRange);
  }
}

function captureXScale(chart) {
  const scale = chart.scales?.x;
  if (!scale || !Number.isFinite(scale.min) || !Number.isFinite(scale.max)) return null;
  return { min: scale.min, max: scale.max };
}

function restoreXScale(chart, range) {
  chart.options.scales.x.min = range.min;
  chart.options.scales.x.max = range.max;
  chart.update("none");
}

function applyThemeToChart(chart) {
  const styles = getComputedStyle(document.documentElement);
  const text = styles.getPropertyValue("--muted").trim() || "#64748b";
  const grid = styles.getPropertyValue("--line-soft").trim() || "#eef2f7";
  const border = styles.getPropertyValue("--line").trim() || "#d8e2ec";
  const isDark = document.documentElement.dataset.theme === "dark";
  const tooltipBg = isDark ? styles.getPropertyValue("--panel-soft").trim() || "#243244" : "#172033";
  const tooltipText = isDark ? styles.getPropertyValue("--text").trim() || "#edf2f7" : "#ffffff";

  chart.options.plugins.legend.labels.color = text;
  chart.options.plugins.tooltip.backgroundColor = tooltipBg;
  chart.options.plugins.tooltip.titleColor = tooltipText;
  chart.options.plugins.tooltip.bodyColor = tooltipText;
  chart.options.scales.x.ticks.color = text;
  chart.options.scales.x.grid.color = grid;
  chart.options.scales.x.border.color = border;
  chart.options.scales.y.ticks.color = text;
  chart.options.scales.y.grid.color = grid;
  chart.options.scales.y.border.color = border;

  if (chart.options.scales.y1) {
    chart.options.scales.y1.ticks.color = text;
    chart.options.scales.y1.border.color = border;
  }
}

function smartBounds(datasets, metricName) {
  const values = datasets.flatMap((dataset) => dataset.data.map((point) => Number(point.y))).filter(Number.isFinite);
  if (!values.length) return { min: undefined, max: undefined };

  let min = Math.min(...values);
  let max = Math.max(...values);

  if (metricName === "price_change_pct" || metricName === "relative") {
    const bound = Math.max(Math.abs(min), Math.abs(max), 0.01);
    return { min: -bound * 1.12, max: bound * 1.12 };
  }

  if (min === max) {
    const pad = Math.max(Math.abs(min) * 0.0005, 0.01);
    return { min: min - pad, max: max + pad };
  }

  const range = max - min;
  const ratio = METRIC_UNITS[metricName] === "$" ? 0.0008 : 0.08;
  const pad = Math.max(range * 0.12, Math.abs((min + max) / 2) * ratio, 0.000001);
  return { min: min - pad, max: max + pad };
}

function defaultBounds(datasets, metricName, beginAtZero) {
  if (beginAtZero) return { min: 0, max: undefined };
  return smartBounds(datasets, metricName);
}

function displayMetricName(metricName, mode = state.filters.chartMode) {
  if (mode === "relative") return "relative";
  if (mode === "indexed") return "indexed";
  return metricName;
}

function formatAxisValue(value, metricName) {
  if (metricName === "relative") return formatPercent(value);
  if (metricName === "indexed") return formatNumber(value, 2);
  if (METRIC_UNITS[metricName] === "$") return formatCurrency(value);
  if (METRIC_UNITS[metricName] === "%") return formatPercent(value);
  if (METRIC_UNITS[metricName] === "ms") return `${formatNumber(value, 0)} ms`;
  return formatNumber(value, 2);
}

function formatByMetric(value, metricName) {
  if (metricName === "relative") return formatPercent(value);
  if (metricName === "indexed") return formatNumber(value, 3);
  if (METRIC_UNITS[metricName] === "$") return formatCurrency(value);
  if (METRIC_UNITS[metricName] === "%") return formatPercent(value);
  if (METRIC_UNITS[metricName] === "ms") return `${formatNumber(value, 1)} ms`;
  return formatNumber(value, 6);
}

function sourceLabel(source) {
  return source === "binance" ? "Binance" : "Coinbase";
}

function sourceColor(source, alpha = 1) {
  const color = SOURCE_COLORS[source] || SOURCE_COLORS.unknown;
  if (alpha === 1) return color;

  const hex = color.replace("#", "");
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}
