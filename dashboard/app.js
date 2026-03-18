const COLORS = {
  binance: "#c65b21",
  chainlink: "#0b6e73",
  theoUp: "#224162",
  upTop: "#ca8a27",
};

const REFRESH_INTERVAL_MS = 5000;

const state = {
  indexPayload: null,
  marketDetails: {},
  globalWindows: {},
  selectedMarketId: null,
  loadingMarketId: null,
  loadingGlobalMarketId: null,
  marketLoadError: null,
  globalLoadError: null,
  searchTerm: "",
  chartConfigs: {},
  hoverTsByGroup: {},
  refreshTimerId: null,
  refreshInFlight: false,
};


async function bootstrap() {
  try {
    state.indexPayload = await fetchJson("./data/dashboard-index.json");
    state.selectedMarketId = state.indexPayload.market_order[0] || null;
    bindControls();
    renderApp();
    await ensureMarketDetail(state.selectedMarketId);
    startAutoRefresh();
  } catch (error) {
    document.body.innerHTML = `
      <main class="main-content">
        <div class="empty-state">
          <div>
            <strong>仪表盘数据加载失败</strong>
            <p>${escapeHtml(String(error))}</p>
          </div>
        </div>
      </main>
    `;
  }
}


async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}


async function ensureMarketDetail(marketId) {
  if (!marketId || state.marketDetails[marketId] || state.loadingMarketId === marketId) {
    if (state.marketDetails[marketId]) {
      void ensureGlobalWindow(marketId);
    }
    return;
  }
  state.loadingMarketId = marketId;
  state.marketLoadError = null;
  renderSelectedMarket();
  try {
    const payload = await fetchJson(`./data/markets/${marketId}.json`);
    state.marketDetails[marketId] = payload.market;
  } catch (error) {
    state.marketLoadError = String(error);
  } finally {
    if (state.loadingMarketId === marketId) {
      state.loadingMarketId = null;
    }
    if (state.marketDetails[marketId]) {
      void ensureGlobalWindow(marketId);
    }
    renderSelectedMarket();
  }
}


async function ensureGlobalWindow(marketId) {
  if (!marketId || state.globalWindows[marketId] || state.loadingGlobalMarketId === marketId) {
    return;
  }
  state.loadingGlobalMarketId = marketId;
  state.globalLoadError = null;
  renderSelectedMarket();
  try {
    const payload = await fetchJson(`./data/global-window.json?market_id=${encodeURIComponent(marketId)}`);
    state.globalWindows[marketId] = payload.global;
  } catch (error) {
    state.globalLoadError = String(error);
  } finally {
    if (state.loadingGlobalMarketId === marketId) {
      state.loadingGlobalMarketId = null;
    }
    renderSelectedMarket();
  }
}


function startAutoRefresh() {
  if (state.refreshTimerId !== null) {
    return;
  }
  state.refreshTimerId = window.setInterval(() => {
    void refreshDashboard();
  }, REFRESH_INTERVAL_MS);

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      void refreshDashboard();
    }
  });
  window.addEventListener("focus", () => {
    void refreshDashboard();
  });
}


async function refreshDashboard() {
  if (state.refreshInFlight || !state.indexPayload) {
    return;
  }
  state.refreshInFlight = true;
  try {
    const latestIndex = await fetchJson("./data/dashboard-index.json");
    state.indexPayload = latestIndex;

    if (!latestIndex.market_order.includes(state.selectedMarketId)) {
      state.selectedMarketId = latestIndex.market_order[0] || null;
    }

    const marketId = state.selectedMarketId;
    if (marketId) {
      const [marketResult, globalResult] = await Promise.allSettled([
        fetchJson(`./data/markets/${marketId}.json`),
        fetchJson(`./data/global-window.json?market_id=${encodeURIComponent(marketId)}`),
      ]);

      if (marketResult.status === "fulfilled") {
        state.marketDetails[marketId] = marketResult.value.market;
        state.marketLoadError = null;
      } else if (!state.marketDetails[marketId]) {
        state.marketLoadError = String(marketResult.reason);
      }

      if (globalResult.status === "fulfilled") {
        state.globalWindows[marketId] = globalResult.value.global;
        state.globalLoadError = null;
      } else if (!state.globalWindows[marketId]) {
        state.globalLoadError = String(globalResult.reason);
      }
    }

    renderApp();
  } catch (error) {
    console.error("dashboard auto refresh failed", error);
  } finally {
    state.refreshInFlight = false;
  }
}


function bindControls() {
  document.getElementById("market-search").addEventListener("input", (event) => {
    state.searchTerm = event.target.value.trim().toLowerCase();
    renderMarketList();
  });
}


function renderApp() {
  renderMeta();
  renderMarketList();
  renderGlobalStats();
  renderSelectedMarket();
}


function renderMeta() {
  const payload = state.indexPayload;
  document.getElementById("records-root").textContent = `records: ${payload.records_root}`;
  document.getElementById("generated-at").textContent = `generated: ${formatTimestamp(payload.generated_at)}`;
}


function renderMarketList() {
  const payload = state.indexPayload;
  const container = document.getElementById("market-list");
  const marketIds = payload.market_order.filter((marketId) => marketId.includes(state.searchTerm));

  if (!marketIds.length) {
    container.innerHTML = `<div class="empty-state">没有匹配的市场</div>`;
    return;
  }

  if (!marketIds.includes(state.selectedMarketId)) {
    state.selectedMarketId = marketIds[0];
  }

  container.innerHTML = marketIds.map((marketId) => {
    const market = payload.markets[marketId];
    const phase = market.summary.latest_phase || market.metadata?.status || "unknown";
    const progress = formatMetric(market.summary.progress_pct, "percent");
    const latestQuote = formatMetric(market.summary.latest_quote_bid_sum, "probability");
    return `
      <button class="market-button ${marketId === state.selectedMarketId ? "active" : ""}" data-market-id="${marketId}">
        <span class="market-id">#${marketId}</span>
        <span class="market-meta">${escapeHtml(phase)} · progress ${progress}</span>
        <span class="market-meta">quote sum ${latestQuote}</span>
      </button>
    `;
  }).join("");

  container.querySelectorAll("[data-market-id]").forEach((button) => {
    button.addEventListener("click", async () => {
      state.selectedMarketId = button.dataset.marketId;
      renderApp();
      await ensureMarketDetail(state.selectedMarketId);
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  });
}


function renderGlobalStats() {
  const payload = state.indexPayload;
  const global = payload.global;
  const cards = [
    statCard("市场数量", String(payload.market_order.length), "已解析的 numeric market"),
    statCard("Binance 最新中间价", formatMetric(global.binance.latest?.mid, "price"), `${global.binance.count} records`),
    statCard("Chainlink 最新价格", formatMetric(global.chainlink.latest?.price, "price"), `${global.chainlink.count} records`),
    statCard("最新 Basis", formatMetric(global.basis.latest?.basis, "basis"), `${global.basis.count} aligned points`),
  ];
  document.getElementById("global-stats").innerHTML = cards.join("");
}


function renderSelectedMarket() {
  const market = getSelectedMarketSummary();
  if (!market) {
    return;
  }
  state.chartConfigs = {};

  document.getElementById("market-title").textContent = `Market ${state.selectedMarketId}`;
  document.getElementById("market-phase-pill").textContent = market.summary.latest_phase || market.metadata?.status || "UNKNOWN";

  document.getElementById("market-summary").innerHTML = [
    statCard("Theo UP", formatMetric(market.summary.latest_theo_up, "probability"), "最新理论概率"),
    statCard("Quote Bid Sum", formatMetric(market.summary.latest_quote_bid_sum, "probability"), "双腿做市买价合计"),
    statCard("Best Ask Sum", formatMetric(market.summary.latest_sum_best_ask, "probability"), "盘口最优卖价合计"),
    statCard("External Basis", formatMetric(market.summary.latest_basis, "basis"), "Binance mid - Chainlink"),
  ].join("");

  document.getElementById("market-facts").innerHTML = [
    factCard("市场窗口", `${formatTimestamp(market.window.start_ts)} -> ${formatTimestamp(market.window.end_ts)}`),
    factCard("参考信息", formatReference(market.metadata)),
    factCard("事件计数", [
      `theo ${market.summary.event_counts.theo}`,
      `quote ${market.summary.event_counts.quote}`,
      `book ${market.summary.event_counts.pair_book}`,
      `depth ${market.summary.event_counts.depth}`,
    ].join(" / ")),
  ].join("");

  const detail = getSelectedMarketDetail();
  if (state.marketLoadError && state.loadingMarketId === null) {
    const message = renderInfoState(`市场明细加载失败: ${state.marketLoadError}`);
    document.getElementById("lifecycle-strip").innerHTML = message;
    document.getElementById("external-chart").innerHTML = message;
    document.getElementById("probability-chart").innerHTML = message;
    return;
  }
  if (!detail) {
    void ensureMarketDetail(state.selectedMarketId);
    const message = renderInfoState("正在按需加载当前市场数据...");
    document.getElementById("lifecycle-strip").innerHTML = message;
    document.getElementById("external-chart").innerHTML = message;
    document.getElementById("probability-chart").innerHTML = message;
    return;
  }

  document.getElementById("lifecycle-strip").innerHTML = renderLifecycle(detail.lifecycle);
  const sharedDomain = getSharedMarketDomain(detail);
  const globalWindow = getSelectedGlobalWindow();
  if (state.globalLoadError && state.loadingGlobalMarketId === null) {
    document.getElementById("external-chart").innerHTML = renderInfoState(`外部价格窗口加载失败: ${state.globalLoadError}`);
  } else if (!globalWindow) {
    void ensureGlobalWindow(state.selectedMarketId);
    document.getElementById("external-chart").innerHTML = renderInfoState("正在按需加载外部价格窗口...");
  } else {
    document.getElementById("external-chart").innerHTML = renderExternalChart(globalWindow);
  }
  document.getElementById("probability-chart").innerHTML = renderProbabilityChart(detail, sharedDomain);
  bindInteractiveCharts();
}


function renderLifecycle(items) {
  if (!items?.length) {
    return `<div class="empty-state">没有 lifecycle transition 记录</div>`;
  }
  return items.map((item) => `
    <div class="timeline-pill">
      <strong>${escapeHtml(item.phase)}</strong>
      <span>${formatTimestamp(item.ts)}</span>
    </div>
  `).join("");
}


function renderExternalChart(globalWindow) {
  return createLineChart("external", {
    group: "market-main",
    xDomain: globalWindow.window,
    series: [
      { name: "Binance Mid", color: COLORS.binance, points: (globalWindow.binance?.series || []).map((point) => ({ ts: point.ts, value: point.mid })) },
      { name: "Chainlink", color: COLORS.chainlink, points: (globalWindow.chainlink?.series || []).map((point) => ({ ts: point.ts, value: point.price })) },
    ],
    formatter: (value) => formatMetric(value, "price"),
    note: "按市场时间窗切片后的外部价格锚。用于观察市场记录期内的价格差。",
  });
}


function renderProbabilityChart(market, sharedDomain) {
  return createLineChart("probability", {
    group: "market-main",
    xDomain: sharedDomain,
    series: [
      { name: "Theo UP", color: COLORS.theoUp, points: market.series.theo.map((point) => ({ ts: point.ts, value: point.theo_up })) },
      { name: "UP Best Ask", color: COLORS.upTop, points: market.series.pair_book.map((point) => ({ ts: point.ts, value: point.up_ask })) },
    ],
    yDomain: [0, 1],
    formatter: (value) => formatMetric(value, "probability"),
    note: "对比理论概率与市场 UP 腿实时可买入价（best ask）。",
  });
}


function renderInfoState(message) {
  return `<div class="empty-state">${escapeHtml(message)}</div>`;
}


function createLineChart(chartKey, {
  series,
  group = "default",
  xDomain = null,
  yDomain = null,
  formatter = (value) => String(value),
  note = "",
}) {
  const rawSeries = series
    .map((entry) => ({
      ...entry,
      points: entry.points.filter((point) => Number.isFinite(point.ts) && Number.isFinite(point.value)),
    }))
    .filter((entry) => entry.points.length >= 1);

  if (!rawSeries.length) {
    return `<div class="empty-state">当前视图没有足够的时间序列数据</div>`;
  }

  const rawPoints = rawSeries.flatMap((entry) => entry.points);
  const minX = xDomain?.minX ?? Math.min(...rawPoints.map((point) => point.ts));
  const maxX = xDomain?.maxX ?? Math.max(...rawPoints.map((point) => point.ts));
  const prepared = rawSeries
    .map((entry) => ({
      ...entry,
      points: clipPointsToDomain(entry.points, minX, maxX),
    }))
    .filter((entry) => entry.points.length >= 1);

  if (!prepared.length) {
    return `<div class="empty-state">当前时间窗内没有足够的时间序列数据</div>`;
  }

  const allPoints = prepared.flatMap((entry) => entry.points);
  const rawMinY = yDomain ? yDomain[0] : Math.min(...allPoints.map((point) => point.value));
  const rawMaxY = yDomain ? yDomain[1] : Math.max(...allPoints.map((point) => point.value));
  const [minY, maxY] = expandYDomain(rawMinY, rawMaxY, yDomain !== null);

  const width = 960;
  const height = 320;
  const margin = { top: 20, right: 22, bottom: 34, left: 56 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xScale = (value) => margin.left + ((value - minX) / Math.max(maxX - minX, 1)) * plotWidth;
  const yScale = (value) => margin.top + (1 - ((value - minY) / Math.max(maxY - minY, 1e-9))) * plotHeight;

  state.chartConfigs[chartKey] = {
    chartKey,
    group,
    series: prepared,
    minX,
    maxX,
    minY,
    maxY,
    formatter,
    width,
    height,
    margin,
    allTimestamps: uniqueSorted(prepared.flatMap((entry) => entry.points.map((point) => point.ts))),
  };

  const yTicks = 5;
  const gridLines = [];
  const yLabels = [];
  for (let index = 0; index <= yTicks; index += 1) {
    const ratio = index / yTicks;
    const y = margin.top + plotHeight * ratio;
    const value = maxY - (maxY - minY) * ratio;
    gridLines.push(`<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(28,38,48,0.1)" stroke-dasharray="4 5" />`);
    yLabels.push(`<text x="${margin.left - 12}" y="${y + 4}" text-anchor="end" fill="rgba(93,106,115,0.9)" font-size="12">${escapeHtml(formatter(value))}</text>`);
  }

  const paths = prepared.map((entry) => {
    const path = entry.points
      .map((point, index) => `${index === 0 ? "M" : "L"} ${xScale(point.ts).toFixed(2)} ${yScale(point.value).toFixed(2)}`)
      .join(" ");
    const latest = entry.points[entry.points.length - 1];
    return `
      <path d="${path}" fill="none" stroke="${entry.color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />
      <circle cx="${xScale(latest.ts).toFixed(2)}" cy="${yScale(latest.value).toFixed(2)}" r="4.6" fill="${entry.color}" />
    `;
  }).join("");

  return `
    <div class="chart-frame">
      <div class="chart-legend">
        ${prepared.map((entry) => `
          <div class="legend-chip">
            <span class="legend-swatch" style="background:${entry.color}"></span>
            <span>${escapeHtml(entry.name)}</span>
          </div>
        `).join("")}
      </div>
      <div class="interactive-chart" data-chart-key="${chartKey}" data-chart-group="${group}">
        <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="time series chart">
          ${gridLines.join("")}
          ${yLabels.join("")}
          <line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="rgba(28,38,48,0.16)" />
          ${paths}
          <line class="hover-line" x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}" stroke="rgba(28,38,48,0.28)" stroke-dasharray="4 4" visibility="hidden" />
          <g class="hover-points"></g>
        </svg>
        <div class="chart-tooltip" hidden></div>
      </div>
      <div class="chart-footer">
        <span>${formatTimestamp(minX)}</span>
        <span>${note}</span>
        <span>${formatTimestamp(maxX)}</span>
      </div>
    </div>
  `;
}


function expandYDomain(minY, maxY, fixed) {
  if (fixed) {
    return [minY, maxY];
  }
  if (minY === maxY) {
    return [minY - 1, maxY + 1];
  }
  const padding = (maxY - minY) * 0.08;
  return [minY - padding, maxY + padding];
}


function sliceByWindow(points, startTs, endTs) {
  if (!Array.isArray(points) || !points.length) {
    return [];
  }
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs)) {
    return points;
  }
  return points.filter((point) => point.ts >= startTs && point.ts <= endTs);
}


function bindInteractiveCharts() {
  document.querySelectorAll(".interactive-chart").forEach((node) => {
    const chartKey = node.dataset.chartKey;
    const config = state.chartConfigs[chartKey];
    if (!config) {
      return;
    }

    node.addEventListener("mousemove", (event) => {
      const ts = hoverTimestampForEvent(event, node, config);
      state.hoverTsByGroup[config.group] = ts;
      updateHoverGroup(config.group, ts);
    });
    node.addEventListener("mouseleave", () => {
      state.hoverTsByGroup[config.group] = null;
      updateHoverGroup(config.group, null);
    });

    updateChartHover(node, config, state.hoverTsByGroup[config.group] ?? null);
  });
}


function hoverTimestampForEvent(event, node, config) {
  const svg = node.querySelector(".svg-chart");
  const rect = svg.getBoundingClientRect();
  const relative = clamp((event.clientX - rect.left) / Math.max(rect.width, 1), 0, 1);
  const rawTs = config.minX + relative * (config.maxX - config.minX);
  return nearestTimestamp(config.allTimestamps, rawTs);
}


function updateHoverGroup(group, ts) {
  document.querySelectorAll(`.interactive-chart[data-chart-group="${group}"]`).forEach((node) => {
    const config = state.chartConfigs[node.dataset.chartKey];
    if (!config) {
      return;
    }
    updateChartHover(node, config, ts);
  });
}


function updateChartHover(node, config, ts) {
  const hoverLine = node.querySelector(".hover-line");
  const hoverPoints = node.querySelector(".hover-points");
  const tooltip = node.querySelector(".chart-tooltip");
  if (!hoverLine || !hoverPoints || !tooltip) {
    return;
  }
  if (!Number.isFinite(ts)) {
    hoverLine.setAttribute("visibility", "hidden");
    hoverPoints.innerHTML = "";
    tooltip.hidden = true;
    return;
  }

  const x = scaleX(ts, config);
  hoverLine.setAttribute("x1", x.toFixed(2));
  hoverLine.setAttribute("x2", x.toFixed(2));
  hoverLine.setAttribute("visibility", "visible");

  const nearestRows = config.series.map((entry) => {
    const point = nearestPoint(entry.points, ts);
    return point ? { ...entry, point } : null;
  }).filter(Boolean);

  hoverPoints.innerHTML = nearestRows.map((entry) => {
    const cy = scaleY(entry.point.value, config);
    return `<circle cx="${x.toFixed(2)}" cy="${cy.toFixed(2)}" r="4.5" fill="${entry.color}" stroke="white" stroke-width="1.5" />`;
  }).join("");

  tooltip.innerHTML = `
    <div class="tooltip-time">${formatTimestamp(ts)}</div>
    ${nearestRows.map((entry) => `
      <div class="tooltip-row">
        <span class="legend-swatch" style="background:${entry.color}"></span>
        <span>${escapeHtml(entry.name)}</span>
        <strong>${escapeHtml(config.formatter(entry.point.value))}</strong>
      </div>
    `).join("")}
  `;
  tooltip.hidden = false;
  positionTooltip(node, tooltip, x, config);
}


function positionTooltip(node, tooltip, x, config) {
  const svg = node.querySelector(".svg-chart");
  const chartRect = svg.getBoundingClientRect();
  const nodeRect = node.getBoundingClientRect();
  const ratio = (x - config.margin.left) / Math.max((config.width - config.margin.left - config.margin.right), 1);
  const leftPx = ratio * chartRect.width + (chartRect.left - nodeRect.left);
  const tooltipWidth = Math.max(tooltip.offsetWidth, 180);
  const clampedLeft = clamp(leftPx - tooltipWidth / 2, 8, node.clientWidth - tooltipWidth - 8);
  tooltip.style.left = `${clampedLeft}px`;
  tooltip.style.top = "10px";
}


function getSharedMarketDomain(market) {
  if (
    Number.isFinite(market.window?.start_ts)
    && Number.isFinite(market.window?.end_ts)
    && market.window.end_ts > market.window.start_ts
  ) {
    return {
      minX: market.window.start_ts,
      maxX: market.window.end_ts,
    };
  }

  const seriesPoints = [
    ...market.series.theo,
    ...market.series.quote,
    ...market.series.pair_book,
  ];
  const timestamps = seriesPoints
    .map((point) => point.ts)
    .filter((value) => Number.isFinite(value));
  if (!timestamps.length) {
    return { minX: market.window.start_ts || 0, maxX: market.window.end_ts || 1 };
  }
  const observedMin = Math.min(...timestamps);
  const observedMax = Math.max(...timestamps);
  const minX = Number.isFinite(market.window.start_ts) ? Math.min(market.window.start_ts, observedMin) : observedMin;
  const maxX = observedMax;
  return {
    minX,
    maxX: maxX > minX ? maxX : minX + 1,
  };
}


function clipPointsToDomain(points, minX, maxX) {
  if (!Number.isFinite(minX) || !Number.isFinite(maxX) || maxX <= minX) {
    return points;
  }
  return points.filter((point) => point.ts >= minX && point.ts <= maxX);
}


function getSelectedMarketSummary() {
  return state.indexPayload?.markets?.[state.selectedMarketId] || null;
}


function getSelectedMarketDetail() {
  return state.marketDetails?.[state.selectedMarketId] || null;
}


function getSelectedGlobalWindow() {
  const globalWindow = state.globalWindows?.[state.selectedMarketId];
  if (!globalWindow) {
    return null;
  }
  const detail = getSelectedMarketDetail();
  return {
    ...globalWindow,
    window: detail?.window ? {
      minX: detail.window.start_ts,
      maxX: detail.window.end_ts,
    } : null,
  };
}


function scaleX(value, config) {
  const plotWidth = config.width - config.margin.left - config.margin.right;
  return config.margin.left + ((value - config.minX) / Math.max(config.maxX - config.minX, 1)) * plotWidth;
}


function scaleY(value, config) {
  const plotHeight = config.height - config.margin.top - config.margin.bottom;
  return config.margin.top + (1 - ((value - config.minY) / Math.max(config.maxY - config.minY, 1e-9))) * plotHeight;
}


function nearestTimestamp(values, target) {
  if (!values.length) {
    return target;
  }
  let low = 0;
  let high = values.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid] < target) {
      low = mid + 1;
    } else if (values[mid] > target) {
      high = mid - 1;
    } else {
      return values[mid];
    }
  }
  const left = values[Math.max(high, 0)];
  const right = values[Math.min(low, values.length - 1)];
  return Math.abs(left - target) <= Math.abs(right - target) ? left : right;
}


function nearestPoint(points, targetTs) {
  if (!points.length) {
    return null;
  }
  let low = 0;
  let high = points.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    if (points[mid].ts < targetTs) {
      low = mid + 1;
    } else if (points[mid].ts > targetTs) {
      high = mid - 1;
    } else {
      return points[mid];
    }
  }
  const left = points[Math.max(high, 0)];
  const right = points[Math.min(low, points.length - 1)];
  return Math.abs(left.ts - targetTs) <= Math.abs(right.ts - targetTs) ? left : right;
}


function uniqueSorted(values) {
  return [...new Set(values)].sort((left, right) => left - right);
}


function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}


function statCard(label, value, note) {
  return `
    <div class="stat-card">
      <div class="stat-label">${escapeHtml(label)}</div>
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(note)}</span>
    </div>
  `;
}


function factCard(label, value) {
  return `
    <div class="fact-card">
      <div class="stat-label">${escapeHtml(label)}</div>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}


function formatMetric(value, type) {
  if (!Number.isFinite(value)) {
    return "-";
  }
  switch (type) {
    case "price":
      return value >= 1000 ? value.toLocaleString("en-US", { maximumFractionDigits: 2 }) : value.toFixed(4);
    case "probability":
      return value.toFixed(4);
    case "percent":
      return `${value.toFixed(2)}%`;
    case "basis":
      return `${value.toFixed(2)}`;
    case "size":
      return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
    default:
      return String(value);
  }
}


function formatReference(metadata) {
  if (!metadata) {
    return "-";
  }
  if (Number.isFinite(metadata.reference_price)) {
    return `reference ${formatMetric(metadata.reference_price, "price")}`;
  }
  if (Number.isFinite(metadata.raw_reference_price)) {
    return `raw ${formatMetric(metadata.raw_reference_price, "price")}`;
  }
  return "无 reference price";
}


function formatTimestamp(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    const iso = new Date(value);
    return Number.isNaN(iso.getTime()) ? String(value) : iso.toLocaleString("zh-CN");
  }
  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}


function escapeHtml(input) {
  return String(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}


bootstrap();
