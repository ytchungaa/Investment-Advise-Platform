const form = document.getElementById("stock-form");
const symbolInput = document.getElementById("symbol-input");
const symbolSuggestionList = document.getElementById("symbol-suggestion-list");
const startDateInput = document.getElementById("start-date-input");
const endDateInput = document.getElementById("end-date-input");
const rangePresetButtons = Array.from(document.querySelectorAll("[data-range-preset]"));
const intervalInputs = Array.from(document.querySelectorAll('input[name="interval"]'));
const refreshDataButton = document.getElementById("refresh-data-button");
const statusMessage = document.getElementById("status-message");
const rangeSummary = document.getElementById("range-summary");
const chartTitle = document.getElementById("chart-title");
const chartDescription = document.getElementById("chart-description");
const timeseriesChart = document.getElementById("timeseries-chart");
const fundamentalsWidget = document.getElementById("fundamentals-widget");
const fundamentalsDescription = document.getElementById("fundamentals-description");
const chartToast = document.getElementById("chart-toast");
const appTabs = Array.from(document.querySelectorAll("[data-app-tab]"));
const appViews = {
  stock: document.getElementById("stock-view"),
  portfolio: document.getElementById("portfolio-view"),
};
const portfolioRefreshButton = document.getElementById("portfolio-refresh-button");
const portfolioStatusMessage = document.getElementById("portfolio-status-message");
const portfolioAsOf = document.getElementById("portfolio-as-of");
const portfolioKpiGrid = document.getElementById("portfolio-kpi-grid");
const portfolioAllocationChart = document.getElementById("portfolio-allocation-chart");
const portfolioRiskPanel = document.getElementById("portfolio-risk-panel");
const portfolioBalancePanel = document.getElementById("portfolio-balance-panel");
const portfolioHistoryChart = document.getElementById("portfolio-history-chart");
const portfolioHoldingsBody = document.getElementById("portfolio-holdings-body");
const portfolioHoldingsSortButtons = Array.from(
  document.querySelectorAll("[data-holdings-sort]")
);

let suggestionRequestId = 0;
let suggestionTimerId = null;
let currentSuggestions = [];
let activeSuggestionIndex = -1;
let toastTimerId = null;
let portfolioLoaded = false;
let currentPortfolioPayload = null;
let portfolioHoldingsSort = { key: "category_weight", direction: "desc" };

function formatDateInputValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function cloneDate(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function subtractCalendarDays(date, days) {
  const result = cloneDate(date);
  result.setDate(result.getDate() - days);
  return result;
}

function subtractCalendarMonths(date, months) {
  const targetDate = cloneDate(date);
  targetDate.setDate(1);
  targetDate.setMonth(targetDate.getMonth() - months);
  const targetYear = targetDate.getFullYear();
  const targetMonth = targetDate.getMonth();
  const maxDay = new Date(targetYear, targetMonth + 1, 0).getDate();
  targetDate.setDate(Math.min(date.getDate(), maxDay));
  return targetDate;
}

function subtractCalendarYears(date, years) {
  const targetDate = cloneDate(date);
  targetDate.setFullYear(targetDate.getFullYear() - years);
  if (targetDate.getMonth() !== date.getMonth()) {
    targetDate.setDate(0);
  }
  return targetDate;
}

function getPresetDateRange(preset, endDate = new Date()) {
  const normalizedEndDate = cloneDate(endDate);

  if (preset === "1d") {
    return { start: normalizedEndDate, end: normalizedEndDate };
  }
  if (preset === "7d") {
    return { start: subtractCalendarDays(normalizedEndDate, 6), end: normalizedEndDate };
  }
  if (preset === "1m") {
    return { start: subtractCalendarMonths(normalizedEndDate, 1), end: normalizedEndDate };
  }
  if (preset === "3m") {
    return { start: subtractCalendarMonths(normalizedEndDate, 3), end: normalizedEndDate };
  }
  if (preset === "6m") {
    return { start: subtractCalendarMonths(normalizedEndDate, 6), end: normalizedEndDate };
  }
  if (preset === "ytd") {
    return {
      start: new Date(normalizedEndDate.getFullYear(), 0, 1),
      end: normalizedEndDate,
    };
  }
  if (preset === "1y") {
    return { start: subtractCalendarYears(normalizedEndDate, 1), end: normalizedEndDate };
  }
  if (preset === "5y") {
    return { start: subtractCalendarYears(normalizedEndDate, 5), end: normalizedEndDate };
  }

  return { start: subtractCalendarMonths(normalizedEndDate, 1), end: normalizedEndDate };
}

function setActiveRangePreset(activePreset) {
  rangePresetButtons.forEach((button) => {
    const isActive = button.dataset.rangePreset === activePreset;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function applyRangePreset(preset, shouldSubmit = false) {
  const range = getPresetDateRange(preset);
  startDateInput.value = formatDateInputValue(range.start);
  endDateInput.value = formatDateInputValue(range.end);
  setActiveRangePreset(preset);

  if (shouldSubmit && form.reportValidity()) {
    form.requestSubmit();
  }
}

function getSelectedInterval() {
  const selectedInput = intervalInputs.find((input) => input.checked);
  return selectedInput ? selectedInput.value : "hourly";
}

function getIntervalLabel(interval) {
  return interval === "daily" ? "Daily" : "Hourly";
}

function updateChartHeader(interval = getSelectedInterval()) {
  const intervalLabel = getIntervalLabel(interval);
  chartTitle.textContent = `${intervalLabel} Candlestick Chart`;
  chartDescription.textContent =
    `${intervalLabel} open, high, low, and close across the selected date range.`;
}

function resetCharts() {
  Plotly.purge(timeseriesChart);
  timeseriesChart.classList.add("empty-surface");
  fundamentalsDescription.textContent =
    "Latest quote, valuation, profitability, and balance-sheet metrics.";
  setEmptySurface(fundamentalsWidget, "Enter a symbol to render company fundamentals.");
}

function setSuggestionsOpen(isOpen) {
  symbolSuggestionList.classList.toggle("open", isOpen && currentSuggestions.length > 0);
  symbolInput.setAttribute(
    "aria-expanded",
    isOpen && currentSuggestions.length > 0 ? "true" : "false"
  );
}

function clearSuggestions() {
  suggestionRequestId += 1;
  clearTimeout(suggestionTimerId);
  currentSuggestions = [];
  activeSuggestionIndex = -1;
  symbolSuggestionList.innerHTML = "";
  symbolInput.removeAttribute("aria-activedescendant");
  setSuggestionsOpen(false);
}

function updateActiveSuggestion(nextIndex) {
  const items = symbolSuggestionList.querySelectorAll(".suggestion-item");
  activeSuggestionIndex = nextIndex;

  items.forEach((item, index) => {
    const isActive = index === activeSuggestionIndex;
    item.classList.toggle("is-active", isActive);
    item.setAttribute("aria-selected", isActive ? "true" : "false");
    if (isActive) {
      symbolInput.setAttribute("aria-activedescendant", item.id);
      item.scrollIntoView({ block: "nearest" });
    }
  });

  if (activeSuggestionIndex < 0) {
    symbolInput.removeAttribute("aria-activedescendant");
  }
}

function selectSuggestion(index) {
  const suggestion = currentSuggestions[index];
  if (!suggestion) {
    return;
  }

  symbolInput.value = suggestion.symbol;
  clearSuggestions();
}

function renderSuggestions(results) {
  currentSuggestions = results;
  activeSuggestionIndex = -1;
  symbolSuggestionList.innerHTML = "";

  if (!results.length) {
    setSuggestionsOpen(false);
    return;
  }

  results.forEach((item, index) => {
    const option = document.createElement("button");
    const symbol = document.createElement("span");
    const description = document.createElement("span");

    option.type = "button";
    option.id = `symbol-suggestion-${index}`;
    option.className = "suggestion-item";
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", "false");

    symbol.className = "suggestion-symbol";
    symbol.textContent = item.symbol;
    description.className = "suggestion-description";
    description.textContent = item.description || "Matching symbol";

    option.appendChild(symbol);
    option.appendChild(description);
    option.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      selectSuggestion(index);
    });
    symbolSuggestionList.appendChild(option);
  });

  setSuggestionsOpen(true);
}

async function loadSymbolSuggestions() {
  const query = symbolInput.value.trim();
  const requestId = ++suggestionRequestId;

  if (!query) {
    clearSuggestions();
    return;
  }

  try {
    const searchParams = new URLSearchParams({
      q: query,
      interval: getSelectedInterval(),
    });
    const response = await fetch(`/api/symbol-search?${searchParams.toString()}`);
    const payload = await response.json();
    if (requestId !== suggestionRequestId) {
      return;
    }
    renderSuggestions(payload.results || []);
  } catch (error) {
    if (requestId === suggestionRequestId) {
      clearSuggestions();
    }
  }
}

function scheduleSuggestionLoad() {
  clearTimeout(suggestionTimerId);
  suggestionTimerId = window.setTimeout(loadSymbolSuggestions, 150);
}

function setStatus(message, type = "") {
  statusMessage.textContent = message || "";
  statusMessage.className = `status-message ${type}`.trim();
}

function showChartToast(message) {
  clearTimeout(toastTimerId);
  chartToast.textContent = message;
  chartToast.classList.add("show");
  toastTimerId = window.setTimeout(() => {
    chartToast.classList.remove("show");
  }, 2200);
}

function setEmptySurface(element, message) {
  element.innerHTML = "";
  element.textContent = message;
  element.classList.add("empty-surface");
}

function isPresent(value) {
  return value !== null && value !== undefined && value !== "";
}

function firstPresent(...values) {
  return values.find((value) => isPresent(value));
}

function formatDateTime(value) {
  if (!isPresent(value)) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatMetricValue(value, format = "number") {
  if (!isPresent(value)) {
    return "n/a";
  }
  const numericValue = Number(value);
  if (format === "text") {
    return String(value);
  }
  if (!Number.isFinite(numericValue)) {
    return String(value);
  }
  if (format === "currency") {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: numericValue >= 100 ? 0 : 2,
    }).format(numericValue);
  }
  if (format === "compact") {
    return new Intl.NumberFormat(undefined, {
      notation: "compact",
      maximumFractionDigits: 2,
    }).format(numericValue);
  }
  if (format === "percent") {
    return `${new Intl.NumberFormat(undefined, {
      maximumFractionDigits: 2,
    }).format(numericValue)}%`;
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(numericValue);
}

function formatRatio(value, options = {}) {
  if (!isPresent(value)) {
    return "n/a";
  }
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return "n/a";
  }
  const signDisplay = options.signDisplay || "auto";
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signDisplay,
  }).format(numericValue);
}

function formatPlainNumber(value) {
  if (!isPresent(value)) {
    return "n/a";
  }
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return String(value);
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(numericValue);
}

function valueTone(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue) || numericValue === 0) {
    return "";
  }
  return numericValue > 0 ? "positive" : "negative";
}

function createMetric(label, value, format = "number") {
  const item = document.createElement("div");
  const labelElement = document.createElement("span");
  const valueElement = document.createElement("strong");

  item.className = "fundamental-metric";
  labelElement.textContent = label;
  valueElement.textContent =
    format === "ratio" ? formatRatio(value, { signDisplay: "always" }) : formatMetricValue(value, format);

  item.appendChild(labelElement);
  item.appendChild(valueElement);
  return item;
}

function createMetricGroup(title, metrics) {
  const group = document.createElement("section");
  const heading = document.createElement("h3");
  const list = document.createElement("div");

  group.className = "fundamental-group";
  heading.textContent = title;
  list.className = "fundamental-metric-list";
  metrics.forEach((metric) => {
    list.appendChild(createMetric(metric.label, metric.value, metric.format));
  });

  group.appendChild(heading);
  group.appendChild(list);
  return group;
}

function createPortfolioKpi(label, value, format = "currency", toneValue = null) {
  const item = document.createElement("div");
  const labelElement = document.createElement("span");
  const valueElement = document.createElement("strong");

  item.className = "portfolio-kpi";
  labelElement.textContent = label;
  if (format === "ratio") {
    valueElement.textContent = formatRatio(value, { signDisplay: "always" });
  } else if (format === "number") {
    valueElement.textContent = formatPlainNumber(value);
  } else {
    valueElement.textContent = formatMetricValue(value, format);
  }

  const tone = valueTone(toneValue ?? value);
  if (tone) {
    valueElement.classList.add(tone);
  }

  item.appendChild(labelElement);
  item.appendChild(valueElement);
  return item;
}

function renderMetricPanel(container, metrics) {
  container.innerHTML = "";
  metrics.forEach((metric) => {
    container.appendChild(createMetric(metric.label, metric.value, metric.format));
  });
}

function renderPortfolioKpis(payload) {
  const summary = payload.summary || {};
  portfolioKpiGrid.innerHTML = "";
  portfolioKpiGrid.appendChild(
    createPortfolioKpi("Total Value", summary.total_portfolio_value, "currency")
  );
  portfolioKpiGrid.appendChild(createPortfolioKpi("Cash", summary.total_cash, "currency"));
  portfolioKpiGrid.appendChild(
    createPortfolioKpi("1Y Return", summary.portfolio_return_1y, "ratio")
  );
  portfolioKpiGrid.appendChild(
    createPortfolioKpi(`${payload.benchmark_symbol || "SPY"} 1Y`, summary.benchmark_return_1y, "ratio")
  );
  portfolioKpiGrid.appendChild(createPortfolioKpi("Alpha", summary.alpha_1y, "ratio"));
  portfolioKpiGrid.appendChild(createPortfolioKpi("Beta", summary.beta_1y, "number"));
}

function renderPortfolioAllocation(payload) {
  const allocation = (payload.allocation || []).filter((row) => Number(row.market_value) > 0);
  if (!allocation.length) {
    Plotly.purge(portfolioAllocationChart);
    setEmptySurface(portfolioAllocationChart, "No positive market value is available for allocation.");
    return;
  }

  portfolioAllocationChart.classList.remove("empty-surface");
  portfolioAllocationChart.innerHTML = "";
  Plotly.newPlot(
    portfolioAllocationChart,
    [
      {
        type: "pie",
        hole: 0.58,
        labels: allocation.map((row) => row.category),
        values: allocation.map((row) => row.market_value),
        textinfo: "label+percent",
        hovertemplate: "%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>",
        marker: {
          colors: ["#0a9396", "#005f73", "#94d2bd", "#ee9b00"],
        },
      },
    ],
    {
      margin: { t: 8, r: 8, b: 8, l: 8 },
      paper_bgcolor: "#ffffff",
      showlegend: true,
      legend: { orientation: "h", y: -0.05 },
    },
    { responsive: true }
  );
}

function renderPortfolioRisk(payload) {
  const summary = payload.summary || {};
  renderMetricPanel(portfolioRiskPanel, [
    { label: "Alpha vs SPY", value: summary.alpha_1y, format: "ratio" },
    { label: "Beta vs SPY", value: summary.beta_1y },
    { label: "Portfolio 1Y", value: summary.portfolio_return_1y, format: "ratio" },
    { label: `${payload.benchmark_symbol || "SPY"} 1Y`, value: summary.benchmark_return_1y, format: "ratio" },
    { label: "Priced MV", value: summary.priced_holding_weight, format: "currency" },
    { label: "Beta MV", value: summary.beta_holding_weight, format: "currency" },
  ]);
  if (payload.warnings && payload.warnings.length) {
    const warning = document.createElement("p");
    warning.className = "portfolio-panel-note";
    warning.textContent = payload.warnings.join(" ");
    portfolioRiskPanel.appendChild(warning);
  }
}

function renderPortfolioBalance(payload) {
  const balance = payload.balance || {};
  renderMetricPanel(portfolioBalancePanel, [
    { label: "Liquidation", value: balance.total_liquidation_value, format: "currency" },
    { label: "Cash balance", value: balance.total_cash_balance, format: "currency" },
    { label: "Total cash", value: balance.total_cash, format: "currency" },
    { label: "Long market", value: balance.total_long_market_value, format: "currency" },
    { label: "Short market", value: balance.total_short_market_value, format: "currency" },
    { label: "Accounts", value: balance.account_count },
  ]);
}

function renderPortfolioHistory(payload) {
  const history = payload.history || {};
  const balanceSeries = history.balance_series || [];
  const realizedEvents = history.realized_events || [];
  if (!balanceSeries.length) {
    Plotly.purge(portfolioHistoryChart);
    setEmptySurface(portfolioHistoryChart, "No portfolio history snapshots are available.");
    return;
  }

  portfolioHistoryChart.classList.remove("empty-surface");
  portfolioHistoryChart.innerHTML = "";

  const traces = [
    {
      type: "scatter",
      mode: "lines+markers",
      name: "Portfolio value",
      x: balanceSeries.map((row) => row.as_of_time || row.date),
      y: balanceSeries.map((row) => row.total_value),
      line: { color: "#005f73", width: 2.5 },
      marker: { color: "#005f73", size: 7 },
      hovertemplate: "%{x}<br>Value: %{y:$,.2f}<extra></extra>",
    },
  ];

  if (realizedEvents.length) {
    const maxBubbleAmount = Math.max(
      ...realizedEvents.map((event) => Math.abs(Number(event.realized_amount) || 0)),
      1
    );
    traces.push({
      type: "scatter",
      mode: "markers",
      name: "Realized gain/loss",
      x: realizedEvents.map((event) => event.date),
      y: realizedEvents.map((event) => event.realized_amount),
      text: realizedEvents.map((event) => event.symbol || "Trade"),
      customdata: realizedEvents.map((event) => [event.transaction_count]),
      marker: {
        size: realizedEvents.map((event) => {
          const amount = Math.abs(Number(event.realized_amount) || 0);
          return Math.max(9, Math.min(32, 9 + (amount / maxBubbleAmount) * 23));
        }),
        color: realizedEvents.map((event) =>
          Number(event.realized_amount) >= 0 ? "#2a9d8f" : "#ae2012"
        ),
        opacity: 0.72,
        line: { color: "#ffffff", width: 1 },
      },
      yaxis: "y2",
      hovertemplate:
        "%{text}<br>%{x}<br>Amount: %{y:$,.2f}<br>Trades: %{customdata[0]}<extra></extra>",
    });
  }

  Plotly.newPlot(
    portfolioHistoryChart,
    traces,
    {
      margin: { t: 8, r: 58, b: 48, l: 64 },
      paper_bgcolor: "#ffffff",
      plot_bgcolor: "#f8fbfb",
      legend: { orientation: "h", y: -0.18 },
      xaxis: {
        title: "Date",
        gridcolor: "#d6e3e6",
      },
      yaxis: {
        title: "Portfolio value",
        gridcolor: "#d6e3e6",
        tickprefix: "$",
      },
      yaxis2: {
        title: "Realized amount",
        overlaying: "y",
        side: "right",
        tickprefix: "$",
        zeroline: true,
        zerolinecolor: "#9ba8ad",
        showgrid: false,
      },
    },
    { responsive: true }
  );
}

function compareHoldingValues(left, right, key) {
  const textKeys = new Set(["symbol", "category"]);
  if (textKeys.has(key)) {
    return String(left[key] || "").localeCompare(String(right[key] || ""));
  }

  const leftValue = Number(left[key]);
  const rightValue = Number(right[key]);
  const leftFinite = Number.isFinite(leftValue);
  const rightFinite = Number.isFinite(rightValue);
  if (!leftFinite && !rightFinite) {
    return 0;
  }
  if (!leftFinite) {
    return 1;
  }
  if (!rightFinite) {
    return -1;
  }
  return leftValue - rightValue;
}

function sortedPortfolioHoldings(holdings) {
  if (portfolioHoldingsSort.key === "category_weight") {
    return [...holdings].sort((left, right) => {
      const categoryCompared = String(right.category || "").localeCompare(
        String(left.category || "")
      );
      if (categoryCompared !== 0) {
        return categoryCompared;
      }
      const weightCompared = compareHoldingValues(left, right, "weight");
      if (weightCompared !== 0) {
        return weightCompared * -1;
      }
      return String(left.symbol || "").localeCompare(String(right.symbol || ""));
    });
  }

  const directionMultiplier = portfolioHoldingsSort.direction === "asc" ? 1 : -1;
  return [...holdings].sort((left, right) => {
    const compared = compareHoldingValues(left, right, portfolioHoldingsSort.key);
    if (compared !== 0) {
      return compared * directionMultiplier;
    }
    return String(left.symbol || "").localeCompare(String(right.symbol || ""));
  });
}

function updateHoldingsSortButtons() {
  portfolioHoldingsSortButtons.forEach((button) => {
    const isActive = button.dataset.holdingsSort === portfolioHoldingsSort.key;
    button.classList.toggle("is-active", isActive);
    button.setAttribute(
      "aria-sort",
      isActive ? (portfolioHoldingsSort.direction === "asc" ? "ascending" : "descending") : "none"
    );
  });
}

function renderPortfolioHoldings(payload) {
  const holdings = sortedPortfolioHoldings(payload.holdings || []);
  updateHoldingsSortButtons();
  portfolioHoldingsBody.innerHTML = "";
  if (!holdings.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 9;
    cell.textContent = "No portfolio holdings are available. Load account data into ODS/DWD first.";
    row.appendChild(cell);
    portfolioHoldingsBody.appendChild(row);
    return;
  }

  holdings.forEach((holding) => {
    const row = document.createElement("tr");
    const cells = [
      holding.symbol,
      holding.category,
      formatPlainNumber(holding.quantity),
      formatMetricValue(holding.average_price, "currency"),
      formatMetricValue(holding.total_cost, "currency"),
      formatMetricValue(holding.market_value, "currency"),
      formatRatio(holding.weight, { signDisplay: "auto" }),
      formatMetricValue(holding.day_profit_loss, "currency"),
      formatRatio(
        isPresent(holding.day_profit_loss_percentage)
          ? Number(holding.day_profit_loss_percentage) / 100
          : null,
        { signDisplay: "always" }
      ),
    ];

    cells.forEach((value, index) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      if (index === 7 || index === 8) {
        const tone = valueTone(
          index === 7 ? holding.day_profit_loss : holding.day_profit_loss_percentage
        );
        if (tone) {
          cell.classList.add(tone);
        }
      }
      row.appendChild(cell);
    });
    portfolioHoldingsBody.appendChild(row);
  });
}

function renderPortfolioData(payload) {
  currentPortfolioPayload = payload;
  renderPortfolioKpis(payload);
  renderPortfolioAllocation(payload);
  renderPortfolioRisk(payload);
  renderPortfolioBalance(payload);
  renderPortfolioHistory(payload);
  renderPortfolioHoldings(payload);
  portfolioAsOf.textContent = payload.as_of_time
    ? `Snapshot as of ${formatDateTime(payload.as_of_time)}.`
    : "Latest account and position snapshot.";

  if (payload.warnings && payload.warnings.length) {
    portfolioStatusMessage.textContent = payload.warnings.join(" ");
    portfolioStatusMessage.className = "status-message warning";
  } else {
    portfolioStatusMessage.textContent = "";
    portfolioStatusMessage.className = "status-message";
  }
}

async function ensurePortfolioData(forceRefresh = false) {
  const response = await fetch("/api/refresh-portfolio-data", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ force_refresh: forceRefresh }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || "Failed to refresh portfolio data.");
  }
  return payload;
}

async function loadPortfolioData({ ensureHistory = false } = {}) {
  const originalButtonText = portfolioRefreshButton.textContent;
  portfolioRefreshButton.disabled = true;
  portfolioRefreshButton.textContent = "Loading...";
  portfolioStatusMessage.textContent = ensureHistory
    ? "Checking portfolio history in ODS/DWD..."
    : "Loading portfolio data...";
  portfolioStatusMessage.className = "status-message info";

  try {
    let refreshMessage = "";
    if (ensureHistory) {
      const refreshPayload = await ensurePortfolioData(false);
      refreshMessage = refreshPayload.message || "";
    }
    const response = await fetch("/api/portfolio-data");
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || "Failed to load portfolio data.");
    }
    renderPortfolioData(payload);
    if (refreshMessage && !(payload.warnings && payload.warnings.length)) {
      portfolioStatusMessage.textContent = refreshMessage;
      portfolioStatusMessage.className = "status-message success";
    }
    portfolioLoaded = true;
  } catch (error) {
    portfolioStatusMessage.textContent = error.message;
    portfolioStatusMessage.className = "status-message error";
  } finally {
    portfolioRefreshButton.disabled = false;
    portfolioRefreshButton.textContent = originalButtonText;
  }
}

async function refreshPortfolioData() {
  const originalButtonText = portfolioRefreshButton.textContent;
  portfolioRefreshButton.disabled = true;
  portfolioRefreshButton.textContent = "Refreshing...";
  portfolioStatusMessage.textContent = "Fetching Schwab account and history data...";
  portfolioStatusMessage.className = "status-message info";

  try {
    const refreshPayload = await ensurePortfolioData(true);
    await loadPortfolioData();
    if (refreshPayload.message) {
      portfolioStatusMessage.textContent = refreshPayload.message;
      portfolioStatusMessage.className = "status-message success";
    }
  } catch (error) {
    portfolioStatusMessage.textContent = error.message;
    portfolioStatusMessage.className = "status-message error";
  } finally {
    portfolioRefreshButton.disabled = false;
    portfolioRefreshButton.textContent = originalButtonText;
  }
}

function activateAppTab(tabName) {
  appTabs.forEach((tab) => {
    const isActive = tab.dataset.appTab === tabName;
    tab.classList.toggle("is-active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  Object.entries(appViews).forEach(([name, view]) => {
    const isActive = name === tabName;
    view.classList.toggle("is-active", isActive);
    view.hidden = !isActive;
  });

  if (tabName === "portfolio" && !portfolioLoaded) {
    loadPortfolioData({ ensureHistory: true });
  }
  if (tabName === "portfolio" && portfolioLoaded) {
    Plotly.Plots.resize(portfolioAllocationChart);
    Plotly.Plots.resize(portfolioHistoryChart);
  }
}

function renderCompanyInfo(payload) {
  const companyInfo = payload.company_info || {};
  const profile = companyInfo.profile || {};
  const quote = companyInfo.quote || {};
  const fundamentals = companyInfo.fundamentals || {};

  if (!companyInfo.quote && !companyInfo.fundamentals) {
    fundamentalsDescription.textContent = "No fundamentals or quote snapshot is available.";
    setEmptySurface(fundamentalsWidget, "No company fundamentals have been loaded for this symbol.");
    return;
  }

  fundamentalsWidget.innerHTML = "";
  fundamentalsWidget.classList.remove("empty-surface");
  fundamentalsDescription.textContent = fundamentals.as_of_time
    ? `Fundamentals as of ${formatDateTime(fundamentals.as_of_time)}.`
    : "Latest company and market snapshot.";

  const header = document.createElement("div");
  const symbolBlock = document.createElement("div");
  const symbol = document.createElement("strong");
  const description = document.createElement("span");
  const status = document.createElement("span");

  header.className = "fundamentals-topline";
  symbolBlock.className = "fundamentals-symbol";
  symbol.textContent = payload.symbol || symbolInput.value.trim().toUpperCase();
  description.textContent = payload.description || profile.asset_type || "Company";
  status.className = "quote-status";
  status.textContent = profile.realtime === true ? "Realtime" : quote.security_status || "Snapshot";

  symbolBlock.appendChild(symbol);
  symbolBlock.appendChild(description);
  header.appendChild(symbolBlock);
  header.appendChild(status);
  fundamentalsWidget.appendChild(header);

  fundamentalsWidget.appendChild(
    createMetricGroup("Market", [
      { label: "Last", value: firstPresent(quote.last_price, quote.mark), format: "currency" },
      {
        label: "Change",
        value: firstPresent(quote.net_change, quote.mark_change),
        format: "currency",
      },
      {
        label: "Change %",
        value: firstPresent(quote.net_percent_change, quote.mark_percent_change),
        format: "percent",
      },
      {
        label: "Volume",
        value: firstPresent(quote.total_volume, fundamentals.avg_10_days_volume),
        format: "compact",
      },
      {
        label: "52W high",
        value: firstPresent(fundamentals.week_52_high, quote.week_52_high),
        format: "currency",
      },
      {
        label: "52W low",
        value: firstPresent(fundamentals.week_52_low, quote.week_52_low),
        format: "currency",
      },
    ])
  );

  fundamentalsWidget.appendChild(
    createMetricGroup("Valuation", [
      { label: "Market cap", value: fundamentals.market_cap, format: "compact" },
      { label: "P/E", value: fundamentals.pe_ratio },
      { label: "PEG", value: fundamentals.peg_ratio },
      { label: "Price/book", value: fundamentals.pb_ratio },
      { label: "EPS TTM", value: firstPresent(fundamentals.eps_ttm, fundamentals.eps) },
      { label: "Beta", value: fundamentals.beta },
    ])
  );

  fundamentalsWidget.appendChild(
    createMetricGroup("Quality", [
      { label: "Gross margin", value: fundamentals.gross_margin_ttm, format: "percent" },
      { label: "Operating margin", value: fundamentals.operating_margin_ttm, format: "percent" },
      { label: "Net margin", value: fundamentals.net_profit_margin_ttm, format: "percent" },
      { label: "ROE", value: fundamentals.return_on_equity, format: "percent" },
      { label: "Current ratio", value: fundamentals.current_ratio },
      { label: "Debt/equity", value: fundamentals.total_debt_to_equity },
    ])
  );
}

function renderTimeSeries(payload) {
  const interval = payload.interval || getSelectedInterval();
  const intervalLabel = getIntervalLabel(interval).toLowerCase();
  if (!payload.timeseries.length) {
    setEmptySurface(timeseriesChart, `No ${intervalLabel} price data is available for this range.`);
    return;
  }

  timeseriesChart.classList.remove("empty-surface");
  updateChartHeader(interval);
  Plotly.newPlot(
    timeseriesChart,
    [
      {
        type: "candlestick",
        x: payload.timeseries.map((row) => row.date),
        open: payload.timeseries.map((row) => row.open),
        high: payload.timeseries.map((row) => row.high),
        low: payload.timeseries.map((row) => row.low),
        close: payload.timeseries.map((row) => row.close),
        increasing: {
          line: { color: "#0a9396", width: 1.5 },
          fillcolor: "#94d2bd",
        },
        decreasing: {
          line: { color: "#bb3e03", width: 1.5 },
          fillcolor: "#ee9b00",
        },
      },
    ],
    {
      margin: { t: 24, r: 20, b: 48, l: 64 },
      paper_bgcolor: "#ffffff",
      plot_bgcolor: "#f8fbfb",
      xaxis: {
        title: interval === "daily" ? "Date" : "Time",
        gridcolor: "#d6e3e6",
        rangeslider: { visible: false },
      },
      yaxis: {
        title: "Price",
        gridcolor: "#d6e3e6",
      },
    },
    { responsive: true }
  );
}

function renderRangeSummary(payload) {
  const description = payload.description ? ` - ${payload.description}` : "";
  const selected = payload.selected_range;
  const available = payload.available_range;
  const intervalLabel = getIntervalLabel(payload.interval || getSelectedInterval());
  rangeSummary.textContent =
    `${payload.symbol}${description} | View: ${intervalLabel}` +
    ` | Selected: ${selected.start} to ${selected.end}` +
    ` | Available: ${available.start || "n/a"} to ${available.end || "n/a"}` +
    ` | Prices: ${payload.summary.price_points}`;
}

function buildStockDataQuery() {
  const interval = getSelectedInterval();
  const query = new URLSearchParams({
    symbol: symbolInput.value.trim().toUpperCase(),
    interval,
  });
  if (startDateInput.value) {
    query.set("start_date", startDateInput.value);
  }
  if (endDateInput.value) {
    query.set("end_date", endDateInput.value);
  }
  return query;
}

async function renderStockData({
  loadingMessage = "Loading data...",
  successMessage = "Chart updated.",
} = {}) {
  const interval = getSelectedInterval();
  updateChartHeader(interval);
  clearSuggestions();
  resetCharts();
  setStatus(loadingMessage, "info");
  rangeSummary.textContent = "";
  const query = buildStockDataQuery();

  try {
    const response = await fetch(`/api/stock-data?${query.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.message || "Failed to load stock data.");
    }

    renderRangeSummary(payload);
    renderCompanyInfo(payload);
    renderTimeSeries(payload);
    if (payload.message) {
      setStatus(payload.message, "warning");
    } else {
      setStatus("");
      showChartToast(successMessage);
    }
  } catch (error) {
    setEmptySurface(timeseriesChart, "Enter a symbol to render the candlestick chart.");
    setEmptySurface(fundamentalsWidget, "Enter a symbol to render company fundamentals.");
    setStatus(error.message, "error");
  }
}

async function loadStockData(event) {
  event.preventDefault();
  await renderStockData();
}

async function refreshLatestData() {
  if (!form.reportValidity()) {
    return;
  }

  const originalButtonText = refreshDataButton.textContent;
  refreshDataButton.disabled = true;
  refreshDataButton.textContent = "Refreshing...";
  clearSuggestions();
  setStatus("Fetching latest market data...", "info");

  try {
    const response = await fetch("/api/refresh-stock-data", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        symbol: symbolInput.value.trim().toUpperCase(),
      }),
    });
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.message || "Failed to refresh market data.");
    }

    await renderStockData({
      loadingMessage: "Loading refreshed chart...",
      successMessage: payload.message || "Latest data loaded.",
    });
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    refreshDataButton.disabled = false;
    refreshDataButton.textContent = originalButtonText;
  }
}

function handleSymbolInput() {
  activeSuggestionIndex = -1;
  symbolInput.removeAttribute("aria-activedescendant");
  setSuggestionsOpen(false);
  scheduleSuggestionLoad();
}

function handleSymbolKeydown(event) {
  if (event.key === "Escape") {
    clearSuggestions();
    return;
  }

  if (!currentSuggestions.length) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      scheduleSuggestionLoad();
    }
    return;
  }

  if (event.key === "ArrowDown") {
    event.preventDefault();
    const nextIndex = (activeSuggestionIndex + 1) % currentSuggestions.length;
    updateActiveSuggestion(nextIndex);
    setSuggestionsOpen(true);
    return;
  }

  if (event.key === "ArrowUp") {
    event.preventDefault();
    const nextIndex =
      activeSuggestionIndex <= 0 ? currentSuggestions.length - 1 : activeSuggestionIndex - 1;
    updateActiveSuggestion(nextIndex);
    setSuggestionsOpen(true);
    return;
  }

  if (event.key === "Enter" && activeSuggestionIndex >= 0) {
    event.preventDefault();
    selectSuggestion(activeSuggestionIndex);
  }
}

form.addEventListener("submit", loadStockData);
refreshDataButton.addEventListener("click", refreshLatestData);
portfolioRefreshButton.addEventListener("click", refreshPortfolioData);
appTabs.forEach((tab) => {
  tab.addEventListener("click", () => activateAppTab(tab.dataset.appTab));
});
portfolioHoldingsSortButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const sortKey = button.dataset.holdingsSort;
    if (portfolioHoldingsSort.key === sortKey) {
      portfolioHoldingsSort.direction =
        portfolioHoldingsSort.direction === "asc" ? "desc" : "asc";
    } else {
      portfolioHoldingsSort = {
        key: sortKey,
        direction: sortKey === "symbol" || sortKey === "category" ? "asc" : "desc",
      };
    }
    if (currentPortfolioPayload) {
      renderPortfolioHoldings(currentPortfolioPayload);
    } else {
      updateHoldingsSortButtons();
    }
  });
});
rangePresetButtons.forEach((button) => {
  button.addEventListener("click", () => {
    applyRangePreset(button.dataset.rangePreset, true);
  });
});
intervalInputs.forEach((input) => {
  input.addEventListener("change", () => {
    updateChartHeader();
    clearSuggestions();
  });
});
startDateInput.addEventListener("input", () => setActiveRangePreset(null));
endDateInput.addEventListener("input", () => setActiveRangePreset(null));
symbolInput.addEventListener("input", handleSymbolInput);
symbolInput.addEventListener("keydown", handleSymbolKeydown);
symbolInput.addEventListener("focus", () => {
  if (symbolInput.value.trim()) {
    scheduleSuggestionLoad();
  }
});
symbolInput.addEventListener("blur", () => {
  window.setTimeout(clearSuggestions, 120);
});

applyRangePreset("1m");
symbolInput.value = "AAPL";
updateChartHeader();
