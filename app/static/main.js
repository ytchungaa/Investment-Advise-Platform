const form = document.getElementById("stock-form");
const symbolInput = document.getElementById("symbol-input");
const symbolSuggestionList = document.getElementById("symbol-suggestion-list");
const startDateInput = document.getElementById("start-date-input");
const endDateInput = document.getElementById("end-date-input");
const statusMessage = document.getElementById("status-message");
const rangeSummary = document.getElementById("range-summary");
const timeseriesChart = document.getElementById("timeseries-chart");

let suggestionRequestId = 0;
let suggestionTimerId = null;
let currentSuggestions = [];
let activeSuggestionIndex = -1;

function formatDateInputValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function subtractCalendarMonth(date) {
  const targetYear = date.getMonth() === 0 ? date.getFullYear() - 1 : date.getFullYear();
  const targetMonth = (date.getMonth() + 11) % 12;
  const maxDay = new Date(targetYear, targetMonth + 1, 0).getDate();
  const targetDay = Math.min(date.getDate(), maxDay);
  return new Date(targetYear, targetMonth, targetDay);
}

function resetCharts() {
  Plotly.purge(timeseriesChart);
  timeseriesChart.classList.add("empty-surface");
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
    const response = await fetch(`/api/symbol-search?q=${encodeURIComponent(query)}`);
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

function setEmptySurface(element, message) {
  element.innerHTML = "";
  element.textContent = message;
  element.classList.add("empty-surface");
}

function renderTimeSeries(payload) {
  if (!payload.timeseries.length) {
    setEmptySurface(timeseriesChart, "No daily price data is available for this range.");
    return;
  }

  timeseriesChart.classList.remove("empty-surface");
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
        title: "Date",
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
  rangeSummary.textContent =
    `${payload.symbol}${description} | Selected: ${selected.start} to ${selected.end}` +
    ` | Available: ${available.start || "n/a"} to ${available.end || "n/a"}` +
    ` | Prices: ${payload.summary.price_points}`;
}

async function loadStockData(event) {
  event.preventDefault();
  clearSuggestions();
  resetCharts();
  setStatus("Loading data...", "info");
  rangeSummary.textContent = "";

  const query = new URLSearchParams({
    symbol: symbolInput.value.trim().toUpperCase(),
  });
  if (startDateInput.value) {
    query.set("start_date", startDateInput.value);
  }
  if (endDateInput.value) {
    query.set("end_date", endDateInput.value);
  }

  try {
    const response = await fetch(`/api/stock-data?${query.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.message || "Failed to load stock data.");
    }

    renderRangeSummary(payload);
    renderTimeSeries(payload);
    setStatus(payload.message || "Chart updated.", payload.message ? "warning" : "success");
  } catch (error) {
    setEmptySurface(timeseriesChart, "Enter a symbol to render the candlestick chart.");
    setStatus(error.message, "error");
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

const endDate = new Date();
const startDate = subtractCalendarMonth(endDate);
startDateInput.value = formatDateInputValue(startDate);
endDateInput.value = formatDateInputValue(endDate);
symbolInput.value = "AAPL";
