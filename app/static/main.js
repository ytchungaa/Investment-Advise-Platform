const form = document.getElementById("stock-form");
const symbolInput = document.getElementById("symbol-input");
const symbolSuggestionList = document.getElementById("symbol-suggestion-list");
const startDateInput = document.getElementById("start-date-input");
const endDateInput = document.getElementById("end-date-input");
const statusMessage = document.getElementById("status-message");
const rangeSummary = document.getElementById("range-summary");
const timeseriesChart = document.getElementById("timeseries-chart");
const boxplotChart = document.getElementById("boxplot-chart");

let suggestionRequestId = 0;
let suggestionTimerId = null;

function isoDateDaysAgo(daysAgo) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - daysAgo);
  return date.toISOString().slice(0, 10);
}

function resetCharts() {
  Plotly.purge(timeseriesChart);
  Plotly.purge(boxplotChart);
  timeseriesChart.classList.add("empty-surface");
  boxplotChart.classList.add("empty-surface");
}

function clearSuggestions() {
  symbolSuggestionList.innerHTML = "";
}

function renderSuggestions(results) {
  clearSuggestions();
  results.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.symbol;
    option.label = item.description || item.symbol;
    symbolSuggestionList.appendChild(option);
  });
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
    setEmptySurface(timeseriesChart, "No close-price data is available for this range.");
    return;
  }

  timeseriesChart.classList.remove("empty-surface");
  Plotly.newPlot(
    timeseriesChart,
    [
      {
        type: "scatter",
        mode: "lines",
        x: payload.timeseries.map((row) => row.date),
        y: payload.timeseries.map((row) => row.close),
        line: {
          color: "#005f73",
          width: 2.5,
        },
        hovertemplate: "%{x}<br>Close: %{y:.2f}<extra></extra>",
      },
    ],
    {
      margin: { t: 24, r: 16, b: 48, l: 56 },
      paper_bgcolor: "#ffffff",
      plot_bgcolor: "#f8fbfb",
      xaxis: {
        title: "Date",
        gridcolor: "#d6e3e6",
      },
      yaxis: {
        title: "Close",
        gridcolor: "#d6e3e6",
      },
    },
    { responsive: true }
  );
}

function renderBoxPlot(payload) {
  if (!payload.monthly_return_boxes.length) {
    setEmptySurface(
      boxplotChart,
      "Return distribution is unavailable because there are fewer than two price points."
    );
    return;
  }

  boxplotChart.classList.remove("empty-surface");
  const traces = payload.monthly_return_boxes.map((box) => ({
    type: "box",
    name: box.month_label,
    y: box.returns,
    marker: { color: "#ee9b00" },
    line: { color: "#bb3e03" },
    boxpoints: "outliers",
    hovertemplate: `${box.month_label}<br>Return: %{y:.2%}<extra></extra>`,
  }));

  Plotly.newPlot(
    boxplotChart,
    traces,
    {
      margin: { t: 24, r: 16, b: 48, l: 56 },
      paper_bgcolor: "#ffffff",
      plot_bgcolor: "#fff8ef",
      xaxis: {
        title: "Month",
      },
      yaxis: {
        title: "Daily Return",
        tickformat: ".0%",
        gridcolor: "#f2dcc1",
      },
      showlegend: false,
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
    ` | Prices: ${payload.summary.price_points} | Returns: ${payload.summary.return_points}`;
}

async function loadStockData(event) {
  event.preventDefault();
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
    renderBoxPlot(payload);
    setStatus(payload.message || "Charts updated.", payload.message ? "warning" : "success");
  } catch (error) {
    setEmptySurface(timeseriesChart, "Enter a symbol to render the time series.");
    setEmptySurface(boxplotChart, "The box plot will appear here after the data loads.");
    setStatus(error.message, "error");
  }
}

form.addEventListener("submit", loadStockData);
symbolInput.addEventListener("input", scheduleSuggestionLoad);
symbolInput.addEventListener("focus", () => {
  if (symbolInput.value.trim()) {
    scheduleSuggestionLoad();
  }
});
startDateInput.value = isoDateDaysAgo(365);
endDateInput.value = isoDateDaysAgo(0);
symbolInput.value = "AAPL";
