# Poly-15minBTC Dashboard Task

## Goal

Build a standalone local dashboard for the recorded `runtime_data/records` data so that the system can:

1. clearly display the current recorded markets,
2. switch between markets quickly,
3. compare several data streams inside one market,
4. keep the frontend decoupled from the trading runtime.

## Deliverables

### 1. Data parser

- File: `scripts/build_dashboard_data.py`
- Responsibility:
  - parse `runtime_data/records/global/*.jsonl`
  - parse `runtime_data/records/markets/<market_id>/*.jsonl`
  - tolerate missing topic files
  - emit one frontend-friendly JSON file
- Output:
  - `dashboard/data/market-dashboard.json`

### 2. Static frontend

- Files:
  - `dashboard/index.html`
  - `dashboard/styles.css`
  - `dashboard/app.js`
- Responsibility:
  - render a clear overview of the recorded system
  - support market switching
  - show global context and per-market context

### 3. Analysis views

The page should include at least these views:

- Global overview:
  - total markets
  - latest Binance mid
  - latest Chainlink price
  - latest basis
- Selected market summary:
  - phase
  - time window
  - event counts
  - latest theo / quote / book metrics
- Single-market comparison charts:
  - Binance vs Chainlink during market window
  - Theo UP vs UP mid vs quote prices
  - pair best bid/ask sums vs quote bid sum vs target cost

## Data mapping

### Global sources

- `feeds_binance_tick.jsonl`
  - latest Binance mid
  - global price context chart
- `feeds_chainlink_tick.jsonl`
  - latest Chainlink price
  - basis calculation

### Market sources

- `market_metadata.jsonl`
  - market identity
  - token mapping
  - time window
- `market_lifecycle_transition.jsonl`
  - phase display
  - lifecycle strip
- `feeds_polymarket_market_book_top.jsonl`
  - UP / DOWN best bid and ask
  - pair sum comparison
- `feeds_polymarket_market_depth.jsonl`
  - optional raw depth source, not shown in the current dashboard
- `pricing_theo.jsonl`
  - theo UP / DOWN
  - sigma
  - target full-set cost
- `pricing_quote_plan.jsonl`
  - market quote prices
  - quote bid sum / ask sum

## Implementation choice

The dashboard is implemented as a lightweight local service:

- `dashboard/server.py` serves the HTML / CSS / JS
- `scripts/build_dashboard_data.py` is imported by the server and executed on demand

Reason:

- the browser cannot directly run Python or read project files safely,
- every page refresh should reflect the latest `runtime_data/records`,
- no need to manually rebuild `dashboard/data/market-dashboard.json`,
- still no need to add a full frontend toolchain.

## Usage

### Serve dashboard locally

```bash
python dashboard/server.py
```

Then open:

`http://localhost:8000`

### Optional static snapshot build

If you still want to export a static JSON snapshot for inspection:

```bash
python scripts/build_dashboard_data.py
```

## Follow-up tasks

- add user execution / order state analytics when those recordings are available
- add snapshot and alert views
- add replay report ingestion
- add per-market export to CSV
- add direct link from one market card to raw JSONL files
