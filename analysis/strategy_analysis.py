#!/usr/bin/env python3
"""
Polymarket 15min BTC - Trading Strategy Analysis
=================================================
Analyzes three opportunity types:
  1. Sum-deviation arbitrage (best_ask_up + best_ask_down < threshold)
  2. Binance-Poly lag arbitrage (Binance moved, Poly hasn't updated)
  3. Phase-based edge (FAST_CLOSE / FINAL_SECONDS price drift)
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

RECORDS_ROOT = Path("/Volumes/captain/code/poly-15min/Poly-15minBTC/runtime_data/records")
OUTPUT_DIR = Path("/Volumes/captain/code/poly-15min/Poly-15minBTC/analysis/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── helpers ──────────────────────────────────────────────────────────────────

def iter_jsonl(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_topic_across_markets(
    topic_dir_name: str,
    max_markets: int | None = None,
    max_lines_per_file: int | None = None,
) -> list[dict]:
    """Load all JSONL records for a topic from every market directory."""
    rows: list[dict] = []
    market_dirs = sorted((RECORDS_ROOT / "markets").iterdir())
    if max_markets:
        market_dirs = market_dirs[:max_markets]

    for mdir in market_dirs:
        topic_dir = mdir / topic_dir_name
        if not topic_dir.exists():
            continue
        for jfile in sorted(topic_dir.iterdir()):
            count = 0
            for rec in iter_jsonl(jfile):
                rows.append(rec)
                count += 1
                if max_lines_per_file and count >= max_lines_per_file:
                    break
    return rows


def load_global_topic(topic_dir_name: str) -> list[dict]:
    topic_dir = RECORDS_ROOT / "global" / topic_dir_name
    rows = []
    if topic_dir.exists():
        for jfile in sorted(topic_dir.iterdir()):
            for rec in iter_jsonl(jfile):
                rows.append(rec)
    return rows


# ─── 1. Sum-deviation arbitrage ───────────────────────────────────────────────

def analyze_sum_deviation():
    """
    Find moments when best_ask_up + best_ask_down < target_full_set_cost (≈0.996).
    This is a RISK-FREE arbitrage: buy both tokens for < $1, collect $1 at settlement.

    Also find best_bid_up + best_bid_down > 1.0 (sell-side arb, harder to execute).
    """
    print("\n=== 1. SUM-DEVIATION ARBITRAGE ===")

    # We need to join book_top events (two per tick: one per token side)
    # Group by (market_id, last_update_ts_ms) to pair UP and DOWN tokens
    book_top_raw = load_topic_across_markets("feeds_polymarket_market_book_top")
    print(f"Loaded {len(book_top_raw):,} book_top events from per-market dirs")

    # Also load theo for target_full_set_cost
    theo_raw = load_topic_across_markets("pricing_theo")
    # Build a per-market target cost lookup (use latest value)
    target_cost: dict[str, float] = {}
    for rec in theo_raw:
        snap = rec["payload"]["snapshot"]
        target_cost[snap["market_id"]] = float(snap["target_full_set_cost"])

    # Build per-market token → side mapping from metadata
    # Token IDs: one is UP token, one is DOWN. We identify by checking which
    # ask price is low (near 0 → loser) or by finding pairs.
    # Strategy: group by (market_id, last_update_ts_ms), then pair the two tokens.

    from collections import defaultdict

    # Group by (market_id, update_ts)
    groups: dict[tuple, list] = defaultdict(list)
    for rec in book_top_raw:
        top = rec["payload"]["top"]
        key = (rec["market_id"], top["last_update_ts_ms"])
        groups[key].append({
            "recv_ts_ms": rec["recv_ts_ms"],
            "token_id": top["token_id"],
            "best_bid_px": float(top["best_bid_px"]),
            "best_bid_sz": float(top["best_bid_sz"]),
            "best_ask_px": float(top["best_ask_px"]),
            "best_ask_sz": float(top["best_ask_sz"]),
        })

    arb_rows = []
    total_pairs = 0
    for (market_id, ts), sides in groups.items():
        if len(sides) != 2:
            continue
        total_pairs += 1
        a, b = sides[0], sides[1]

        sum_ask = a["best_ask_px"] + b["best_ask_px"]
        sum_bid = a["best_bid_px"] + b["best_bid_px"]
        cost = target_cost.get(market_id, 0.996)

        # Buy-side arb: buy both for sum_ask < cost
        buy_arb_pnl = cost - sum_ask  # profit if positive (before fees)
        # Sell-side arb: if sum_bid > 1.0 you collect > $1 but need collateral
        sell_arb_pnl = sum_bid - 1.0

        arb_rows.append({
            "market_id": market_id,
            "ts_ms": ts,
            "sum_ask": sum_ask,
            "sum_bid": sum_bid,
            "target_cost": cost,
            "buy_arb_pnl": buy_arb_pnl,
            "sell_arb_pnl": sell_arb_pnl,
            "min_ask_size": min(a["best_ask_sz"], b["best_ask_sz"]),
            "min_bid_size": min(a["best_bid_sz"], b["best_bid_sz"]),
        })

    df = pd.DataFrame(arb_rows)
    print(f"Total paired snapshots: {total_pairs:,}")

    # sum_ask stats
    print(f"\nsum_ask distribution:")
    print(df["sum_ask"].describe().to_string())

    # Buy arb opportunities (sum_ask < 0.996)
    buy_arb = df[df["buy_arb_pnl"] > 0].copy()
    print(f"\nBuy-arb opportunities (sum_ask < target_cost): {len(buy_arb):,} / {len(df):,} = {len(buy_arb)/len(df)*100:.2f}%")
    if len(buy_arb) > 0:
        print(f"  avg pnl per unit: ${buy_arb['buy_arb_pnl'].mean():.4f}")
        print(f"  max pnl per unit: ${buy_arb['buy_arb_pnl'].max():.4f}")
        print(f"  avg tradable size: {buy_arb['min_ask_size'].mean():.1f} USDC")
        print(f"  markets affected: {buy_arb['market_id'].nunique()}")
        print(f"\n  Top 10 buy-arb moments:")
        top = buy_arb.nlargest(10, "buy_arb_pnl")[
            ["market_id", "ts_ms", "sum_ask", "buy_arb_pnl", "min_ask_size"]
        ]
        print(top.to_string(index=False))

    # Sell arb (sum_bid > 1.0)
    sell_arb = df[df["sell_arb_pnl"] > 0]
    print(f"\nSell-arb opportunities (sum_bid > 1.0): {len(sell_arb):,} / {len(df):,} = {len(sell_arb)/len(df)*100:.2f}%")
    if len(sell_arb) > 0:
        print(f"  avg pnl per unit: ${sell_arb['sell_arb_pnl'].mean():.4f}")
        print(f"  avg tradable size: {sell_arb['min_bid_size'].mean():.1f} USDC")

    # sum_ask histogram buckets
    print(f"\nsum_ask frequency by bucket:")
    buckets = [0.95, 0.97, 0.990, 0.992, 0.994, 0.996, 0.998, 1.000, 1.002, 1.005, 1.01, 1.05]
    hist, edges = np.histogram(df["sum_ask"].dropna(), bins=buckets)
    for i, cnt in enumerate(hist):
        print(f"  [{edges[i]:.3f}, {edges[i+1]:.3f}): {cnt:>8,}  ({cnt/len(df)*100:.2f}%)")

    df.to_parquet(OUTPUT_DIR / "sum_deviation_analysis.parquet")
    buy_arb.to_parquet(OUTPUT_DIR / "buy_arb_opportunities.parquet")
    print(f"\nSaved to {OUTPUT_DIR}/sum_deviation_analysis.parquet")
    return df, buy_arb


# ─── 2. Binance-Poly lag arbitrage ────────────────────────────────────────────

def analyze_binance_poly_lag():
    """
    Identify moments where Binance BTC price moved significantly but
    Polymarket prices haven't updated yet. This is a time-lag opportunity.

    Win condition: after Binance impulse, Poly eventually catches up.
    High-win-rate definition: trade BEFORE the catch-up, exit AFTER.

    Uses the pre-computed research events for signal detection,
    then quantifies edge from actual data.
    """
    print("\n=== 2. BINANCE-POLY LAG ARBITRAGE ===")

    research_dir = Path("/Volumes/captain/code/poly-15min/Poly-15minBTC/runtime_data/research/events")
    lag_events_path = research_dir / "binance_impulse_without_poly_refresh.jsonl"
    lag_summary_path = research_dir / "binance_impulse_without_poly_refresh.summary.json"

    with open(lag_summary_path) as f:
        summary = json.load(f)

    print(f"Total lag events: {summary['count']}")
    print(f"Unique markets: {summary['unique_markets']}")
    print(f"\nBy direction:")

    for direction, stats in summary["directional"].items():
        cnt = stats["count"]
        delay_ms = stats["avg_poly_refresh_delay_ms"]
        impulse_bps = stats["avg_impulse_abs_bps"]
        refresh_move = stats["avg_first_refresh_up_mid_move_signed"]
        gap_close = stats["avg_first_refresh_theo_gap_close_signed"]

        print(f"\n  {direction.upper()} ({cnt} events):")
        print(f"    avg Binance impulse: {impulse_bps:.2f} bps")
        print(f"    avg Poly refresh delay: {delay_ms/1000:.1f}s")
        print(f"    avg up_mid move on first refresh (signed): {refresh_move:+.4f}")
        print(f"    avg theo gap closure on first refresh: {gap_close:+.4f}")

        # Win rate logic:
        # For DOWN: enter short UP (buy DOWN) before refresh → profit when up_mid falls
        # For UP:   enter long UP (buy UP) before refresh → profit when up_mid rises
        # "Winning" = price moved in the expected direction

    # Load individual lag events for deeper analysis
    lag_events = []
    if lag_events_path.exists():
        for rec in iter_jsonl(lag_events_path):
            lag_events.append(rec)

    print(f"\nLoaded {len(lag_events)} individual lag event records")

    if lag_events:
        df_lag = pd.DataFrame(lag_events)
        print(f"Columns: {list(df_lag.columns)}")

        # Extract key fields
        if "impulse_direction" in df_lag.columns:
            print(f"\nImpulse direction distribution:")
            print(df_lag["impulse_direction"].value_counts().to_string())

        if "impulse_bps" in df_lag.columns and "poly_refresh_delay_ms" in df_lag.columns:
            print(f"\nImpulse size vs delay:")
            print(df_lag[["impulse_bps", "poly_refresh_delay_ms"]].describe().to_string())

        df_lag.to_parquet(OUTPUT_DIR / "lag_events.parquet")

    print(f"""
Key takeaways for lag strategy:
  - DOWN impulses: avg delay 39.2s → strong window before Poly reacts
  - Signal: Binance drops ≥ 5bps with no Poly book update in 3s
  - Entry: buy DOWN token (or sell UP) immediately after signal
  - Target exit: Poly first refresh, or +10s timeout
  - Risk: Binance reversal before Poly refreshes (30% of gap closes to 0 in first refresh)
  - High-win-rate filter: only trade when delay already >3s AND impulse ≥7bps
""")

    return lag_events


# ─── 3. Phase-based edge (FAST_CLOSE / FINAL_SECONDS) ─────────────────────────

def analyze_phase_edge():
    """
    When market enters FAST_CLOSE or FINAL_SECONDS, prices drift hard
    toward 0 or 1. Find the momentum edge.

    Data shows: entering FAST_CLOSE, up_mid_move at 30s = -0.019
    → systematic downward drift in UP token during close phase
    (likely because slightly-losing markets go from ~0.4 → 0)
    """
    print("\n=== 3. FAST_CLOSE PHASE EDGE ===")

    research_dir = Path("/Volumes/captain/code/poly-15min/Poly-15minBTC/runtime_data/research/events")

    with open(research_dir / "market_fast_close_enter.summary.json") as f:
        fc_summary = json.load(f)

    print(f"Fast-close events: {fc_summary['count']}")
    print(f"\nPrice drift after FAST_CLOSE entry:")
    for horizon, stats in fc_summary["horizons"].items():
        up_move = stats.get("avg_up_mid_move")
        sum_ask_move = stats.get("avg_sum_best_ask_move")
        size = stats.get("avg_tradable_size")
        if up_move is not None:
            print(f"  +{horizon}: up_mid={up_move:+.4f}, sum_ask_move={sum_ask_move:+.4f}, tradable_size={size:.0f}")

    print(f"""
Key insight:
  - sum_best_ask RISES during FAST_CLOSE (+0.0023 to +0.0031)
  - This means BOTH tokens become more expensive to buy near the end
  - Tradable size INCREASES dramatically (157 → 611 USDC) near close
  - Strategy: if you're already holding a position, FAST_CLOSE is exit time
  - Maker strategy: quote WIDE during FAST_CLOSE (get filled by panicking traders)

Adjacent market handoff:
""")

    with open(research_dir / "adjacent_market_handoff.summary.json") as f:
        handoff_summary = json.load(f)

    print(f"Handoff events: {handoff_summary['count']} (PREWARM: {handoff_summary['phase_counts']['PREWARM']}, ACTIVE: {handoff_summary['phase_counts']['ACTIVE']})")
    for horizon, stats in handoff_summary["horizons"].items():
        up_move = stats.get("avg_up_mid_move")
        mfe = stats.get("avg_mfe_up_mid")
        mae = stats.get("avg_mae_up_mid")
        print(f"  +{horizon}: up_mid={up_move:+.4f}, MFE={mfe:+.4f}, MAE={mae:+.4f}")

    print(f"""
Handoff insight:
  - At market open (handoff), up_mid jumps +0.003 in first 1s
  - MFE (+0.008) > MAE (-0.003) at 1s horizon → positive skew
  - But MAE grows fast: at 30s, MFE=+0.052 vs MAE=-0.052 (symmetric)
  - Short-lived edge: the opening 1-3 seconds have positive UP bias
  - This is likely because the system initializes conservative (under-pricing UP)
""")


# ─── 4. Theoretical price model ───────────────────────────────────────────────

def explain_theo_model():
    """
    Explain how theo_up/theo_down are computed.
    """
    print("\n=== 4. THEORETICAL PRICE MODEL ===")
    print("""
The theo prices are computed using a binary option pricing model:

  theo_up  = P(BTC_end > K | BTC_now, σ, T)
  theo_down = 1 - theo_up

Where:
  K     = reference_price (strike, fixed at market open)
  BTC_now = current Binance/Chainlink mid price
  σ     = sigma_short (short-window realized volatility, updated continuously)
  T     = time remaining to market expiry (seconds)

The formula approximates Black-Scholes binary call:
  theo_up = N(d2)
  d2 = [log(S/K) + (0 - 0.5σ²)T] / (σ√T)

Key behaviors:
  - When S >> K (BTC well above strike): theo_up → 1.0, theo_down → 0.001
  - When T → 0 (final seconds): theo converges rapidly to 0 or 1
  - sigma_short starts very large (>0.0005) at open, compresses as time passes
  - target_full_set_cost = 0.996 = 1 - protocol_fee (0.4% round-trip implied)

The GAP between theo and market price is your edge signal:
  edge = theo_up - market_mid_up  (positive = UP is cheap vs fair value)
""")

    # Show theo convergence across a sample market
    print("Sample theo evolution (first 10 records from market 1893294):")
    theo_file = RECORDS_ROOT / "markets/1893294/pricing_theo/00000000000000000001.jsonl"
    if theo_file.exists():
        rows = []
        for i, rec in enumerate(iter_jsonl(theo_file)):
            if i >= 10:
                break
            snap = rec["payload"]["snapshot"]
            rows.append({
                "ts_ms": snap["ts_ms"],
                "theo_up": float(snap["theo_up"]),
                "theo_down": float(snap["theo_down"]),
                "sigma_short": float(snap["sigma_short"]),
            })
        df = pd.DataFrame(rows)
        print(df.to_string(index=False))


# ─── 5. High win-rate definition framework ────────────────────────────────────

def explain_win_rate_framework():
    """
    Define what "high win rate" means for each strategy.
    """
    print("""
=== 5. HIGH WIN-RATE FRAMEWORK ===

The question: "What is a high-probability trade?"

For POLYMARKET binary markets, we need to distinguish 3 types of edge:

┌─────────────────────────────────────────────────────────────────────────────┐
│ TYPE 1: STRUCTURAL / MATHEMATICAL EDGE (≈100% win rate)                    │
│                                                                              │
│  Condition: sum_best_ask < target_full_set_cost (0.996)                     │
│  Action: buy UP + DOWN simultaneously                                        │
│  Outcome: always collect $1 at settlement, paid $0.996 or less              │
│  Win rate: ~100% (counterparty/settlement risk only)                        │
│  Frequency: rare - need to monitor continuously                             │
│  Execution: MUST be atomic or near-simultaneous (race condition)            │
│                                                                              │
│  DEFINE "PROFITABLE": any fill where up_ask + down_ask < 0.996              │
│  (Realized PnL = $1 - sum_fill_prices, always positive by construction)    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ TYPE 2: INFORMATION-LAG EDGE (estimated 65-75% win rate)                   │
│                                                                              │
│  Condition: Binance impulse ≥7bps AND poly not updated in >3s              │
│  Action: buy DOWN (or UP, direction-matched) at stale market ask            │
│  Outcome: Poly reprices toward BTC → your position gains                   │
│  Win rate: ~60-75% (Binance sometimes reverses before Poly catches up)     │
│                                                                              │
│  DEFINE "PROFITABLE":                                                        │
│    Entry price: market_ask at signal time                                   │
│    Exit price: market_bid at first Poly refresh (or +10s timeout)          │
│    Profitable if: exit_bid > entry_ask (you lifted ask, sold into new bid) │
│    i.e., theo_gap > 2 * market_spread                                       │
│                                                                              │
│  From data: avg first_refresh up_mid move (signed) = +0.003-0.009          │
│  Typical market spread: 0.01-0.04                                           │
│  → Edge is marginal. Only works with tight spreads or large impulses.      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ TYPE 3: THEO-VS-MARKET EDGE (statistical, ~55-65% win rate)                │
│                                                                              │
│  Condition: |theo - market_mid| > threshold (e.g., 0.015)                 │
│  AND sigma_short is stable (not in opening chaos)                           │
│  Action: buy cheap side, wait for convergence                               │
│  Outcome: market eventually aligns to theo                                  │
│                                                                              │
│  DEFINE "PROFITABLE":                                                        │
│    Entry: buy at ask when theo - ask > 0.01                                │
│    Exit: sell at bid when bid > entry_ask, OR at settlement for $1 or $0   │
│    Profitable if: (bid - entry_ask) > 0 within holding period              │
│                                                                              │
│  Key: holding to settlement is ALWAYS the correct decision if              │
│    you bought at < theo_value, because E[settlement] = theo                │
│    (assuming theo model is correct)                                         │
└─────────────────────────────────────────────────────────────────────────────┘

PRACTICAL HIGH-WIN-RATE FILTER:
  1. Only trade when theo-to-market gap > 1.5x the market spread
  2. For lag trades: require confirmed hold (signal held >5s without reversal)
  3. Always compute: expected_pnl = (theo_probability × pnl_if_right)
                                   - ((1-theo_probability) × pnl_if_wrong)
  4. Only trade if expected_pnl / capital > 0.5% per trade
""")


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    print("Polymarket 15min BTC – Strategy Analysis")
    print("=" * 60)

    explain_theo_model()
    explain_win_rate_framework()

    try:
        df_arb, buy_arb = analyze_sum_deviation()
    except Exception as e:
        print(f"Sum deviation analysis failed: {e}")

    analyze_binance_poly_lag()
    analyze_phase_edge()

    print("\n=== SUMMARY: RECOMMENDED PRIORITY ORDER ===")
    print("""
PRIORITY 1 — Sum Arb (sum_ask < 0.996)
  Risk-free. Execute if detected. Rare but zero-risk.
  Action: Monitor sum_best_ask in real-time. Alert when < 0.994 (margin for slippage).

PRIORITY 2 — Lag Arb (Binance moved, Poly stale >5s)
  Most consistent edge in the data. 39s average delay for DOWN moves.
  Action: Build a signal detector: if |Δbinance_mid| > 5bps in 1s AND
          last poly book update > 3s ago → enter direction-matched trade.
  Exit: at first poly refresh or +15s timeout.

PRIORITY 3 — FAST_CLOSE maker quoting
  During FAST_CLOSE, sum_ask rises (market gets expensive).
  If you can post bids/asks wide, you capture the spread from panicking traders.
  Requires: maker infrastructure (post limit orders, not market orders).

PRIORITY 4 — Opening bias (first 1-3s of new market)
  Handoff data shows +0.003 up_mid move in first second.
  Small but consistent. Only useful with very low latency.
""")


if __name__ == "__main__":
    main()
