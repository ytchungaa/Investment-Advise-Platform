#!/usr/bin/env python3

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
import json
import sys
import threading
import time
from pathlib import Path

import pandas as pd

REPO_DIR = Path(__file__).resolve().parents[1]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

from database_connect import connector
from get_market_data import schwab_api_market


WINDOW_DAYS = 10
DEFAULT_EMPTY_WINDOW_STOP = 6
DEFAULT_SLEEP_SECONDS = 0.0
DEFAULT_PAUSE_EVERY = 0
DEFAULT_PAUSE_SECONDS = 0.0
DEFAULT_MAX_WORKERS = 4
DEFAULT_REQUESTS_PER_MINUTE = 110


_worker_state = threading.local()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill 1-minute price history for the active watch list in 10-day windows. "
            "Progress is checkpointed so the job can be resumed."
        )
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=20,
        help="How many years back to attempt. Default: 20.",
    )
    parser.add_argument(
        "--end-date",
        default=pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
        help="UTC end date in YYYY-MM-DD format. Default: today in UTC.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help=f"Sleep after each price-history request. Default: {DEFAULT_SLEEP_SECONDS}.",
    )
    parser.add_argument(
        "--pause-every",
        type=int,
        default=DEFAULT_PAUSE_EVERY,
        help=f"Take a longer pause every N requests. Default: {DEFAULT_PAUSE_EVERY}.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=DEFAULT_PAUSE_SECONDS,
        help=f"Pause duration when the request counter hits --pause-every. Default: {DEFAULT_PAUSE_SECONDS}.",
    )
    parser.add_argument(
        "--stop-after-empty-windows",
        type=int,
        default=DEFAULT_EMPTY_WINDOW_STOP,
        help=(
            "Stop going further back for one symbol after this many consecutive empty 10-day "
            f"windows. Default: {DEFAULT_EMPTY_WINDOW_STOP}."
        ),
    )
    parser.add_argument(
        "--checkpoint-file",
        default="logs/watch_list_one_minute_backfill_checkpoint.json",
        help="Checkpoint file path. Default: logs/watch_list_one_minute_backfill_checkpoint.json",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Optional cap for testing on a subset of the watch list.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore any previous checkpoint and start from the newest window again.",
    )
    parser.add_argument(
        "--frontload-years",
        type=int,
        default=0,
        help=(
            "Process the most recent N years for the full watch list before continuing "
            "into older windows. Set to 1 to load the current year first. Default: 0."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Concurrent price-history workers. Default: {DEFAULT_MAX_WORKERS}.",
    )
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=DEFAULT_REQUESTS_PER_MINUTE,
        help=(
            "Global request pace cap across all workers. Set to 0 to disable pacing. "
            f"Default: {DEFAULT_REQUESTS_PER_MINUTE}."
        ),
    )
    return parser.parse_args()


def load_watch_list(db_dwd: connector, max_symbols: int | None = None) -> list[str]:
    watch_list_df = db_dwd.query_dataframe(
        """
        SELECT symbol
        FROM watch_list
        WHERE is_active IS TRUE
        ORDER BY symbol;
        """
    )
    if watch_list_df.empty:
        return []

    symbols = watch_list_df["symbol"].dropna().drop_duplicates().tolist()
    if max_symbols is not None:
        return symbols[:max_symbols]
    return symbols


def build_windows(end_date: str, years_back: int) -> list[dict[str, str]]:
    end_ts = pd.Timestamp(end_date, tz="UTC").normalize()
    start_limit = end_ts - pd.DateOffset(years=years_back)
    windows: list[dict[str, str]] = []
    window_end = end_ts

    while window_end >= start_limit:
        window_start = max(start_limit, window_end - pd.Timedelta(days=WINDOW_DAYS - 1))
        period = (window_end - window_start).days + 1
        windows.append(
            {
                "start_date": window_start.strftime("%Y-%m-%d"),
                "end_date": window_end.strftime("%Y-%m-%d"),
                "period": str(period),
            }
        )
        window_end = window_start - pd.Timedelta(days=1)

    return windows


def build_window_ranges(
    end_date: str,
    years_back: int,
    frontload_years: int,
    total_windows: int,
) -> list[tuple[int, int]]:
    if frontload_years <= 0 or frontload_years >= years_back:
        return [(0, total_windows)]

    frontload_window_count = len(build_windows(end_date, frontload_years))
    frontload_window_count = min(frontload_window_count, total_windows)
    if frontload_window_count <= 0 or frontload_window_count >= total_windows:
        return [(0, total_windows)]

    return [(0, frontload_window_count), (frontload_window_count, total_windows)]


def load_checkpoint(path: Path, reset: bool) -> dict:
    if reset or not path.exists():
        return {"symbols": {}, "request_count": 0}
    return json.loads(path.read_text())


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["saved_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    path.write_text(json.dumps(checkpoint, indent=2, sort_keys=True))


def ensure_instruments(db_ods: connector, api: schwab_api_market, symbols: list[str]) -> pd.DataFrame:
    instruments_df = api.fetch_instruments(symbols)
    if instruments_df.empty:
        return pd.DataFrame()

    db_ods.upsert_dataframe(
        instruments_df,
        table_name="instrument",
        conflict_columns=["symbol", "asset_type"],
        update_columns=[
            "cusip",
            "description",
            "exchange",
            "asset_main_type",
            "asset_sub_type",
            "quote_type",
            "ssid",
            "realtime",
            "last_seen_at",
        ],
        chunksize=100,
    )

    return db_ods.query_dataframe(
        """
        SELECT id AS instrument_id, symbol, asset_type
        FROM instrument
        ORDER BY symbol, id;
        """
    )


def ensure_price_history_frequency_types(db_ods: connector) -> None:
    db_ods.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history_frequency_type (
            id SMALLINT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE
        );
        """
    )
    db_ods.execute(
        """
        INSERT INTO price_history_frequency_type (id, code)
        VALUES
            (1, 'minute'),
            (2, 'daily'),
            (3, 'weekly'),
            (4, 'monthly')
        ON CONFLICT (id) DO UPDATE
        SET code = EXCLUDED.code;
        """
    )


def upsert_price_history(db_ods: connector, instrument_id: int, price_df: pd.DataFrame) -> int:
    if price_df.empty:
        return 0

    upload_df = price_df.copy()
    upload_df["instrument_id"] = instrument_id

    success = db_ods.upsert_dataframe(
        upload_df.drop(columns=["symbol"]),
        table_name="price_history",
        conflict_columns=["instrument_id", "frequency_type", "frequency", "candle_time"],
        update_columns=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "previous_close",
            "previous_close_time",
            "need_extended_hours_data",
        ],
        chunksize=500,
    )
    if not success:
        raise RuntimeError(f"Failed to upsert price history for instrument_id={instrument_id}.")
    return len(upload_df)


def maybe_pause(request_count: int, pause_every: int, pause_seconds: float) -> None:
    if pause_every <= 0:
        return
    if request_count % pause_every == 0:
        print(
            f"[throttle] completed {request_count} requests, pausing for {pause_seconds} seconds",
            flush=True,
        )
        time.sleep(pause_seconds)


def default_symbol_state() -> dict[str, int | bool]:
    return {
        "next_window_index": 0,
        "empty_streak": 0,
        "rows_loaded": 0,
        "done": False,
    }


@dataclass
class FetchTaskResult:
    symbol: str
    window_index: int
    price_df: pd.DataFrame
    error: str | None = None


class RequestPacer:
    def __init__(self, requests_per_minute: int):
        self._interval_seconds = 0.0
        if requests_per_minute > 0:
            self._interval_seconds = 60.0 / requests_per_minute
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0

    def wait_for_slot(self) -> None:
        if self._interval_seconds <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed_at:
                    self._next_allowed_at = now + self._interval_seconds
                    return
                sleep_seconds = self._next_allowed_at - now
            time.sleep(sleep_seconds)


def get_worker_api() -> schwab_api_market:
    api = getattr(_worker_state, "api", None)
    if api is None:
        api = schwab_api_market()
        _worker_state.api = api
    return api


def fetch_price_history_window(symbol: str, window_index: int, window: dict[str, str]) -> FetchTaskResult:
    try:
        price_df = get_worker_api().fetch_price_history(
            symbol=symbol,
            period_type="day",
            period=window["period"],
            frequency_type="minute",
            frequency="1",
            start_date=window["start_date"],
            end_date=window["end_date"],
            need_extended_hours_data=True,
            need_previous_close=True,
        )
        return FetchTaskResult(symbol=symbol, window_index=window_index, price_df=price_df)
    except Exception as exc:  # pragma: no cover - defensive path for long-running jobs
        return FetchTaskResult(
            symbol=symbol,
            window_index=window_index,
            price_df=pd.DataFrame(),
            error=str(exc),
        )


def process_window_range(
    *,
    symbols: list[str],
    windows: list[dict[str, str]],
    range_start: int,
    range_end: int,
    instrument_map: dict[str, int],
    checkpoint: dict,
    checkpoint_path: Path,
    db_ods: connector,
    args: argparse.Namespace,
) -> int:
    if range_start >= range_end:
        return 0

    total_rows = 0
    request_count = int(checkpoint.get("request_count", 0))
    pacer = RequestPacer(args.requests_per_minute)
    max_workers = max(1, args.max_workers)
    failed_symbols: set[str] = set()

    print(
        f"[phase] processing windows {range_start + 1}-{range_end} of {len(windows)}",
        flush=True,
    )

    for symbol in symbols:
        if symbol not in checkpoint["symbols"]:
            checkpoint["symbols"][symbol] = default_symbol_state()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for window_index in range(range_start, range_end):
            window = windows[window_index]
            pending_symbols: list[tuple[str, int]] = []

            for symbol in symbols:
                instrument_id = instrument_map.get(symbol)
                if instrument_id is None:
                    print(f"[skip] {symbol}: no instrument_id found after instrument sync", flush=True)
                    continue

                symbol_state = checkpoint["symbols"].setdefault(symbol, default_symbol_state())
                if symbol_state.get("done"):
                    continue
                if int(symbol_state["next_window_index"]) != window_index:
                    continue
                pending_symbols.append((symbol, instrument_id))

            if not pending_symbols:
                continue

            active_futures: dict = {}
            next_symbol_index = 0

            while next_symbol_index < len(pending_symbols) or active_futures:
                while next_symbol_index < len(pending_symbols) and len(active_futures) < max_workers:
                    symbol, instrument_id = pending_symbols[next_symbol_index]
                    next_symbol_index += 1
                    pacer.wait_for_slot()
                    request_count += 1
                    print(
                        (
                            f"[fetch] {symbol} window {window_index + 1}/{len(windows)} "
                            f"{window['start_date']} -> {window['end_date']}"
                        ),
                        flush=True,
                    )
                    future = executor.submit(fetch_price_history_window, symbol, window_index, window)
                    active_futures[future] = (symbol, instrument_id)
                    maybe_pause(request_count, args.pause_every, args.pause_seconds)

                done_futures, _ = wait(active_futures, return_when=FIRST_COMPLETED)

                for future in done_futures:
                    symbol, instrument_id = active_futures.pop(future)
                    symbol_state = checkpoint["symbols"].setdefault(symbol, default_symbol_state())
                    result = future.result()
                    checkpoint["request_count"] = request_count

                    if result.error:
                        failed_symbols.add(symbol)
                        print(
                            f"[error] {symbol}: failed to fetch {window['start_date']} -> {window['end_date']}: {result.error}",
                            flush=True,
                        )
                        save_checkpoint(checkpoint_path, checkpoint)
                        continue

                    inserted_rows = upsert_price_history(db_ods, instrument_id, result.price_df)
                    total_rows += inserted_rows
                    symbol_state["rows_loaded"] += inserted_rows

                    if inserted_rows == 0:
                        symbol_state["empty_streak"] += 1
                        print(
                            f"[empty] {symbol}: no data for {window['start_date']} -> {window['end_date']}",
                            flush=True,
                        )
                    else:
                        symbol_state["empty_streak"] = 0
                        print(
                            f"[loaded] {symbol}: {inserted_rows} rows for {window['start_date']} -> {window['end_date']}",
                            flush=True,
                        )

                    symbol_state["next_window_index"] = window_index + 1
                    if int(symbol_state["empty_streak"]) >= args.stop_after_empty_windows:
                        symbol_state["done"] = True
                        print(
                            (
                                f"[stop] {symbol}: reached {symbol_state['empty_streak']} consecutive empty "
                                "windows; assuming minute history is no longer available further back"
                            ),
                            flush=True,
                        )
                    elif int(symbol_state["next_window_index"]) >= len(windows):
                        symbol_state["done"] = True
                        print(
                            f"[done] {symbol}: cumulative loaded rows = {symbol_state['rows_loaded']}",
                            flush=True,
                        )
                    elif args.sleep_seconds > 0:
                        time.sleep(args.sleep_seconds)

                    checkpoint["symbols"][symbol] = symbol_state
                    save_checkpoint(checkpoint_path, checkpoint)

            save_checkpoint(checkpoint_path, checkpoint)

    if failed_symbols:
        failed_list = ", ".join(sorted(failed_symbols))
        raise SystemExit(
            f"One-minute backfill hit fetch errors for {len(failed_symbols)} symbols: {failed_list}"
        )

    return total_rows


def main() -> None:
    args = parse_args()
    checkpoint_path = Path(args.checkpoint_file)

    db_ods = connector(schema="ods")
    db_dwd = connector(schema="dwd")
    api = schwab_api_market()
    ensure_price_history_frequency_types(db_ods)

    symbols = load_watch_list(db_dwd, max_symbols=args.max_symbols)
    if not symbols:
        raise SystemExit("No active symbols found in dwd.watch_list.")

    windows = build_windows(args.end_date, args.years_back)
    checkpoint = load_checkpoint(checkpoint_path, reset=args.reset_checkpoint)
    checkpoint["settings"] = {
        "years_back": args.years_back,
        "frontload_years": args.frontload_years,
        "end_date": args.end_date,
        "window_days": WINDOW_DAYS,
        "sleep_seconds": args.sleep_seconds,
        "pause_every": args.pause_every,
        "pause_seconds": args.pause_seconds,
        "stop_after_empty_windows": args.stop_after_empty_windows,
        "max_workers": args.max_workers,
        "requests_per_minute": args.requests_per_minute,
    }

    instrument_lookup = ensure_instruments(db_ods, api, symbols)
    if instrument_lookup.empty:
        raise SystemExit("No instruments were fetched for the watch list.")

    instrument_lookup = instrument_lookup.drop_duplicates(subset=["symbol"], keep="first")
    instrument_map = {
        row["symbol"]: int(row["instrument_id"])
        for _, row in instrument_lookup.iterrows()
        if pd.notna(row["instrument_id"])
    }

    total_rows = 0
    for range_start, range_end in build_window_ranges(
        args.end_date,
        args.years_back,
        args.frontload_years,
        len(windows),
    ):
        total_rows += process_window_range(
            symbols=symbols,
            windows=windows,
            range_start=range_start,
            range_end=range_end,
            instrument_map=instrument_map,
            checkpoint=checkpoint,
            checkpoint_path=checkpoint_path,
            db_ods=db_ods,
            args=args,
        )

    print(
        f"[complete] processed {len(symbols)} symbols, loaded {total_rows} rows in this run",
        flush=True,
    )


if __name__ == "__main__":
    main()
