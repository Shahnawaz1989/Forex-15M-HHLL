from datetime import datetime
import os
import json
import pandas as pd

from backtest_engine_1h_orb import BacktestEngine1HORB
from live_data_mt5 import fetch_live_15m
from order_mt5 import init_mt5, shutdown_mt5
from live_cleanup import run_startup_cleanup
from live_registry_file_sync import sync_registry_from_mt5_files_for_pair_day


HEARTBEAT_FILE = r"C:\trading_bot\heartbeats\live_runner_heartbeat.json"

PAIRS = [
    "AUDCAD.ecn",
    "AUDUSD.ecn",
    "AUDCHF.ecn",
    "CADCHF.ecn",
    "EURAUD.ecn",
    "EURCAD.ecn",
    "EURCHF.ecn",
    "EURUSD.ecn",
    "EURGBP.ecn",
    "GBPAUD.ecn",
    "GBPCAD.ecn",
    "GBPCHF.ecn",
    "GBPUSD.ecn",
    "NZDCAD.ecn",
    "NZDUSD.ecn",
    "NZDCHF.ecn",
    "USDCAD.ecn",
    "USDCHF.ecn",

]

INITIAL_FUND = 30.0
INITIAL_RISK = 8.0
DEFAULT_PAIR = PAIRS[0]
LOOKBACK_DAYS = 30
MAX_SPREAD_POINTS = 25
MAX_SLIPPAGE_POINTS = 15

SIGNAL_DIR = r"C:\Users\Uzair Khan\AppData\Roaming\MetaQuotes\Terminal\D0E8209F77C8CF37AD8BF550E51FF075\MQL5\Files"
REGISTRY_FILE = r"live_registry/hl_live_registry.json"


def write_heartbeat(stage="alive", extra=None):
    os.makedirs(os.path.dirname(HEARTBEAT_FILE), exist_ok=True)

    payload = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "pid": os.getpid(),
    }

    if extra and isinstance(extra, dict):
        payload.update(extra)

    tmp = HEARTBEAT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    os.replace(tmp, HEARTBEAT_FILE)


def process_pair(engine: BacktestEngine1HORB, pair: str):
    write_heartbeat("processing_pair", {"pair": pair})

    print("\n" + "=" * 60)
    print(f"Processing live HL dual signals for {pair}")

    df = fetch_live_15m(pair, lookback_days=LOOKBACK_DAYS)
    if df is None or df.empty:
        print("  -> No MT5 15m data")
        return

    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values(
        "datetime").reset_index(drop=True)

    if df.empty:
        print("  -> Data empty after datetime parsing")
        return

    df["time"] = df["datetime"]

    latest_day = df["datetime"].dt.date.max()
    print(f"  -> {pair} max datetime in 15m df = {df['datetime'].max()}")
    print(df[["datetime", "open", "high", "low", "close"]].tail(5))

    # 1) Reconcile old/open rows from market data
    try:
        engine._reconcile_open_registry_signals_with_market_data(
            pair=pair,
            df=df,
        )
    except Exception as e:
        print(f"  -> First reconcile failed for {pair}: {e}")

    # 1.5) Sync MT5 file statuses back into registry
    try:
        sync_registry_from_mt5_files_for_pair_day(
            pair=pair,
            day=latest_day,
            signal_dir=SIGNAL_DIR,
        )
    except Exception as e:
        print(f"  -> File-status sync failed for {pair}: {e}")

    # 2) Generate / refresh latest day signals
    result = engine.generate_live_dual_signals_for_latest_day(
        pair=pair,
        df_15m=df,
        signal_dir=SIGNAL_DIR,
        max_spread_points=MAX_SPREAD_POINTS,
        max_slippage_points=MAX_SLIPPAGE_POINTS,
    )

    # 3) Reconcile newly generated rows
    try:
        engine._reconcile_open_registry_signals_with_market_data(
            pair=pair,
            df=df,
        )
    except Exception as e:
        print(f"  -> Second reconcile failed for {pair}: {e}")

    # 4) Force direct CANCEL write for completed rows
    try:
        reg = engine._load_live_registry()

        TERMINAL_CANCEL_STATUSES = {
            "CANCELLED",
            "CANCELLEDNEWHHLL",
            "CANCELLED_NEW_HHLL",
            "FILLED_BUY",
            "FILLED_SELL",
            "BE_APPLIED",
        }

        for signal_id, row in reg.items():
            row_pair = str(row.get("pair", "")).strip()
            row_day = str(row.get("day", "")).strip()
            row_status = str(row.get("registry_status", "")).strip().upper()
            row_completed = bool(row.get("completed", False))
            side = str(row.get("side", "")).strip().upper()

            if row_pair != pair or row_day != str(latest_day):
                continue

            if not (row_completed or row_status == "COMPLETED"):
                continue

            if side not in ("B", "S"):
                continue

            suffix = "BUY" if side == "B" else "SELL"
            signal_file = os.path.join(
                SIGNAL_DIR,
                f"live_signal_{pair}_{suffix}.txt",
            )

            existing = {}
            existing_status = ""
            try:
                existing = engine._read_live_signal_file(signal_file) or {}
                existing_status = str(existing.get(
                    "status", "")).strip().upper()
            except Exception:
                existing = {}
                existing_status = ""

            if str(existing.get("signal_id", "")).strip() == signal_id and existing_status in TERMINAL_CANCEL_STATUSES:
                print(
                    f"  -> Skip direct CANCEL, file already terminal: {signal_id} status={existing_status}")
                continue

            row_day_obj = pd.to_datetime(row_day, errors="coerce")
            if pd.isna(row_day_obj):
                print(
                    f"  -> Skipping CANCEL for {signal_id}: invalid row_day={row_day}")
                continue
            row_day_obj = row_day_obj.date()

            cancel_payload = engine._build_live_cancel_payload(
                pair=pair,
                day=row_day_obj,
                max_spread_points=MAX_SPREAD_POINTS,
                max_slippage_points=MAX_SLIPPAGE_POINTS,
            )

            cancel_payload["signal_id"] = signal_id
            cancel_payload["symbol"] = pair
            cancel_payload["side"] = side
            cancel_payload["status"] = "NEW"

            print(f"  -> Writing direct CANCEL for completed {signal_id}")
            engine._write_live_signal_file(signal_file, cancel_payload)

    except Exception as e:
        print(f"  -> Direct completed CANCEL write failed for {pair}: {e}")

    # 5) Snapshot
    try:
        reg = engine._load_live_registry()
        active_count = 0
        completed_count = 0

        for _, row in reg.items():
            row_pair = str(row.get("pair", "")).strip()
            row_day = str(row.get("day", "")).strip()
            row_status = str(row.get("registry_status", "")).strip().upper()
            row_completed = bool(row.get("completed", False))

            if row_pair != pair or row_day != str(latest_day):
                continue

            if row_completed or row_status == "COMPLETED":
                completed_count += 1
            else:
                active_count += 1

        print(
            f"  -> Registry snapshot for {pair} day={latest_day}: "
            f"active={active_count}, completed={completed_count}"
        )
    except Exception as e:
        print(f"  -> Registry snapshot failed for {pair}: {e}")

    print(f"  -> Result for {pair}: {result}")


def main():
    write_heartbeat("startup")

    try:
        run_startup_cleanup(REGISTRY_FILE, SIGNAL_DIR)

        init_mt5()

        engine = BacktestEngine1HORB(
            initial_fund=INITIAL_FUND,
            initial_risk_percent=INITIAL_RISK,
            pair=DEFAULT_PAIR,
        )

        engine.use_live_equity_sizing = True
        engine.live_source_fund = None
        engine.live_strategy_start_fund = INITIAL_FUND

        write_heartbeat("startup_reconcile_begin")
        print(
            "  -> Startup reconcile skipped (per-pair reconcile will run in process_pair)")
        write_heartbeat("startup_reconcile_done")

        write_heartbeat("cycle_start")

        for pair in PAIRS:
            try:
                process_pair(engine, pair)
            except Exception as e:
                write_heartbeat("exception", {"pair": pair, "error": str(e)})
                print(f"  -> Failed for {pair}: {e}")

        write_heartbeat("cycle_done")

    except Exception as e:
        write_heartbeat("exception", {"error": str(e)})
        print(f"Runner fatal exception: {e}")
        raise
    finally:
        try:
            shutdown_mt5()
        except Exception:
            pass


if __name__ == "__main__":
    main()
