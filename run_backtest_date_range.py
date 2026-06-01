import os
from datetime import datetime, timedelta

import MetaTrader5 as mt5
import pandas as pd

from backtest_engine_1h_orb import BacktestEngine1HORB


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

DATA_DIR = "."
START_DATE = "2026-05-01"
END_DATE = "2026-05-29"

EXPORT_NAME = f"backtest_{START_DATE}_to_{END_DATE}.xlsx"
TIMEFRAME = mt5.TIMEFRAME_M15


def pair_to_temp_csv(pair: str) -> str:
    pair_name = pair.replace(".", "_")
    return f"_temp_{pair_name}.csv"


def init_mt5() -> bool:
    if not mt5.initialize():
        print("MT5 initialization failed")
        return False

    terminal = mt5.terminal_info()
    print("MT5 initialized")
    if terminal:
        print(f"Terminal: {terminal.name}")
    return True


def fetch_pair_data(pair: str, start_date: str, end_date: str) -> bool:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    print(f"\n[{pair}] Fetching data from {start_dt.date()} to {end_dt.date()}")

    rates = mt5.copy_rates_range(pair, TIMEFRAME, start_dt, end_dt)

    if rates is None or len(rates) == 0:
        print(f"  -> No MT5 data returned for {pair}")
        return False

    df = pd.DataFrame(rates)
    df["datetime"] = pd.to_datetime(df["time"], unit="s")
    df = df[["datetime", "open", "high", "low", "close"]].copy()

    output_file = os.path.join(DATA_DIR, pair_to_temp_csv(pair))
    df.to_csv(output_file, index=False)

    print(f"  -> Saved {len(df)} rows to {output_file}")
    print(f"  -> Range: {df['datetime'].min()} to {df['datetime'].max()}")

    return True


def refresh_csv_data(pairs: list[str], start_date: str, end_date: str) -> list[str]:
    updated_pairs = []

    print("\n" + "=" * 70)
    print("REFRESHING CSV DATA FROM MT5")
    print("=" * 70)

    for pair in pairs:
        ok = fetch_pair_data(pair, start_date, end_date)
        if ok:
            updated_pairs.append(pair)

    print("\n" + "=" * 70)
    print(
        f"CSV refresh complete: {len(updated_pairs)}/{len(pairs)} pairs updated")
    print("=" * 70)

    return updated_pairs


def build_specs(data_dir: str, pairs: list[str]) -> list[dict]:
    specs = []
    missing = []

    for pair in pairs:
        filename = pair_to_temp_csv(pair)
        csv_path = os.path.join(data_dir, filename)

        if not os.path.exists(csv_path):
            missing.append((pair, csv_path))
            continue

        specs.append({
            "pair": pair,
            "csv": csv_path,
        })

    if missing:
        print("\nMissing CSVs for these pairs:")
        for pair, path in missing:
            print(f"  - {pair} -> expected: {path}")

    print("\nResolved CSV mapping:")
    for spec in specs:
        print(f"  {spec['pair']} -> {spec['csv']}")

    return specs


def main():
    if not init_mt5():
        raise RuntimeError("MT5 not initialized. Open MT5 and login first.")

    try:
        updated_pairs = refresh_csv_data(PAIRS, START_DATE, END_DATE)

        if not updated_pairs:
            raise RuntimeError(
                "No pair data fetched from MT5 for selected date range.")

        engine = BacktestEngine1HORB(
            initial_fund=INITIAL_FUND,
            initial_risk_percent=INITIAL_RISK,
            pair=updated_pairs[0],
        )

        engine.start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        engine.end_date = datetime.strptime(END_DATE, "%Y-%m-%d").date()

        specs = build_specs(DATA_DIR, updated_pairs)

        if not specs:
            raise RuntimeError(
                "No matching CSV files found for selected pairs.")

        print("\n" + "=" * 70)
        print("RUNNING BACKTEST")
        print(f"Date range: {engine.start_date} -> {engine.end_date}")
        print(f"Pairs loaded: {len(specs)}")
        print("=" * 70)

        engine.run_backtest(specs)
        engine.export_to_excel(EXPORT_NAME)

        print("\nBacktest complete.")
        print(f"Trades: {len(engine.trades)}")
        print(f"Final fund: {engine.current_fund:.2f}")
        print(f"Excel exported: backtests/{EXPORT_NAME}")

    finally:
        mt5.shutdown()
        print("MT5 shutdown")


if __name__ == "__main__":
    main()
