import json
import os
from datetime import datetime, time
from typing import Dict

import pandas as pd

REGISTRY_FILE = r"live_registry/hl_live_registry.json"
REGISTRY_STATUS_COMPLETED = "COMPLETED"


def ensure_registry_file():
    os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
    if not os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)


def load_live_registry() -> Dict:
    ensure_registry_file()
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"  -> Registry load failed: {e}")
        return {}


def save_live_registry(data: Dict):
    ensure_registry_file()
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def fmt_live_ts(x):
    if x is None:
        return ""
    try:
        return pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(x)


def live_signal_expiry_server(day):
    return datetime.combine(day, time(23, 50))


def make_signal_id_from_setup(pair: str, day, setup: dict) -> str:
    side = str(setup.get("side", "")).strip().upper()
    trigger = fmt_live_ts(setup.get("trigger_time")).replace(
        " ", "_").replace(":", "-")
    entry = round(float(setup.get("entry", 0.0)), 5)
    sl = round(float(setup.get("sl", 0.0)), 5)
    tp = round(float(setup.get("tp", 0.0)), 5)
    return f"{pair}_{day}_{side}_{trigger}_{entry:.5f}_{sl:.5f}_{tp:.5f}"


def mark_signal_completed_in_registry(signal_id: str, trade: Dict):
    reg = load_live_registry()
    if signal_id not in reg:
        reg[signal_id] = {"signal_id": signal_id}

    result = str(trade.get("result", "")).lower()
    reg[signal_id]["entry_hit"] = True
    reg[signal_id]["exit_result"] = result
    reg[signal_id]["entry_time"] = str(trade.get("entry_time", ""))
    reg[signal_id]["exit_time"] = str(trade.get("exit_time", ""))
    reg[signal_id]["registry_status"] = "COMPLETED"
    reg[signal_id]["completed"] = True
    reg[signal_id]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_live_registry(reg)


def mark_signal_non_completed_in_registry(signal_id: str, status: str):
    reg = load_live_registry()
    row = reg.get(signal_id)

    if not row:
        return

    row_status = str(row.get("registry_status", "")).strip().upper()
    row_completed = bool(row.get("completed", False))
    row_exit_result = str(row.get("exit_result", "")).strip().lower()

    if (
        row_completed
        or row_status == "COMPLETED"
        or row_exit_result in {
            "tp",
            "sl",
            "sl_lock10",
            "session_exit",
            "orderexpired1930",
            "failed",
        }
    ):
        print(f" -> Refusing to downgrade finalized row: {signal_id}")
        return

    row["registry_status"] = str(status).strip().upper()
    row["completed"] = False
    row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    reg[signal_id] = row
    save_live_registry(reg)


def is_signal_completed_in_registry(signal_id: str) -> bool:
    reg = load_live_registry()
    row = reg.get(signal_id, {})

    row_status = str(row.get("registry_status", "")).strip().upper()
    row_exit_result = str(row.get("exit_result", "")).strip().lower()

    return (
        bool(row.get("completed", False))
        or row_status == "COMPLETED"
        or row_exit_result in {
            "tp",
            "sl",
            "sl_lock10",
            "session_exit",
            "orderexpired1930",
            "failed",
        }
    )


def is_same_completed_trade_prices(
    pair: str,
    day,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    price_tol: float = 0.00005,
) -> bool:
    reg = load_live_registry()
    day_str = str(day)

    def _as_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    def _same_price(a, b):
        return abs(_as_float(a) - _as_float(b)) <= price_tol

    for _, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_day = str(row.get("day", "")).strip()
        row_side = str(row.get("side", "")).strip().upper()
        row_completed = bool(row.get("completed", False))
        row_status = str(row.get("registry_status", "")).strip().upper()

        if row_pair != pair:
            continue
        if row_day != day_str:
            continue
        if row_side != side.upper():
            continue
        if not (row_completed or row_status == "COMPLETED"):
            continue

        row_entry = row.get("entry", 0.0)
        row_sl = row.get("sl", 0.0)
        row_tp = row.get("tp", 0.0)

        if _same_price(row_entry, entry) and _same_price(row_sl, sl) and _same_price(row_tp, tp):
            return True

    return False


def has_any_completed_trade_for_pair_day(pair: str, day) -> bool:
    reg = load_live_registry()
    day_str = str(day)

    for _, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_day = str(row.get("day", "")).strip()
        row_completed = bool(row.get("completed", False))
        row_status = str(row.get("registry_status", "")).strip().upper()

        if row_pair != pair:
            continue
        if row_day != day_str:
            continue

        if row_completed or row_status == REGISTRY_STATUS_COMPLETED:
            return True

    return False


def has_active_registry_signal_for_pair_day_side(pair: str, day, side: str) -> bool:
    reg = load_live_registry()
    day_str = str(day)
    side = str(side).strip().upper()

    active_statuses = {
        "GENERATED",
        "NEW",
        "PLACED",
        "ENTRY_HIT",
        "BE_APPLIED",
        "LOCK10_APPLIED",
        "ACTIVE",
    }

    for _, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_day = str(row.get("day", "")).strip()
        row_side = str(row.get("side", "")).strip().upper()
        row_completed = bool(row.get("completed", False))
        row_status = str(row.get("registry_status", "")).strip().upper()

        if row_pair != pair:
            continue
        if row_day != day_str:
            continue
        if row_side != side:
            continue
        if row_completed:
            continue

        if row_status in active_statuses:
            return True

    return False


def get_active_registry_signal_for_pair_day_side(pair: str, day, side: str):
    reg = load_live_registry()
    day_str = str(day)
    side = str(side).strip().upper()

    active_statuses = {
        "GENERATED",
        "NEW",
        "PLACED",
        "ENTRY_HIT",
        "BE_APPLIED",
        "LOCK10_APPLIED",
        "ACTIVE",
    }

    candidates = []

    for signal_id, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_day = str(row.get("day", "")).strip()
        row_side = str(row.get("side", "")).strip().upper()
        row_completed = bool(row.get("completed", False))
        row_status = str(row.get("registry_status", "")).strip().upper()

        if row_pair != pair or row_day != day_str or row_side != side:
            continue
        if row_completed:
            continue
        if row_status not in active_statuses:
            continue

        candidates.append((signal_id, row))

    if not candidates:
        return None, None

    candidates.sort(
        key=lambda x: (
            pd.to_datetime(x[1].get("trigger_time"), errors="coerce")
            if str(x[1].get("trigger_time", "")).strip()
            else pd.Timestamp.min
        )
    )
    return candidates[-1]


def is_same_setup_signature(row: Dict, setup: dict, price_tol: float = 0.00005) -> bool:
    if not row or not setup:
        return False

    def _as_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    def _same_price(a, b):
        return abs(_as_float(a) - _as_float(b)) <= price_tol

    row_trigger = fmt_live_ts(row.get("trigger_time"))
    new_trigger = fmt_live_ts(setup.get("trigger_time"))

    return (
        str(row.get("side", "")).strip().upper() == str(
            setup.get("side", "")).strip().upper()
        and row_trigger == new_trigger
        and _same_price(row.get("entry", 0.0), setup.get("entry", 0.0))
        and _same_price(row.get("sl", 0.0), setup.get("sl", 0.0))
        and _same_price(row.get("tp", 0.0), setup.get("tp", 0.0))
    )


def is_newer_setup_than_row(row: Dict, setup: dict) -> bool:
    if not row or not setup:
        return False

    row_trigger = pd.to_datetime(row.get("trigger_time"), errors="coerce")
    new_trigger = pd.to_datetime(setup.get("trigger_time"), errors="coerce")

    if pd.isna(new_trigger):
        return False
    if pd.isna(row_trigger):
        return True

    return new_trigger > row_trigger


def is_setup_in_hhll_disable_window(setup: dict, disable_start_server, disable_end_server) -> bool:
    if not setup:
        return False

    ref_time = setup.get("picked_candle_time")

    if ref_time is None or str(ref_time).strip() == "":
        ref_time = setup.get("trigger_time")

    if ref_time is None or str(ref_time).strip() == "":
        return False

    try:
        ref_time = pd.to_datetime(ref_time)
    except Exception:
        return False

    t = ref_time.time()

    if disable_start_server <= disable_end_server:
        return disable_start_server <= t < disable_end_server

    return t >= disable_start_server or t < disable_end_server


def parse_registry_ts(x):
    if x is None or str(x).strip() == "":
        return None
    try:
        return pd.to_datetime(x)
    except Exception:
        return None


def get_signal_expiry_from_row(row: Dict):
    day_str = str(row.get("day", "")).strip()
    if not day_str:
        return None
    try:
        day_dt = pd.to_datetime(day_str).date()
        return live_signal_expiry_server(day_dt)
    except Exception:
        return None


def scan_signal_outcome_from_df(df: pd.DataFrame, row: Dict):
    if df is None or df.empty:
        return None

    df = df.copy()

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    elif "datetime" in df.columns:
        df["time"] = pd.to_datetime(df["datetime"], errors="coerce")
    else:
        return None

    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    side = str(row.get("side", "")).upper().strip()
    entry = float(row.get("entry", 0.0))
    sl = float(row.get("sl", 0.0))
    tp = float(row.get("tp", 0.0))
    trigger_time = parse_registry_ts(row.get("trigger_time"))
    expiry = get_signal_expiry_from_row(row)

    if trigger_time is None:
        return None

    work_df = df[df["time"] >= trigger_time].copy()
    if expiry is not None:
        work_df = work_df[work_df["time"] <= expiry].copy()

    if work_df.empty:
        return None

    entry_hit = False
    entry_time = None

    for _, candle in work_df.iterrows():
        t = candle["time"]
        high = float(candle["high"])
        low = float(candle["low"])

        if not entry_hit:
            if side == "B":
                if high >= entry:
                    entry_hit = True
                    entry_time = t
                    if low <= sl:
                        return {
                            "entry_hit": True,
                            "result": "sl",
                            "entry_time": entry_time,
                            "exit_time": t,
                        }
                    if high >= tp:
                        return {
                            "entry_hit": True,
                            "result": "tp",
                            "entry_time": entry_time,
                            "exit_time": t,
                        }

            elif side == "S":
                if low <= entry:
                    entry_hit = True
                    entry_time = t
                    if high >= sl:
                        return {
                            "entry_hit": True,
                            "result": "sl",
                            "entry_time": entry_time,
                            "exit_time": t,
                        }
                    if low <= tp:
                        return {
                            "entry_hit": True,
                            "result": "tp",
                            "entry_time": entry_time,
                            "exit_time": t,
                        }
            continue

        if side == "B":
            if low <= sl:
                return {
                    "entry_hit": True,
                    "result": "sl",
                    "entry_time": entry_time,
                    "exit_time": t,
                }
            if high >= tp:
                return {
                    "entry_hit": True,
                    "result": "tp",
                    "entry_time": entry_time,
                    "exit_time": t,
                }

        elif side == "S":
            if high >= sl:
                return {
                    "entry_hit": True,
                    "result": "sl",
                    "entry_time": entry_time,
                    "exit_time": t,
                }
            if low <= tp:
                return {
                    "entry_hit": True,
                    "result": "tp",
                    "entry_time": entry_time,
                    "exit_time": t,
                }

    if entry_hit:
        last_time = work_df.iloc[-1]["time"]
        return {
            "entry_hit": True,
            "result": "open_or_expired",
            "entry_time": entry_time,
            "exit_time": last_time,
        }

    return {
        "entry_hit": False,
        "result": "not_triggered",
        "entry_time": None,
        "exit_time": work_df.iloc[-1]["time"],
    }


def reconcile_open_registry_signals_with_market_data(engine, pair: str, df: pd.DataFrame):
    reg = load_live_registry()
    changed = False

    now_ts = None
    try:
        if df is not None and not df.empty:
            tmp_df = df.copy()

            if "time" in tmp_df.columns:
                tmp_df["time"] = pd.to_datetime(
                    tmp_df["time"], errors="coerce")
            elif "datetime" in tmp_df.columns:
                tmp_df["time"] = pd.to_datetime(
                    tmp_df["datetime"], errors="coerce")
            else:
                tmp_df["time"] = pd.NaT

            tmp_df = tmp_df.dropna(subset=["time"]).sort_values(
                "time").reset_index(drop=True)

            if not tmp_df.empty:
                now_ts = tmp_df.iloc[-1]["time"]
    except Exception:
        now_ts = None

    for signal_id, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_completed = bool(row.get("completed", False))
        row_status = str(row.get("registry_status", "")).strip().upper()

        if row_pair != pair:
            continue

        if row_completed or row_status == "COMPLETED":
            continue

        expiry = get_signal_expiry_from_row(row)
        outcome = scan_signal_outcome_from_df(df, row)

        if outcome is None:
            if expiry is not None and now_ts is not None and now_ts > expiry:
                if row_status not in {"ENTRY_HIT", "COMPLETED"}:
                    row["registry_status"] = "ORDEREXPIRED1930"
                    row["completed"] = False
                    row["exit_result"] = "orderexpired1930"
                    row["last_updated"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    reg[signal_id] = row
                    changed = True
            continue

        result = str(outcome.get("result", "")).lower()
        entry_hit = bool(outcome.get("entry_hit", False))
        exit_time = str(outcome.get("exit_time", ""))
        entry_time = str(outcome.get("entry_time", ""))

        if entry_hit:
            row["entry_hit"] = True
            row["entry_time"] = entry_time
            row["exit_time"] = exit_time

            if result in {"tp", "sl", "sl_lock10"}:
                row["exit_result"] = result
                row["registry_status"] = "COMPLETED"
                row["completed"] = True

            elif result == "open_or_expired":
                if expiry is not None and now_ts is not None and now_ts > expiry:
                    row["exit_result"] = "session_exit"
                    row["registry_status"] = "COMPLETED"
                    row["completed"] = True
                else:
                    row["registry_status"] = "ENTRY_HIT"
                    row["completed"] = False

            else:
                row["registry_status"] = "ENTRY_HIT"
                row["completed"] = False

        else:
            if result == "not_triggered":
                if expiry is not None and now_ts is not None and now_ts > expiry:
                    row["registry_status"] = "ORDEREXPIRED1930"
                    row["completed"] = False
                    row["exit_result"] = "orderexpired1930"
                else:
                    row["registry_status"] = "GENERATED"
                    row["completed"] = False

        row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        reg[signal_id] = row
        changed = True

    if changed:
        save_live_registry(reg)
