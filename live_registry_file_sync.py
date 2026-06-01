import os
from datetime import datetime
from typing import Dict, Optional

from live_registry_manager import load_live_registry, save_live_registry

TERMINAL_FILLED_STATUSES = {"FILLED_BUY", "FILLED_SELL", "BE_APPLIED"}
TERMINAL_DEAD_STATUSES = {
    "FAILED",
    "CANCELLEDEOD",
    "CANCELLEDNEWHHLL",
    "EXPIRED",
    "ORDEREXPIRED1930",
}
ALL_SYNC_STATUSES = TERMINAL_FILLED_STATUSES | TERMINAL_DEAD_STATUSES


def parse_signal_file_line(line: str) -> Optional[Dict]:
    if not line:
        return None
    parts = line.strip().split("|")
    if len(parts) < 17:
        return None
    return {
        "action": parts[0],
        "signal_id": parts[1],
        "symbol": parts[2],
        "side": parts[3],
        "expiry_server": parts[4],
        "entry": parts[5],
        "sl": parts[6],
        "tp": parts[7],
        "lot": parts[8],
        "entry_mode": parts[9],
        "atr": parts[10],
        "trigger_time": parts[11],
        "picked_candle_time": parts[12],
        "breakout_candle_time": parts[13],
        "status": parts[14].strip().upper(),
        "max_spread_points": parts[15],
        "max_slippage_points": parts[16],
    }


def read_signal_file(path: str) -> Optional[Dict]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            line = f.read().strip()
        return parse_signal_file_line(line) if line else None
    except Exception as e:
        print(f"[sync] failed reading signal file {path}: {e}")
        return None


def sync_registry_row_from_file_status(row: Dict, file_payload: Dict) -> Dict:
    file_status = str(file_payload.get("status", "")).upper().strip()
    side = str(file_payload.get("side", "")).upper().strip()

    if file_status not in ALL_SYNC_STATUSES:
        return row

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row.setdefault("side", side)
    row.setdefault("entry_hit", False)
    row.setdefault("entry_time", "")
    row.setdefault("exit_time", "")
    row.setdefault("exit_result", "")
    row.setdefault("registry_status", "")
    row.setdefault("completed", False)

    if file_status in {"FILLED_BUY", "FILLED_SELL"}:
        if not row.get("entry_hit"):
            row["entry_hit"] = True
            row["entry_time"] = row.get("entry_time") or now_str

        if str(row.get("registry_status", "")).upper() not in {"COMPLETED", "BE_APPLIED"}:
            row["registry_status"] = "ENTRY_HIT"

        row["completed"] = False

    elif file_status == "BE_APPLIED":
        if not row.get("entry_hit"):
            row["entry_hit"] = True
            row["entry_time"] = row.get("entry_time") or now_str

        row["registry_status"] = "BE_APPLIED"
        row["completed"] = False

    elif file_status == "CANCELLEDEOD":
        row["exit_result"] = "session_exit"
        row["registry_status"] = "COMPLETED"
        row["completed"] = True
        row["exit_time"] = row.get("exit_time") or now_str

    elif file_status in {"CANCELLEDNEWHHLL", "EXPIRED", "ORDEREXPIRED1930"}:
        row["exit_result"] = "orderexpired1930"
        row["registry_status"] = "COMPLETED"
        row["completed"] = True
        row["exit_time"] = row.get("exit_time") or now_str

    elif file_status == "FAILED":
        row["exit_result"] = "failed"
        row["registry_status"] = "COMPLETED"
        row["completed"] = True
        row["exit_time"] = row.get("exit_time") or now_str

    row["last_updated"] = now_str
    return row


def sync_registry_from_mt5_files_for_pair_day(pair: str, day, signal_dir: str) -> bool:
    day_str = str(day)
    reg = load_live_registry()
    changed = False

    buy_file = os.path.join(signal_dir, f"live_signal_{pair}_BUY.txt")
    sell_file = os.path.join(signal_dir, f"live_signal_{pair}_SELL.txt")

    buy_payload = read_signal_file(buy_file)
    sell_payload = read_signal_file(sell_file)

    for signal_id, row in reg.items():
        row_pair = str(row.get("pair", "")).strip()
        row_day = str(row.get("day", "")).strip()
        row_side = str(row.get("side", "")).strip().upper()

        if row_pair != pair or row_day != day_str:
            continue

        before = dict(row)

        if row_side == "B" and buy_payload:
            row = sync_registry_row_from_file_status(row, buy_payload)
        elif row_side == "S" and sell_payload:
            row = sync_registry_row_from_file_status(row, sell_payload)

        if row != before:
            reg[signal_id] = row
            changed = True

    if changed:
        save_live_registry(reg)
        print(
            f"[sync] registry updated from MT5 files for {pair} day={day_str}")

    return changed
