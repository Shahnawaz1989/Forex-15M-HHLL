from strategy_calculator import StrategyCalculator
from gann_fetcher import GannFetcher
from live_data_mt5 import fetch_live_1m
from live_fund_manager import get_live_usable_fund
from backtest_orb_runner_live_style import process_pair_day_live_style
import os
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from typing import List, Dict, Optional
import pytz
import json
import bisect
import numpy as np
from backtest_orb_runner_helpers import (
    prepare_backtest_data,
    get_weekly_risk_percent,
    process_pair_day,
)
from backtest_orb_setup_builder import (
    build_high_setup_for_day,
    build_low_setup_for_day,
    select_entry_target_from_gate_and_39,
)
from backtest_orb_trade_simulator import (
    resolve_same_candle_exit_with_m1,
    fetch_m1_data_for_window,
    compute_m1_mae_after_entry,
    simulate_trade,
)
from live_registry_manager import (
    ensure_registry_file,
    load_live_registry,
    save_live_registry,
    fmt_live_ts,
    live_signal_expiry_server,
    make_signal_id_from_setup,
    mark_signal_completed_in_registry,
    mark_signal_non_completed_in_registry,
    is_signal_completed_in_registry,
    is_same_completed_trade_prices,
    has_any_completed_trade_for_pair_day,
    has_active_registry_signal_for_pair_day_side,
    get_active_registry_signal_for_pair_day_side,
    is_same_setup_signature,
    is_newer_setup_than_row,
    is_setup_in_hhll_disable_window,
    parse_registry_ts,
    get_signal_expiry_from_row,
    scan_signal_outcome_from_df,
    reconcile_open_registry_signals_with_market_data,
)

from live_signal_file_manager import (
    ACTIVE_FILE_STATUSES,
    is_same_live_payload,
    build_live_cancel_payload,
    build_live_place_payload,
    live_payload_to_line,
    read_existing_live_signal,
    write_live_signal_file,
    cancel_existing_signal_strict,
    write_fresh_signal_after_strict_delete,
)
from live_signal_orchestrator import (
    is_existing_from_old_day,
    choose_live_setup_for_day,
    generate_live_dual_signals_for_latest_day,
)
from live_registry_file_sync import sync_registry_from_mt5_files_for_pair_day
# Simple ANSI colors for terminal
RESET = "\033[0m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"

TERMINAL_FILLED_STATUSES = {"FILLED_BUY", "FILLED_SELL", "BE_APPLIED"}

TERMINAL_DEAD_STATUSES = {
    "FAILED",
    "CANCELLEDEOD",
    "CANCELLEDNEWHHLL",
    "EXPIRED",
    "ORDEREXPIRED1930",
}


REGISTRY_DIR = "live_registry"
REGISTRY_FILE = os.path.join(REGISTRY_DIR, "hl_live_registry.json")

RESULT_TP = "tp"
RESULT_SL = "sl"
RESULT_SL_LOCK10 = "sl_lock10"
RESULT_SESSION_EXIT = "session_exit"
RESULT_ORDER_EXPIRED = "orderexpired1930"

REGISTRY_STATUS_GENERATED = "GENERATED"
REGISTRY_STATUS_COMPLETED = "COMPLETED"
REGISTRY_STATUS_ENTRY_HIT = "ENTRY_HIT"
REGISTRY_STATUS_CANCELLED_EOD = "CANCELLEDEOD"
REGISTRY_STATUS_CANCELLED_NEW_HH_LL = "CANCELLEDNEWHHLL"
REGISTRY_STATUS_ORDER_EXPIRED = "ORDEREXPIRED1930"

COMPLETED_RESULTS = {RESULT_TP, RESULT_SL, RESULT_SL_LOCK10}
NON_COMPLETED_RESULTS = {RESULT_ORDER_EXPIRED, RESULT_SESSION_EXIT}


class DSTHelper:
    """
    MT5 SERVER (Athens time, Europe/Athens) ↔ IST conversion with real DST.
    CSV: 'datetime' already server time (Athens).
    """

    @staticmethod
    def ist_to_server(ist_dt: datetime) -> datetime:
        ist = pytz.timezone("Asia/Kolkata")
        athens = pytz.timezone("Europe/Athens")

        ist_loc = ist.localize(ist_dt)
        # IST -> UTC -> Athens (handles DST automatically)
        utc = ist_loc.astimezone(pytz.utc)
        server = utc.astimezone(athens)
        return server.replace(tzinfo=None)  # naive datetime

    @staticmethod
    def server_to_ist(server_dt: datetime) -> datetime:
        """
        Opposite direction: server (Athens) -> IST.
        server_dt is naive datetime from CSV in Athens local time.
        """
        athens = pytz.timezone("Europe/Athens")
        ist = pytz.timezone("Asia/Kolkata")

        server_loc = athens.localize(server_dt)
        utc = server_loc.astimezone(pytz.utc)
        ist_dt = utc.astimezone(ist)
        return ist_dt


class BacktestEngine1HORB:
    """
    Single-session ORB on 1H candles:
      - ORB = 00:00 1H candle high/low (server)
      - Day VALID if (H-L of 00:00 candle / ATR14_RMA at that candle) < 1.2
      - Close breakout after ORB
      - Gann dual-side (same StrategyCalculator)
      - Entry window: 7:31–19:30 IST (pending orders only)
      - If pending not filled by 19:30 IST → order_expired_1930 (no trade)
      - If trade filled, TP/SL normal (no 19:30 force exit)
    """

    def __init__(self, initial_fund: float, initial_risk_percent: float, pair: str):
        self.initial_fund = initial_fund
        self.current_fund = initial_fund

        # Script-se-controlled risk%
        self.initial_risk_percent = float(initial_risk_percent)
        self.base_risk_percent = float(
            initial_risk_percent)  # weekly ramp ka base

        self.pair = pair

        # ---- Live equity sizing config ----
        self.use_live_equity_sizing = False
        self.live_source_fund = None
        self.live_strategy_start_fund = None

        # Date filter / weekly risk reference
        self.start_date = None
        self.end_date = None

        self.trades: List[Dict] = []
        self.max_drawdown = 0.0
        self.equity_high = initial_fund
        self.total_trades = 0
        self.win_rate = 0.0
        self.stop_requested = False

        # NEW: final list for trades > 2 hours
        self.long_duration_trades = []

        # Volatility filter
        self.atr_period = 14
        self.vol_ratio_threshold = 1.20  # < 1.20 = VALID

        # IST-based entry window
        self.entry_start_ist = time(7, 31)
        self.expire_ist = time(19, 30)

        # SERVER-time HH/LL disable window:
        # Detection allowed, but new order processing blocked in this window.
        self.hhll_disable_start_server = time(12, 00)
        self.hhll_disable_end_server = time(23, 45)

        # 🔹 Local Gann lookup load (JSON)
        self.gann_lookup = self._load_gann_lookup("forex_gann_lookup_1_3.json")

        # 🔹 OLD fixed-lot logic ko effectively disable kar do
        # Saara lot sizing ab StrategyCalculator.calculate_lot_size ke through hoga
        self.max_backtest_lot = None
        self.fixed_lot_mode = False
        self.fixed_lot_value = None

        # summaries
        self.daily_briefings = []
        # ------------ EXTRA HELPERS FOR ORB SHIFT LOGIC ------------

    def _compute_bo_ratio(
        self, first_candle: pd.Series, bo_candle: pd.Series
    ) -> float:
        first_hl = first_candle["high"] - first_candle["low"]
        if first_hl <= 0:
            return 0.0

        bo_hl = bo_candle["high"] - bo_candle["low"]
        if bo_hl <= 0:
            return 0.0

        ratio = bo_hl / first_hl
        return ratio

    # ----------------------------------------------------------------

    def _load_gann_lookup(self, path: str) -> Dict:
        """
        JSON: { "1.23456": { "buy_at": ..., "buy_t1": ..., "buy_t2": ..., ..., "sell_at": ..., "sell_t1": ... } }
        """
        try:
            with open(path, "r") as f:
                data = json.load(f)

            items = sorted(
                [(float(k), v) for k, v in data.items()],
                key=lambda x: x[0]
            )
            prices = [p for p, _ in items]
            levels = [lv for _, lv in items]
            print(f"  -> Loaded {len(prices)} Gann lookup keys from {path}")
            return {"prices": prices, "levels": levels}
        except Exception as e:
            print(f"  -> Gann lookup load failed: {e}")
            return {"prices": [], "levels": []}

    def _get_gann_from_lookup(self, price: float) -> Optional[Dict]:
        """
        Nearest price lookup in forex_gann_lookup_1_3.json
        Expect JSON structure per price key:
        {
          "buy_at": 1.2345,
          "buy_t1": 1.2350,
          "buy_t2": 1.2360,
          "sell_at": 1.2335,
          "sell_t1": 1.2330,
          "sell_t2": 1.2320
        }
        """
        prices = self.gann_lookup["prices"]
        levels = self.gann_lookup["levels"]
        if not prices:
            return None

        pos = bisect.bisect_left(prices, price)
        if pos == 0:
            idx = 0
        elif pos == len(prices):
            idx = len(prices) - 1
        else:
            before = prices[pos - 1]
            after = prices[pos]
            idx = pos - 1 if abs(price - before) <= abs(price - after) else pos

        lv = levels[idx]
        nearest_price = prices[idx]

        # Safe extraction with fallbacks
        buy_t1 = lv.get("buy_t1") or lv.get("buyT1")
        buy_t2 = lv.get("buy_t2") or lv.get("buyT2")
        sell_t1 = lv.get("sell_t1") or lv.get("sellT1")
        sell_t2 = lv.get("sell_t2") or lv.get("sellT2")

        if buy_t1 is None or buy_t2 is None or sell_t1 is None or sell_t2 is None:
            print(f"  -> Missing T1/T2 keys in JSON for price {nearest_price}")
            return None

        return {
            "input_price": nearest_price,
            "buy_at": lv["buy_at"],
            "buy_targets": [buy_t1, buy_t2],   # sirf T1, T2
            "sell_at": lv["sell_at"],
            "sell_targets": [sell_t1, sell_t2],  # sirf T1, T2
        }

    # ------------------ ATR(14) RMA on 1H ------------------

    def _add_atr_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add ATR(14) RMA/Wilder on 1H candles.
        df: sorted by time (server)
        """
        high = df["high"].values
        low = df["low"].values
        close = df["close"].values

        tr = np.zeros(len(df))
        tr[0] = high[0] - low[0]

        for i in range(1, len(df)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i - 1])
            lc = abs(low[i] - close[i - 1])
            tr[i] = max(hl, hc, lc)

        atr = np.zeros(len(df))
        p = self.atr_period

        if len(df) < p:
            df["atr"] = np.nan
            return df

        # First ATR = SMA of first p TRs
        first_atr = np.mean(tr[:p])
        atr[p - 1] = first_atr

        # Wilder RMA
        for i in range(p, len(df)):
            atr[i] = atr[i - 1] + (tr[i] - atr[i - 1]) / p

        # first (p-1) candles ATR undefined
        atr[:p - 1] = np.nan

        df["atr"] = atr
        return df

    def _debug_atr_row(self, df: pd.DataFrame, ts):
        row = df[df["time"] == pd.Timestamp(ts)]
        if row.empty:
            print(f"ATR DEBUG: no row for {ts}")
            return

        i = row.index[0]
        prev_close = df.loc[i - 1, "close"] if i > 0 else np.nan
        tr = max(
            df.loc[i, "high"] - df.loc[i, "low"],
            abs(df.loc[i, "high"] - prev_close) if pd.notna(prev_close) else 0,
            abs(df.loc[i, "low"] - prev_close) if pd.notna(prev_close) else 0,
        )

        print(
            f"ATR DEBUG | time={df.loc[i, 'time']} | "
            f"high={df.loc[i, 'high']:.5f} low={df.loc[i, 'low']:.5f} "
            f"close={df.loc[i, 'close']:.5f} prev_close={prev_close:.5f} "
            f"TR={tr:.5f} ATR={df.loc[i, 'atr']:.5f}"
        )

    # ------------------ DAY VALIDATION (basic) ------------------

    def _validate_day(self, day_df: pd.DataFrame) -> bool:
        """
        New timed-session strategy validation:
        - Day must have candles
        - ATR column should exist
        - At least some valid ATR values should be present
        """
        if day_df.empty:
            return False

        if "atr" not in day_df.columns:
            print("  -> ATR column missing")
            return False

        valid_atr = day_df["atr"].dropna()
        valid_atr = valid_atr[valid_atr > 0]

        if valid_atr.empty:
            print("  -> ATR not available for this day")
            return False

        return True

    def _select_entry_target_from_gate_and_39(
        self,
        side: str,
        gate_touched: bool,
        r39_touched: bool,
        at: float,
        t1: float,
        t15: float,
    ) -> Dict:
        return select_entry_target_from_gate_and_39(
            side=side,
            gate_touched=gate_touched,
            r39_touched=r39_touched,
            at=at,
            t1=t1,
            t15=t15,
        )

    def _build_low_setup_for_day(
        self,
        day_df: pd.DataFrame,
        fund: float,
        risk_percent: float,
    ) -> Optional[Dict]:
        return build_low_setup_for_day(
            engine=self,
            day_df=day_df,
            fund=fund,
            risk_percent=risk_percent,
        )

    def _build_high_setup_for_day(
        self,
        day_df: pd.DataFrame,
        fund: float,
        risk_percent: float,
    ) -> Optional[Dict]:
        return build_high_setup_for_day(
            engine=self,
            day_df=day_df,
            fund=fund,
            risk_percent=risk_percent,
        )

    def _wait_for_entry_in_window(
        self,
        day_df: pd.DataFrame,
        setup: Dict,
        window_start_server: datetime,
        window_end_server: datetime,
        session_atr: float,
    ) -> Optional[Dict]:
        """
        Generic entry window on SERVER time.

        STRICT ATR BUFFER MODE:
        - Session ORB ka ATR use hoga
        - Buffer formula _check_atr_buffer_entry ke hisaab se apply hoga
        - Buffer hit nahi hua to entry nahi hogi
        - Direct touch fallback DISABLED
        """
        entry_price = float(setup["entry"])
        side = setup["side"]

        print(
            f"  -> Entry window (server): {window_start_server} to {window_end_server}"
        )
        print(f"  -> Session ATR for buffer: {session_atr:.5f}")

        mask = (day_df["time"] >= window_start_server) & (
            day_df["time"] < window_end_server
        )
        search_df = day_df.loc[mask]

        if search_df.empty:
            print("  -> No candles in entry window (pending expires)")
            return None

        if session_atr is None or session_atr <= 0:
            print("  -> Invalid session ATR, buffer entry disabled for this session")
            return None

        for idx in search_df.index:
            row = day_df.loc[idx]
            row_time = row["time"]
            high = float(row["high"])
            low = float(row["low"])

            actual_entry = self._check_atr_buffer_entry(
                entry_price=entry_price,
                side=side,
                atr=session_atr,
                high=high,
                low=low,
                is_new_orb_shifted=False,
            )

            if actual_entry is not None:
                print(
                    f"  -> {side} ATR-buffer entry hit at {row_time}, actual_entry={actual_entry:.5f}"
                )
                return {
                    "entry_idx": idx,
                    "entry_time": row_time,
                    "actual_entry": actual_entry,
                }

        print(f"  -> {side} pending not filled with ATR buffer in this session")
        return None

    def _wait_for_first_fill_in_window_session(
        self,
        day_df: pd.DataFrame,
        buy_setup: Dict,
        sell_setup: Dict,
        window_start_server: datetime,
        window_end_server: datetime,
        session_atr: float,
    ) -> Optional[Dict]:
        """
        BUY/SELL dono ko same session window me race mode me dekho.
        Jo side pehle fill ho wahi final trade.

        Dono sides ke liye same session ORB ATR-based buffer use hoga.
        """
        buy_result = self._wait_for_entry_in_window(
            day_df=day_df,
            setup=buy_setup,
            window_start_server=window_start_server,
            window_end_server=window_end_server,
            session_atr=session_atr,
        )

        sell_result = self._wait_for_entry_in_window(
            day_df=day_df,
            setup=sell_setup,
            window_start_server=window_start_server,
            window_end_server=window_end_server,
            session_atr=session_atr,
        )

        # Dono side expire ho gaye
        if not buy_result and not sell_result:
            return None

        # Sirf BUY fill
        if buy_result and not sell_result:
            return {"setup": buy_setup, "entry_result": buy_result}

        # Sirf SELL fill
        if sell_result and not buy_result:
            return {"setup": sell_setup, "entry_result": sell_result}

        # Dono fill hue -> jo pehle time pe aaya
        buy_time = buy_result["entry_time"]
        sell_time = sell_result["entry_time"]

        if buy_time < sell_time:
            return {"setup": buy_setup, "entry_result": buy_result}

        if sell_time < buy_time:
            return {"setup": sell_setup, "entry_result": sell_result}

        # Same candle pe dono fill -> open ke close side ko choose karo
        same_row = day_df[day_df["time"] == buy_time]
        if same_row.empty:
            return {"setup": buy_setup, "entry_result": buy_result}

        row = same_row.iloc[0]
        open_price = float(row["open"])

        buy_dist = abs(float(buy_setup["entry"]) - open_price)
        sell_dist = abs(open_price - float(sell_setup["entry"]))

        if buy_dist <= sell_dist:
            return {"setup": buy_setup, "entry_result": buy_result}
        else:
            return {"setup": sell_setup, "entry_result": sell_result}

    # ------------------ ENTRY WINDOW (PENDING) ------------------
    def _resolve_same_candle_exit_with_m1(
        self,
        side: str,
        entry_time,
        actual_entry: float,
        sl: float,
        tp: float,
    ):
        return resolve_same_candle_exit_with_m1(
            engine=self,
            side=side,
            entry_time=entry_time,
            actual_entry=actual_entry,
            sl=sl,
            tp=tp,
        )

    def _fetch_m1_data_for_window(self, start_time, end_time):
        return fetch_m1_data_for_window(
            engine=self,
            start_time=start_time,
            end_time=end_time,
        )

    def _compute_m1_mae_after_entry(
        self,
        side: str,
        entry_time,
        exit_time,
        actual_entry: float,
        lot_size: float,
    ):
        return compute_m1_mae_after_entry(
            engine=self,
            side=side,
            entry_time=entry_time,
            exit_time=exit_time,
            actual_entry=actual_entry,
            lot_size=lot_size,
        )

    def _simulate_trade(
        self,
        df: pd.DataFrame,
        setup: Dict,
        entry_idx,
        actual_entry: float,
    ):
        return simulate_trade(
            engine=self,
            df=df,
            setup=setup,
            entry_idx=entry_idx,
            actual_entry=actual_entry,
        )

    def run_backtest(self, specs) -> None:
        data_by_pair, all_dates = prepare_backtest_data(self, specs)

        self.daily_briefings = []
        self.long_duration_trades = []

        if not data_by_pair or not all_dates:
            print("No data available for given specs/date range.")
            return

        print(f"\nTotal unique days in range: {len(all_dates)}")

        for day in all_dates:
            print("\n" + "=" * 60)
            print(f"PROCESSING DAY: {day}")

            day_open_balance = self.current_fund
            day_profit = 0.0
            day_trade_count = 0
            day_tp_hits = 0
            day_risk_percent = None
            day_max_lot = 0.0

            for pair, df in data_by_pair.items():
                if getattr(self, "stop_requested", False):
                    break

                week_num, risk_percent = get_weekly_risk_percent(self, day)
                day_risk_percent = risk_percent

                trades = process_pair_day_live_style(self, day, pair, df)

                for trade in trades:
                    day_trade_count += 1
                    day_profit += float(trade["pnl_amount"])
                    day_max_lot = max(day_max_lot, float(
                        trade.get("lot_size", 0.0)))

                    if trade["result"] == "tp":
                        day_tp_hits += 1

                    duration_hours = (
                        pd.to_datetime(trade["exit_time"]) -
                        pd.to_datetime(trade["entry_time"])
                    ).total_seconds() / 3600.0

                    if duration_hours > 2:
                        enriched_trade = dict(trade)
                        enriched_trade["duration_hours"] = round(
                            duration_hours, 2)
                        self.long_duration_trades.append(enriched_trade)

            day_close_balance = self.current_fund
            self.daily_briefings.append({
                "date": day,
                "open_balance": round(day_open_balance, 2),
                "close_balance": round(day_close_balance, 2),
                "profit": round(day_profit, 2),
                "trade_count": day_trade_count,
                "tp_hits": day_tp_hits,
                "risk_percent": day_risk_percent,
                "max_lot": round(day_max_lot, 2),
            })

            print(
                f"DAY SUMMARY | {day} | "
                f"Open=${day_open_balance:.2f} | "
                f"Close=${day_close_balance:.2f} | "
                f"PnL=${day_profit:.2f} | "
                f"Trades={day_trade_count} | "
                f"TP={day_tp_hits} | "
                f"MaxLot={day_max_lot:.2f}"
            )

            if getattr(self, "stop_requested", False):
                print(" -> Stop requested, backtest halted.")
                break

    def _human_amount(self, n: float) -> str:
        n = float(n)
        abs_n = abs(n)

        if abs_n >= 1_000_000_000_000:
            return f"{n / 1_000_000_000_000:.2f} trillion"
        elif abs_n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.2f} billion"
        elif abs_n >= 1_000_000:
            return f"{n / 1_000_000:.2f} million"
        elif abs_n >= 1_000:
            return f"{n / 1_000:.2f} thousand"
        else:
            return f"{n:.2f}"

    def _ensure_registry_file(self):
        return ensure_registry_file()

    def _load_live_registry(self) -> Dict:
        return load_live_registry()

    def _save_live_registry(self, data: Dict):
        return save_live_registry(data)

    def _get_live_fund_for_sizing(self) -> float:
        try:
            from live_fund_manager import get_live_usable_fund

            return float(get_live_usable_fund(
                currentfund=self.current_fund,
                initialfund=self.initial_fund,
                use_live_equity_sizing=getattr(
                    self, "use_live_equity_sizing", False),
                live_source_fund=getattr(self, "live_source_fund", None),
                live_strategy_start_fund=getattr(
                    self, "live_strategy_start_fund", None),
            ))
        except Exception as e:
            print(f"  -> _get_live_fund_for_sizing fallback current fund: {e}")
            return float(self.current_fund)

    def _fmt_live_ts(self, x):
        return fmt_live_ts(x)

    def _live_signal_expiry_server(self, day):
        return live_signal_expiry_server(day)

    def _make_signal_id_from_setup(self, pair: str, day, setup: dict) -> str:
        return make_signal_id_from_setup(pair, day, setup)

    def _mark_signal_completed_in_registry(self, signal_id: str, trade: Dict):
        return mark_signal_completed_in_registry(signal_id, trade)

    def _mark_signal_non_completed_in_registry(self, signal_id: str, status: str):
        return mark_signal_non_completed_in_registry(signal_id, status)

    def _is_signal_completed_in_registry(self, signal_id: str) -> bool:
        return is_signal_completed_in_registry(signal_id)

    def _is_same_completed_trade_prices(
        self,
        pair: str,
        day,
        side: str,
        entry: float,
        sl: float,
        tp: float,
        price_tol: float = 0.00005,
    ) -> bool:
        return is_same_completed_trade_prices(
            pair=pair,
            day=day,
            side=side,
            entry=entry,
            sl=sl,
            tp=tp,
            price_tol=price_tol,
        )

    def _has_any_completed_trade_for_pair_day(self, pair: str, day) -> bool:
        return has_any_completed_trade_for_pair_day(pair, day)

    def _has_active_registry_signal_for_pair_day_side(self, pair: str, day, side: str) -> bool:
        return has_active_registry_signal_for_pair_day_side(pair, day, side)

    def _get_active_registry_signal_for_pair_day_side(self, pair: str, day, side: str):
        return get_active_registry_signal_for_pair_day_side(pair, day, side)

    def _is_same_setup_signature(self, row: Dict, setup: dict, price_tol: float = 0.00005) -> bool:
        return is_same_setup_signature(row, setup, price_tol=price_tol)

    def _is_newer_setup_than_row(self, row: Dict, setup: dict) -> bool:
        return is_newer_setup_than_row(row, setup)

    def _is_setup_in_hhll_disable_window(self, setup: dict) -> bool:
        return is_setup_in_hhll_disable_window(
            setup=setup,
            disable_start_server=self.hhll_disable_start_server,
            disable_end_server=self.hhll_disable_end_server,
        )

    def _parse_registry_ts(self, x):
        return parse_registry_ts(x)

    def _get_signal_expiry_from_row(self, row: Dict):
        return get_signal_expiry_from_row(row)

    def _scan_signal_outcome_from_df(self, df: pd.DataFrame, row: Dict):
        return scan_signal_outcome_from_df(df, row)

    def _finalize_signal_from_market_before_cancel(self, signal_id: str, pair: str, day) -> bool:
        try:
            reg = self._load_live_registry()
            row = reg.get(signal_id)
            if not row:
                return False

            row_completed = bool(row.get("completed", False))
            row_status = str(row.get("registry_status", "")).strip().upper()
            row_exit_result = str(row.get("exit_result", "")).strip().lower()

            # Already finalized, kuch karne ki zarurat nahi
            if (
                row_completed
                or row_status == "COMPLETED"
                or row_exit_result in {"tp", "sl", "sl_lock10", "session_exit"}
            ):
                return True

            # Latest 15m data fetch
            from live_data_mt5 import fetch_live_15m

            df = fetch_live_15m(pair, lookback_days=30)
            if df is None or df.empty:
                return False

            df = df.copy()
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], errors="coerce")
            elif "datetime" in df.columns:
                df["time"] = pd.to_datetime(df["datetime"], errors="coerce")
            else:
                return False

            df = df.dropna(subset=["time"]).sort_values(
                "time").reset_index(drop=True)
            if df.empty:
                return False

            outcome = self._scan_signal_outcome_from_df(df, row)
            if outcome is None:
                return False

            result = str(outcome.get("result", "")).strip().lower()
            entry_hit = bool(outcome.get("entry_hit", False))
            entry_time = outcome.get("entry_time")
            exit_time = outcome.get("exit_time")

            if entry_hit:
                row["entry_hit"] = True
                row["entry_time"] = self._fmt_live_ts(entry_time)
                row["exit_time"] = self._fmt_live_ts(exit_time)

            if result in {"tp", "sl", "sl_lock10"}:
                row["exit_result"] = result
                row["registry_status"] = "COMPLETED"
                row["completed"] = True
                row["last_updated"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S")
                reg[signal_id] = row
                self._save_live_registry(reg)
                print(
                    f" -> Finalized before cancel: {signal_id} result={result}")
                return True

            if result == "open_or_expired":
                expiry = self._get_signal_expiry_from_row(row)
                now_ts = df.iloc[-1]["time"] if not df.empty else None
                if expiry is not None and now_ts is not None and now_ts > expiry:
                    row["exit_result"] = "session_exit"
                    row["registry_status"] = "COMPLETED"
                    row["completed"] = True
                    row["last_updated"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    reg[signal_id] = row
                    self._save_live_registry(reg)
                    print(
                        f" -> Finalized before cancel: {signal_id} result=session_exit")
                    return True

            return False

        except Exception as e:
            print(
                f" -> _finalize_signal_from_market_before_cancel failed for {signal_id}: {e}")
            return False

    def _reconcile_open_registry_signals_with_market_data(self, pair: str, df: pd.DataFrame):
        return reconcile_open_registry_signals_with_market_data(
            engine=self,
            pair=pair,
            df=df,
        )

    def _is_same_live_payload(self, existing: Optional[Dict], payload: Dict) -> bool:
        return is_same_live_payload(existing, payload)

    def _cancel_existing_signal_strict(
        self,
        pair: str,
        day,
        signal_file: str,
        existing: Optional[Dict],
        max_spread_points: int,
        max_slippage_points: int,
        reason: str = "CANCELLEDNEWHHLL",
    ):
        return cancel_existing_signal_strict(
            build_live_cancel_payload_fn=lambda **kwargs: self._build_live_cancel_payload(
                **kwargs),
            write_live_signal_file_fn=lambda signal_file, payload: self._write_live_signal_file(
                signal_file, payload),
            mark_signal_non_completed_in_registry_fn=self._mark_signal_non_completed_in_registry,
            terminal_filled_statuses=TERMINAL_FILLED_STATUSES,
            pair=pair,
            day=day,
            signal_file=signal_file,
            existing=existing,
            max_spread_points=max_spread_points,
            max_slippage_points=max_slippage_points,
            reason=reason,
            pre_cancel_finalize_fn=lambda signal_id: self._finalize_signal_from_market_before_cancel(
                signal_id=signal_id,
                pair=pair,
                day=day,
            ),
        )

    def _write_fresh_signal_after_strict_delete(
        self,
        pair: str,
        day,
        signal_file: str,
        setup: dict,
        existing: Optional[Dict],
        existing_status: str,
        max_spread_points: int,
        max_slippage_points: int,
        reason: str = "CANCELLEDNEWHHLL",
    ):
        return write_fresh_signal_after_strict_delete(
            load_live_registry_fn=self._load_live_registry,
            has_active_registry_signal_for_pair_day_side_fn=self._has_active_registry_signal_for_pair_day_side,
            make_signal_id_from_setup_fn=self._make_signal_id_from_setup,
            is_signal_completed_in_registry_fn=self._is_signal_completed_in_registry,
            is_same_completed_trade_prices_fn=self._is_same_completed_trade_prices,
            build_live_place_payload_fn=lambda **kwargs: self._build_live_place_payload(
                **kwargs),
            is_same_live_payload_fn=self._is_same_live_payload,
            cancel_existing_signal_strict_fn=lambda **kwargs: self._cancel_existing_signal_strict(
                **kwargs),
            write_live_signal_file_fn=lambda signal_file, payload: self._write_live_signal_file(
                signal_file, payload),
            active_file_statuses=ACTIVE_FILE_STATUSES,
            terminal_filled_statuses=TERMINAL_FILLED_STATUSES,
            pair=pair,
            day=day,
            signal_file=signal_file,
            setup=setup,
            existing=existing,
            existing_status=existing_status,
            max_spread_points=max_spread_points,
            max_slippage_points=max_slippage_points,
            reason=reason,
        )

    def _build_live_cancel_payload(
        self,
        pair: str,
        day,
        existing_signal_id: str = "",
        existing_side: str = "",
        max_spread_points=25,
        max_slippage_points=15,
    ):
        return build_live_cancel_payload(
            live_signal_expiry_server_fn=self._live_signal_expiry_server,
            pair=pair,
            day=day,
            existing_signal_id=existing_signal_id,
            existing_side=existing_side,
            max_spread_points=max_spread_points,
            max_slippage_points=max_slippage_points,
        )

    def _build_live_place_payload(
        self,
        pair: str,
        day,
        setup: dict,
        action: str = "PLACE",
        max_spread_points=25,
        max_slippage_points=15,
    ):
        return build_live_place_payload(
            fmt_live_ts_fn=self._fmt_live_ts,
            make_signal_id_from_setup_fn=self._make_signal_id_from_setup,
            is_signal_completed_in_registry_fn=self._is_signal_completed_in_registry,
            is_same_completed_trade_prices_fn=self._is_same_completed_trade_prices,
            load_live_registry_fn=self._load_live_registry,
            save_live_registry_fn=self._save_live_registry,
            live_signal_expiry_server_fn=self._live_signal_expiry_server,
            pair=pair,
            day=day,
            setup=setup,
            action=action,
            max_spread_points=max_spread_points,
            max_slippage_points=max_slippage_points,
        )

    def _live_payload_to_line(self, payload: dict) -> str:
        return live_payload_to_line(payload)

    def _read_existing_live_signal(self, signal_file: str):
        return read_existing_live_signal(signal_file)

    def _write_live_signal_file(self, signal_file: str, payload: dict):
        return write_live_signal_file(
            signal_file=signal_file,
            payload=payload,
            read_existing_live_signal_fn=self._read_existing_live_signal,
            live_payload_to_line_fn=self._live_payload_to_line,
            is_same_live_payload_fn=self._is_same_live_payload,
        )

    def _choose_live_setup_for_day(self, day_df: pd.DataFrame, fund: float, risk_percent: float):
        return choose_live_setup_for_day(
            engine=self,
            day_df=day_df,
            fund=fund,
            risk_percent=risk_percent,
        )

    def generate_live_dual_signals_for_latest_day(
        self,
        pair: str,
        df_15m: pd.DataFrame,
        signal_file: str = None,
        signal_dir: str = None,
        max_spread_points: int = 25,
        max_slippage_points: int = 15,
    ):
        return generate_live_dual_signals_for_latest_day(
            engine=self,
            terminal_filled_statuses=TERMINAL_FILLED_STATUSES,
            pair=pair,
            df_15m=df_15m,
            signal_file=signal_file,
            signal_dir=signal_dir,
            max_spread_points=max_spread_points,
            max_slippage_points=max_slippage_points,
        )

    def export_to_excel(self, output_path: str) -> None:
        folder = "backtests"
        os.makedirs(folder, exist_ok=True)
        full_path = os.path.join(folder, os.path.basename(output_path))

        total_trades = len(self.trades)
        net_pnl = self.current_fund - self.initial_fund

        # Result counts
        wins = sum(1 for t in self.trades if t["result"] == "tp")
        sl_lock10 = sum(1 for t in self.trades if t["result"] == "sl_lock10")
        losses = sum(1 for t in self.trades if t["result"] == "sl")
        expired = sum(
            1 for t in self.trades
            if str(t.get("result", "")).lower() == RESULT_ORDER_EXPIRED
        )
        others = total_trades - (wins + sl_lock10 + losses + expired)

        summary = {
            "Metric": [
                "Initial Fund",
                "Final Fund",
                "Final Fund (words)",
                "Net PNL",
                "Total Records",
                "Win Rate (TP only)",
                "Max Drawdown",
                "Total Trades",
                "Wins (TP)",
                "SL Lock10 Hits",
                "Losses (SL)",
                "Expired Orders",
                "Other Results",
            ],
            "Value": [
                self.initial_fund,
                self.current_fund,
                self._human_amount(self.current_fund),
                net_pnl,
                total_trades,
                f"{self.win_rate:.2f}%" if total_trades > 0 else "N/A",
                self.max_drawdown,
                total_trades,
                wins,
                sl_lock10,
                losses,
                expired,
                others,
            ],
        }

        with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
            if self.trades:
                trades_df = pd.DataFrame(self.trades)

                # Ensure columns exist for backward compatibility
                if "entry_mode" not in trades_df.columns:
                    trades_df["entry_mode"] = ""

                if "sl_mode" not in trades_df.columns:
                    trades_df["sl_mode"] = "NORMAL"

                # Entry From column from entry_mode
                trades_df["Entry From"] = trades_df["entry_mode"].apply(
                    lambda x: "T1" if isinstance(x, str) and x.endswith("_T1")
                    else ("AT" if isinstance(x, str) and x.endswith("_AT") else "")
                )

                # Put "Entry From" right after entry_price
                if "entry_price" in trades_df.columns:
                    insert_pos = trades_df.columns.get_loc("entry_price") + 1
                    col = trades_df.pop("Entry From")
                    trades_df.insert(insert_pos, "Entry From", col)

                # Optional: make sl_mode column display-friendly
                trades_df["SL Mode"] = trades_df["sl_mode"].apply(
                    lambda x: "SL_LOCK10" if x == "LOCK10_TP80" else "NORMAL"
                )

                # Optional column ordering for readability
                preferred_order = [
                    "date",
                    "pair",
                    "side",
                    "entry_time",
                    "entry_price",
                    "Entry From",
                    "sl",
                    "tp",
                    "exit_time",
                    "exit_price",
                    "result",
                    "SL Mode",
                    "pnl_pips",
                    "pnl_amount",
                    "fund_after",
                    "max_adverse_pips",
                    "max_adverse_amount",
                    "balance_before_trade",
                    "min_available_balance_during_trade",
                    "entry_mode",
                    "sl_mode",
                ]

                existing_cols = [
                    c for c in preferred_order if c in trades_df.columns]
                remaining_cols = [
                    c for c in trades_df.columns if c not in existing_cols]
                trades_df = trades_df[existing_cols + remaining_cols]

                trades_df.to_excel(writer, sheet_name="Trades", index=False)

            summary_df = pd.DataFrame(summary)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            if hasattr(self, "daily_briefings") and self.daily_briefings:
                daily_df = pd.DataFrame(self.daily_briefings)

                expected_cols = [
                    "date",
                    "open_balance",
                    "risk_percent",
                    "no_trades",
                    "tp_hits",
                    "max_lot",
                    "profit",
                    "final_balance",
                ]

                for col in expected_cols:
                    if col not in daily_df.columns:
                        if col == "max_lot":
                            daily_df[col] = 0.0
                        else:
                            daily_df[col] = None

                daily_df = daily_df[expected_cols]

                daily_df.columns = [
                    "Date",
                    "Open Balance",
                    "Risk %",
                    "No Trades",
                    "TP Hits",
                    "Max Lot",
                    "Profit",
                    "Final Balance",
                ]

                daily_df.to_excel(
                    writer, sheet_name="Day Wise Briefing", index=False
                )

        print(f"\nBacktest results exported to: {full_path}")
