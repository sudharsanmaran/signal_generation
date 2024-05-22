from typing import Dict, Optional

from source.constants import MarketDirection, TradeExitType


class Trade:
    portfolio_ids: tuple
    strategy_ids: Optional[tuple] = None
    entry_id_counter: int = 0
    fractal_exit_count: Optional[int] = None
    instrument: Optional[str] = None
    trade_start_time = None
    trade_end_time = None
    check_fractal: bool = False
    check_bb_band: bool = False
    check_trail_bb_band: bool = False
    bb_band_column: Optional[str] = None
    trail_bb_band_column: Optional[str] = None
    type: Optional[str] = None
    market_direction_conditions: Dict = {}
    allowed_direction: Optional[str] = None
    trail_bb_band_direction: Optional[str] = None
    trail_compare_func: Optional[callable] = None
    trail_opposite_compare_func: Optional[callable] = None
    signal_columns: Optional[tuple] = None

    def __init__(self, entry_signal, entry_datetime, entry_price, signal_count):
        Trade.entry_id_counter += 1
        self.entry_id = Trade.entry_id_counter

        self.entry_signal = entry_signal
        self.signal_count = signal_count
        self.entry_datetime = entry_datetime
        self.entry_price = entry_price
        self.exits = []
        self.trade_closed = False
        self.exit_id_counter = 0

    def calculate_pnl(self, exit_price):
        pnl = 0
        if self.entry_signal == MarketDirection.LONG:
            pnl = exit_price - self.entry_price
        else:
            pnl = self.entry_price - exit_price
        return pnl

    def add_exit(self, exit_datetime, exit_price, exit_type):
        if not self.trade_closed:
            self.exit_id_counter += 1

            if exit_type in (
                TradeExitType.SIGNAL,
                TradeExitType.TRAILING,
                TradeExitType.END,
            ):
                self.trade_closed = True

            if Trade.fractal_exit_count:
                if (
                    exit_type == TradeExitType.FRACTAL
                    and self.exit_id_counter == Trade.fractal_exit_count
                ):
                    self.exits.append(
                        {
                            "exit_id": self.exit_id_counter,
                            "exit_datetime": exit_datetime,
                            "exit_price": exit_price,
                            "exit_type": exit_type,
                            "pnl": self.calculate_pnl(exit_price),
                        }
                    )
            else:
                self.exits.append(
                    {
                        "exit_id": self.exit_id_counter,
                        "exit_datetime": exit_datetime,
                        "exit_price": exit_price,
                        "exit_type": exit_type,
                        "pnl": self.calculate_pnl(exit_price),
                    }
                )

    def is_trade_closed(self):
        return self.trade_closed

    def formulate_output(self, strategy_pair, portfolio_pair=None):
        return [
            {
                "Instrument": Trade.instrument,
                "Portfolios": portfolio_pair,
                "Strategy IDs": strategy_pair,
                "Signal": self.entry_signal.value,
                "Signal Number": self.signal_count,
                "Entry Datetime": self.entry_datetime,
                "Entry ID": self.entry_id,
                "Exit ID": exit["exit_id"],
                "Exit Datetime": exit["exit_datetime"],
                "Exit Type": exit["exit_type"].value,
                "Intraday/ Positional": Trade.type.value,
                "Entry Price": self.entry_price,
                "Exit Price": exit["exit_price"],
                "Net points": exit["pnl"],
            }
            for exit in self.exits
        ]


def initialize(**kwargs):
    Trade.portfolio_ids = kwargs.get("portfolio_ids")
    Trade.strategy_ids = kwargs.get("strategy_ids")
    Trade.instrument = kwargs.get("instrument")
    Trade.trade_start_time = kwargs.get("trade_start_time")
    Trade.trade_end_time = kwargs.get("trade_end_time")
    Trade.check_fractal = kwargs.get("check_fractal")
    Trade.check_bb_band = kwargs.get("check_bb_band")
    Trade.check_trail_bb_band = kwargs.get("check_trail_bb_band")
    Trade.type = kwargs.get("trade_type")
    Trade.market_direction_conditions = {
        "entry": {
            MarketDirection.LONG: kwargs.get("long_entry_signals"),
            MarketDirection.SHORT: kwargs.get("short_entry_signals"),
        },
        "exit": {
            MarketDirection.LONG: kwargs.get("long_exit_signals"),
            MarketDirection.SHORT: kwargs.get("short_exit_signals"),
        },
    }
    Trade.bb_band_column = (
        f"P_1_{kwargs.get('bb_band_column').upper()}_BAND_{kwargs.get('bb_band_sd')}"
    )
    Trade.trail_bb_band_column = f"P_1_{kwargs.get('trail_bb_band_column').upper()}_BAND_{kwargs.get('trail_bb_band_sd')}"
    Trade.allowed_direction = kwargs.get("allowed_direction")
    Trade.signal_columns = [f"TAG_{id}" for id in kwargs.get("portfolio_ids")]

    fractal_exit_count = kwargs.get("fractal_exit_count")
    Trade.fractal_exit_count = (
        fractal_exit_count if isinstance(fractal_exit_count, int) else None
    )

    if kwargs.get("trail_bb_band_direction") == "higher":
        Trade.trail_compare_func = lambda a, b: a > b
        Trade.trail_opposite_compare_func = lambda a, b: a < b
    else:
        Trade.trail_compare_func = lambda a, b: a < b
        Trade.trail_opposite_compare_func = lambda a, b: a > b
