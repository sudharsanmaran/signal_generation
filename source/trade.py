from typing import Dict, List, Optional

from source.constants import MarketDirection, TradeExitType


class Trade:
    strategy_ids: Optional[List] = None
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
    signal_columns: Optional[List] = None

    def __init__(self, entry_signal, entry_datetime, entry_price):
        Trade.entry_id_counter += 1
        self.entry_id = Trade.entry_id_counter

        self.entry_signal = entry_signal
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

    def formulate_output(self, strategy_pair):
        return [
            {
                "Instrument": Trade.instrument,
                "Strategy ID": strategy_pair,
                "Signal": self.entry_signal.value,
                "Entry Datetime": self.entry_datetime,
                "Entry ID": self.entry_id,
                "Exit ID": exit["exit_id"],
                "Exit Datetime": exit["exit_datetime"],
                "Exit Type": exit["exit_type"].value,
                "Entry Price": self.entry_price,
                "Exit Price": exit["exit_price"],
                "Profit/Loss": exit["pnl"],
            }
            for exit in self.exits
        ]
