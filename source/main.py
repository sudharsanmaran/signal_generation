from enum import Enum
from itertools import chain
import time
from typing import Dict, List, Optional
import pandas as pd


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
        if self.entry_signal == "long":
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

    def formulate_output(self):
        return [
            {
                "Instrument": Trade.instrument,
                "Strategy ID": Trade.strategy_ids,
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


def validate_input(
    instrument,
    strategy_id,
    start_date,
    end_date,
    fractal_file_number,
    bb_file_number,
    bb_band_sd,
):
    # Validate the input parameters
    # todo
    # 1. extract the inputs, even for multiple strategies go with squential manner
    pass


def read_data(
    instrument,
    portfolio_ids,
    strategy_ids,
    start_date,
    end_date,
    entry_fractal_file_number,
    exit_fractal_file_number,
    bb_file_number,
    bb_band_column,
    trail_bb_file_number,
    trail_bb_band_column,
):

    # Convert start and end dates to datetime
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")

    strategy_dfs, is_close_read = [], False
    for portfolio_id, strategy_id in zip(portfolio_ids, strategy_ids):
        strategy_path = f"~/Downloads/Portfolio Database/{portfolio_id}/{instrument}/{strategy_id}_result.csv"
        columns = []
        if not is_close_read:
            columns = ["TIMESTAMP", "Close", f"TAG_{portfolio_id}"]
            is_close_read = True
        else:
            columns = ["TIMESTAMP", f"TAG_{portfolio_id}"]
        strategy_df = pd.read_csv(
            strategy_path,
            parse_dates=["TIMESTAMP"],
            date_format="%Y-%m-%d %H:%M:%S",
            usecols=columns,
            index_col="TIMESTAMP",
        )
        strategy_df.index = pd.to_datetime(strategy_df.index)
        strategy_df = strategy_df.loc[start_date:end_date]
        strategy_dfs.append(strategy_df)

    all_strategies_df = pd.concat(strategy_dfs, axis=1)

    entry_fractal_path = f"~/Downloads/Portfolio Database/Fractal/{instrument}/{entry_fractal_file_number}_result.csv"
    exit_fractal_path = f"~/Downloads/Portfolio Database/Fractal/{instrument}/{exit_fractal_file_number}_result.csv"
    bb_band_path = f"~/Downloads/Portfolio Database/BB Band/{instrument}/{bb_file_number}_result.csv"
    trail_bb_band_path = f"~/Downloads/Portfolio Database/BB Band/{instrument}/{trail_bb_file_number}_result.csv"

    entry_fractal_df = pd.read_csv(
        entry_fractal_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M",
        usecols=[
            "TIMESTAMP",
            "P_1_PRE_FRACTAL_LONG",
            "P_1_PRE_FRACTAL_SHORT",
            "P_1_FRACTAL_CONFIRMED_LONG",
            "P_1_FRACTAL_CONFIRMED_SHORT",
        ],
        dtype={
            "P_1_PRE_FRACTAL_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_SHORT": "boolean",
            "P_1_PRE_FRACTAL_SHORT": "boolean",
        },
        index_col="TIMESTAMP",
    )
    # convert dt to datetime
    entry_fractal_df.index = pd.to_datetime(entry_fractal_df.index)

    exit_fractal_df = pd.read_csv(
        exit_fractal_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M",
        usecols=[
            "TIMESTAMP",
            "P_1_PRE_FRACTAL_LONG",
            "P_1_PRE_FRACTAL_SHORT",
            "P_1_FRACTAL_CONFIRMED_LONG",
            "P_1_FRACTAL_CONFIRMED_SHORT",
        ],
        dtype={
            "P_1_PRE_FRACTAL_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_SHORT": "boolean",
            "P_1_PRE_FRACTAL_SHORT": "boolean",
        },
        index_col="TIMESTAMP",
    )
    # convert dt to datetime
    exit_fractal_df.index = pd.to_datetime(entry_fractal_df.index)

    # Define the columns to read from BB band file based on bb_band_sd
    bb_band_cols = ["TIMESTAMP", bb_band_column]

    # Read the BB band file with date filtering, parsing, and indexing
    bb_band_df = pd.read_csv(
        bb_band_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=bb_band_cols,
        index_col="TIMESTAMP",
    )

    # Rename BB band columns for consistency
    bb_band_df.rename(
        columns={bb_band_column: f"bb_{bb_band_column}"},
        inplace=True,
    )

    # Define the columns to read from Trail BB band file based on bb_band_sd
    trail_bb_band_cols = ["TIMESTAMP", trail_bb_band_column]

    # Read the Trail BB band file with date filtering, parsing, and indexing
    trail_bb_band_df = pd.read_csv(
        trail_bb_band_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=trail_bb_band_cols,
        index_col="TIMESTAMP",
    )

    trail_bb_band_df.rename(
        columns={bb_band_column: f"trail_{trail_bb_band_column}"},
        inplace=True,
    )

    entry_fractal_df = entry_fractal_df[
        (entry_fractal_df.index >= start_date) & (entry_fractal_df.index <= end_date)
    ]
    exit_fractal_df = exit_fractal_df[
        (exit_fractal_df.index >= start_date) & (exit_fractal_df.index <= end_date)
    ]
    bb_band_df = bb_band_df[
        (bb_band_df.index >= start_date) & (bb_band_df.index <= end_date)
    ]
    trail_bb_band_df = trail_bb_band_df[
        (trail_bb_band_df.index >= start_date) & (trail_bb_band_df.index <= end_date)
    ]

    all_strategies_df = all_strategies_df.dropna(axis=0)
    entry_fractal_df = entry_fractal_df.dropna(axis=0)
    exit_fractal_df = exit_fractal_df.dropna(axis=0)
    bb_band_df = bb_band_df.dropna(axis=0)
    trail_bb_band_df = trail_bb_band_df.dropna(axis=0)

    return (
        all_strategies_df,
        entry_fractal_df,
        exit_fractal_df,
        bb_band_df,
        trail_bb_band_df,
    )


def merge_data(
    strategy_df, entry_fractal_df, exit_fractal_df, bb_band_df, trail_bb_band_df
):
    merged_df = strategy_df.join(entry_fractal_df, how="left")
    merged_df = strategy_df.join(exit_fractal_df, how="left")
    merged_df = merged_df.join(bb_band_df, how="left")
    merged_df = merged_df.join(trail_bb_band_df, how="left")
    return merged_df


def is_trade_start_time_crossed(row):
    """Check if the trade start time is crossed for the given row"""

    if row.name.time() >= Trade.trade_start_time:
        return True
    return False


class MarketDirection(Enum):
    LONG = "long"
    SHORT = "short"
    PREVIOUS = "previous"
    ALL = "all"


def get_market_direction(row, condition_key):
    """Get the market direction based on the entry or exit conditions for a trade."""

    row_directions = row.get(Trade.signal_columns)
    for direction, signals in Trade.market_direction_conditions[condition_key].items():
        for signal in signals:
            if all(dir == sig for dir, sig in zip(row_directions, signal)):
                return direction
    return None


def reset_last_fractal(last_fractal, market_direction):
    """Reset the last fractal for the opposite direction when the market direction changes"""
    if not market_direction:
        return
    opposite_direction = (
        MarketDirection.SHORT
        if market_direction == MarketDirection.LONG
        else MarketDirection.LONG
    )
    last_fractal[opposite_direction] = None


def update_last_fractal(last_fractal, market_direction, row):
    """Update the last fractal for the current market direction if a new fractal is found"""
    if not market_direction:
        return
    fractal_keys = {
        MarketDirection.LONG: "P_1_PRE_FRACTAL_LONG",
        MarketDirection.SHORT: "P_1_PRE_FRACTAL_SHORT",
    }
    if row[fractal_keys[market_direction]]:
        last_fractal[market_direction] = (row.name, row["Close"])


def check_fractal_conditions(row, last_fractal, market_direction):
    """Check the fractal entry conditions for a trade based on the given row"""
    fractal_columns = {
        MarketDirection.LONG: "P_1_FRACTAL_CONFIRMED_LONG",
        MarketDirection.SHORT: "P_1_FRACTAL_CONFIRMED_SHORT",
    }

    if last_fractal.get(market_direction) and row[fractal_columns[market_direction]]:
        return True

    return False


def check_bb_band_entry(row, last_fractal, market_direction):
    """Check the BB band entry conditions for a trade based on the given row"""

    if not last_fractal.get(market_direction):
        return False

    fractal_value = last_fractal[market_direction][1]
    bb_band_value = row[f"bb_{Trade.bb_band_column}"]

    compare = (
        (lambda a, b: a < b)
        if market_direction == MarketDirection.LONG
        else (lambda a, b: a > b)
    )

    return compare(fractal_value, bb_band_value)


def check_entry_conditions(row, last_fractal):
    """Check the entry conditions for a trade based on the given row"""

    if not is_trade_start_time_crossed(row):
        return False, None
    if Trade.type == TradeType.INTRADAY and row.name.time() >= Trade.trade_end_time:
        return False, None

    market_direction = get_market_direction(row, "entry")
    if not market_direction:
        return False, None

    if (
        not Trade.allowed_direction == MarketDirection.ALL
        and not market_direction == Trade.allowed_direction
    ):
        return False, None

    reset_last_fractal(last_fractal, market_direction)
    update_last_fractal(last_fractal, market_direction, row)

    if Trade.check_fractal:
        is_fractal_entry = check_fractal_conditions(row, last_fractal, market_direction)
    if Trade.check_bb_band:
        is_bb_band_entry = check_bb_band_entry(row, last_fractal, market_direction)

    if Trade.check_fractal and Trade.check_bb_band:
        return is_fractal_entry and is_bb_band_entry, market_direction
    elif Trade.check_fractal:
        return is_fractal_entry, market_direction
    elif Trade.check_bb_band:
        return is_bb_band_entry, market_direction

    return False


def is_trade_end_time_reached(row):
    if Trade.type == TradeType.INTRADAY and row.name.time() >= Trade.trade_end_time:
        return True
    return False


def check_bb_band_trail_exit(row, last_fractal, market_direction):
    if not last_fractal.get(market_direction):
        return False

    fractal_value = last_fractal[market_direction][1]
    bb_band_value = row[f"trail_{Trade.trail_bb_band_column}"]

    if last_fractal.get("trail_first_found", False):
        if Trade.trail_opposite_compare_func(fractal_value, bb_band_value):
            last_fractal["trail_first_found"] = False
            return True
    else:
        if Trade.trail_compare_func(fractal_value, bb_band_value):
            last_fractal["trail_first_found"] = True
            last_fractal["first_trail_time"] = row.name

    return False


def tag_change_exit(previous_direction, market_direction):
    if market_direction != previous_direction:
        return True
    return False


class TradeExitType(Enum):
    FRACTAL = "Fractal Exit"
    SIGNAL = "Signal Change"
    TRAILING = "Trailling"
    END = "Trade End Exit"


def identify_exit_signals(row, last_fractal):

    if is_trade_end_time_reached(row):
        return True, TradeExitType.END

    market_direction = get_market_direction(row, "exit")
    if not market_direction:
        return False, None

    reset_last_fractal(last_fractal, market_direction)
    update_last_fractal(last_fractal, market_direction, row)

    exit_type, is_trail_bb_band_exit, is_fractal_exit = None, False, False
    if Trade.check_trail_bb_band:
        is_trail_bb_band_exit = check_bb_band_trail_exit(
            row, last_fractal, market_direction
        )

    if Trade.check_fractal:
        is_fractal_exit = check_fractal_conditions(row, last_fractal, market_direction)

    previous_direction = last_fractal.get(MarketDirection.PREVIOUS, None)
    last_fractal[MarketDirection.PREVIOUS] = market_direction
    if previous_direction and tag_change_exit(previous_direction, market_direction):
        return True, TradeExitType.SIGNAL

    if is_trail_bb_band_exit and is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    elif is_trail_bb_band_exit:
        exit_type = TradeExitType.TRAILING
    elif is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    return is_trail_bb_band_exit or is_fractal_exit, exit_type


pd.set_option("display.max_rows", None)  # None means show all rows
pd.set_option("display.max_columns", None)  # None means show all columns
pd.set_option("display.width", 800)  # Adjust the width to your preference
pd.set_option("display.max_colwidth", None)


class TradeType(Enum):
    INTRADAY = "Intraday"
    POSITIONAL = "Positional"


def main():
    start = time.time()
    instrument = "BANKNIFTY"
    portfolio_ids = "F13, F13_1"
    # todo
    # 1. handle multiple strategy pair sequently
    strategy_ids = "1, 4"
    long_entry_signals = "RED, GREEN"
    long_exit_signals = "GREEN, RED | RED, RED | GREEN, GREEN"
    short_entry_signals = "GREEN, RED"
    short_exit_signals = "GREEN, GREEN | RED, GREEN | RED, RED"
    start_date = "3/01/2017 12:30:00"
    end_date = "3/07/2023 16:00:00"
    entry_fractal_file_number = 1
    exit_fractal_file_number = 2
    fractal_exit_count = "ALL"  # or 1 or 2 or 3 etc.
    bb_file_number = 1
    trail_bb_file_number = 1
    bb_band_sd = 2.0  # standard deviations (2.0, 2.25, 2.5, 2.75, 3.0)
    trail_bb_band_sd = 2.0  # standard deviations (2.0, 2.25, 2.5, 2.75, 3.0)
    bb_band_column = "mean"  # (mean, upper, lower)
    trail_bb_band_column = "mean"
    trade_start_time = "09:15:00"
    trade_end_time = "15:20:00"
    check_fractal = True
    check_bb_band = True
    check_trail_bb_band = False
    trail_bb_band_direction = "higher"  # or "lower"
    trade_type = TradeType.POSITIONAL
    allowed_direction = MarketDirection.ALL

    # todo
    # Validate the input parameters
    portfolio_ids = [id.strip() for id in portfolio_ids.split(",")]
    strategy_ids = [id.strip() for id in strategy_ids.split(",")]

    def parse_signals(signals):
        return [signal.strip() for signal in signals]

    # Create the strategy_signal_map for each strategy
    conditions = {
        "entry": {
            MarketDirection.LONG: [
                parse_signals(cond.split(",")) for cond in long_entry_signals.split("|")
            ],
            MarketDirection.SHORT: [
                parse_signals(cond.split(","))
                for cond in short_entry_signals.split("|")
            ],
        },
        "exit": {
            MarketDirection.LONG: [
                parse_signals(cond.split(",")) for cond in long_exit_signals.split("|")
            ],
            MarketDirection.SHORT: [
                parse_signals(cond.split(",")) for cond in short_exit_signals.split("|")
            ],
        },
    }

    # set the class variables
    Trade.strategy_ids = strategy_ids
    Trade.instrument = instrument
    Trade.trade_start_time = pd.to_datetime(trade_start_time).time()
    Trade.trade_end_time = pd.to_datetime(trade_end_time).time()
    Trade.check_fractal = check_fractal
    Trade.check_bb_band = check_bb_band
    Trade.check_trail_bb_band = check_trail_bb_band
    Trade.type = trade_type
    Trade.market_direction_conditions = conditions
    Trade.bb_band_column = f"P_1_{bb_band_column.upper()}_BAND_{bb_band_sd}"
    Trade.trail_bb_band_column = (
        f"P_1_{trail_bb_band_column.upper()}_BAND_{trail_bb_band_sd}"
    )
    Trade.allowed_direction = allowed_direction
    Trade.signal_columns = [f"TAG_{id}" for id in portfolio_ids]

    if trail_bb_band_direction == "higher":
        Trade.trail_compare_func = lambda a, b: a > b
        Trade.trail_opposite_compare_func = lambda a, b: a < b
    else:
        Trade.trail_compare_func = lambda a, b: a < b
        Trade.trail_opposite_compare_func = lambda a, b: a > b

    try:
        Trade.fractal_exit_count = int(fractal_exit_count)
    except ValueError:
        pass

    strategy_df, entry_fractal_df, exit_fractal_df, bb_band_df, trail_bb_band_df = (
        read_data(
            Trade.instrument,
            portfolio_ids,
            strategy_ids,
            start_date,
            end_date,
            entry_fractal_file_number,
            exit_fractal_file_number,
            bb_file_number,
            Trade.bb_band_column,
            trail_bb_file_number,
            Trade.trail_bb_band_column,
        )
    )

    # Merge data
    merged_df = merge_data(
        strategy_df, entry_fractal_df, entry_fractal_df, bb_band_df, trail_bb_band_df
    )

    merged_df.to_csv(f"merged_df.csv", index=True)

    # Dictionaries to track last fractals for both entry and exit
    entry_last_fractal = {
        MarketDirection.LONG: None,
        MarketDirection.SHORT: None,
    }
    exit_last_fractal = {
        MarketDirection.LONG: None,
        MarketDirection.SHORT: None,
        MarketDirection.PREVIOUS: None,
    }
    active_trades, completed_trades = [], []
    for index, row in merged_df.iterrows():
        is_entry, direction = check_entry_conditions(row, entry_last_fractal)
        is_exit, exit_type = identify_exit_signals(row, exit_last_fractal)
        if is_entry:
            trade = Trade(
                entry_signal=direction,
                entry_datetime=index,
                entry_price=row["Close"],
            )
            active_trades.append(trade)

        if is_exit:
            for trade in active_trades[:]:
                trade.add_exit(row.name, row["Close"], exit_type)
                if trade.is_trade_closed():
                    completed_trades.append(trade)
                    active_trades.remove(trade)

    trade_outputs = []
    for trade in chain(completed_trades, active_trades):
        trade_outputs.extend(trade.formulate_output())

    output_df = pd.DataFrame(trade_outputs)

    output_df.to_csv("output.csv", index=False)
    stop = time.time()
    print(f"Time taken: {stop-start} seconds")


if __name__ == "__main__":
    main()
