from enum import Enum
from itertools import chain
import time
from typing import List, Optional
import pandas as pd


class Trade:
    entry_id_counter: int = 0
    max_exits = float("inf")
    instrument: Optional[str] = None
    strategy_id: Optional[List[int]] = None
    trade_start_time = None
    trade_end_time = None
    check_fractal: bool = False
    check_bb_band: bool = False
    bb_band_column: Optional[str] = None
    type: Optional[str] = None
    # todo
    # 1. add trade start time and end time
    # 2. adjust logic for fractal_exit for specific nth position exits
    # 3. trade type for intraday or positional
    # 4. trade direction to consider for the trade(LONG, SHORT, ALL)

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
            self.exits.append(
                {
                    "exit_id": self.exit_id_counter,
                    "exit_datetime": exit_datetime,
                    "exit_price": exit_price,
                    "exit_type": exit_type,
                    "pnl": self.calculate_pnl(exit_price),
                }
            )

            # Check for trade closure based on max_exits and exit_type
            if (
                self.exit_id_counter >= Trade.max_exits
                or exit_type == "Tag Change Exit"
            ):
                self.trade_closed = True

    def is_trade_closed(self):
        return self.trade_closed

    def formulate_output(self):
        return [
            {
                "Instrument": Trade.instrument,
                "Strategy ID": Trade.strategy_id,
                "Signal": self.entry_signal,
                "Entry Datetime": self.entry_datetime,
                "Entry ID": self.entry_id,
                "Exit ID": exit["exit_id"],
                "Exit Datetime": exit["exit_datetime"],
                "Exit Type": exit["exit_type"],
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
    strategy_id,
    start_date,
    end_date,
    fractal_file_number,
    bb_file_number,
    bb_band_sd,
):
    # Define the hardcoded paths to the files
    strategy_path = f"~/Downloads/Test case Database/Strategy/F13/{instrument}/{strategy_id}_result.csv"
    fractal_path = f"~/Downloads/Test case Database/Entry & Exit/Fractal/{instrument}/combined_{fractal_file_number}.csv"
    bb_band_path = f"~/Downloads/Test case Database/Entry & Exit/BB/{instrument}/combined_{bb_file_number}.csv"

    # Read the strategy file with date filtering, parsing, and indexing
    strategy_df = pd.read_csv(
        strategy_path,
        parse_dates=["dt"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=["dt", "Close", "TAG", "Strategy Number"],
        dtype={"Strategy Number": int},
        index_col="dt",
    )
    strategy_df.rename(columns={"TAG": "tag"}, inplace=True)

    # Read the fractal file with date filtering, parsing, and indexing
    fractal_df = pd.read_csv(
        fractal_path,
        parse_dates=["dt"],
        date_format="%Y-%m-%d %H:%M",
        usecols=[
            "dt",
            "P_1_FRACTAL_LONG",
            "P_1_FRACTAL_SHORT",
            "P_1_FRACTAL_CONFIRMED_LONG",
            "P_1_FRACTAL_CONFIRMED_SHORT",
        ],
        dtype={
            "P_1_FRACTAL_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_SHORT": "boolean",
            "P_1_FRACTAL_SHORT": "boolean",
        },
        index_col="dt",
    )
    # convert dt to datetime
    fractal_df.index = pd.to_datetime(fractal_df.index)

    # Define the columns to read from BB band file based on bb_band_sd
    bb_band_cols = [
        "DT",
        f"P_1_MEAN_BAND_{bb_band_sd}",
        f"P_1_UPPER_BAND_{bb_band_sd}",
        f"P_1_LOWER_BAND_{bb_band_sd}",
    ]

    # Read the BB band file with date filtering, parsing, and indexing
    bb_band_df = pd.read_csv(
        bb_band_path,
        parse_dates=["DT"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=bb_band_cols,
        index_col="DT",
    )

    # Rename BB band columns for consistency
    bb_band_df.rename(
        columns={
            f"CLOSE_{bb_band_sd}": "close",
            f"P_1_MEAN_BAND_{bb_band_sd}": "mean_band",
            f"P_1_UPPER_BAND_{bb_band_sd}": "upper_band",
            f"P_1_LOWER_BAND_{bb_band_sd}": "lower_band",
        },
        inplace=True,
    )

    # Convert start and end dates to datetime
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")

    # Filter data by date range
    strategy_df = strategy_df[
        (strategy_df.index >= start_date) & (strategy_df.index <= end_date)
    ]
    fractal_df = fractal_df[
        (fractal_df.index >= start_date) & (fractal_df.index <= end_date)
    ]
    bb_band_df = bb_band_df[
        (bb_band_df.index >= start_date) & (bb_band_df.index <= end_date)
    ]

    strategy_df = strategy_df.dropna(axis=0)
    fractal_df = fractal_df.dropna(axis=0)
    bb_band_df = bb_band_df.dropna(axis=0)

    return strategy_df, fractal_df, bb_band_df


def merge_data(strategy_df, fractal_df, bb_band_df):
    # Join the strategy and fractal dataframes on their index (datetime)
    merged_df = strategy_df.join(fractal_df, how="left")

    # Join the resulting dataframe with the BB band dataframe on the index (datetime)
    merged_df = merged_df.join(bb_band_df, how="left")
    return merged_df


def merge_data_without_duplicates(strategy_df, fractal_df, bb_band_df):
    # Concatenate DataFrames with how='outer' to keep all rows from any DataFrame
    merged_df = pd.concat([strategy_df, fractal_df, bb_band_df], axis=1, join="outer")
    # Forward fill missing values to propagate non-NaN values from previous rows
    merged_df.fillna(method="ffill", inplace=True)
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


def get_market_direction(row):
    return (
        MarketDirection.LONG
        if row["tag"] == "GREEN"
        else MarketDirection.SHORT if row["tag"] == "RED" else None
    )


def reset_last_fractal(last_fractal, market_direction):
    """Reset the last fractal for the opposite direction when the market direction changes"""

    if market_direction == MarketDirection.LONG:
        last_fractal[MarketDirection.SHORT] = None
    elif market_direction == MarketDirection.SHORT:
        last_fractal[MarketDirection.LONG] = None


def update_last_fractal(last_fractal, market_direction, row):
    """Update the last fractal for the current market direction if a new fractal is found"""

    if market_direction == MarketDirection.LONG and row["P_1_FRACTAL_LONG"]:
        last_fractal[MarketDirection.LONG] = (row.name, row["Close"])
    elif market_direction == MarketDirection.SHORT and row["P_1_FRACTAL_SHORT"]:
        last_fractal[MarketDirection.SHORT] = (row.name, row["Close"])


def check_fractal_entry(row, last_fractal):
    pass


def check_bb_band_entry(row, last_fractal):
    pass


def check_entry_conditions(row, last_fractal):
    """Check the entry conditions for a trade based on the given row"""
    # todo
    # 1. add the logic to check for trade start time in the class variable
    # 2. make it modular farctal check, bb_band check.

    if not is_trade_start_time_crossed(row):
        return False

    market_direction = get_market_direction(row)

    reset_last_fractal(last_fractal, market_direction)

    update_last_fractal(last_fractal, market_direction, row)

    if Trade.check_fractal:
        is_fractal_entry = check_fractal_entry(row, last_fractal)

    if Trade.check_bb_band:
        is_bb_band_entry = check_bb_band_entry(row, last_fractal)

    # Check for confirmed entry based on last_fractal, price, and mean band
    if (
        market_direction == "long"
        and row["P_1_FRACTAL_CONFIRMED_LONG"]
        and last_fractal["long"]
        and last_fractal["long"][1] < row["mean_band"]
    ):
        return True

    elif (
        market_direction == "short"
        and row["P_1_FRACTAL_CONFIRMED_SHORT"]
        and last_fractal["short"]
        and last_fractal["short"][1] > row["mean_band"]
    ):
        return True

    return False


def tag_change_exit(row, last_fractal):
    pass


def fractal_exit(row, last_fractal):
    pass


def trail_BB_band_exit(row, last_fractal):
    pass


def identify_exit_signals(row, last_fractal):
    # todo
    # 1. add the logic to check for trade end time in the class variable only if the trade is intraday
    # 2. make it modular trail_BB_band check, fractal check and tag change exit
    # 3. formulate trail_BB_band check function

    market_direction = row["tag"]
    exit_type, is_fractal_change_exit = None, False
    if market_direction == "GREEN" and row["P_1_FRACTAL_CONFIRMED_SHORT"]:
        exit_type = "Fractal Change Exit"
        is_fractal_change_exit = True
    elif market_direction == "RED" and row["P_1_FRACTAL_CONFIRMED_LONG"]:
        exit_type = "Fractal Change Exit"
        is_fractal_change_exit = True

    previous_direction = last_fractal.get(MarketDirection.PREVIOUS, None)
    if not previous_direction and not is_fractal_change_exit:
        return False, None

    if market_direction != previous_direction:
        exit_type = "Tag Change Exit"
        return True, exit_type

    return is_fractal_change_exit, exit_type


pd.set_option("display.max_rows", None)  # None means show all rows
pd.set_option("display.max_columns", None)  # None means show all columns
pd.set_option("display.width", 500)  # Adjust the width to your preference
pd.set_option("display.max_colwidth", None)


class TradeType(Enum):
    INTRADAY = "Intraday"
    POSITIONAL = "Positional"

def main():
    start = time.time()
    instrument = "BANKNIFTY"
    strategy_id = 1
    start_date = "3/1/2019 9:35:00"
    end_date = "3/1/2019 11:00:00"
    fractal_file_number = 136
    fractal_exit = "ALL"  # or 1 or 2 or 3 etc.
    bb_file_number = 1
    bb_band_sd = 2.0  # version number (2.0, 2.25, 2.5, 2.75, 3.0)
    trade_start_time = "9:30:00"
    trade_end_time = "15:30:00"
    check_fractal = True
    check_bb_band = True

    # todo
    # Validate the input parameters

    # set the class variables
    Trade.instrument = instrument
    Trade.strategy_id = strategy_id
    Trade.trade_start_time = pd.to_datetime(trade_start_time).time()
    Trade.trade_end_time = pd.to_datetime(trade_end_time).time()
    Trade.check_fractal = check_fractal
    Trade.check_bb_band = check_bb_band
    Trade.type = 

    # upadate the max exits if fractal_exit is numeric identify by try block
    try:
        Trade.max_exits = int(fractal_exit)
    except ValueError:
        pass

    # Read and filter data
    strategy_df, fractal_df, bb_band_df = read_data(
        instrument,
        strategy_id,
        start_date,
        end_date,
        fractal_file_number,
        bb_file_number,
        bb_band_sd,
    )

    # Merge data
    merged_df = merge_data(strategy_df, fractal_df, bb_band_df)

    # Dictionary to track last fractals for both directions
    last_fractal = {
        MarketDirection.LONG: None,
        MarketDirection.SHORT: None,
        MarketDirection.PREVIOUS: None,
    }
    active_trades, completed_trades = [], []
    for index, row in merged_df.iterrows():
        is_entry = check_entry_conditions(row, last_fractal)
        is_exit, exit_type = identify_exit_signals(row, last_fractal)
        # Check for entry using check_entry_conditions
        if is_entry:
            trade = Trade(
                entry_signal=row["tag"],
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

        last_fractal[MarketDirection.PREVIOUS] = row["tag"]

    trade_outputs = []
    for trade in chain(completed_trades, active_trades):
        trade_outputs.extend(trade.formulate_output())

    output_df = pd.DataFrame(trade_outputs)

    # print(len(trade_outputs))
    # write the output to csv
    output_df.to_csv("output.csv", index=False)
    stop = time.time()
    print(f"Time taken: {stop-start} seconds")


if __name__ == "__main__":
    main()


# questions
# 1. for multiple strategies, how to formulate the output


# status
# 1. read the data from the files is hold cuz i don't have the files with me
# 2. the entry and exit conditions for trade are formulated, including trail_BB_band_exit and base trade start and end time
# 3. adujusted Trade class to for new configuration(like on/off for specific conditions, etc..)
