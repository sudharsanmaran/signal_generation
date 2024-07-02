"""
This Python module defines functions for processing trades based on a specific strategy. Here's a breakdown of the key components:

**1. Trade Conditions and Signals:**

* `is_trade_start_time_crossed`: Checks if the data row corresponds to a time after the designated trade start time.
* `get_market_direction`: Identifies the market direction (LONG or SHORT) based on pre-defined signal conditions for entry or exit (uses `Trade.market_direction_conditions` dictionary).
* `reset_last_state`: Resets the record of the previous fractal for the opposite direction when the market direction changes.
* `update_last_state`: Updates the record of the last fractal for the current direction if a new fractal is found based on the `check_entry_fractal` or `check_exit_fractal` flags from the `Trade` class.
* `check_fractal_conditions`: Checks if the current data row satisfies the conditions for a fractal entry or exit (uses `confirm_fractal_column_dict` dictionary).
* `check_bb_band_entry`: Checks if the current data row satisfies the Bollinger Band conditions for entry based on the `Trade.bb_band_column` and comparison logic.

**2. Entry and Exit Logic:**

* `check_entry_based`: Implements entry logic based on counters and skip logic (uses `Trade.max_limit_entry_based` and `Trade.steps_entry_based` for control).
* `check_entry_conditions`: The main function for checking entry conditions. It considers various factors:
    * Trade start time (`is_trade_start_time_crossed`)
    * Market direction (`get_market_direction`)
    * Signal change (`signal_change`)
    * Entry strategy based on counters (`check_entry_based`)
    * Allowed trade direction (`Trade.allowed_direction`)
    * Trade type (intraday end time check for `TradeType.INTRADAY`)
    * Fractal check (`check_fractal_conditions` if `Trade.check_entry_fractal`)
    * Bollinger Band check (`check_bb_band_entry` if `Trade.check_bb_band`)
* `is_trade_end_time_reached`: Checks if the intraday trade end time has been reached for the data row.
* `check_bb_band_trail_exit`: Checks if the current data row satisfies the trailing Bollinger Band exit condition based on comparison with the last recorded fractal value.
* `signal_change`: Checks if there's a change in market direction between two values.
* `identify_exit_signals`: The main function for checking exit conditions. It considers:
    * Trade end time (`is_trade_end_time_reached`)
    * Market direction (`get_market_direction`)
    * Signal change (`signal_change`) and updates exit signal count
    * Trail exit based on Bollinger Band (`check_bb_band_trail_exit` if `Trade.check_trail_bb_band`)
    * Fractal exit (`check_fractal_conditions` if `Trade.check_exit_fractal`)
    * Determines the final exit type (END, SIGNAL, FRACTAL, TRAILING)

**3. Trade Processing and Output:**

* `process_trade`: The main function that orchestrates the trade processing. It takes various file numbers for reading entry/exit fractal data and Bollinger Band data.
    * Reads data for the specified instrument, portfolio, and strategy combination using `read_data`.
    * Merges the data frames using `merge_all_df`.
    * Initializes separate dictionaries `entry_state` and `exit_state` to track relevant information for entry and exit signals.
    * Iterates through each data row:
        * Checks for entry conditions using `check_entry_conditions`.
        * Checks for exit signals using `identify_exit_signals`.
        * Creates `Trade` objects for entries and updates them with exits.
    * Generates trade outputs using `formulate_output` from the `Trade` class for both completed and active trades.
    * Saves the outputs to CSV files.
"""

from collections import deque
from itertools import chain
import multiprocessing
import os
import pandas as pd

from source.constants import (
    MERGED_DF_FOLDER,
    SG_OUTPUT_FOLDER,
    MarketDirection,
    TradeExitType,
    TradeType,
    fractal_column_dict,
    confirm_fractal_column_dict,
    cpu_percent_to_use,
)
from source.data_reader import merge_all_df, read_data
from source.trade import Trade, initialize
from source.utils import write_dataframe_to_csv
from tradesheet.index import generate_tradesheet


DEBUG = os.getenv("DEBUG", False) == "True"


def is_trade_start_time_crossed(row):
    """Check if the trade start time is crossed for the given row

    Args:
        row (pandas.Series): A row from the DataFrame containing data

    Returns:
        bool: True if the trade start time is crossed, False otherwise"""

    if (
        Trade.type == TradeType.INTRADAY
        and row.name.time() < Trade.trade_start_time
    ):
        return False
    return True


def get_market_direction(
    row,
    condition_key,
    signal_columns=Trade.signal_columns,
    market_direction_conditions=Trade.market_direction_conditions,
):
    """Get the market direction based on the entry or exit conditions for a trade.

    Args:
        row (pandas.Series): A row from the DataFrame containing data
        condition_key (str): "entry" or "exit" indicating the condition type

    Returns:
        str: Market direction (LONG or SHORT) or None if no match found"""

    row_directions = row.get(signal_columns)
    for direction, signals in market_direction_conditions[
        condition_key
    ].items():
        for signal in signals:
            if all(dir == sig for dir, sig in zip(row_directions, signal)):
                return direction
    return None


def reset_last_state(state, market_direction):
    """
    Reset the last fractal for the opposite direction when the market direction changes

    Args:
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)

    Returns:
        None
    """
    if not market_direction:
        return
    opposite_direction = get_opposite_direction(market_direction)
    state[opposite_direction].clear()


def get_opposite_direction(market_direction):
    if market_direction not in {MarketDirection.LONG, MarketDirection.SHORT}:
        return None

    opposite_direction = (
        MarketDirection.SHORT
        if market_direction == MarketDirection.LONG
        else MarketDirection.LONG
    )

    return opposite_direction


def update_last_state(state, market_direction, row, key):
    """
    Update the last fractal for the current market direction if a new fractal is found

    Args:
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)
        row (pandas.Series): A row from the DataFrame containing data
        key (str): "entry" or "exit" indicating the condition type

    Returns:
        None
    """
    check_fractal = None
    if key == "entry":
        check_fractal = Trade.check_entry_fractal
    elif key == "exit":
        check_fractal = Trade.check_exit_fractal

    if (
        check_fractal
        and market_direction
        and row[fractal_column_dict[key][market_direction]]
    ):
        state[market_direction].append((row.name, row["Close"]))


def check_fractal_conditions(row, state, market_direction, key):
    """
     Check the fractal entry conditions for a trade based on the given row

    Args:
        row (pandas.Series): A row from the DataFrame containing data
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)
        key (str): "entry" or "exit" indicating the condition type

    Returns:
        bool: True if fractal conditions are met, False otherwise
    """

    if (
        len(state.get(market_direction))
        and row[confirm_fractal_column_dict[key][market_direction]]
    ):
        return True

    return False


def check_exit_fractal_condition(row, market_direction, state):
    if market_direction:
        return row[confirm_fractal_column_dict["exit"][market_direction]]
    elif state.get(MarketDirection.PREVIOUS, None):
        return row[
            confirm_fractal_column_dict["exit"][
                state[MarketDirection.PREVIOUS]
            ]
        ]


def check_bb_band_entry(row, state, market_direction):
    """
    Check the BB band entry conditions for a trade based on the given row

    Args:
        row (pandas.Series): A row from the DataFrame containing data
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)

    Returns:
        bool: True if BB band conditions are met, False otherwise
    """

    if not len(state.get(market_direction)):
        return False

    _, fractal_value = state[market_direction].popleft()
    bb_band_value = row[f"bb_{Trade.bb_band_column}"]

    compare = (
        (lambda a, b: a < b)
        if market_direction == MarketDirection.LONG
        else (lambda a, b: a > b)
    )

    return compare(fractal_value, bb_band_value)


def check_entry_based(state, market_direction):
    """
    Check entry limits based on a counter and skip logic

    Args:
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)

    Returns:
        bool: True if entry is allowed based on limits and skips, False otherwise
    """
    entry_key = (market_direction, "entry_based")
    entry_count = state.get(entry_key, 0)
    if entry_count >= Trade.max_limit_entry_based:
        return False

    if entry_count == 0:
        state[entry_key] = 1
        return True

    state["skip_count"] = state.get("skip_count", Trade.steps_entry_based)
    if state["skip_count"] == 1:
        state["skip_count"] = Trade.steps_entry_based
        state[entry_key] += 1
        return True

    state["skip_count"] -= 1
    return False


def check_entry_conditions(row, state):
    """
    Check the entry conditions for a trade based on the given row

        Args:
            row (pandas.Series): A row from the DataFrame containing data
            state (dict): Dictionary to store trade state information

        Returns:
            tuple: (bool, str) - (is_entry, market_direction)
    """

    if not is_trade_start_time_crossed(row):
        return False, None

    if Trade.no_of_rows_to_skip:
        Trade.no_of_rows_to_skip -= 1
        return False, None

    market_direction = get_market_direction(
        row,
        "entry",
        signal_columns=Trade.signal_columns,
        market_direction_conditions=Trade.market_direction_conditions,
    )

    if not market_direction:
        return False, None

    previous_direction = state.get(MarketDirection.PREVIOUS, None)
    state[MarketDirection.PREVIOUS] = market_direction
    if previous_direction and signal_change(
        previous_direction, market_direction
    ):
        state[(market_direction, "entry_based")] = 0
        state["skip_count"] = Trade.steps_entry_based

    reset_last_state(state, market_direction)
    update_last_state(state, market_direction, row, "entry")

    is_entry = False
    if Trade.check_entry_based:
        is_entry = check_entry_based(state, market_direction)

    if (
        not Trade.allowed_direction == MarketDirection.ALL
        and not market_direction == Trade.allowed_direction
    ):
        return False, None

    if (
        Trade.type == TradeType.INTRADAY
        and row.name.time() >= Trade.trade_end_time
    ):
        return False, None

    if Trade.check_entry_fractal:
        is_fractal_entry = check_fractal_conditions(
            row, state, market_direction, "entry"
        )
        if is_fractal_entry and Trade.check_bb_band:
            is_bb_band_entry = check_bb_band_entry(
                row, state, market_direction
            )

    if Trade.check_entry_fractal and Trade.check_bb_band:
        return is_fractal_entry and is_bb_band_entry, market_direction
    elif Trade.check_entry_fractal:
        return is_fractal_entry, market_direction
    elif Trade.check_bb_band:
        return is_bb_band_entry, market_direction
    elif Trade.check_entry_based:
        return is_entry, market_direction

    return False, None


def is_trade_end_time_reached(row):
    """
    Check if the intraday trade end time is reached for the given row

    Args:
        row (pandas.Series): A row from the DataFrame containing data

    Returns:
        bool: True if trade end time is reached, False otherwise
    """
    if (
        Trade.type == TradeType.INTRADAY
        and row.name.time() >= Trade.trade_end_time
    ):
        return True
    return False


def check_bb_band_trail_exit(row, state, market_direction):
    """
    Check the BB band trail exit conditions for a trade

    Args:
        row (pandas.Series): A row from the DataFrame containing data
        state (dict): Dictionary to store trade state information
        market_direction (str): Market direction (LONG or SHORT)

    Returns:
        bool: True if BB band trail exit condition is met, False otherwise
    """
    if not market_direction:
        return False
    trail_bb_band_value = row[f"trail_{Trade.trail_bb_band_column}"]

    if state.get("trail_first_found", False):
        if Trade.trail_compare_functions[market_direction][
            "opposite_compare_func"
        ](row["Close"], trail_bb_band_value):
            state["trail_first_found"] = False
            return True
    else:
        if Trade.trail_compare_functions[market_direction]["compare_func"](
            row["Close"], trail_bb_band_value
        ):
            state["trail_first_found"] = True
            state["first_trail_time"] = row.name

    return False


def signal_change(previous_direction, market_direction):
    """
        Checks if there's a change in signal between two market directions.

    Args:
        previous_direction (str): The market direction before the current one (LONG or SHORT)
        market_direction (str): The current market direction (LONG or SHORT)

    Returns:
        bool: True if there's a change in signal, False otherwise
    """
    if market_direction != previous_direction:
        return True
    return False


def reset_max_limit_entry_based(state):
    """
        Reset the maximum limit for entry based on the given state.

    Args:
        state (dict): Dictionary to store trade state information

    Returns:
        None
    """
    keys = [
        (MarketDirection.LONG, "entry_based"),
        (MarketDirection.SHORT, "entry_based"),
    ]
    for key in keys:
        state[key] = 0
    state["skip_count"] = Trade.steps_entry_based


def identify_exit_signals(row, exit_state, entry_state):
    """
      Identifies potential exit signals for a trade based on the given data row and state.

    Args:
        row (pandas.Series): A row from the DataFrame containing data
        state (dict): Dictionary to store trade state information

    Returns:
        tuple: (bool, TradeExitType or None) - (is_exit, exit_type)
            is_exit (bool): True if an exit signal is identified, False otherwise
            exit_type (TradeExitType or None): The type of exit signal (END, SIGNAL, FRACTAL, TRAILING) or None if no exit is identified
    """

    market_direction = get_market_direction(
        row,
        "exit",
        signal_columns=Trade.signal_columns,
        market_direction_conditions=Trade.market_direction_conditions,
    )

    # reset_last_state(state, market_direction)
    # update_last_state(state, market_direction, row, "exit")

    if is_trade_end_time_reached(row):
        reset_max_limit_entry_based(entry_state)
        return True, TradeExitType.END

    exit_type, is_trail_bb_band_exit, is_fractal_exit = None, False, False
    if Trade.check_exit_fractal:
        is_fractal_exit = check_exit_fractal_condition(
            row, market_direction, exit_state
        )

    if market_direction:
        previous_direction = exit_state.get(MarketDirection.PREVIOUS, None)
        exit_state[MarketDirection.PREVIOUS] = market_direction
        if previous_direction and signal_change(
            previous_direction, market_direction
        ):
            exit_state["signal_count"] += 1
            Trade.reset_trade_entry_id_counter()
            return True, TradeExitType.SIGNAL

    if Trade.check_trail_bb_band:
        is_trail_bb_band_exit = check_bb_band_trail_exit(
            row, exit_state, entry_state.get(MarketDirection.PREVIOUS, None)
        )

    if is_trail_bb_band_exit and is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    elif is_trail_bb_band_exit:
        exit_type = TradeExitType.TRAILING
    elif is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    return is_trail_bb_band_exit or is_fractal_exit, exit_type


def multiple_process(validated_input, process: callable):
    """
        Processes trades based on a defined strategy and outputs results.

    This function reads market data for a specified instrument, portfolio, and strategy combination.
    It then iterates through the data and checks for entry and exit signals based on the configured trading strategy.

    Args:
        start_date (str): Start date for data retrieval (YYYY-MM-DD)
        end_date (str): End date for data retrieval (YYYY-MM-DD)
        entry_fractal_file_number (str): File number for entry fractal data
        exit_fractal_file_number (str): File number for exit fractal data
        bb_file_number (str): File number for Bollinger Band data
        trail_bb_file_number (str): File number for trailing Bollinger Band data

    Returns:
        None
    """

    strategy_pairs = validated_input.get("strategy_pairs", [])
    instruments = validated_input.get("instruments", [])

    # Dynamic worker count
    num_workers = min(
        int(multiprocessing.cpu_count() * cpu_percent_to_use),
        len(strategy_pairs) * len(instruments),
    )
    # pool = multiprocessing.Pool(processes=num_workers)
    results = []
    with multiprocessing.Pool(processes=num_workers) as pool:
        for instrument in instruments:
            for strategy_pair in strategy_pairs:
                try:
                    result = pool.apply_async(
                        process,
                        args=(
                            validated_input,
                            strategy_pair,
                            instrument,
                        ),
                    )
                    results.append(result)
                    # Process results as they become available (if desired)
                except Exception as e:
                    print(f"Error encountered during multiprocessing: {e}")
                    raise e

        pool.close()
        pool.join()
    for result in results:
        try:
            result.get()  # Will raise exception if the process raised one
        except Exception as e:
            print(f"Error encountered during multiprocessing execution: {e}")
            raise e

    return


def process_strategy(validated_input, strategy_pair, instrument):
    initialize(validated_input)
    portfolio_ids_str = " - ".join(Trade.portfolio_ids)
    strategy_pair_str = "_".join(map(lambda a: str(a), strategy_pair))
    file_name = f"df_{instrument}_{strategy_pair_str}.csv"

    try:
        all_df = read_data(
            instrument,
            Trade.portfolio_ids,
            strategy_pair,
            validated_input.get("start_date"),
            validated_input.get("end_date"),
            validated_input.get("entry_fractal_file_number"),
            validated_input.get("exit_fractal_file_number"),
            validated_input.get("bb_file_number"),
            Trade.bb_band_column,
            validated_input.get("trail_bb_file_number"),
            Trade.trail_bb_band_column,
            read_entry_fractal=Trade.check_entry_fractal,
            read_exit_fractal=Trade.check_exit_fractal,
            read_bb_fractal=Trade.check_bb_band,
            read_trail_bb_fractal=Trade.check_trail_bb_band,
        )
    except FileNotFoundError as e:
        raise e

    # Merge data
    merged_df = merge_all_df(all_df)

    if DEBUG:
        write_dataframe_to_csv(
            merged_df,
            folder_name=MERGED_DF_FOLDER,
            file_name=file_name,
        )

    # Dictionaries to track last fractals for both entry and exit
    entry_state = {
        MarketDirection.LONG: deque(),
        MarketDirection.SHORT: deque(),
        MarketDirection.PREVIOUS: None,
    }
    exit_state = {
        MarketDirection.PREVIOUS: None,
        "signal_count": 1,
    }
    output_df = process_trade(
        instrument,
        portfolio_ids_str,
        strategy_pair_str,
        merged_df,
        entry_state,
        exit_state,
    )
    if DEBUG:
        write_dataframe_to_csv(output_df, SG_OUTPUT_FOLDER, file_name)

    if Trade.trigger_trade_management:
        generate_tradesheet(
            validated_input, output_df, strategy_pair_str, instrument
        )


def process_trade(
    instrument,
    portfolio_ids_str,
    strategy_pair_str,
    merged_df,
    entry_state,
    exit_state,
    entry_func: callable = check_entry_conditions,
    exit_func: callable = identify_exit_signals,
):

    active_trades, completed_trades = [], []
    for index, row in merged_df.iterrows():
        is_entry, direction = entry_func(row, entry_state)
        is_exit, exit_type = exit_func(row, exit_state, entry_state)
        if is_exit:
            for trade in active_trades[:]:
                trade.add_exit(row.name, row["Close"], exit_type)
                if trade.is_trade_closed():
                    completed_trades.append(trade)
                    active_trades.remove(trade)
        if is_entry:
            trade = Trade(
                entry_signal=direction,
                entry_datetime=index,
                entry_price=row["Close"],
                signal_count=exit_state["signal_count"],
            )
            active_trades.append(trade)

    trade_outputs = []
    for trade in chain(completed_trades, active_trades):
        trade_outputs.extend(
            trade.formulate_output(
                instrument, strategy_pair_str, portfolio_ids_str
            )
        )

    output_df = pd.DataFrame(trade_outputs)
    return output_df
