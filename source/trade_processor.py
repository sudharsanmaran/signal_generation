from itertools import chain
import pandas as pd

from source.constants import MarketDirection, TradeExitType, TradeType
from source.data_reader import merge_data, read_data
from source.trade import Trade


def is_trade_start_time_crossed(row):
    """Check if the trade start time is crossed for the given row"""

    if row.name.time() >= Trade.trade_start_time:
        return True
    return False


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
    fractal_keys = {
        MarketDirection.LONG: "P_1_FRACTAL_LONG",
        MarketDirection.SHORT: "P_1_FRACTAL_SHORT",
    }
    if market_direction and row[fractal_keys[market_direction]]:
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

    market_direction = get_market_direction(row, "entry")

    reset_last_fractal(last_fractal, market_direction)
    update_last_fractal(last_fractal, market_direction, row)

    if (
        not Trade.allowed_direction == MarketDirection.ALL
        and not market_direction == Trade.allowed_direction
    ):
        return False, None

    if Trade.type == TradeType.INTRADAY and row.name.time() >= Trade.trade_end_time:
        return False, None

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


def signal_change_exit(previous_direction, market_direction):
    if market_direction != previous_direction:
        return True
    return False


def identify_exit_signals(row, last_fractal):

    market_direction = get_market_direction(row, "exit")

    reset_last_fractal(last_fractal, market_direction)
    update_last_fractal(last_fractal, market_direction, row)

    if is_trade_end_time_reached(row):
        return True, TradeExitType.END

    if not market_direction:
        return False, None

    previous_direction = last_fractal.get(MarketDirection.PREVIOUS, None)
    last_fractal[MarketDirection.PREVIOUS] = market_direction
    if previous_direction and signal_change_exit(previous_direction, market_direction):
        last_fractal["signal_count"] += 1
        return True, TradeExitType.SIGNAL

    exit_type, is_trail_bb_band_exit, is_fractal_exit = None, False, False
    if Trade.check_trail_bb_band:
        is_trail_bb_band_exit = check_bb_band_trail_exit(
            row, last_fractal, market_direction
        )

    if Trade.check_fractal:
        is_fractal_exit = check_fractal_conditions(row, last_fractal, market_direction)

    if is_trail_bb_band_exit and is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    elif is_trail_bb_band_exit:
        exit_type = TradeExitType.TRAILING
    elif is_fractal_exit:
        exit_type = TradeExitType.FRACTAL
    return is_trail_bb_band_exit or is_fractal_exit, exit_type


def process_trade(
    start_date,
    end_date,
    entry_fractal_file_number,
    exit_fractal_file_number,
    bb_file_number,
    trail_bb_file_number,
):
    portfolio_pair_str = " - ".join(Trade.portfolio_ids)
    for strategy_pair in Trade.strategy_ids:
        strategy_pair_str = " - ".join(map(lambda a: str(a), strategy_pair))
        strategy_df, entry_fractal_df, exit_fractal_df, bb_band_df, trail_bb_band_df = (
            read_data(
                Trade.instrument,
                Trade.portfolio_ids,
                strategy_pair,
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
            strategy_df,
            entry_fractal_df,
            exit_fractal_df,
            bb_band_df,
            trail_bb_band_df,
        )

        merged_df.to_csv(f"merged_df_{strategy_pair_str}.csv", index=True)

        # Dictionaries to track last fractals for both entry and exit
        entry_last_fractal = {
            MarketDirection.LONG: None,
            MarketDirection.SHORT: None,
        }
        exit_last_fractal = {
            MarketDirection.LONG: None,
            MarketDirection.SHORT: None,
            MarketDirection.PREVIOUS: None,
            "signal_count": 1,
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
                    signal_count=exit_last_fractal["signal_count"],
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
            trade_outputs.extend(
                trade.formulate_output(strategy_pair_str, portfolio_pair_str)
            )

        output_df = pd.DataFrame(trade_outputs)

        output_df.to_csv(f"output_{strategy_pair_str}.csv", index=False)
