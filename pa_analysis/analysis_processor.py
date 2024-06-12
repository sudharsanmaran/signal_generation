import os

import pandas as pd
from pa_analysis.constants import OutputHeader, SignalColumns
from source.constants import MarketDirection
from source.data_reader import load_strategy_data
from source.trade_processor import get_market_direction, get_opposite_direction


def format_dates(start_date, end_date):
    start_datetime = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_datetime = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")
    return start_datetime, end_datetime


def process(validated_data):
    strategy_pairs = validated_data.get("strategy_pairs", [])
    instruments = validated_data.get("instruments", [])
    result = {}
    for instrument in instruments:
        for strategy_pair in strategy_pairs:
            result[(instrument, strategy_pair)] = process_strategy(
                validated_data, strategy_pair, instrument
            )
    return result


def process_strategy(validated_data, strategy_pair, instrument):

    base_path = os.getenv("DB_PATH")

    start_datetime, end_datetime = format_dates(
        validated_data.get("start_date"), validated_data.get("end_date")
    )

    strategy_df = load_strategy_data(
        instrument,
        validated_data.get("portfolio_ids"),
        strategy_pair,
        start_datetime,
        end_datetime,
        base_path,
    )[0]

    base_df = get_base_df(validated_data, strategy_df)

    # based on base df need to generate output analytic df
    result_base_df = generate_analytics(base_df)

    return result_base_df


def generate_analytics(base_df) -> dict:
    """
    Generate the analytics for the strategy.

    Parameters:
        base_df (DataFrame): The base DataFrame containing the strategy data.

    Returns:
        dict: The dict containing the analytics for the strategy.
    """

    result = {
        OutputHeader.SIGNAL.value: {},
        OutputHeader.POINTS.value: {},
        OutputHeader.PROBABILITY.value: {},
        OutputHeader.POINTS_PER_SIGNAL.value: {},
        OutputHeader.RISK_REWARD.value: {},
        OutputHeader.SIGNAL_DURATION.value: {},
        OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value: {},
    }

    for direction in ["LONG", "SHORT"]:
        mask, plus_mask, minus_mask = get_masks(base_df, direction)

        update_signals(
            result[OutputHeader.SIGNAL.value], direction, plus_mask, minus_mask
        )

        update_points(
            base_df,
            result[OutputHeader.POINTS.value],
            direction,
            mask,
            plus_mask,
            minus_mask,
        )

        update_probability(
            result[OutputHeader.PROBABILITY.value],
            base_df,
            direction,
            mask,
        )

        update_net_points_per_signal(
            result,
            direction,
        )

        update_risk_reward(result, direction)

        update_signal_duration(
            result[OutputHeader.SIGNAL_DURATION.value],
            direction,
            base_df,
            mask,
            plus_mask,
            minus_mask,
        )

    # Calculate totals
    update_totals(result)

    # Create the result DataFrame
    results_dfs = {
        key: pd.DataFrame([metric]) for key, metric in result.items()
    }
    return results_dfs


def get_masks(base_df, direction):
    mask = base_df["market_direction"].shift() == MarketDirection[direction]
    plus_mask = (base_df["points"] > 0) & mask
    minus_mask = (base_df["points"] < 0) & mask
    return mask, plus_mask, minus_mask


def update_totals(result):
    result[OutputHeader.SIGNAL.value]["Total Signals"] = (
        result[OutputHeader.SIGNAL.value][SignalColumns.LONG_NET.value]
        + result[OutputHeader.SIGNAL.value][SignalColumns.SHORT_NET.value]
    )
    result[OutputHeader.POINTS.value]["Total Points"] = (
        result[OutputHeader.POINTS.value][SignalColumns.LONG_NET.value]
        + result[OutputHeader.POINTS.value][SignalColumns.SHORT_NET.value]
    )
    result[OutputHeader.PROBABILITY.value]["Total"] = (
        result[OutputHeader.PROBABILITY.value][SignalColumns.LONG.value]
        + result[OutputHeader.PROBABILITY.value][SignalColumns.SHORT.value]
    )
    result[OutputHeader.POINTS_PER_SIGNAL.value]["Total"] = (
        result[OutputHeader.POINTS_PER_SIGNAL.value][
            SignalColumns.LONG_NET.value
        ]
        + result[OutputHeader.POINTS_PER_SIGNAL.value][
            SignalColumns.SHORT_NET.value
        ]
    )
    result[OutputHeader.SIGNAL_DURATION.value]["Total"] = (
        result[OutputHeader.SIGNAL_DURATION.value][
            SignalColumns.LONG_NET.value
        ]
        + result[OutputHeader.SIGNAL_DURATION.value][
            SignalColumns.SHORT_NET.value
        ]
    )


def update_signal_duration(
    result, direction, base_df, mask, plus_mask, minus_mask
):
    plus, minus, net = get_col_name(direction)
    result[plus] = base_df.loc[plus_mask, "time"].sum()
    result[minus] = base_df.loc[minus_mask, "time"].sum()
    result[net] = base_df.loc[mask, "time"].sum()


def update_risk_reward(result, direction):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.RISK_REWARD.value][net] = (
        result[OutputHeader.POINTS_PER_SIGNAL.value][plus]
        / result[OutputHeader.POINTS_PER_SIGNAL.value][minus]
    )


def update_net_points_per_signal(result, direction):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.POINTS_PER_SIGNAL.value][plus] = (
        result[OutputHeader.POINTS.value][plus]
        / result[OutputHeader.SIGNAL.value][plus]
    )
    result[OutputHeader.POINTS_PER_SIGNAL.value][minus] = (
        result[OutputHeader.POINTS.value][minus]
        / result[OutputHeader.SIGNAL.value][minus]
    )
    result[OutputHeader.POINTS_PER_SIGNAL.value][net] = (
        result[OutputHeader.POINTS.value][net]
        / result[OutputHeader.SIGNAL.value][net]
    )


def update_probability(result, base_df, direction, mask):
    if direction == "LONG":
        col_name = SignalColumns.LONG.value
    else:
        col_name = SignalColumns.SHORT.value
    result[col_name] = base_df.loc[mask, "profit_loss"].mean() * 100


def update_points(base_df, result, direction, mask, plus_mask, minus_mask):
    plus, minus, net = get_col_name(direction)
    result[plus] = base_df.loc[plus_mask, "points"].sum()
    result[minus] = base_df.loc[minus_mask, "points"].sum()
    result[net] = base_df.loc[mask, "points"].sum()


def get_col_name(direction):
    if direction == "LONG":
        plus, minus, net = (
            SignalColumns.LONG_PLUS.value,
            SignalColumns.LONG_MINUS.value,
            SignalColumns.LONG_NET.value,
        )
    else:
        plus, minus, net = (
            SignalColumns.SHORT_PLUS.value,
            SignalColumns.SHORT_MINUS.value,
            SignalColumns.SHORT_NET.value,
        )

    return plus, minus, net


def update_signals(result, direction, plus_mask, minus_mask):
    plus, minus, net = get_col_name(direction)
    result[plus] = plus_mask.sum()
    result[minus] = minus_mask.sum()
    result[net] = plus_mask.sum() + minus_mask.sum()


def get_base_df(validated_data, strategy_df):
    """
    Get the base DataFrame for the strategy.

    Parameters:
        validated_data (dict): The validated data from the user.
        strategy_df (DataFrame): The DataFrame containing the strategy data.

    Returns:
        DataFrame: The DataFrame containing the base strategy data.
    """

    # Initialize columns for results
    strategy_df["signal_change"] = False
    strategy_df["time"] = 0.0

    signal_columns = [
        f"TAG_{id}" for id in validated_data.get("portfolio_ids")
    ]
    market_direction_conditions = {
        "entry": {
            MarketDirection.LONG: validated_data["long_entry_signals"],
            MarketDirection.SHORT: validated_data["short_entry_signals"],
        }
    }

    def get_direction(row):
        market_direction = get_market_direction(
            row,
            condition_key="entry",
            signal_columns=signal_columns,
            market_direction_conditions=market_direction_conditions,
        )
        if market_direction:
            return market_direction
        return MarketDirection.UNKNOWN

    # display full df
    pd.set_option("display.max_rows", None)

    strategy_df["market_direction"] = strategy_df.apply(get_direction, axis=1)
    strategy_df["previous_market_direction"] = strategy_df[
        "market_direction"
    ].shift(
        fill_value=get_opposite_direction(
            strategy_df["market_direction"].iloc[0]
        )
    )

    strategy_df["signal_change"] = (
        strategy_df["market_direction"]
        != strategy_df["previous_market_direction"]
    )

    # Filter out rows where there was no signal change
    filtered_df = strategy_df[strategy_df["signal_change"]].copy()

    filtered_df["time"] = (
        filtered_df.index.to_series().diff().shift(-1).dt.days
    )

    filtered_df["points"] = filtered_df["Close"].diff().shift(-1).fillna(0)
    filtered_df["profit_loss"] = 0

    # Calculate points based on market direction
    long_mask = filtered_df["market_direction"].shift() == MarketDirection.LONG
    short_mask = (
        filtered_df["market_direction"].shift() == MarketDirection.SHORT
    )
    filtered_df.loc[long_mask, "points"] = (
        filtered_df["Close"].shift(-1) - filtered_df["Close"]
    )
    filtered_df.loc[short_mask, "points"] = filtered_df["Close"] - filtered_df[
        "Close"
    ].shift(-1)

    # Determine profit or loss
    filtered_df["profit_loss"] = (filtered_df["points"] > 0).astype(int)
    return filtered_df
