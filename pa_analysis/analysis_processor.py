import os

import pandas as pd
from source.constants import MarketDirection
from source.data_reader import load_strategy_data
from source.trade_processor import get_market_direction, get_opposite_direction


def format_dates(start_date, end_date):
    start_datetime = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_datetime = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")
    return start_datetime, end_datetime


def process(validated_data, strategy_pair, instrument):
    portfolio_ids_str = " - ".join(validated_data.get("portfolio_ids"))
    strategy_pair_str = "_".join(map(lambda a: str(a), strategy_pair))
    file_name = f"df_{instrument}_{strategy_pair_str}.csv"

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
    # todo
    # 1. filter only signal changing rows skip same signal rows

    # Initialize columns for results
    strategy_df["signal_change"] = False
    strategy_df["duration_days"] = 0.0

    # Determine the signal columns to check
    long_signals = validated_data["long_entry_signals"]
    short_signals = validated_data["short_entry_signals"]

    # Process the DataFrame
    last_direction = None
    last_time = None
    signal_columns = [
        f"TAG_{id}" for id in validated_data.get("portfolio_ids")
    ]
    market_direction_conditions = {
        "entry": {
            MarketDirection.LONG: long_signals,
            MarketDirection.SHORT: short_signals,
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
        return pd.NA

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
    strategy_df["previous_market_direction"] = strategy_df[
        "previous_market_direction"
    ].ffill()

    strategy_df["signal_change"] = (
        strategy_df["market_direction"] == MarketDirection.LONG
    ) & (strategy_df["previous_market_direction"] == MarketDirection.SHORT) | (
        strategy_df["market_direction"] == MarketDirection.SHORT
    ) & (
        strategy_df["previous_market_direction"] == MarketDirection.LONG
    )

    strategy_df["duration_days"] = 0.0
    last_change_time = None
    for index, row in strategy_df.iterrows():
        if row["signal_change"]:
            if last_change_time is not None:
                duration = (index - last_change_time).days
                strategy_df.loc[last_change_time, "duration_days"] = duration
            last_change_time = index

    # Filter out rows where there was no signal change
    filtered_df = strategy_df[strategy_df["signal_change"]].copy()
    filtered_df["returns"] = filtered_df["Close"].diff().shift(-1).fillna(0)
    filtered_df["profit_loss"] = 0

    # Calculate returns based on market direction
    long_mask = filtered_df["market_direction"].shift() == MarketDirection.LONG
    short_mask = (
        filtered_df["market_direction"].shift() == MarketDirection.SHORT
    )
    filtered_df.loc[long_mask, "returns"] = (
        filtered_df["Close"].shift(-1) - filtered_df["Close"]
    )
    filtered_df.loc[short_mask, "returns"] = filtered_df[
        "Close"
    ] - filtered_df["Close"].shift(-1)

    # Determine profit or loss
    filtered_df["profit_loss"] = (filtered_df["returns"] > 0).astype(int)

    # Drop intermediate calculation columns
    # filtered_df.drop(
    #     columns=["previous_market_direction", "market_direction"], inplace=True
    # )

    # Save the filtered DataFrame to a CSV file
    filtered_df.to_csv(os.path.join(base_path, file_name))

    return filtered_df

    # for index, row in strategy_df.iterrows():
    #     market_direction = get_market_direction(
    #         row,
    #         condition_key="entry",
    #         signal_columns=signal_columns,
    #         market_direction_conditions=market_direction_conditions,
    #     )

    #     if market_direction is None:
    #         continue

    #     if market_direction != last_direction:
    #         if last_direction is not None:

    #             duration = (index - last_time).days

    #             # Update the DataFrame
    #             strategy_df.loc[last_time, "signal_change"] = True
    #             strategy_df.loc[last_time, "duration_days"] = duration

    #         last_direction = market_direction
    #         last_time = index

    # # Filter out rows where there was no signal change
    # filtered_df = strategy_df[strategy_df["signal_change"]]
    # filtered_df["returns"] = 0.0
    # filtered_df["profit_loss"] = 0

    # # 2. calculate returns(points) for each row
    # # 5. profit or loss column 1 if profit 0 if loss

    # # Save the filtered DataFrame to a CSV file
    # filtered_df.to_csv(os.path.join(base_path, file_name))

    # return filtered_df
