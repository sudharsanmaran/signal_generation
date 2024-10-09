from collections import defaultdict, deque
from itertools import chain
import os

import numpy as np
import pandas as pd


from source.constants import (
    SG_CYCLE_OUTPUT_FOLDER,
    VOLATILE_OUTPUT_FOLDER,
    VOLUME_OUTPUT_FOLDER,
    BB_Band_Columns,
    CycleType,
    FirstCycleColumns,
    GroupAnalytics,
    MarketDirection,
    SecondCycleIDColumns,
    TargetProfitColumns,
    TradeExitType,
    TradeType,
    fractal_column_dict,
    confirm_fractal_column_dict,
)
from source.data_reader import (
    load_strategy_data,
    merge_all_df,
    read_files,
    update_entry_fractal_file,
    update_exit_fractal_file,
)
from source.processors.cycle_analysis_processor import (
    update_MTM_CTC_cols,
    update_target_profit_analysis,
)
from source.processors.signal_trade_processor import (
    get_market_direction,
    is_trade_end_time_reached,
    is_trade_start_time_crossed,
    process_trade,
)
from source.trade import Trade, initialize
from source.utils import format_dates, make_round, write_dataframe_to_csv


DEBUG = os.getenv("DEBUG", False).lower() == "true"


def get_base_df(validated_data, strategy_df, strategy_pair_str, instrument):
    """
    Get the base DataFrame for the strategy.

    Parameters:
        validated_data (dict): The validated data from the user.
        strategy_df (DataFrame): The DataFrame containing the strategy data.

    Returns:
        DataFrame: The DataFrame containing the base strategy data.
    """

    def get_market_direction_vectorized(
        df, condition_key, signal_columns, market_direction_conditions
    ):
        # Create a default column for market direction as UNKNOWN
        market_direction_col = np.full(len(df), MarketDirection.UNKNOWN)

        # Iterate over each market direction (LONG/SHORT)
        for direction, signals in market_direction_conditions[
            condition_key
        ].items():
            # Create a boolean mask for rows matching the signals
            mask = np.full(len(df), True)
            for signal_col, signal_val in zip(
                signal_columns, np.array(signals).T
            ):
                mask &= df[signal_col].isin(signal_val)

            # Assign the market direction to matching rows
            market_direction_col[mask] = direction

        return market_direction_col

    # Include the last row of strategy_df in filtered_df
    def include_last_row(filtered_df):
        # Check if the last row is already in filtered_df
        last_row = strategy_df.tail(1)
        if not filtered_df.index.isin(last_row.index).any():
            # Append the last row to filtered_df
            filtered_df = pd.concat([filtered_df, last_row])
        return filtered_df

    # Initialize columns for results
    strategy_df["signal_change"] = False
    strategy_df["time"] = 0.0

    signal_columns = [col for col in strategy_df.columns if "TAG" in col]
    market_direction_conditions = {
        "entry": {
            MarketDirection.LONG: validated_data["long_entry_signals"],
            MarketDirection.SHORT: validated_data["short_entry_signals"],
        },
        "exit": {
            MarketDirection.LONG: validated_data["long_exit_signals"],
            MarketDirection.SHORT: validated_data["short_exit_signals"],
        },
    }

    # Calculate market direction in a vectorized manner
    strategy_df["market_direction"] = get_market_direction_vectorized(
        strategy_df,
        condition_key="entry",
        signal_columns=signal_columns,
        market_direction_conditions=market_direction_conditions,
    )

    strategy_df["exit_market_direction"] = get_market_direction_vectorized(
        strategy_df,
        condition_key="exit",
        signal_columns=signal_columns,
        market_direction_conditions=market_direction_conditions,
    )

    strategy_df["previous_market_direction"] = strategy_df[
        "market_direction"
    ].shift(fill_value=strategy_df["market_direction"].iloc[0])

    strategy_df["signal_change"] = (
        strategy_df["market_direction"]
        != strategy_df["previous_market_direction"]
    )

    # Filter out rows where there was no signal change
    filtered_df = strategy_df[strategy_df["signal_change"]].copy()

    filtered_df = include_last_row(filtered_df)

    filtered_df["time"] = make_round(
        filtered_df.index.to_series().diff().shift(-1).dt.total_seconds()
        / (3600 * 24)
    )

    filtered_df["points"] = filtered_df["Close"].diff().shift(-1).fillna(0)
    filtered_df["profit_loss"] = 0

    # Calculate points based on market direction
    long_mask = filtered_df["market_direction"] == MarketDirection.LONG
    short_mask = filtered_df["market_direction"] == MarketDirection.SHORT
    filtered_df.loc[long_mask, "points"] = make_round(
        filtered_df["Close"].shift(-1) - filtered_df["Close"]
    )
    filtered_df.loc[short_mask, "points"] = make_round(
        filtered_df["Close"] - filtered_df["Close"].shift(-1)
    )

    # Determine profit or loss
    filtered_df["profit_loss"] = (filtered_df["points"] > 0).astype(int)

    filtered_df["points_percent"] = make_round(
        filtered_df["points"] / filtered_df["Close"] * 100
    )

    filtered_df["temp"] = filtered_df["points"] * filtered_df["time"]

    # filtered_df = filtered_df[:-1]

    write_dataframe_to_csv(
        filtered_df,
        SG_CYCLE_OUTPUT_FOLDER,
        f"filtered_df_{instrument}_{strategy_pair_str}.csv".replace(":", "-"),
    )

    return filtered_df


def get_bb_cols(periods, sds, parameter_id, col_type="MEAN"):
    bb_1_cols = {
        f"P_{parameter_id}_{col_type}_BAND_{period}_{sd}"
        for period in periods
        for sd in sds
    }

    return bb_1_cols


def formulate_files_to_read(kwargs):
    instrument = kwargs.get("instrument")
    kwargs["name"] += instrument
    close_time_frames_1 = kwargs.get("close_time_frames_1")
    kwargs["name"] += (
        f'_closeTF_{"-".join(map(lambda x: str(x), close_time_frames_1))}'
        if close_time_frames_1
        else ""
    )
    bb_time_frames_1 = kwargs.get("bb_time_frames_1")
    kwargs["name"] += (
        f'_bb1TF_{"-".join(map(lambda x: str(x), bb_time_frames_1))}'
        if bb_time_frames_1
        else ""
    )
    bb_time_frames_2 = kwargs.get("bb_time_frames_2")
    kwargs["name"] += (
        f'_bb2TF_{"-".join(map(lambda x: str(x), bb_time_frames_2))}'
        if bb_time_frames_2
        else ""
    )

    # Combine time frames while keeping track of their origin
    close_time_frames_with_origin = [(tf, 1) for tf in close_time_frames_1]
    bb_time_frames_with_origin = [(tf, 1) for tf in bb_time_frames_1]

    if bb_time_frames_2:
        bb_time_frames_with_origin.extend([(tf, 2) for tf in bb_time_frames_2])

    base_path = os.getenv("BB_DB_PATH")
    index = "dt"
    base_cols = {
        "Open",
        "High",
        "Low",
        "Close",
    }

    tf_bb_cols = {
        (tf, origin): get_bb_cols(
            kwargs.get(f"periods_{origin}"),
            kwargs.get(f"sds_{origin}"),
            kwargs.get(f"parameter_id_{origin}"),
        )
        for tf, origin in bb_time_frames_with_origin
    }

    if kwargs.get("include_higher_and_lower"):
        for tf, origin in bb_time_frames_with_origin:
            if origin == 1:

                tf_bb_cols[(tf, origin)].update(
                    get_bb_cols(
                        kwargs.get(f"periods_{origin}"),
                        kwargs.get(f"sds_{origin}"),
                        parameter_id=kwargs.get(f"parameter_id_{origin}"),
                        col_type=BB_Band_Columns.UPPER.value,
                    )
                )

                tf_bb_cols[(tf, origin)].update(
                    get_bb_cols(
                        kwargs.get(f"periods_{origin}"),
                        kwargs.get(f"sds_{origin}"),
                        parameter_id=kwargs.get(f"parameter_id_{origin}"),
                        col_type=BB_Band_Columns.LOWER.value,
                    )
                )

    rename_dict = {
        (tf, origin): {
            col: f"{origin}_{tf}_{col[4:5]}_{col[-4:]}"
            for col in tf_bb_cols[(tf, origin)]
        }
        for tf, origin in bb_time_frames_with_origin
    }

    files_to_read = {
        (tf, "bb", origin): {
            "read": True,
            "cols": [index, *tf_bb_cols[(tf, origin)]],
            "index_col": "dt",
            "file_path": os.path.join(
                base_path,
                instrument,
                f"{instrument}_TF_{tf}.csv",
            ),
            "rename": rename_dict[(tf, origin)],
        }
        for tf, origin in bb_time_frames_with_origin
    }

    files_to_read.update(
        {
            (time_frame, "close", origin): {
                "read": True,
                "cols": [index, *base_cols],
                "index_col": "dt",
                "file_path": os.path.join(
                    base_path,
                    instrument,
                    f"{instrument}_TF_{time_frame}.csv",
                ),
            }
            for time_frame, origin in close_time_frames_with_origin
        }
    )
    # fractalcyle file

    if kwargs.get("fractal_cycle"):
        kwargs[
            "name"
        ] += f"_fractalTF_{kwargs.get('fractal_tf')}_SD_{kwargs.get('fractal_sd')}"
        fractal_cycle_columns = get_fractal_cycle_columns(
            fractal_sd=kwargs.get("fractal_sd")
        )
        files_to_read.update(
            {
                (kwargs.get("fractal_tf"), "fractal_cycle", None): {
                    "read": True,
                    "cols": [index, *fractal_cycle_columns],
                    "index_col": "dt",
                    "file_path": os.path.join(
                        os.getenv("FRACTAL_DB_PATH"),
                        instrument,
                        f"{instrument}_TF_{kwargs.get('fractal_count_tf')}.csv",
                    ),
                }
            }
        )

    if kwargs.get("fractal_cycle") and kwargs.get("fractal_count"):

        fractal_count_columns = get_fractal_count_columns(
            fractal_sd=kwargs.get("fractal_count_sd")
        )

        files_to_read.update(
            {
                (kwargs.get("fractal_count_tf"), "fractal_count", None): {
                    "read": True,
                    "cols": [index, *fractal_cycle_columns],
                    "index_col": "dt",
                    "file_path": os.path.join(
                        os.getenv("FRACTAL_DB_PATH"),
                        instrument,
                        f"{instrument}_TF_{kwargs.get('fractal_count_tf')}.csv",
                    ),
                    "rename": {
                        col: name
                        for col, name in zip(
                            fractal_cycle_columns, fractal_count_columns
                        )
                    },
                }
            }
        )
    result = [
        files_to_read,
        rename_dict,
    ]

    if kwargs.get("fractal_cycle"):
        result.append(fractal_cycle_columns)
    else:
        result.append(None)

    if kwargs.get("fractal_cycle") and kwargs.get("fractal_count"):
        result.append(fractal_count_columns)
    else:
        result.append(None)
    return tuple(result)


def get_fractal_cycle_columns(fractal_sd):
    fractal_cycle_columns = [
        f"P_1_FRACTAL_CONFIRMED_LONG_{fractal_sd}",
        f"P_1_FRACTAL_CONFIRMED_SHORT_{fractal_sd}",
    ]

    return fractal_cycle_columns


def get_fractal_count_columns(fractal_sd):
    fractal_cycle_columns = get_fractal_cycle_columns(fractal_sd)
    fractal_count_columns = [
        f"fractal_count_{col}" for col in fractal_cycle_columns
    ]
    return fractal_count_columns


def update_cycle_columns(df, base_df, start_datetime, kwargs):
    # update direction
    base_df = base_df.reset_index().rename(columns={"index": "TIMESTAMP"})
    df = df.reset_index().rename(columns={"index": "dt"})

    signal_cols = [col for col in base_df.columns if "TAG" in col]
    cols = ["dt", "market_direction", "exit_market_direction"]

    if kwargs.get("include_volume"):
        cols.append("category")
    if kwargs.get("include_volatile"):
        volatile_cols = [
            col for col in base_df.columns if "volatile_tag" in col
        ]
        cols.extend(volatile_cols)

    merged_df = pd.merge_asof(
        df,
        base_df[cols],
        left_on="dt",
        right_on="dt",
        direction="backward",
    )

    merged_df = merged_df[merged_df["dt"] >= start_datetime]

    # update group id
    group_condition = merged_df["market_direction"] != merged_df[
        "market_direction"
    ].shift(1)
    merged_df["group_id"] = group_condition.cumsum()

    # update close percent
    merged_df["adjusted_colse"] = merged_df["Close"] * (
        kwargs.get("close_percent") / 100
    )

    return merged_df


def update_signal_start_price(merged_df):
    condition = merged_df["market_direction"] != merged_df[
        "market_direction"
    ].shift(1)

    merged_df["signal_start_price"] = pd.NA
    merged_df.loc[condition, "signal_start_price"] = merged_df["Close"]
    merged_df["signal_start_price"] = merged_df["signal_start_price"].ffill()


def merge_dataframes(
    base_df,
    time_frame_2_dfs: dict,
    tf_bb_cols: dict,
    index_reset=True,
    direction="backward",
):
    for key, df in time_frame_2_dfs.items():
        if index_reset:
            df = df.reset_index().rename(columns={"index": "dt"})
        cols = ["dt"]
        cols.extend([col for col in tf_bb_cols[key].values()])
        base_df = pd.merge_asof(
            base_df,
            df[cols],
            left_on="dt",
            right_on="dt",
            direction=direction,
        )

    return base_df


def updated_yes_no_columns(bb_cols, merged_df):
    for col in bb_cols:
        # update yes/no
        short_condition = (
            merged_df["market_direction"] == MarketDirection.SHORT
        ) & (merged_df["Close"] > merged_df[col])
        long_condition = (
            merged_df["market_direction"] == MarketDirection.LONG
        ) & (merged_df["Close"] < merged_df[col])

        merged_df[f"close_to_{col}"] = "NO"
        merged_df.loc[short_condition | long_condition, f"close_to_{col}"] = (
            "YES"
        )


def is_cycle_end(merged_df, bb_2_cols, idx):
    if bb_2_cols:
        return any(
            merged_df[bb_2_col].iloc[idx] == "YES" for bb_2_col in bb_2_cols
        )
    return False


def confirm_start_condition(merged_df, col, idx):
    return (merged_df[f"close_to_{col}"].iloc[idx] == "YES") & (
        merged_df[f"close_to_{col}"].shift(1).iloc[idx] == "NO"
    )


def is_cycle_start(merged_df, col, bb_2_cols, idx):
    start_condition = confirm_start_condition(merged_df, col, idx)

    if bb_2_cols:
        bb_2_start_condition = (
            merged_df[bb_col].iloc[idx] == "NO" for bb_col in bb_2_cols
        )
        start_condition = (
            (merged_df[f"close_to_{col}"].iloc[idx] == "YES")
        ) & all(bb_2_start_condition)
    return start_condition


def update_cycle_count_2(merged_df, col, bb_2_cols=None):
    if bb_2_cols is None:
        # starts with colse_to_2*
        bb_2_cols = [col for col in merged_df.columns if "close_to_2" in col]

    # Initialize the cycle number column with zeros
    merged_df[f"cycle_no_{col}"] = 0

    # Initialize cycle counter
    cycle_counter = 1
    in_cycle = False

    for idx in range(1, len(merged_df)):
        # Reset cycle counter for group changes
        if (
            idx > 0
            and merged_df["group_id"].iloc[idx]
            != merged_df["group_id"].iloc[idx - 1]
        ):
            cycle_counter = 1
            in_cycle = False

        # Start condition
        start_condition = is_cycle_start(merged_df, col, bb_2_cols, idx)

        # End conditions
        end_condition = is_cycle_end(merged_df, bb_2_cols, idx)

        if (start_condition and not in_cycle) or (
            start_condition and confirm_start_condition(merged_df, col, idx)
        ):
            cycle_counter += 1
            in_cycle = True

        if end_condition:
            in_cycle = False

        if in_cycle:
            merged_df[f"cycle_no_{col}"].iloc[idx] = cycle_counter


def update_cycle_number_by_condition(
    df,
    col,
    cycle_start_condition,
    cycle_end_condition,
    id_column_name=None,
    counter_starter=1,
    group_by_col="group_id",
    group_start=0,
):
    for group, group_df in df.groupby(group_by_col):
        # Initialize
        if group < group_start:
            continue
        current_cycle = counter_starter
        group_indices = group_df.index
        cycle_counter = pd.Series(0, index=group_indices)
        in_cycle = pd.Series(False, index=group_indices)
        market_direction = group_df["market_direction"].iloc[0]

        if market_direction == MarketDirection.UNKNOWN:
            continue

        start_indices = group_indices[
            cycle_start_condition[market_direction][group_indices]
        ]
        end_indices = group_indices[
            cycle_end_condition[market_direction][group_indices]
        ]

        for start_idx in start_indices:
            if not in_cycle[start_idx]:
                current_cycle += 1
                end_idx = end_indices[end_indices > start_idx].min()
                if not pd.isna(end_idx):
                    in_cycle.loc[start_idx : end_idx - 1] = True
                    cycle_counter.loc[start_idx:end_idx] = current_cycle
                else:
                    in_cycle.loc[start_idx:] = True
                    cycle_counter.loc[start_idx:] = current_cycle

        if id_column_name:
            df.loc[group_indices, id_column_name] = cycle_counter
        else:
            df.loc[group_indices, f"cycle_no_{col}"] = cycle_counter


def update_cycle_count_2_L_H(df, col, bb_2_cols=None):
    upper_col, lower_col = col.replace("M", "U"), col.replace("M", "L")

    if bb_2_cols is None:
        # starts with colse_to_2*
        bb_2_cols = [col for col in df.columns if "close_to_2" in col]

    bb_2_start_condition = True
    for bb_col in bb_2_cols:
        bb_2_start_condition &= df[bb_col] == "NO"
    bb_2_end_condition = False
    for bb_col in bb_2_cols:
        bb_2_end_condition |= df[bb_col] == "YES"

    cycle_start_condition = {
        MarketDirection.LONG: (
            # (df[f"close_to_{col}"] == "YES")
            (df["market_direction"] == MarketDirection.LONG)
            & (df[f"close_to_{lower_col}"] == "YES")
            & bb_2_start_condition
        ),
        MarketDirection.SHORT: (
            # (df[f"close_to_{col}"] == "NO")
            (df["market_direction"] == MarketDirection.SHORT)
            & (df[f"close_to_{upper_col}"] == "YES")
            & bb_2_start_condition
        ),
    }

    cycle_end_condition = {
        MarketDirection.LONG: (
            (
                (df["market_direction"] == MarketDirection.LONG)
                & (df[f"close_to_{lower_col}"] == "YES")
                & (df[f"close_to_{lower_col}"].shift(1) == "NO")
            )
            | bb_2_end_condition
        ),
        MarketDirection.SHORT: (
            (
                (df["market_direction"] == MarketDirection.SHORT)
                & (df[f"close_to_{upper_col}"] == "NO")
                & (df[f"close_to_{upper_col}"].shift(1) == "YES")
            )
            | bb_2_end_condition
        ),
    }

    # Initialize the cycle number column
    df[f"cycle_no_{col}"] = 0

    update_cycle_number_by_condition(
        df, col, cycle_start_condition, cycle_end_condition
    )


def update_cycle_count_1_L_H(df, col):
    upper_col, lower_col = col.replace("M", "U"), col.replace("M", "L")

    cycle_start_condition = {
        MarketDirection.LONG: (
            # (df[f"close_to_{col}"] == "YES")
            (df["market_direction"] == MarketDirection.LONG)
            & (df[f"close_to_{lower_col}"] == "YES")
        ),
        MarketDirection.SHORT: (
            # (df[f"close_to_{col}"] == "NO")
            (df["market_direction"] == MarketDirection.SHORT)
            & (df[f"close_to_{upper_col}"] == "YES")
        ),
    }

    cycle_end_condition = {
        MarketDirection.LONG: (
            (df["market_direction"] == MarketDirection.LONG)
            & (df[f"close_to_{lower_col}"] == "YES")
            & (df[f"close_to_{lower_col}"].shift(1) == "NO")
        ),
        MarketDirection.SHORT: (
            (df["market_direction"] == MarketDirection.SHORT)
            & (df[f"close_to_{upper_col}"] == "NO")
            & (df[f"close_to_{upper_col}"].shift(1) == "YES")
        ),
    }

    # Initialize the cycle number column
    df[f"cycle_no_{col}"] = 0

    update_cycle_number_by_condition(
        df, col, cycle_start_condition, cycle_end_condition
    )


def update_cycle_count_1(merged_df, col):
    # Create the cycle condition column
    cycle_condition = (merged_df[f"close_to_{col}"] == "YES") & (
        merged_df[f"close_to_{col}"].shift(1) == "NO"
    )

    # Initialize thef cycle_no_{col} with zeros
    merged_df[f"cycle_no_{col}"] = 0

    # Define a function to calculate the cycle number within each group
    def calculate_cycle_no(group):
        cycle_no = group.cumsum() + 1
        return cycle_no

    # Apply the function to each group
    merged_df[f"cycle_no_{col}"] = (
        cycle_condition.groupby(merged_df["group_id"])
        .apply(calculate_cycle_no)
        .reset_index(level=0, drop=True)
    )

    # Adjust the initial counter
    initial_counter = 1 if merged_df[f"close_to_{col}"].iloc[0] == "YES" else 0
    merged_df[f"cycle_no_{col}"] += initial_counter


def merge_fractal_data(base_df, fractal_df, fractal_count_df):

    # Reset index and rename for merging
    if fractal_df is not None:
        fractal_df = fractal_df.reset_index().rename(columns={"index": "dt"})
        # Merge fractal cycle data
        base_df = pd.merge_asof(
            base_df,
            fractal_df,
            left_on="dt",
            right_on="dt",
            direction="backward",
        )

    if fractal_count_df is not None:
        fractal_count_df = fractal_count_df.reset_index().rename(
            columns={"index": "dt"}
        )

        # Merge fractal count data
        base_df = pd.merge_asof(
            base_df,
            fractal_count_df,
            left_on="dt",
            right_on="dt",
            direction="backward",
        )

    return base_df


def get_cycle_base_df(**kwargs):
    kwargs["name"] = ""
    base_df = kwargs.get("base_df")
    start_datetime, end_datetime = (
        base_df.index[0],
        base_df.index[-1],
    )

    (
        files_to_read,
        tf_bb_cols,
        fractal_cycle_columns,
        fractal_count_columns,
    ) = formulate_files_to_read(kwargs)

    kwargs["name"] += f"_{start_datetime}_{end_datetime}"
    kwargs["fractal_cycle_columns"] = fractal_cycle_columns
    kwargs["fractal_count_columns"] = fractal_count_columns

    max_tf = max(tf_bb_cols.keys())
    adjusted_start_datetime = start_datetime - pd.Timedelta(minutes=max_tf[0])
    dfs = read_files(
        adjusted_start_datetime,
        end_datetime,
        files_to_read,
    )

    (
        bb_time_frames_1_dfs,
        bb_time_frames_2_dfs,
        close_time_frames_1_dfs,
        fractal_df,
        fractal_count_df,
    ) = ({}, {}, {}, None, None)
    for key, df in dfs.items():
        tf, type, origin = key
        if type == "close" and origin == 1:
            close_time_frames_1_dfs[(tf, origin)] = df

        if type == "bb" and origin == 1:
            bb_time_frames_1_dfs[(tf, origin)] = df

        if type == "bb" and origin == 2:
            bb_time_frames_2_dfs[(tf, origin)] = df

        if type == "fractal_cycle":
            fractal_df = df

        if type == "fractal_count":
            fractal_count_df = df

    # merge bb1 the dataframes
    for key, df in close_time_frames_1_dfs.items():
        df = update_cycle_columns(df, base_df, start_datetime, kwargs)
        df = merge_fractal_data(df, fractal_df, fractal_count_df)
        update_signal_start_price(df)
        update_group_analytics(df)
        merged_df = merge_dataframes(df, bb_time_frames_1_dfs, tf_bb_cols)
        close_time_frames_1_dfs[key] = merged_df

    # update cycle count
    df_to_analyze = {}
    if kwargs.get("check_bb_2"):
        for key, df in close_time_frames_1_dfs.items():
            # merge bb2 the dataframes
            tf, origin = key
            merged_df = merge_dataframes(
                df, bb_time_frames_2_dfs, tf_bb_cols, direction="backward"
            )
            bb_cols = []
            for bb_key in chain(bb_time_frames_1_dfs, bb_time_frames_2_dfs):
                bb_cols.extend(tf_bb_cols[bb_key].values())
            updated_yes_no_columns(
                bb_cols,
                merged_df,
            )

            # updated the cycle count
            if kwargs.get("include_higher_and_lower"):
                mean_columns = [
                    col
                    for bb_key in bb_time_frames_1_dfs
                    for col in tf_bb_cols[bb_key].values()
                    if "M" in col
                ]
                for col in mean_columns:
                    update_cycle_count_2_L_H(merged_df, col)
            else:
                for col in bb_cols:
                    update_cycle_count_2(merged_df, col)

            df_to_analyze[tf] = merged_df

            # write_dataframe_to_csv(
            #     merged_df,
            #     "pa_analysis_output/cycle_outpts/base_df",
            #     f"base_df_tf_{tf}.csv",
            # )
    else:
        for key, df in close_time_frames_1_dfs.items():
            bb_cols = []
            for bb_key in bb_time_frames_1_dfs:
                bb_cols.extend(tf_bb_cols[bb_key].values())
            updated_yes_no_columns(bb_cols, df)
            if kwargs.get("include_higher_and_lower"):
                mean_columns = [col for col in bb_cols if "M" in col]
                for col in mean_columns:
                    update_cycle_count_1_L_H(df, col)
            else:
                for col in bb_cols:
                    update_cycle_count_1(df, col)
            df_to_analyze[tf] = df

    return df_to_analyze, kwargs["name"]


def update_group_analytics(df):
    for key, group in df.groupby("group_id"):
        direction = group["market_direction"].iloc[0]

        if direction == MarketDirection.UNKNOWN:
            continue

        # Prepare the initial values
        if direction == MarketDirection.LONG:
            min_max_idx = group["High"].idxmax()
            min_max = group.loc[min_max_idx, "High"]
        elif direction == MarketDirection.SHORT:
            min_max_idx = group["Low"].idxmin()
            min_max = group.loc[min_max_idx, "Low"]
        else:
            continue

        # Validate the next index and compute next_close
        next_idx = group.index[-1] + 1
        if next_idx in df.index:
            if direction == MarketDirection.LONG:
                close = df.loc[next_idx, "High"]
            elif direction == MarketDirection.SHORT:
                close = df.loc[next_idx, "Low"]
        else:
            print("close not found for group id:", key)
            continue

        # Calculate values
        df.loc[group.index[-1], GroupAnalytics.MIN_MAX.value] = min_max
        df.loc[group.index[-1], GroupAnalytics.CLOSE.value] = close
        df.loc[
            group.index[-1], GroupAnalytics.CLOSE_TO_MIN_MAX_PERCENT.value
        ] = (close / min_max - 1) * 100
        df.loc[
            group.index[-1], GroupAnalytics.CLOSE_TO_MIN_MAX_POINTS.value
        ] = make_round(close - min_max)
        df.loc[group.index[-1], GroupAnalytics.DURATION.value] = (
            df.loc[next_idx, "dt"] - group.loc[min_max_idx, "dt"]
        )


def update_fractal_counter(
    base_df,
    fractal_cycle_columns,
    group_by_col="group_id",
    condition=None,
    skip_count=0,
):
    for col in fractal_cycle_columns:
        # Initialize the count column
        count_col = f"count_{col}"
        base_df[count_col] = 0

        # Iterate over each group
        for group_id, group in base_df.groupby(group_by_col):

            if group_id < 2:
                continue

            # Calculate cumulative sum for True values in the column
            # Apply condition if provided, otherwise default to the column being True
            if condition is not None:
                cumulative_sum = (
                    (group[col] == True) & (condition.loc[group.index])
                ).cumsum()
            else:
                cumulative_sum = (group[col] == True).cumsum()

            # Update the base_df with the cumulative sum for the group
            base_df.loc[base_df[group_by_col] == group_id, count_col] = (
                cumulative_sum
            )

            # Reset count to 0 where the column value is False
            base_df.loc[
                (base_df[col] == False) & (base_df[group_by_col] == group_id),
                count_col,
            ] = 0


def update_fractal_counter_1(
    base_df,
    fractal_cycle_columns,
    group_by_col="group_id",
    condition=None,
    skip_count=0,
):
    for col in fractal_cycle_columns:
        # Initialize the count column
        count_col = f"count_{col}"
        base_df[count_col] = 0

        # Iterate over each group
        for group_id, group in base_df.groupby(group_by_col):
            grp_id, bb_cycle, fractal_cycle = group_id
            if bb_cycle < 1 or fractal_cycle < 1:
                continue

            # Calculate the boolean mask for the condition
            if condition is not None:
                mask = (group[col] == True) & (condition.loc[group.index])
            else:
                mask = group[col] == True

            # Find the indices where the condition is True
            true_indices = np.where(mask)[0]

            # Skip the first `skip_count` occurrences
            if len(true_indices) > skip_count:
                true_indices = true_indices[skip_count:]

            # Initialize a zero array for the cumulative sum
            cumsum_array = np.zeros(len(group), dtype=int)

            # Set the positions of remaining True values to 1
            cumsum_array[true_indices] = 1

            # Calculate the cumulative sum
            cumulative_sum = np.cumsum(cumsum_array)

            # Update the base_df with the cumulative sum for the group
            base_df.loc[group.index, count_col] = cumulative_sum

        # Reset count to 0 where the column value is False
        base_df.loc[
            base_df[col] == False,
            count_col,
        ] = 0

    return base_df


def update_fractal_cycle_id(kwargs, df, bb_cycle_col, end_condition_col):
    cycle_start_condition = {
        MarketDirection.LONG: (
            (df["market_direction"] == MarketDirection.LONG)
            & (
                df[f"P_1_FRACTAL_CONFIRMED_LONG_{kwargs.get('fractal_sd')}"]
                == True
            )
            & (
                df[
                    f"count_P_1_FRACTAL_CONFIRMED_LONG_{kwargs.get('fractal_sd')}"
                ]
                > kwargs.get("fractal_cycle_start")
            )
            & (df[bb_cycle_col] > 1)
        ),
        MarketDirection.SHORT: (
            (df["market_direction"] == MarketDirection.SHORT)
            & (
                df[f"P_1_FRACTAL_CONFIRMED_SHORT_{kwargs.get('fractal_sd')}"]
                == True
            )
            & (
                df[
                    f"count_P_1_FRACTAL_CONFIRMED_SHORT_{kwargs.get('fractal_sd')}"
                ]
                > kwargs.get("fractal_cycle_start")
            )
            & (df[bb_cycle_col] > 1)
        ),
    }

    # adjust max to min
    end_condition = (
        df[end_condition_col] < df[FirstCycleColumns.MAX_TO_MIN.value]
    )

    cycle_end_condition = {
        MarketDirection.LONG: end_condition,
        MarketDirection.SHORT: end_condition,
    }

    update_cycle_number_by_condition(
        df,
        None,
        cycle_start_condition,
        cycle_end_condition,
        id_column_name=SecondCycleIDColumns.FRACTAL_CYCLE_ID.value,
        counter_starter=0,
        group_by_col=bb_cycle_col,
        group_start=1,
    )


def get_fractal_dataframes(
    validated_data, instrument, base_path, start_datetime, end_datetime
):
    # read fractal files
    index = "TIMESTAMP"
    fractal_files = {
        "entry_fractal": update_entry_fractal_file(
            instrument,
            validated_data.get("entry_fractal_file_number"),
            validated_data.get("check_entry_fractal"),
            base_path,
            index,
        ),
        "exit_fractal": update_exit_fractal_file(
            instrument,
            validated_data.get("exit_fractal_file_number"),
            validated_data.get("check_exit_fractal"),
            base_path,
            index,
        ),
    }

    fractal_files = read_files(
        start_datetime,
        end_datetime,
        fractal_files,
    )

    return fractal_files


def update_second_cycle_id(
    df,
    id_col_name,
    end_condition_col,
    first_cycle_columns,
    second_cycle_columns,
):
    """
    Update cycle ID in the DataFrame based on the specified cycle ID type.

    Parameters:
    df (DataFrame): The DataFrame containing the cycle data.

    """

    for cycle_col in first_cycle_columns:
        id_column_name = f"{cycle_col}_{id_col_name}"
        second_cycle_columns.append(id_column_name)
        df[id_column_name] = 0

        start_condition = df[cycle_col] > 1

        if id_col_name == SecondCycleIDColumns.CTC_CYCLE_ID.value:
            end_condition = (
                df[end_condition_col]
                < df[FirstCycleColumns.CLOSE_TO_CLOSE.value]
            )
        elif id_col_name == SecondCycleIDColumns.MTM_CYCLE_ID.value:
            end_condition = (
                df[end_condition_col] < df[FirstCycleColumns.MAX_TO_MIN.value]
            )

        cycle_start_condition = {
            MarketDirection.LONG: start_condition,
            MarketDirection.SHORT: start_condition,
        }

        cycle_end_condition = {
            MarketDirection.LONG: end_condition,
            MarketDirection.SHORT: end_condition,
        }

        update_cycle_number_by_condition(
            df,
            cycle_col,
            cycle_start_condition,
            cycle_end_condition,
            id_column_name=id_column_name,
            counter_starter=0,
        )


def update_secondary_cycle_ids(df, validated_data, cycle_cols: dict):
    # update max_to_min
    update_MTM_CTC_cols(df, validated_data)

    # mtm cycle
    update_second_cycle_id(
        df,
        id_col_name=SecondCycleIDColumns.MTM_CYCLE_ID.value,
        end_condition_col="adjusted_close_for_max_to_min",
        first_cycle_columns=cycle_cols[CycleType.FIRST_CYCLE],
        second_cycle_columns=cycle_cols[CycleType.MTM_CYCLE],
    )

    # ctc cycle
    update_second_cycle_id(
        df,
        id_col_name=SecondCycleIDColumns.CTC_CYCLE_ID.value,
        end_condition_col="adjusted_colse",
        first_cycle_columns=cycle_cols[CycleType.FIRST_CYCLE],
        second_cycle_columns=cycle_cols[CycleType.CTC_CYCLE],
    )


def is_initial_cycles(row: pd.Series) -> bool:
    # real cycle starts from 1 for secondary cycles and 2 for first cycle

    if pd.isna(row[Trade.current_cycle]):
        return True

    if (
        Trade.cycle_to_consider == CycleType.FIRST_CYCLE
        and row[Trade.current_cycle] < 2
    ):
        return True

    if (
        Trade.cycle_to_consider == CycleType.MTM_CYCLE
        or Trade.cycle_to_consider == CycleType.CTC_CYCLE
    ) and row[Trade.current_cycle] < 1:
        return True
    return False


def update_last_state(row, state, key, market_direction):
    """
    Update the last state of the entry.

    Parameters:
        row (Series): The row containing the data.
        state (dict): The state of the entry.
        key (str): "entry" or "exit" indicating the condition type
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
        state[(row["group_id"], row[Trade.current_cycle])].append(
            (row.name, row["Close"])
        )


def is_cycle_entry_fractal(row, state, market_direction, key):
    """
    Check if the cycle entry fractal condition is met.

    Parameters:
        row (Series): The row containing the data.
        market_direction (MarketDirection): The market direction.
        state (dict): The state of the entry.

    Returns:
        bool: True if the entry fractal condition is met, False otherwise.
    """
    if (
        market_direction
        # and len(state[(row['group_id'], row[Trade.current_cycle])])
        and row[confirm_fractal_column_dict[key][market_direction]]
    ):
        return True
    return False


def is_cycle_exit_fractal(row, market_direction, exit_state):
    """
    Check if the cycle exit fractal condition is met.

    Parameters:
        row (Series): The row containing the data.
        market_direction (MarketDirection): The market direction.
        exit_state (dict): The state of the exit.

    Returns:
        bool: True if the exit fractal condition is met, False otherwise.
    """
    if market_direction:
        return row[confirm_fractal_column_dict["exit"][market_direction]]
    elif exit_state.get(MarketDirection.PREVIOUS, None):
        return row[
            confirm_fractal_column_dict["exit"][
                exit_state[MarketDirection.PREVIOUS]
            ]
        ]
    return False


def check_cycle_entry_condition(row: pd.Series, state: dict) -> bool:
    """
    Check if the entry condition is met.

    Parameters:
        row (Series): The row containing the data.
        state (dict): The state of the entry.

    Returns:
        bool: True if the entry condition is met, False otherwise.
    """
    if not is_trade_start_time_crossed(row):
        return False, None, None

    market_direction = row["market_direction"]
    if (
        pd.isna(market_direction)
        or market_direction == MarketDirection.UNKNOWN
    ):
        market_direction = None

    if is_initial_cycles(row):
        return False, None, None

    # update state for the cycle
    # update_last_state(row, state, "entry", market_direction)

    if (
        not Trade.allowed_direction == MarketDirection.ALL
        and not market_direction == Trade.allowed_direction
    ):
        # clear the state if the cycle changes
        # clear_state(row, state)
        return False, None, None

    if (
        Trade.type == TradeType.INTRADAY
        and row.name.time() >= Trade.trade_end_time
    ):
        # clear the state if the cycle changes
        # clear_state(row, state)
        return False, None, None

    is_fractal_entry = False
    if Trade.check_entry_fractal:
        is_fractal_entry = is_cycle_entry_fractal(
            row, state, market_direction, "entry"
        )

    is_cycle_change_entry = False
    if row[Trade.current_cycle] != row["previous_cycle_id"]:
        is_cycle_change_entry = True

    # clear the state if the cycle changes
    # clear_state(row, state)

    if is_cycle_change_entry:
        return (
            is_cycle_change_entry,
            market_direction,
            TradeExitType.CYCLE_CHANGE,
        )

    if Trade.check_entry_fractal:
        return is_fractal_entry, market_direction, TradeExitType.FRACTAL

    return False, None, None


def clear_state(row, state):
    if row[Trade.current_cycle] != row["previous_cycle_id"]:
        state[row["previous_cycle_id"]].clear()


def is_tp_exit(row, exit_state):
    is_tp_exit = False
    if row[TargetProfitColumns.TP_END.value] == "YES":
        is_tp_exit = True

    return is_tp_exit


def check_cycle_exit_signals(row, exit_state, entry_state):
    market_direction = row["exit_market_direction"]
    if (
        pd.isna(market_direction)
        or market_direction == MarketDirection.UNKNOWN
    ):
        market_direction = None

    if is_trade_end_time_reached(row):
        return True, TradeExitType.END

    if Trade.check_exit_fractal:
        is_fractal_exit = is_cycle_exit_fractal(
            row, market_direction, exit_state
        )

    if Trade.calculate_tp:
        tp_exit = is_tp_exit(row, exit_state)

    # reset entry id if the signal changes
    # if market_direction:
    #     previous_direction = exit_state.get(MarketDirection.PREVIOUS, None)
    #     exit_state[MarketDirection.PREVIOUS] = market_direction
    #     if previous_direction and signal_change(
    #         previous_direction, market_direction
    #     ):
    #         Trade.reset_trade_entry_id_counter()
    #         return True, TradeExitType.SIGNAL

    # clear the state if the cycle changes
    if (
        not pd.isna(row[Trade.current_cycle])
        and not pd.isna(row["previous_cycle_id"])
        and row[Trade.current_cycle] != row["previous_cycle_id"]
    ):
        Trade.reset_trade_entry_id_counter()
        return True, TradeExitType.CYCLE_CHANGE

    is_exit, exit_type = False, None
    if Trade.check_exit_fractal and Trade.calculate_tp:
        if is_fractal_exit:
            is_exit, exit_type = is_fractal_exit, TradeExitType.FRACTAL
        if tp_exit:
            is_exit, exit_type = tp_exit, TradeExitType.TP
        return is_exit, exit_type

    if Trade.check_exit_fractal:
        return is_fractal_exit, TradeExitType.FRACTAL

    if Trade.calculate_tp:
        return tp_exit, TradeExitType.TP

    return False, None


def include_volatile_volume_tags(validated_data, strategy_df):
    if validated_data.get("include_volume"):
        volume_df = pd.read_csv(
            f"{VOLUME_OUTPUT_FOLDER}/{validated_data['volume_file']}",
            usecols=["dt", "category"],
            index_col="dt",
            parse_dates=True,
        )
        strategy_df = pd.merge(
            strategy_df, volume_df, left_index=True, right_index=True
        )

        if not strategy_df.loc[
            strategy_df["category"] == validated_data["volume_tag_to_process"],
        ].empty:
            first_occurrence = (
                strategy_df.loc[
                    strategy_df["category"]
                    == validated_data["volume_tag_to_process"],
                ]
                .iloc[0]
                .name
            )
            strategy_df = strategy_df.loc[first_occurrence:]
        else:
            raise ValueError(
                "No data found for the given volume tag condition"
            )

    if validated_data.get("include_volatile"):
        volatile_df = pd.read_csv(
            f"{VOLATILE_OUTPUT_FOLDER}/{validated_data['volatile_file']}",
            index_col="dt",
            parse_dates=True,
        )
        volatile_tags = [
            col for col in volatile_df.columns if "volatile_tag" in col
        ]

        strategy_df = pd.merge(
            strategy_df,
            volatile_df[volatile_tags],
            left_index=True,
            right_index=True,
        )

        condition = True
        for tag in volatile_tags:
            condition &= (
                strategy_df[tag] == validated_data["volatile_tag_to_process"]
            )

        if not strategy_df[condition].empty:
            first_occurrence = strategy_df[condition].iloc[0].name
            strategy_df = strategy_df.loc[first_occurrence:]
        else:
            raise ValueError(
                "No data found for the given volatile tag condition"
            )

    return strategy_df


def process_cycle(validated_data, strategy_pair, instrument):
    strategy_pair_str = "_".join(map(lambda x: str(x), strategy_pair))
    portfolio_ids_str = " - ".join(validated_data.get("portfolio_ids"))

    base_path = os.getenv("STRATEGY_DB_PATH")

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

    strategy_df = include_volatile_volume_tags(validated_data, strategy_df)

    base_df = get_base_df(
        validated_data, strategy_df, strategy_pair_str, instrument
    )

    cycle_base_dfs, _ = get_cycle_base_df(
        **validated_data, base_df=base_df, instrument=instrument
    )

    cycle_base_df = cycle_base_dfs[
        validated_data.get("close_time_frames_1")[0]
    ]

    initialize(validated_data)

    # update exit market direction
    def get_exit_market_direction(row):
        market_direction = get_market_direction(
            row,
            condition_key="exit",
            signal_columns=Trade.signal_columns,
            market_direction_conditions=Trade.market_direction_conditions,
        )
        if market_direction:
            return market_direction
        return MarketDirection.UNKNOWN

    cycle_base_df["exit_market_direction"] = cycle_base_df.apply(
        get_exit_market_direction, axis=1
    )

    # need to update MTM and CTC cycle id
    cycle_cols = defaultdict(list)

    cycle_cols[CycleType.FIRST_CYCLE] = [
        col for col in cycle_base_df.columns if "cycle_no_" in col
    ]

    update_secondary_cycle_ids(
        cycle_base_df,
        validated_data=validated_data,
        cycle_cols=cycle_cols,
    )

    update_target_profit_analysis(
        cycle_base_df,
        validated_data.get("tp_percentage"),
        validated_data.get("tp_method"),
        cycle_col_name=cycle_cols[CycleType.MTM_CYCLE][0],
        close_col_name=cycle_cols[CycleType.FIRST_CYCLE][0].replace(
            "cycle_no", "close_to"
        ),
    )

    # update trade cycle columns
    Trade.cycle_columns = cycle_cols

    fractal_files = get_fractal_dataframes(
        validated_data, instrument, base_path, start_datetime, end_datetime
    )

    # merge all dataframes
    merged_fractal_df = merge_all_df(fractal_files.values())

    merged_fractal_df.reset_index(inplace=True)

    signal_columns = [
        f"TAG_{id}" for id in validated_data.get("portfolio_ids")
    ]

    cols = [
        "dt",
        "Open",
        "High",
        "Low",
        "Close",
        "market_direction",
        "exit_market_direction",
        "group_id",
        *signal_columns,
        *cycle_cols[Trade.cycle_to_consider],
        TargetProfitColumns.TP_END.value,
    ]

    merged_df = pd.merge_asof(
        merged_fractal_df,
        cycle_base_df[cols],
        left_on="TIMESTAMP",
        right_on="dt",
        direction="backward",
    )
    # make TIMESTAMP as index
    merged_df.set_index("TIMESTAMP", inplace=True)
    if DEBUG:
        write_dataframe_to_csv(
            merged_df,
            SG_CYCLE_OUTPUT_FOLDER,
            f"merged_df_{instrument}_{strategy_pair_str}.csv",
        )

    # process trade
    for cycle in Trade.cycle_columns[Trade.cycle_to_consider]:
        Trade.current_cycle = cycle

        # update previous cycle id
        merged_df["previous_cycle_id"] = merged_df[cycle].shift(1)

        file_name = f"df_{instrument}_{strategy_pair_str}_{cycle}.csv"

        entry_state = defaultdict(deque)

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
            entry_func=check_cycle_entry_condition,
            exit_func=check_cycle_exit_signals,
        )

        if DEBUG:
            write_dataframe_to_csv(
                output_df, SG_CYCLE_OUTPUT_FOLDER, file_name
            )
