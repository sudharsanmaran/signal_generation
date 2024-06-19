import os

import pandas as pd

from pa_analysis.constants import CycleOutputColumns
from pa_analysis.utils import (
    format_duration,
    make_positive,
    make_round,
    write_dict_to_csv,
)
from source.constants import MarketDirection
from source.data_reader import read_files
from source.trade_processor import write_dataframe_to_csv


def process_cycles(**kwargs):
    # get the base df
    all_df = get_base_df(kwargs)

    # process the data
    for time_frame, df in all_df.items():
        results = analyze_cycles(df, time_frame)

        headers = results[0].keys()
        write_dict_to_csv(
            results,
            main_header=headers,
            output_dir="pa_analysis_output/cycle_outpts/results",
            csv_filename=f"result_tf_{time_frame}.csv",
        )


def analyze_cycles(df, time_frame):
    # Group by 'group_id'
    groups = df.groupby("group_id")

    results = []
    updates = {col.value: [] for col in CycleOutputColumns}
    updates.update(
        {"index": [], "group_id": [], "period_band": [], "cycle_no": []}
    )
    for group_id, group_data in groups:
        # Identify cycle columns dynamically

        cycle_columns = [
            col for col in group_data.columns if "cycle_no" in col
        ]

        group_start_row = group_data.iloc[0]
        for cycle_col in cycle_columns:
            # Filter the group_data to get only the valid cycles and find unique cycle numbers
            unique_cycles = group_data.loc[
                group_data[cycle_col] > 0, cycle_col
            ].unique()

            for cycle in unique_cycles:
                cycle_analysis = {
                    "group_id": group_id,
                    "period_band": cycle_col[-4:],
                    "cycle_no": cycle,
                }
            for cycle in unique_cycles:
                cycle_data = group_data[group_data[cycle_col] == cycle]

                # real cycle starts from cycle no 2
                if cycle == 1:
                    continue

                updates["index"].append(cycle_data.index[0])

                cycle_analysis[
                    CycleOutputColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value
                ] = format_duration(
                    make_round(
                        (
                            cycle_data.iloc[0]["dt"] - group_start_row["dt"]
                        ).total_seconds()
                    )
                )

                cycle_analysis[CycleOutputColumns.CYCLE_DURATION.value] = (
                    format_duration(
                        make_round(
                            (
                                cycle_data.iloc[-1]["dt"]
                                - cycle_data.iloc[0]["dt"]
                            ).total_seconds()
                        )
                    )
                )

                cycle_analysis[CycleOutputColumns.MOVE.value] = make_positive(
                    make_round(
                        cycle_data.iloc[0]["Close"] - group_start_row["Close"]
                    )
                )
                cycle_analysis[CycleOutputColumns.MOVE_PERCENT.value] = (
                    make_positive(
                        make_round(
                            cycle_analysis[CycleOutputColumns.MOVE.value]
                            / group_start_row["Close"]
                            * 100
                        )
                    )
                )

                if group_start_row["market_direction"] == MarketDirection.LONG:

                    try:
                        min_idx = cycle_data["Low"].idxmin()
                        max_idx = cycle_data.loc[cycle_data.index > min_idx][
                            "High"
                        ].idxmax()
                    except ValueError:
                        print(
                            f"neglecting subsequent calculation rule... for grp:{group_id} cycle no: {cycle}"
                        )
                        min_idx = cycle_data["Low"].idxmin()
                        max_idx = cycle_data["High"].idxmax()
                else:
                    try:
                        max_idx = cycle_data["High"].idxmax()
                        min_idx = cycle_data.loc[cycle_data.index > max_idx][
                            "Low"
                        ].idxmin()
                    except ValueError:
                        print(
                            f"neglecting subsequent calculation rule... for grp:{group_id} cycle no: {cycle}"
                        )
                        max_idx = cycle_data["High"].idxmax()
                        min_idx = cycle_data["Low"].idxmin()

                cycle_analysis[CycleOutputColumns.CYCLE_MAX.value] = (
                    cycle_data.loc[max_idx, "High"]
                )
                cycle_analysis[CycleOutputColumns.DURATION_TO_MAX.value] = (
                    make_round(
                        (
                            (
                                cycle_data.loc[max_idx, "dt"]
                                - cycle_data["dt"].iloc[0]
                            ).total_seconds()
                            / (3600 * 24)
                        )
                    )
                )

                # upadte duration first yes to first yes to no change
                first_yes_index = cycle_data[
                    cycle_data[f"close_to_{cycle_col[9:]}"] == "YES"
                ]["dt"].iloc[0]

                first_yes_change_condition = (
                    cycle_data[f"close_to_{cycle_col[9:]}"] == "YES"
                ) & (cycle_data[f"close_to_{cycle_col[9:]}"].shift(-1) == "NO")

                try:
                    first_yes_change = cycle_data[first_yes_change_condition][
                        "dt"
                    ].iloc[0]
                except IndexError:
                    first_yes_change = cycle_data["dt"].iloc[-1]

                cycle_analysis[CycleOutputColumns.DURATION_ABOVE_BB.value] = (
                    make_round(
                        (
                            (
                                first_yes_change - first_yes_index
                            ).total_seconds()
                            / (3600 * 24)
                        )
                    )
                )

                # signal start to max
                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MAX_POINTS.value
                ] = make_positive(
                    make_round(
                        group_start_row["signal_start_price"]
                        - cycle_analysis[CycleOutputColumns.CYCLE_MAX.value]
                    )
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MAX_PERCENT.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.SIGNAL_START_TO_MAX_POINTS.value
                        ]
                        / group_start_row["signal_start_price"]
                        * 100
                    )
                )

                # move from start of cycle to max
                cycle_analysis[
                    CycleOutputColumns.MOVE_START_TO_MAX_CYCLE_POINTS.value
                ] = make_round(
                    cycle_analysis[CycleOutputColumns.CYCLE_MAX.value]
                    - group_start_row["Close"]
                )

                cycle_analysis[
                    CycleOutputColumns.MOVE_START_TO_MAX_CYCLE_PERCENT.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.MOVE_START_TO_MAX_CYCLE_POINTS.value
                        ]
                        / group_start_row["Close"]
                        * 100
                    )
                )

                cycle_analysis[CycleOutputColumns.AVERAGE_TILL_MAX.value] = (
                    make_round(cycle_data[:max_idx]["High"].mean())
                )

                cycle_analysis[CycleOutputColumns.CYCLE_MIN.value] = (
                    cycle_data.loc[min_idx, "Low"]
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                ] = make_positive(
                    make_round(
                        group_start_row["signal_start_price"]
                        - cycle_analysis[CycleOutputColumns.CYCLE_MIN.value]
                    )
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MINIMUM_PERCENT.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                        ]
                        / group_start_row["signal_start_price"]
                        * 100
                    )
                )

                cycle_analysis[CycleOutputColumns.AVERAGE_TILL_MIN.value] = (
                    make_round(cycle_data[:min_idx]["Low"].mean())
                )

                # category
                if group_start_row["market_direction"] == MarketDirection.LONG:
                    cycle_analysis[CycleOutputColumns.CATEGORY.value] = (
                        "Below"
                        if cycle_analysis[CycleOutputColumns.CYCLE_MIN.value]
                        > group_start_row["signal_start_price"]
                        else "Rebound"
                    )
                else:
                    cycle_analysis[CycleOutputColumns.CATEGORY.value] = (
                        "Rebound"
                        if cycle_analysis[CycleOutputColumns.CYCLE_MAX.value]
                        > group_start_row["signal_start_price"]
                        else "Below"
                    )

                cycle_analysis[
                    CycleOutputColumns.DURATION_BETWEEN_MAX_MIN.value
                ] = format_duration(
                    make_positive(
                        make_round(
                            (
                                (
                                    cycle_data.loc[max_idx, "dt"]
                                    - cycle_data.loc[min_idx, "dt"]
                                ).total_seconds()
                            )
                        )
                    )
                )

                cycle_analysis[
                    CycleOutputColumns.AVG_OF_MAX_TO_AVG_OF_MIN.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.AVERAGE_TILL_MAX.value
                        ]
                        - cycle_analysis[
                            CycleOutputColumns.AVERAGE_TILL_MIN.value
                        ]
                    )
                )

                cycle_analysis[CycleOutputColumns.MAX_TO_MIN.value] = (
                    make_round(
                        cycle_analysis[CycleOutputColumns.CYCLE_MAX.value]
                        - cycle_analysis[CycleOutputColumns.CYCLE_MIN.value]
                    )
                )

                cycle_analysis[CycleOutputColumns.CLOSE_TO_CLOSE.value] = (
                    make_positive(
                        make_round(
                            cycle_data.iloc[-1]["Close"]
                            - cycle_data.iloc[0]["Close"]
                        )
                    )
                )

                results.append(cycle_analysis)

                for key, value in cycle_analysis.items():
                    updates[key].append(value)

    for col, values in updates.items():
        if col != "index":
            df.loc[updates["index"], col] = values

    write_dataframe_to_csv(
        df,
        "pa_analysis_output/cycle_outpts/base_df",
        f"base_df_tf_{time_frame}.csv",
    )
    return results


def get_base_df(kwargs):
    base_df = kwargs.get("base_df")
    start_datetime, end_datetime = (
        base_df.index[0],
        base_df.index[-1],
    )

    files_to_read, tf_bb_cols, time_frames_1, time_frames_2 = (
        formulate_files_to_read(kwargs)
    )

    dfs = read_files(
        start_datetime,
        end_datetime,
        files_to_read,
    )

    updated_df = {}
    for tf, df in dfs.items():
        updated_df[tf] = update_cycle_columns(
            df,
            tf_bb_cols[tf].values(),
            base_df,
        )

    time_frames_1_dfs, time_frames_2_dfs = {}, {}
    for tf, df in updated_df.items():
        if tf in time_frames_1:
            time_frames_1_dfs[tf] = df
        else:
            time_frames_2_dfs[tf] = df

    df_to_analyze = {}
    # update cycle count
    if kwargs.get("check_bb_2"):
        for tf, df in time_frames_1_dfs.items():
            # merge the dataframes
            merged_df = merge_dataframes(df, time_frames_2_dfs, tf_bb_cols)
            updated_yes_no_columns(bb_cols, merged_df)
            # updated the cycle count
            for col in tf_bb_cols[tf].values():
                update_cycle_count_2(merged_df, col)

            df_to_analyze[tf] = merged_df

            write_dataframe_to_csv(
                merged_df,
                "pa_analysis_output/cycle_outpts/base_df",
                f"base_df_tf_{tf}.csv",
            )
    else:
        for tf, df in time_frames_1_dfs.items():
            for col in tf_bb_cols[tf].values():
                update_cycle_count_1(df, col)
            df_to_analyze[tf] = df

    return df_to_analyze


def formulate_files_to_read(kwargs):
    instrument = kwargs.get("instrument")
    time_frames_1 = kwargs.get("time_frames_1")
    time_frames_2 = kwargs.get("time_frames_2")

    # Combine time frames while keeping track of their origin
    time_frames_with_origin = [(tf, 1) for tf in time_frames_1] + [
        (tf, 2) for tf in time_frames_2
    ]
    base_path = os.getenv("BB_DB_PATH")
    base_cols = {"dt", "Open", "High", "Low", "Close"}

    tf_bb_cols = {
        tf: get_bb_cols(
            kwargs.get(f"periods_{origin}"),
            kwargs.get(f"sds_{origin}"),
        )
        for tf, origin in time_frames_with_origin
    }

    rename_dict = {
        tf: {col: f"{origin}_{tf}_{col[-4:]}" for col in tf_bb_cols[tf]}
        for tf, origin in time_frames_with_origin
    }

    files_to_read = {
        time_frame: {
            "read": True,
            "cols": [*base_cols, *tf_bb_cols[time_frame]],
            "index_col": "dt",
            "file_path": os.path.join(
                base_path,
                f"{instrument}_TF_{time_frame}.csv",
            ),
            "rename": rename_dict[time_frame],
        }
        for time_frame, _ in time_frames_with_origin
    }

    return files_to_read, rename_dict, time_frames_1, time_frames_2


def get_bb_cols(periods, sds):
    bb_1_cols = {
        f"P_{int(int(period)/20)}_MEAN_BAND_{period}_{sd}"
        for period in periods
        for sd in sds
    }

    return bb_1_cols


def merge_dataframes(base_df, time_frame_2_dfs: dict, tf_bb_cols: dict):
    for tf, df in time_frame_2_dfs.items():
        cols = [f"close_to_{col}" for col in tf_bb_cols[tf].values()]
        cols.insert(0, "dt")
        cols.extend([col for col in tf_bb_cols[tf].values()])
        base_df = pd.merge_asof(
            base_df,
            df[cols],
            left_on="dt",
            right_on="dt",
            direction="nearest",
        )

    return base_df


def update_cycle_columns(df, bb_cols, base_df):
    # update direction
    base_df = base_df.reset_index().rename(columns={"index": "TIMESTAMP"})
    df = df.reset_index().rename(columns={"index": "dt"})
    merged_df = pd.merge_asof(
        df,
        base_df[["TIMESTAMP", "market_direction"]],
        left_on="dt",
        right_on="TIMESTAMP",
        direction="nearest",
    )

    # update signal start price
    condition = merged_df["market_direction"] != merged_df[
        "market_direction"
    ].shift(1)

    merged_df["signal_start_price"] = pd.NA
    merged_df.loc[condition, "signal_start_price"] = merged_df["Close"]
    merged_df["signal_start_price"] = merged_df["signal_start_price"].ffill()

    # update group id
    group_condition = merged_df["market_direction"] != merged_df[
        "market_direction"
    ].shift(1)
    merged_df["group_id"] = group_condition.cumsum()



    return merged_df


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

        if idx == 383:
            a = 1

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


def is_cycle_end(merged_df, bb_2_cols, idx):
    if bb_2_cols:
        return any(
            (
                (merged_df[bb_2_col].iloc[idx] == "NO")
                & (
                    merged_df["market_direction"].iloc[idx]
                    == MarketDirection.LONG
                )
            )
            | (
                (merged_df[bb_2_col].iloc[idx] == "YES")
                & (
                    merged_df["market_direction"].iloc[idx]
                    == MarketDirection.SHORT
                )
            )
            for bb_2_col in bb_2_cols
        )
    return False


def is_cycle_start(merged_df, col, bb_2_cols, idx):
    start_condition = confirm_start_condition(merged_df, col, idx)

    if bb_2_cols:
        if merged_df["market_direction"].iloc[idx] == MarketDirection.LONG:
            bb_2_start_condition = (
                merged_df[bb_col].iloc[idx] == "YES" for bb_col in bb_2_cols
            )
        else:
            bb_2_start_condition = (
                merged_df[bb_col].iloc[idx] == "NO" for bb_col in bb_2_cols
            )
        start_condition = (
            (merged_df[f"close_to_{col}"].iloc[idx] == "YES")
        ) & all(bb_2_start_condition)
    return start_condition


def confirm_start_condition(merged_df, col, idx):
    return (merged_df[f"close_to_{col}"].iloc[idx] == "YES") & (
        merged_df[f"close_to_{col}"].shift(1).iloc[idx] == "NO"
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
