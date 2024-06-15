import os

import pandas as pd

from pa_analysis.constants import CycleOutputColumns
from pa_analysis.utils import make_positive, make_round, write_dict_to_csv
from source.constants import MarketDirection
from source.data_reader import read_files
from source.trade_processor import write_dataframe_to_csv


def process_cycles(**kwargs):
    # get start_date, end_date, time_frame, period, sd, instruments
    base_df = kwargs.get("base_df")

    # read the files
    time_frames, periods, sds, instrument = (
        kwargs.get("time_frames"),
        kwargs.get("periods"),
        kwargs.get("sds"),
        kwargs.get("instrument"),
    )
    all_df = get_base_df(base_df, time_frames, periods, sds, instrument)

    # process the data
    for time_frame, df in all_df.items():
        results = analyze_cycles(df)
        headers = list(results[0].values())[0].keys()
        flatten_result = []
        for result in results:
            for band_data in result.values():
                flatten_result.append(band_data)

        write_dict_to_csv(
            flatten_result,
            main_header=headers,
            output_dir="pa_analysis_output/cycle_outpts/results",
            csv_filename=f"result_tf_{time_frame}.csv",
        )


def analyze_cycles(df):
    # Group by 'group_id'
    groups = df.groupby("group_id")

    results = []

    for group_id, group_data in groups:
        # Identify cycle columns dynamically

        cycle_columns = [
            col for col in group_data.columns if "cycle_no" in col
        ]

        group_results = {}
        group_start_row = group_data.iloc[0]
        for cycle_col in cycle_columns:
            unique_cycles = group_data[cycle_col].unique()
            cycle_analysis = {
                "group_id": group_id,
                "period_band": cycle_col[-4:],
            }
            for cycle in unique_cycles:
                cycle_data = group_data[group_data[cycle_col] == cycle]

                # real cycle starts from cycle no 2
                # use cycle no 1 for signal start to cycle start
                if cycle == 1:
                    continue

                cycle_analysis[
                    CycleOutputColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value
                ] = make_round(
                    (
                        (
                            cycle_data.iloc[0]["dt"] - group_start_row["dt"]
                        ).total_seconds()
                        / (3600 * 24)
                    )
                )

                cycle_analysis[CycleOutputColumns.MOVE.value] = make_round(
                    cycle_data.iloc[0]["Close"] - group_start_row["Close"]
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
                ] = make_round(
                    cycle_data.iloc[0]["signal_start_price"]
                    - cycle_analysis[CycleOutputColumns.CYCLE_MAX.value]
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MAX_PERCENT.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.SIGNAL_START_TO_MAX_POINTS.value
                        ]
                        / cycle_data.iloc[0]["signal_start_price"]
                        * 100
                    )
                )

                # category
                cycle_analysis[CycleOutputColumns.CATEGORY.value] = (
                    "Rebound"
                    if cycle_analysis[
                        CycleOutputColumns.SIGNAL_START_TO_MAX_PERCENT.value
                    ]
                    < cycle_analysis[CycleOutputColumns.MOVE_PERCENT.value]
                    else "Below"
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
                    cycle_data[:max_idx]["High"].mean()
                )

                cycle_analysis[CycleOutputColumns.CYCLE_MIN.value] = (
                    cycle_data.loc[min_idx, "Low"]
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                ] = make_round(
                    cycle_data.iloc[0]["signal_start_price"]
                    - cycle_analysis[CycleOutputColumns.CYCLE_MIN.value]
                )

                cycle_analysis[
                    CycleOutputColumns.SIGNAL_START_TO_MINIMUM_PERCENT.value
                ] = make_round(
                    make_positive(
                        cycle_analysis[
                            CycleOutputColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                        ]
                        / cycle_data.iloc[0]["signal_start_price"]
                        * 100
                    )
                )

                cycle_analysis[CycleOutputColumns.AVERAGE_TILL_MIN.value] = (
                    cycle_data[:min_idx]["Low"].mean()
                )

                cycle_analysis[
                    CycleOutputColumns.DURATION_BETWEEN_MAX_MIN.value
                ] = make_positive(
                    make_round(
                        (
                            (
                                cycle_data.loc[max_idx, "dt"]
                                - cycle_data.loc[min_idx, "dt"]
                            ).total_seconds()
                            / (3600 * 24)
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

            group_results[cycle_col] = cycle_analysis

        results.append(group_results)

    return results


def get_base_df(base_df, time_frames, periods, sds, instrument):
    start_datetime, end_datetime = (
        base_df.index[0],
        base_df.index[-1],
    )
    base_path = os.getenv("BB_DB_PATH")
    base_cols = ["dt", "Open", "High", "Low", "Close"]
    bb_cols = [
        f"P_{int(int(period)/20)}_MEAN_BAND_{period}_{sd}"
        for period in periods
        for sd in sds
    ]
    base_cols.extend(bb_cols)
    cols = base_cols
    rename_dict = {col: col[9:] for col in bb_cols}
    files_to_read = {
        time_frame: {
            "read": True,
            "cols": cols,
            "index_col": "dt",
            "file_path": os.path.join(
                base_path,
                f"{instrument}_TF_{time_frame}.csv",
            ),
            "rename": rename_dict,
        }
        for time_frame in time_frames
    }
    all_df = []
    read_files(
        start_datetime,
        end_datetime,
        all_df,
        files_to_read,
    )
    cycle_base_df = {}
    for df, time_frame in zip(all_df, time_frames):
        cycle_base_df[time_frame] = update_base_df(
            df, rename_dict.values(), base_df
        )
        write_dataframe_to_csv(
            cycle_base_df[time_frame],
            "pa_analysis_output/cycle_outpts/base_df",
            f"base_df_tf_{time_frame}.csv",
        )
    return cycle_base_df


def update_base_df(df, bb_cols, base_df):
    # update direction
    base_df = base_df.reset_index().rename(columns={"index": "TIMESTAMP"})
    df = df.reset_index().rename(columns={"index": "dt"})
    merged_df = pd.merge_asof(
        df,
        base_df[["TIMESTAMP", "market_direction"]],
        left_on="dt",
        right_on="TIMESTAMP",
        direction="backward",
    )

    # update signal start price
    condition = merged_df["market_direction"] != merged_df[
        "market_direction"
    ].shift(1)
    merged_df["signal_start_price"] = pd.NA
    merged_df.loc[condition, "signal_start_price"] = merged_df["Close"]
    merged_df["signal_start_price"] = merged_df["signal_start_price"].ffill()

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

        # update group id
        group_condition = merged_df["market_direction"] != merged_df[
            "market_direction"
        ].shift(1)
        merged_df["group_id"] = group_condition.cumsum()

        # update cycle number
        update_cycle_count(merged_df, col)

    return merged_df


def update_cycle_count(merged_df, col):
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
