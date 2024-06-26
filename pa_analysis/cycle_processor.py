from itertools import chain
import os

import pandas as pd

from pa_analysis.constants import (
    BB_Band_Columns,
    FirstCycleColumns,
    MTMCrossedCycleColumns,
    SecondCycleIDColumns,
)
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
        results = analyze_cycles(df, time_frame, kwargs)

        # max to min percent
        update_max_to_min_percent(df, kwargs)

        first_cycle_columns = [col for col in df.columns if "cycle_no_" in col]

        # mtm cycle
        update_second_cycle_id(
            df,
            id_col_name=SecondCycleIDColumns.MTM_CYCLE_ID.value,
            end_condition_col="adjusted_close_for_max_to_min",
            first_cycle_columns=first_cycle_columns,
        )
        update_second_cycle_analytics(
            df,
            results,
            time_frame,
            prefix="MTM",
            cycle_count_col=SecondCycleIDColumns.MTM_CYCLE_ID.value,
        )

        # ctc cycle
        update_second_cycle_id(
            df,
            id_col_name=SecondCycleIDColumns.CTC_CYCLE_ID.value,
            end_condition_col="adjusted_colse",
            first_cycle_columns=first_cycle_columns,
        )
        update_second_cycle_analytics(
            df,
            results,
            time_frame,
            prefix="CTC",
            cycle_count_col=SecondCycleIDColumns.CTC_CYCLE_ID.value,
        )

        headers = results[0].keys()
        write_dict_to_csv(
            results,
            main_header=headers,
            output_dir="pa_analysis_output/cycle_outpts/results",
            csv_filename=f"result_tf_{time_frame}.csv",
        )


def update_max_to_min_percent(df, kwargs):
    df["adjusted_close_for_max_to_min"] = df["Close"] * (
        kwargs.get("max_to_min_percent") / 100
    )

    df[MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_3.value] = "NO"
    df[MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_5.value] = "NO"
    df[MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_75.value] = "NO"
    df[MTMCrossedCycleColumns.IS_MTM_CROSS_1.value] = "NO"

    df.loc[
        df[FirstCycleColumns.MAX_TO_MIN.value] > df["Close"] * (0.3 / 100),
        MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_3.value,
    ] = "YES"

    df.loc[
        df[FirstCycleColumns.MAX_TO_MIN.value] > df["Close"] * (0.5 / 100),
        MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_5.value,
    ] = "YES"

    df.loc[
        df[FirstCycleColumns.MAX_TO_MIN.value] > df["Close"] * (0.75 / 100),
        MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_75.value,
    ] = "YES"

    df.loc[
        df[FirstCycleColumns.MAX_TO_MIN.value] > df["Close"] * (1 / 100),
        MTMCrossedCycleColumns.IS_MTM_CROSS_1.value,
    ] = "YES"


def update_second_cycle_analytics(
    df,
    results,
    time_frame,
    prefix="CTC",
    cycle_count_col=SecondCycleIDColumns.CTC_CYCLE_ID.value,
):
    groups = list(df.groupby("group_id"))

    cycle_columns = [col for col in df.columns if cycle_count_col in col]
    for idx, cycle_col in enumerate(cycle_columns):
        idx += 1
        updates = {
            "index": [],
            f"{idx}_{prefix}_{FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_DURATION.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.MOVE.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.MOVE_PERCENT.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_MAX.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_MIN.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}": [],
            f"{idx}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}": [],
        }

        if cycle_count_col == SecondCycleIDColumns.MTM_CYCLE_ID.value:
            updates.update(
                {
                    f"{idx}_{prefix}_{FirstCycleColumns.AVERAGE_TILL_MAX.value}": [],
                    f"{idx}_{prefix}_{FirstCycleColumns.AVERAGE_TILL_MIN.value}": [],
                    f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}": [],
                }
            )

        # updates[
        #     f"{idx}_{prefix}_{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
        # ] = []
        # updates[
        #     f"{idx}_{prefix}_{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.MAX_TO_MIN.value}"
        # ] = []

        for group_idx, (group_id, group_data) in enumerate(groups):

            group_start_row = group_data.iloc[0]
            market_direction = group_start_row["market_direction"]
            unique_cycles = group_data.loc[
                group_data[cycle_col] > 0, cycle_col
            ].unique()
            for cycle in unique_cycles:
                cycle_analysis = {}
                cycle_data = group_data[group_data[cycle_col] == cycle]

                next_cycle_first_row = get_next_cycle_first_row(
                    group_data,
                    cycle,
                    cycle_col,
                    groups,
                    group_idx,
                    cycle_start=0,
                )

                if not cycle_data.empty and next_cycle_first_row is not None:
                    # Create a new DataFrame with the current cycle's data and the next cycle's first row
                    adjusted_cycle_data = pd.concat(
                        [cycle_data, next_cycle_first_row.to_frame().T]
                    )
                else:
                    adjusted_cycle_data = cycle_data

                # for difference in cycle min and max calculation
                is_last_cycle = False
                if cycle_count_col == SecondCycleIDColumns.CTC_CYCLE_ID.value:
                    is_last_cycle = False
                elif (
                    cycle_count_col == SecondCycleIDColumns.MTM_CYCLE_ID.value
                ):
                    is_last_cycle = cycle == unique_cycles[-1]

                min_idx, max_idx, cycle_min, cycle_max = get_min_max_idx(
                    adjusted_cycle_data,
                    group_start_row,
                    group_id,
                    cycle,
                    is_last_cycle,
                )

                if (not min_idx and not cycle_min) or (
                    not max_idx and not cycle_max
                ):
                    continue

                updates["index"].append(cycle_data.index[-1])

                update_signal_start_duration(
                    group_start_row,
                    cycle_analysis,
                    cycle_data,
                    signal_start_duration_key=f"{idx}_{prefix}_{FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value}",
                )

                update_cycle_duration(
                    cycle_analysis,
                    cycle_data,
                    cycle_duration_key=f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_DURATION.value}",
                )

                update_move_and_move_percent(
                    group_start_row,
                    cycle_analysis,
                    cycle_data,
                    move_key=f"{idx}_{prefix}_{FirstCycleColumns.MOVE.value}",
                    move_percent_key=f"{idx}_{prefix}_{FirstCycleColumns.MOVE_PERCENT.value}",
                )

                min_key = f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_MIN.value}"
                max_key = f"{idx}_{prefix}_{FirstCycleColumns.CYCLE_MAX.value}"
                max_to_min_key = (
                    f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}"
                )
                update_cycle_min_max(
                    cycle_analysis,
                    adjusted_cycle_data,
                    min_idx,
                    max_idx,
                    cycle_min,
                    cycle_max,
                    min_key=min_key,
                    max_key=max_key,
                )

                update_max_to_min(
                    cycle_analysis,
                    min_key=min_key,
                    max_key=max_key,
                    max_to_min_key=max_to_min_key,
                    is_last_cycle=is_last_cycle,
                )

                if cycle_count_col == SecondCycleIDColumns.MTM_CYCLE_ID.value:

                    avg_min_key = f"{idx}_{prefix}_{FirstCycleColumns.AVERAGE_TILL_MIN.value}"
                    avg_max_key = f"{idx}_{prefix}_{FirstCycleColumns.AVERAGE_TILL_MAX.value}"
                    update_avg_till_min_max(
                        market_direction,
                        cycle_analysis,
                        adjusted_cycle_data,
                        min_idx,
                        max_idx,
                        avg_min_key,
                        avg_max_key,
                    )

                    points_frm_avg_till_max_to_min_key = f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
                    update_pnts_frm_avg_till_max_to_min(
                        market_direction,
                        cycle_analysis,
                        max_key=max_key,
                        min_key=min_key,
                        avg_min_key=avg_min_key,
                        avg_max_key=avg_max_key,
                        points_frm_avg_till_max_to_min_key=points_frm_avg_till_max_to_min_key,
                    )

                update_close_to_close(
                    market_direction,
                    cycle_analysis,
                    close_to_close_key=f"{idx}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}",
                    last_close=adjusted_cycle_data.iloc[-1]["Close"],
                    first_close=adjusted_cycle_data.iloc[0]["Close"],
                )

                # if cycle_count_col == SecondCycleIDColumns.MTM_CYCLE_ID.value:
                # cycle_analysis[
                #     f"{idx}_{prefix}_{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
                # ] = (
                #     1
                #     if cycle_analysis[
                #         f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
                #     ]
                #     > 0
                #     else -1
                # )

                # cycle_analysis[
                #     f"{idx}_{prefix}_{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.MAX_TO_MIN.value}"
                # ] = (
                #     1
                #     if cycle_analysis[
                #         f"{idx}_{prefix}_{FirstCycleColumns.MAX_TO_MIN.value}"
                #     ]
                #     > 0
                #     else -1
                # )

                # cycle_analysis[
                #     f"{idx}_{prefix}_{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
                # ] = (
                #     1
                #     if cycle_analysis[
                #         f"{idx}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
                #     ]
                #     > 0
                #     else -1
                # )

                results.append(cycle_analysis)

                for key, value in cycle_analysis.items():
                    updates[key].append(value)

        for col, values in updates.items():
            if col != "index" and values:
                try:
                    df.loc[updates["index"], col] = values
                except Exception:
                    pass

    # remove unnesasory columns
    remove_cols = [
        "adjusted_close_for_max_to_min",
        "signal_start_price",
        # "adjusted_colse",
    ]
    df.drop(
        columns=remove_cols,
        inplace=True,
        errors="ignore",
    )

    write_dataframe_to_csv(
        df,
        "pa_analysis_output/cycle_outpts/base_df",
        f"base_df_tf_{time_frame}.csv",
    )
    return results


def update_close_to_close(
    market_direction,
    cycle_analysis,
    close_to_close_key,
    last_close,
    first_close,
):
    value = pd.NA
    if market_direction == MarketDirection.LONG:
        value = make_round(last_close - first_close)
    else:
        value = make_round(first_close - last_close)

    cycle_analysis[close_to_close_key] = value


def update_pnts_frm_avg_till_max_to_min(
    market_direction,
    cycle_analysis,
    min_key,
    max_key,
    avg_min_key,
    avg_max_key,
    points_frm_avg_till_max_to_min_key,
):
    value = pd.NA
    if market_direction == MarketDirection.LONG:
        value = make_round(
            make_positive(
                cycle_analysis[max_key] - cycle_analysis[avg_min_key]
            )
        )

    else:
        value = make_round(
            make_positive(
                cycle_analysis[avg_max_key] - cycle_analysis[min_key]
            )
        )

    cycle_analysis[points_frm_avg_till_max_to_min_key] = value


def update_avg_till_min_max(
    market_direction,
    cycle_analysis,
    adjusted_cycle_data,
    min_idx,
    max_idx,
    avg_min_key,
    avg_max_key,
):
    if market_direction == MarketDirection.LONG:
        cycle_analysis[avg_min_key] = make_round(
            adjusted_cycle_data.loc[:min_idx]["Low"].mean()
        )

        cycle_analysis[avg_max_key] = pd.NA

    else:
        cycle_analysis[avg_max_key] = make_round(
            adjusted_cycle_data.loc[:max_idx]["High"].mean()
        )

        cycle_analysis[avg_min_key] = pd.NA


def update_max_to_min(
    cycle_analysis, min_key, max_key, max_to_min_key, is_last_cycle=False
):
    value = make_round(cycle_analysis[max_key] - cycle_analysis[min_key])
    if is_last_cycle:
        value *= -1
    cycle_analysis[max_to_min_key] = value


def update_cycle_min_max(
    cycle_analysis,
    adjusted_cycle_data,
    min_idx,
    max_idx,
    cycle_min,
    cycle_max,
    min_key,
    max_key,
):
    if max_idx:
        cycle_analysis[max_key] = adjusted_cycle_data.loc[max_idx, "High"]
    else:
        cycle_analysis[max_key] = cycle_max

    if min_idx:
        cycle_analysis[min_key] = adjusted_cycle_data.loc[min_idx, "Low"]
    else:
        cycle_analysis[min_key] = cycle_min


def update_second_cycle_id(
    df, id_col_name, end_condition_col, first_cycle_columns
):
    """
    Update cycle ID in the DataFrame based on the specified cycle ID type.

    Parameters:
    df (DataFrame): The DataFrame containing the cycle data.

    """

    for cycle_col in first_cycle_columns:
        id_column_name = f"{cycle_col}_{id_col_name}"
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


def get_min_max_idx(
    cycle_data, group_start_row, group_id, cycle, is_last_cycle=False
):
    min_idx, max_idx, cycle_max, cycle_min = None, None, None, None

    try:
        if is_last_cycle:
            if group_start_row["market_direction"] == MarketDirection.LONG:
                min_idx = cycle_data["Low"].idxmin()
                # here max is avg of start of the cycle to max close price
                cycle_max = cycle_data.loc[cycle_data.index < min_idx][
                    "Close"
                ].mean()
            else:
                max_idx = cycle_data["High"].idxmax()

                # here min is avg of start of the cycle to min close price
                cycle_min = cycle_data.loc[cycle_data.index < max_idx][
                    "Close"
                ].mean()
        else:
            if group_start_row["market_direction"] == MarketDirection.LONG:
                min_idx = cycle_data["Low"].idxmin()
                max_idx = cycle_data.loc[cycle_data.index > min_idx][
                    "High"
                ].idxmax()
            else:
                max_idx = cycle_data["High"].idxmax()
                min_idx = cycle_data.loc[cycle_data.index > max_idx][
                    "Low"
                ].idxmin()

    except ValueError:
        print(
            f"can't find data for min and max while following subsequent calculation rule... for grp: {group_id} cycle no: {cycle}"
        )

    return min_idx, max_idx, cycle_min, cycle_max


def get_next_cycle_first_row(
    group_data, cycle, cycle_col, groups, group_idx, cycle_start=1
):
    """
    Retrieves the first row of the next cycle considering both the current group and the next group.

    Parameters:
    group_data (DataFrame): Data of the current group.
    cycle (int): Current cycle number.
    cycle_col (str): The column name representing the cycle in the data.
    groups (list of tuples): List of groups where each group is a tuple containing group_id and group_data.
    group_idx (int): Index of the current group in the groups list.

    Returns:
    Series: The first row of the next cycle, or None if it doesn't exist.
    """
    # Get the data for the next cycle in the current group
    next_cycle_data = group_data[group_data[cycle_col] == cycle + 1]

    if next_cycle_data.empty:
        # If the next cycle is not found in the current group, check the next group
        if group_idx + 1 < len(groups):
            next_group_id, next_group_data = groups[group_idx + 1]
            next_cycle_data = next_group_data[
                next_group_data[cycle_col] == cycle_start
            ]
            if not next_cycle_data.empty:
                return next_cycle_data.iloc[0]
            # If next cycle in the next group is not found, try the second cycle
            next_next_cycle_data = next_group_data[
                next_group_data[cycle_col] == cycle_start + 1
            ]
            if not next_next_cycle_data.empty:
                return next_next_cycle_data.iloc[0]
    else:
        # Return the first row of the next cycle in the current group
        return next_cycle_data.iloc[0]

    # Return None if no next cycle data is found
    return None


def analyze_cycles(df, time_frame, kwargs):
    # Group by 'group_id'
    groups = list(df.groupby("group_id"))

    results = []
    updates = {col.value: [] for col in FirstCycleColumns}
    updates.update(
        {
            "index": [],
            "group_id": [],
            # "period_band": [], "cycle_no": []
        }
    )
    for group_idx, (group_id, group_data) in enumerate(groups):
        # Identify cycle columns dynamically

        cycle_columns = [
            col for col in group_data.columns if "cycle_no" in col
        ]

        group_start_row = group_data.iloc[0]
        market_direction = group_start_row["market_direction"]
        for cycle_col in cycle_columns:
            # Filter the group_data to get only the valid cycles and find unique cycle numbers
            unique_cycles = group_data.loc[
                group_data[cycle_col] > 0, cycle_col
            ].unique()

            for cycle in unique_cycles:
                cycle_analysis = {
                    "group_id": group_id,
                    # "period_band": cycle_col[-4:],
                    # "cycle_no": cycle,
                }
                cycle_data = group_data[group_data[cycle_col] == cycle]

                # real cycle starts from cycle no 2
                if cycle == 1:
                    continue

                next_cycle_first_row = get_next_cycle_first_row(
                    group_data, cycle, cycle_col, groups, group_idx
                )

                if next_cycle_first_row is not None:
                    # Create a new DataFrame with the current cycle's data and the next cycle's first row
                    adjusted_cycle_data = pd.concat(
                        [cycle_data, next_cycle_first_row.to_frame().T]
                    )
                else:
                    adjusted_cycle_data = cycle_data

                min_idx, max_idx, cycle_min, cycle_max = get_min_max_idx(
                    adjusted_cycle_data, group_start_row, group_id, cycle
                )

                if not min_idx or not max_idx:
                    continue

                updates["index"].append(cycle_data.index[-1])

                update_signal_start_duration(
                    group_start_row,
                    cycle_analysis,
                    cycle_data,
                    signal_start_duration_key=FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value,
                )

                update_cycle_duration(
                    cycle_analysis,
                    cycle_data,
                    cycle_duration_key=FirstCycleColumns.CYCLE_DURATION.value,
                )

                update_move_and_move_percent(
                    group_start_row,
                    cycle_analysis,
                    cycle_data,
                    move_key=FirstCycleColumns.MOVE.value,
                    move_percent_key=FirstCycleColumns.MOVE_PERCENT.value,
                )

                min_key = FirstCycleColumns.CYCLE_MIN.value
                max_key = FirstCycleColumns.CYCLE_MAX.value
                max_to_min_key = FirstCycleColumns.MAX_TO_MIN.value
                update_cycle_min_max(
                    cycle_analysis,
                    adjusted_cycle_data,
                    min_idx,
                    max_idx,
                    cycle_min=cycle_min,
                    cycle_max=cycle_max,
                    min_key=min_key,
                    max_key=max_key,
                )

                update_max_to_min(
                    cycle_analysis,
                    min_key=min_key,
                    max_key=max_key,
                    max_to_min_key=max_to_min_key,
                )

                avg_min_key = FirstCycleColumns.AVERAGE_TILL_MIN.value
                avg_max_key = FirstCycleColumns.AVERAGE_TILL_MAX.value
                update_avg_till_min_max(
                    market_direction=market_direction,
                    cycle_analysis=cycle_analysis,
                    adjusted_cycle_data=adjusted_cycle_data,
                    min_idx=min_idx,
                    max_idx=max_idx,
                    avg_min_key=avg_min_key,
                    avg_max_key=avg_max_key,
                )

                points_frm_avg_till_max_to_min_key = (
                    FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value
                )
                update_pnts_frm_avg_till_max_to_min(
                    market_direction,
                    cycle_analysis,
                    max_key=max_key,
                    min_key=min_key,
                    avg_min_key=avg_min_key,
                    avg_max_key=avg_max_key,
                    points_frm_avg_till_max_to_min_key=points_frm_avg_till_max_to_min_key,
                )

                # close to close
                update_close_to_close(
                    market_direction,
                    cycle_analysis,
                    close_to_close_key=FirstCycleColumns.CLOSE_TO_CLOSE.value,
                    last_close=adjusted_cycle_data.iloc[-1]["Close"],
                    first_close=adjusted_cycle_data.iloc[0]["Close"],
                )

                # cycle_analysis[FirstCycleColumns.DURATION_TO_MAX.value] = (
                #     format_duration(
                #         make_positive(
                #             make_round(
                #                 (
                #                     (
                #                         cycle_data.loc[max_idx, "dt"]
                #                         - cycle_data["dt"].iloc[0]
                #                     ).total_seconds()
                #                     / (3600 * 24)
                #                 )
                #             )
                #         )
                #     )
                # )

                # update_duration_above_BB(
                #     kwargs,
                #     market_direction,
                #     cycle_col,
                #     cycle_analysis,
                #     cycle_data,
                # )

                # signal start to max
                # cycle_analysis[
                #     FirstCycleColumns.SIGNAL_START_TO_MAX_POINTS.value
                # ] = make_positive(
                #     make_round(
                #         group_start_row["signal_start_price"]
                #         - cycle_analysis[FirstCycleColumns.CYCLE_MAX.value]
                #     )
                # )

                # cycle_analysis[
                #     FirstCycleColumns.SIGNAL_START_TO_MAX_PERCENT.value
                # ] = make_round(
                #     make_positive(
                #         cycle_analysis[
                #             FirstCycleColumns.SIGNAL_START_TO_MAX_POINTS.value
                #         ]
                #         / group_start_row["signal_start_price"]
                #         * 100
                #     )
                # )

                # move from start of cycle to max
                # cycle_analysis[
                #     FirstCycleColumns.MOVE_START_TO_MAX_CYCLE_POINTS.value
                # ] = make_round(
                #     cycle_analysis[FirstCycleColumns.CYCLE_MAX.value]
                #     - group_start_row["Close"]
                # )

                # cycle_analysis[
                #     FirstCycleColumns.MOVE_START_TO_MAX_CYCLE_PERCENT.value
                # ] = make_round(
                #     make_positive(
                #         cycle_analysis[
                #             FirstCycleColumns.MOVE_START_TO_MAX_CYCLE_POINTS.value
                #         ]
                #         / group_start_row["Close"]
                #         * 100
                #     )
                # )

                # cycle_analysis[
                #     FirstCycleColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                # ] = make_positive(
                #     make_round(
                #         group_start_row["signal_start_price"]
                #         - cycle_analysis[FirstCycleColumns.CYCLE_MIN.value]
                #     )
                # )

                # cycle_analysis[
                #     FirstCycleColumns.SIGNAL_START_TO_MINIMUM_PERCENT.value
                # ] = make_round(
                #     make_positive(
                #         cycle_analysis[
                #             FirstCycleColumns.SIGNAL_START_TO_MINIMUM_POINTS.value
                #         ]
                #         / group_start_row["signal_start_price"]
                #         * 100
                #     )
                # )

                # if market_direction == MarketDirection.LONG:
                #     cycle_analysis[
                #         FirstCycleColumns.AVERAGE_TILL_MIN.value
                #     ] = make_round(cycle_data.loc[:min_idx]["Low"].mean())
                # else:
                #     cycle_analysis[
                #         FirstCycleColumns.AVERAGE_TILL_MIN.value
                #     ] = make_round(
                #         cycle_data.loc[max_idx + 1 : min_idx]["Low"].mean()
                #     )

                # category
                # if group_start_row["market_direction"] == MarketDirection.LONG:
                #     cycle_analysis[FirstCycleColumns.CATEGORY.value] = (
                #         "Below"
                #         if cycle_analysis[FirstCycleColumns.CYCLE_MIN.value]
                #         > group_start_row["signal_start_price"]
                #         else "Rebound"
                #     )
                # else:
                #     cycle_analysis[FirstCycleColumns.CATEGORY.value] = (
                #         "Rebound"
                #         if cycle_analysis[FirstCycleColumns.CYCLE_MAX.value]
                #         > group_start_row["signal_start_price"]
                #         else "Below"
                #     )

                # cycle_analysis[
                #     FirstCycleColumns.DURATION_BETWEEN_MAX_MIN.value
                # ] = format_duration(
                #     make_positive(
                #         make_round(
                #             (
                #                 (
                #                     cycle_data.loc[max_idx, "dt"]
                #                     - cycle_data.loc[min_idx, "dt"]
                #                 ).total_seconds()
                #             )
                #         )
                #     )
                # )

                # cycle_analysis[
                #     FirstCycleColumns.AVG_OF_MAX_TO_AVG_OF_MIN.value
                # ] = make_round(
                #     make_positive(
                #         cycle_analysis[
                #             FirstCycleColumns.AVERAGE_TILL_MAX.value
                #         ]
                #         - cycle_analysis[
                #             FirstCycleColumns.AVERAGE_TILL_MIN.value
                #         ]
                #     )
                # )

                results.append(cycle_analysis)

                for key, value in cycle_analysis.items():
                    updates[key].append(value)

    for col, values in updates.items():
        if col != "index" and values:
            df.loc[updates["index"], col] = values

    return results


def update_cycle_duration(cycle_analysis, cycle_data, cycle_duration_key):

    cycle_analysis[cycle_duration_key] = format_duration(
        make_round(
            (
                cycle_data.iloc[-1]["dt"] - cycle_data.iloc[0]["dt"]
            ).total_seconds()
        )
    )


def update_signal_start_duration(
    group_start_row, cycle_analysis, cycle_data, signal_start_duration_key
):
    cycle_analysis[signal_start_duration_key] = format_duration(
        make_round(
            (cycle_data.iloc[0]["dt"] - group_start_row["dt"]).total_seconds()
        )
    )


def update_move_and_move_percent(
    group_start_row, cycle_analysis, cycle_data, move_key, move_percent_key
):
    cycle_analysis[move_key] = make_positive(
        make_round(cycle_data.iloc[0]["Close"] - group_start_row["Close"])
    )
    cycle_analysis[move_percent_key] = make_positive(
        make_round(cycle_analysis[move_key] / group_start_row["Close"] * 100)
    )


def update_duration_above_BB(
    kwargs, market_direction, cycle_col, cycle_analysis, cycle_data
):
    # upadte duration first yes to first yes to no change
    # Determine the first_yes_index and first_yes_change_condition based on market direction

    if market_direction == MarketDirection.LONG or not kwargs.get(
        "include_higher_and_lower"
    ):
        first_yes_index = cycle_data[
            cycle_data[f"close_to_{cycle_col[9:]}"] == "YES"
        ]["dt"].iloc[0]

        first_yes_change_condition = (
            cycle_data[f"close_to_{cycle_col[9:]}"] == "YES"
        ) & (cycle_data[f"close_to_{cycle_col[9:]}"].shift(-1) == "NO")

    elif market_direction == MarketDirection.SHORT and kwargs.get(
        "include_higher_and_lower"
    ):
        first_yes_index = cycle_data[
            cycle_data[f"close_to_{cycle_col[9:]}"] == "NO"
        ]["dt"].iloc[0]

        first_yes_change_condition = (
            cycle_data[f"close_to_{cycle_col[9:]}"] == "NO"
        ) & (cycle_data[f"close_to_{cycle_col[9:]}"].shift(-1) == "YES")

    try:
        first_yes_change = cycle_data[first_yes_change_condition]["dt"].iloc[0]
    except IndexError:
        first_yes_change = cycle_data["dt"].iloc[-1]

    cycle_analysis[FirstCycleColumns.DURATION_ABOVE_BB.value] = (
        format_duration(
            make_round(
                (
                    (first_yes_change - first_yes_index).total_seconds()
                    / (3600 * 24)
                )
            )
        )
    )


def get_base_df(kwargs):
    base_df = kwargs.get("base_df")
    start_datetime, end_datetime = (
        base_df.index[0],
        base_df.index[-1],
    )

    (
        files_to_read,
        tf_bb_cols,
    ) = formulate_files_to_read(kwargs)

    max_tf = max(tf_bb_cols.keys())
    adjusted_start_datetime = start_datetime - pd.Timedelta(minutes=max_tf[0])
    dfs = read_files(
        adjusted_start_datetime,
        end_datetime,
        files_to_read,
    )

    bb_time_frames_1_dfs, bb_time_frames_2_dfs, close_time_frames_1_dfs = (
        {},
        {},
        {},
    )
    for key, df in dfs.items():
        tf, type, origin = key
        if type == "close" and origin == 1:
            close_time_frames_1_dfs[(tf, origin)] = df

        if type == "bb" and origin == 1:
            bb_time_frames_1_dfs[(tf, origin)] = df

        if type == "bb" and origin == 2:
            bb_time_frames_2_dfs[(tf, origin)] = df

    # merge bb1 the dataframes
    for key, df in close_time_frames_1_dfs.items():
        df = update_cycle_columns(df, base_df, start_datetime, kwargs)
        update_signal_start_price(df)
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

            write_dataframe_to_csv(
                merged_df,
                "pa_analysis_output/cycle_outpts/base_df",
                f"base_df_tf_{tf}.csv",
            )
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

    return df_to_analyze


def formulate_files_to_read(kwargs):
    instrument = kwargs.get("instrument")
    close_time_frames_1 = kwargs.get("close_time_frames_1")
    bb_time_frames_1 = kwargs.get("bb_time_frames_1")
    bb_time_frames_2 = kwargs.get("bb_time_frames_2")

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
                        col_type=BB_Band_Columns.UPPER.value,
                    )
                )

                tf_bb_cols[(tf, origin)].update(
                    get_bb_cols(
                        kwargs.get(f"periods_{origin}"),
                        kwargs.get(f"sds_{origin}"),
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
                    f"{instrument}_TF_{time_frame}.csv",
                ),
            }
            for time_frame, origin in close_time_frames_with_origin
        }
    )

    return (
        files_to_read,
        rename_dict,
    )


def get_bb_cols(periods, sds, col_type="MEAN"):
    bb_1_cols = {
        f"P_{int(int(period)/20)}_{col_type}_BAND_{period}_{sd}"
        for period in periods
        for sd in sds
    }

    return bb_1_cols


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


def update_cycle_columns(df, base_df, start_datetime, kwargs):
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
            merged_df[bb_2_col].iloc[idx] == "YES" for bb_2_col in bb_2_cols
        )
    return False


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
            (df[f"close_to_{col}"] == "YES")
            & (df["market_direction"] == MarketDirection.LONG)
            & (df[f"close_to_{lower_col}"] == "YES")
            & bb_2_start_condition
        ),
        MarketDirection.SHORT: (
            (df[f"close_to_{col}"] == "NO")
            & (df["market_direction"] == MarketDirection.SHORT)
            & (df[f"close_to_{upper_col}"] == "NO")
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
            (df[f"close_to_{col}"] == "YES")
            & (df["market_direction"] == MarketDirection.LONG)
            & (df[f"close_to_{lower_col}"] == "YES")
        ),
        MarketDirection.SHORT: (
            (df[f"close_to_{col}"] == "NO")
            & (df["market_direction"] == MarketDirection.SHORT)
            & (df[f"close_to_{upper_col}"] == "NO")
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


def update_cycle_number_by_condition(
    df,
    col,
    cycle_start_condition,
    cycle_end_condition,
    id_column_name=None,
    counter_starter=1,
):
    for group, group_df in df.groupby("group_id"):
        # Initialize
        current_cycle = counter_starter
        group_indices = group_df.index
        cycle_counter = pd.Series(0, index=group_indices)
        in_cycle = pd.Series(False, index=group_indices)
        market_direction = group_df["market_direction"].iloc[0]

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
                    in_cycle.loc[start_idx:end_idx] = True
                    cycle_counter.loc[start_idx:end_idx] = current_cycle
                else:
                    in_cycle.loc[start_idx:] = True
                    cycle_counter.loc[start_idx:] = current_cycle

        if id_column_name:
            df.loc[group_indices, id_column_name] = cycle_counter
        else:
            df.loc[group_indices, f"cycle_no_{col}"] = cycle_counter
