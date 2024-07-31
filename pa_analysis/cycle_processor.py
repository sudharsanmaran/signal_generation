from collections import defaultdict
from datetime import timedelta
from functools import partial
from typing import List
import pandas as pd
from pandas.tseries.offsets import BDay

from pa_analysis.constants import (
    MTMCrossedCycleColumns,
)
from source.processors.cycle_analysis_processor import (
    adj_close_max_to_min,
    get_min_max_idx,
    get_next_cycle_first_row,
    update_close_to_close,
    update_cycle_min_max,
    update_max_to_min,
)
from source.processors.cycle_trade_processor import (
    get_cycle_base_df,
    get_fractal_count_columns,
    get_fractal_cycle_columns,
    update_fractal_counter,
    update_fractal_counter_1,
    update_fractal_cycle_id,
    update_second_cycle_id,
)
from source.utils import (
    format_duration,
    make_positive,
    make_round,
    write_dict_to_csv,
)
from source.constants import (
    PA_ANALYSIS_CYCLE_FOLDER,
    FirstCycleColumns,
    MarketDirection,
    SecondCycleIDColumns,
)
from source.processors.signal_trade_processor import write_dataframe_to_csv


def update_growth_percent_fractal_count(df, kwargs):
    """
    Update the growth percent for the fractal count
    """
    # df["long_fractal_count_growth_percent"] = pd.NA
    # df["short_fractal_count_growth_percent"] = pd.NA
    fractal_count_columns = get_fractal_count_columns(
        fractal_sd=kwargs["fractal_count_sd"]
    )
    for col in fractal_count_columns:
        df[f"growth_percent_{col}"] = pd.NA
        fractal_count_mask = df[f"count_{col}"] > 0

        # Get the close value of the first row where the mask is True
        first_close_value = df.loc[fractal_count_mask, "Close"].iloc[0]

        # Calculate the growth percent
        df.loc[fractal_count_mask, f"growth_percent_{col}"] = make_round(
            (
                (df.loc[fractal_count_mask, "Close"] - first_close_value)
                / first_close_value
            )
            * 100
        )


def process_cycles(**kwargs):
    # get the base df
    all_df = get_cycle_base_df(**kwargs)

    # process the data
    for time_frame, df in all_df.items():

        results = analyze_cycles(df, time_frame, kwargs)

        # max to min percent
        update_max_to_min_percent(df, kwargs)

        if kwargs.get("fractal_cycle") or kwargs.get("fractal_count"):
            bb_cycle_column = [
                col for col in df.columns if "cycle_no_" in col
            ][0]
        # update fractal count for fracatl cylce for skip initial fractals
        if kwargs.get("fractal_cycle"):
            fractal_cycle_columns = get_fractal_cycle_columns(
                fractal_sd=kwargs["fractal_sd"]
            )

            update_fractal_counter(
                df, fractal_cycle_columns, group_by_col=bb_cycle_column
            )
            # update fractal count cycle
            update_fractal_cycle_id(
                kwargs,
                df,
                bb_cycle_col=bb_cycle_column,
                end_condition_col="adjusted_close_for_max_to_min",
            )

        if kwargs.get("fractal_cycle") and kwargs.get("fractal_count"):
            fractal_count_columns = get_fractal_count_columns(
                fractal_sd=kwargs["fractal_count_sd"]
            )

            update_fractal_counter_1(
                df,
                fractal_count_columns,
                group_by_col=[
                    "group_id",
                    bb_cycle_column,
                    SecondCycleIDColumns.FRACTAL_CYCLE_ID.value,
                ],
                condition=df[SecondCycleIDColumns.FRACTAL_CYCLE_ID.value] > 0,
                skip_count=kwargs["fractal_count_skip"],
            )

            # update fractal count growth percent
            update_growth_percent_fractal_count(df, kwargs)

            update_secondary_cycle_analytics(
                df,
                results,
                time_frame=time_frame,
                prefix="FRACTAL",
                cycle_count_col=SecondCycleIDColumns.FRACTAL_CYCLE_ID.value,
                analytics_needed=[
                    FirstCycleColumns.CYCLE_DURATION.value,
                    FirstCycleColumns.CYCLE_MAX.value,
                    FirstCycleColumns.CYCLE_MIN.value,
                    FirstCycleColumns.POINTS_FROM_MAX.value,
                    FirstCycleColumns.CLOSE_TO_CLOSE.value,
                    FirstCycleColumns.AVERAGE_TILL_MAX.value,
                    FirstCycleColumns.AVERAGE_TILL_MIN.value,
                    FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value,
                    FirstCycleColumns.POINTS_FROM_MAX_TO_CLOSE_PERCENT.value,
                    FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value,
                    FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value,
                ],
            )

        first_cycle_columns = [col for col in df.columns if "cycle_no_" in col]

        secondary_cycle_columns = []
        # mtm cycle
        update_second_cycle_id(
            df,
            id_col_name=SecondCycleIDColumns.MTM_CYCLE_ID.value,
            end_condition_col="adjusted_close_for_max_to_min",
            first_cycle_columns=first_cycle_columns,
            second_cycle_columns=secondary_cycle_columns,
        )
        update_secondary_cycle_analytics(
            df,
            results,
            time_frame,
            prefix="MTM",
            cycle_count_col=SecondCycleIDColumns.MTM_CYCLE_ID.value,
            analytics_needed=[
                FirstCycleColumns.CYCLE_DURATION.value,
                FirstCycleColumns.CYCLE_MAX.value,
                FirstCycleColumns.CYCLE_MIN.value,
                FirstCycleColumns.POINTS_FROM_MAX.value,
                FirstCycleColumns.CLOSE_TO_CLOSE.value,
                FirstCycleColumns.AVERAGE_TILL_MAX.value,
                FirstCycleColumns.AVERAGE_TILL_MIN.value,
                FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value,
                FirstCycleColumns.POINTS_FROM_MAX_TO_CLOSE_PERCENT.value,
                FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value,
                FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value,
            ],
            positive_negative_keys=[
                FirstCycleColumns.POINTS_FROM_MAX.value,
                FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value,
            ],
        )

        # ctc cycle
        update_second_cycle_id(
            df,
            id_col_name=SecondCycleIDColumns.CTC_CYCLE_ID.value,
            end_condition_col="adjusted_colse",
            first_cycle_columns=first_cycle_columns,
            second_cycle_columns=secondary_cycle_columns,
        )
        update_secondary_cycle_analytics(
            df,
            results,
            time_frame,
            prefix="CTC",
            cycle_count_col=SecondCycleIDColumns.CTC_CYCLE_ID.value,
            analytics_needed=[
                FirstCycleColumns.CYCLE_MAX.value,
                FirstCycleColumns.CYCLE_MIN.value,
                FirstCycleColumns.POINTS_FROM_MAX.value,
                FirstCycleColumns.CLOSE_TO_CLOSE.value,
            ],
            positive_negative_keys=[FirstCycleColumns.CLOSE_TO_CLOSE.value],
        )

        headers = results[0].keys()
        write_dict_to_csv(
            results,
            main_header=headers,
            output_dir=PA_ANALYSIS_CYCLE_FOLDER,
            csv_filename=f"result_tf_{time_frame}.csv",
        )


def update_max_to_min_percent(df, kwargs):
    adj_close_max_to_min(df, kwargs)

    columns = {
        MTMCrossedCycleColumns.IS_MTM_CROSS_PNT_5.value: 0.5,
        MTMCrossedCycleColumns.IS_MTM_CROSS_1.value: 1,
        MTMCrossedCycleColumns.IS_MTM_CROSS_2.value: 2,
        MTMCrossedCycleColumns.IS_MTM_CROSS_3.value: 3,
        MTMCrossedCycleColumns.IS_MTM_CROSS_4.value: 4,
        MTMCrossedCycleColumns.IS_MTM_CROSS_5.value: 5,
        MTMCrossedCycleColumns.IS_MTM_CROSS_6.value: 6,
        MTMCrossedCycleColumns.IS_MTM_CROSS_7.value: 7,
        MTMCrossedCycleColumns.IS_MTM_CROSS_8.value: 8,
    }

    for col, val in columns.items():
        df[col] = "NO"
        df.loc[
            df[FirstCycleColumns.MAX_TO_MIN.value] > df["Close"] * (val / 100),
            col,
        ] = "YES"


def update_secondary_cycle_analytics(
    df,
    results: List,
    time_frame,
    prefix="CTC",
    cycle_count_col=SecondCycleIDColumns.CTC_CYCLE_ID.value,
    analytics_needed: List = [],
    positive_negative_keys: List = [],
):

    groups = list(df.groupby("group_id"))
    cycle_columns = [col for col in df.columns if cycle_count_col in col]

    # todo
    #  1. use partial to pass the required arguments to the update functions
    # Define the mapping of keys to their respective update functions
    update_functions = {
        FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value: update_signal_start_duration,
        FirstCycleColumns.CYCLE_DURATION.value: update_cycle_duration,
        FirstCycleColumns.MOVE.value: update_move,
        FirstCycleColumns.MOVE_PERCENT.value: update_move_percent,
        FirstCycleColumns.CYCLE_MAX.value: update_cycle_min_max,
        FirstCycleColumns.CYCLE_MIN.value: update_cycle_min_max,
        FirstCycleColumns.POINTS_FROM_MAX.value: update_max_to_min,
        FirstCycleColumns.CLOSE_TO_CLOSE.value: update_close_to_close,
        FirstCycleColumns.AVERAGE_TILL_MAX.value: update_avg_till_min_max,
        FirstCycleColumns.AVERAGE_TILL_MIN.value: update_avg_till_min_max,
        FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value: update_pnts_frm_avg_till_max_to_min,
        FirstCycleColumns.POINTS_FROM_MAX_TO_CLOSE_PERCENT.value: partial(
            update_percent_with_grp_start_close,
            percent_col=f"{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}",
            percent_key=FirstCycleColumns.POINTS_FROM_MAX_TO_CLOSE_PERCENT.value,
        ),
        FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value: partial(
            update_percent_with_grp_start_close,
            percent_col=f"{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}",
            percent_key=FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value,
        ),
        FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value: partial(
            update_percent_with_grp_start_close,
            percent_col=f"{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}",
            percent_key=FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value,
        ),
    }

    analytics_dict = defaultdict(list)
    for cycle_col in cycle_columns:

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

                adjusted_cycle_data = (
                    pd.concat([cycle_data, next_cycle_first_row.to_frame().T])
                    if not cycle_data.empty
                    and next_cycle_first_row is not None
                    else cycle_data
                )

                is_last_cycle = (
                    cycle == unique_cycles[-1]
                    if cycle_count_col
                    == SecondCycleIDColumns.MTM_CYCLE_ID.value
                    else False
                )

                min_idx, max_idx, cycle_min, cycle_max = get_min_max_idx(
                    adjusted_cycle_data,
                    group_start_row,
                    group_id,
                    cycle,
                    is_last_cycle,
                )

                analytics_dict["index"].append(cycle_data.index[-1])

                # Dynamically call update functions based on the keys in the updates dictionary
                for key in analytics_needed:
                    # keys
                    key = f"{prefix}_{key}"
                    cycle_duration_key = (
                        f"{prefix}_{FirstCycleColumns.CYCLE_DURATION.value}"
                    )
                    signal_start_duration_key = f"{prefix}_{FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value}"
                    min_key = f"{prefix}_{FirstCycleColumns.CYCLE_MIN.value}"
                    max_key = f"{prefix}_{FirstCycleColumns.CYCLE_MAX.value}"
                    max_to_min_key = (
                        f"{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}"
                    )
                    close_to_close_key = (
                        f"{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
                    )
                    avg_min_key = (
                        f"{prefix}_{FirstCycleColumns.AVERAGE_TILL_MIN.value}"
                    )
                    avg_max_key = (
                        f"{prefix}_{FirstCycleColumns.AVERAGE_TILL_MAX.value}"
                    )

                    points_frm_avg_till_max_to_min_key = f"{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"

                    # get the update function
                    func = update_functions.get(key.split("_")[-1], None)

                    if func:
                        func(
                            group_start_row=group_start_row,
                            cycle_analysis=cycle_analysis,
                            cycle_data=cycle_data,
                            signal_start_duration_key=signal_start_duration_key,
                            key=key,
                            adjusted_cycle_data=adjusted_cycle_data,
                            min_idx=min_idx,
                            max_idx=max_idx,
                            cycle_min=cycle_min,
                            cycle_max=cycle_max,
                            market_direction=market_direction,
                            min_key=min_key,
                            max_key=max_key,
                            max_to_min_key=max_to_min_key,
                            close_to_close_key=close_to_close_key,
                            last_close=adjusted_cycle_data.iloc[-1]["Close"],
                            first_close=adjusted_cycle_data.iloc[0]["Close"],
                            avg_min_key=avg_min_key,
                            avg_max_key=avg_max_key,
                            points_frm_avg_till_max_to_min_key=points_frm_avg_till_max_to_min_key,
                            is_last_cycle=is_last_cycle,
                            cycle_duration_key=cycle_duration_key,
                            group_start_close=group_start_row["Close"],
                        )
                    else:
                        raise NotImplementedError(
                            f"Function for key {key} not implemented"
                        )

                for key in positive_negative_keys:
                    key = f"{prefix}_{key}"
                    update_positive_negative(
                        cycle_analysis=cycle_analysis,
                        columns=[key],
                    )

                for key, value in cycle_analysis.items():
                    analytics_dict[key].append(value)

                results.append(cycle_analysis)

        for col, values in analytics_dict.items():
            if col != "index" and values:
                try:
                    df.loc[analytics_dict["index"], col] = values
                except Exception:
                    pass

    if cycle_count_col == SecondCycleIDColumns.CTC_CYCLE_ID.value:
        update_cumulative_avg(
            df,
            cols=[
                f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
            ],
        )

    # Remove unnecessary columns
    cols = [
        "signal_start_price",
        # "adjusted_close",
    ]
    remove_cols(df, cols=cols)

    write_dataframe_to_csv(
        df,
        PA_ANALYSIS_CYCLE_FOLDER,
        f"base_df_tf_{time_frame}.csv",
    )
    return results


def remove_cols(df, cols):

    df.drop(
        columns=cols,
        inplace=True,
        errors="ignore",
    )


def update_pnts_frm_avg_till_max_to_min(**kwargs):

    required_keys = [
        "market_direction",
        "cycle_analysis",
        "points_frm_avg_till_max_to_min_key",
        "is_last_cycle",
    ]

    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            f"Required keys missing. Required keys are {required_keys}"
        )

    market_direction = kwargs["market_direction"]
    cycle_analysis = kwargs["cycle_analysis"]
    min_key = kwargs.get("min_key")
    max_key = kwargs.get("max_key")
    avg_min_key = kwargs.get("avg_min_key")
    avg_max_key = kwargs.get("avg_max_key")
    points_frm_avg_till_max_to_min_key = kwargs[
        "points_frm_avg_till_max_to_min_key"
    ]
    is_last_cycle = kwargs.get("is_last_cycle", False)

    value = pd.NA
    if cycle_analysis[max_key] is None or cycle_analysis[min_key] is None:
        cycle_analysis[points_frm_avg_till_max_to_min_key] = value
        return

    if market_direction == MarketDirection.LONG:
        value = make_round(
            cycle_analysis[max_key] - cycle_analysis[avg_min_key]
        )

    else:
        value = make_round(
            cycle_analysis[avg_max_key] - cycle_analysis[min_key]
        )

    if is_last_cycle:
        value *= -1
    cycle_analysis[points_frm_avg_till_max_to_min_key] = value


def update_avg_till_min_max(**kwargs):
    required_keys = [
        "market_direction",
        "avg_min_key",
        "avg_max_key",
    ]

    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            f"Required keys missing. Required keys are {required_keys}"
        )

    market_direction = kwargs["market_direction"]
    adjusted_cycle_data = kwargs["adjusted_cycle_data"]
    min_idx = kwargs.get("min_idx")
    max_idx = kwargs.get("max_idx")
    avg_min_key = kwargs.get("avg_min_key")
    avg_max_key = kwargs.get("avg_max_key")
    cycle_analysis = kwargs["cycle_analysis"]

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


def analyze_cycles(df, time_frame, kwargs):
    # Group by 'group_id'
    groups = list(df.groupby("group_id"))

    results = []
    updates = defaultdict(list)

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

                updates["index"].append(cycle_data.index[-1])

                update_signal_start_duration(
                    group_start_row=group_start_row,
                    cycle_analysis=cycle_analysis,
                    cycle_data=cycle_data,
                    signal_start_duration_key=FirstCycleColumns.DURATION_SIGNAL_START_TO_CYCLE_START.value,
                )

                update_cycle_duration(
                    cycle_analysis=cycle_analysis,
                    cycle_data=cycle_data,
                    cycle_duration_key=FirstCycleColumns.CYCLE_DURATION.value,
                )

                update_move(
                    group_start_row=group_start_row,
                    cycle_analysis=cycle_analysis,
                    cycle_data=cycle_data,
                    move_key=FirstCycleColumns.MOVE.value,
                )

                update_move_percent(
                    group_start_row=group_start_row,
                    cycle_analysis=cycle_analysis,
                    cycle_data=cycle_data,
                    move_key=FirstCycleColumns.MOVE.value,
                    move_percent_key=FirstCycleColumns.MOVE_PERCENT.value,
                )

                min_key = FirstCycleColumns.CYCLE_MIN.value
                max_key = FirstCycleColumns.CYCLE_MAX.value
                max_to_min_key = FirstCycleColumns.MAX_TO_MIN.value
                update_cycle_min_max(
                    cycle_analysis=cycle_analysis,
                    adjusted_cycle_data=adjusted_cycle_data,
                    min_idx=min_idx,
                    max_idx=max_idx,
                    cycle_min=cycle_min,
                    cycle_max=cycle_max,
                    min_key=min_key,
                    max_key=max_key,
                )

                update_max_to_min(
                    cycle_analysis=cycle_analysis,
                    min_key=min_key,
                    max_key=max_key,
                    max_to_min_key=max_to_min_key,
                    is_last_cycle=False,
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
                    market_direction=market_direction,
                    cycle_analysis=cycle_analysis,
                    max_key=max_key,
                    min_key=min_key,
                    avg_min_key=avg_min_key,
                    avg_max_key=avg_max_key,
                    points_frm_avg_till_max_to_min_key=points_frm_avg_till_max_to_min_key,
                    is_last_cycle=False,
                )

                # close to close
                update_close_to_close(
                    market_direction=market_direction,
                    cycle_analysis=cycle_analysis,
                    close_to_close_key=FirstCycleColumns.CLOSE_TO_CLOSE.value,
                    last_close=adjusted_cycle_data.iloc[-1]["Close"],
                    first_close=adjusted_cycle_data.iloc[0]["Close"],
                )

                update_positive_negative(
                    cycle_analysis=cycle_analysis,
                    columns=[FirstCycleColumns.CLOSE_TO_CLOSE.value],
                )

                group_start_close = group_start_row["Close"]
                update_percent_with_grp_start_close(
                    cycle_analysis=cycle_analysis,
                    percent_col=FirstCycleColumns.CLOSE_TO_CLOSE.value,
                    percent_key=FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value,
                    group_start_close=group_start_close,
                )

                update_percent_with_grp_start_close(
                    cycle_analysis=cycle_analysis,
                    percent_col=FirstCycleColumns.MAX_TO_MIN.value,
                    percent_key=FirstCycleColumns.MAX_TO_MIN_TO_CLOSE_PERCENT.value,
                    group_start_close=group_start_close,
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

    # update_running_avg(
    #     cycle_analysis,
    #     columns=[FirstCycleColumns.CLOSE_TO_CLOSE.value],
    # )

    # update running avg of f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"

    for col, values in updates.items():
        if col != "index" and values:
            df.loc[updates["index"], col] = values

    update_cumulative_avg(
        df,
        cols=[
            f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
        ],
    )

    rolling_avg = {
        FirstCycleColumns.ROLLING_AVG_3.value: timedelta(days=90),
        FirstCycleColumns.ROLLING_AVG_6.value: timedelta(days=180),
    }
    update_rolling_averages(df, time_frame, rolling_avg)

    # trail_dates = {
    #     FirstCycleColumns.TRAILLING_30_DAYS.value: timedelta(days=30),
    #     FirstCycleColumns.TRAILLING_90_DAYS.value: timedelta(days=90),
    #     FirstCycleColumns.TRAILLING_180_DAYS.value: timedelta(days=180),
    #     FirstCycleColumns.TRAILLING_270_DAYS.value: timedelta(days=270),
    #     FirstCycleColumns.TRAILLING_365_DAYS.value: timedelta(days=365),
    # }
    # update_trail_date_close(df, time_frame, trail_dates)
    # update_trail_return(df, trail_dates)

    # key = f"{FirstCycleColumns.TRAILLING_365_DAYS.value}_return"
    # filtered_df = df.loc[df[key].notnull()]
    # update_cumulative_standard_dev(filtered_df, key=key)
    # df[f"Cumulative Std Dev {key}"] = filtered_df[f"Cumulative Std Dev {key}"]
    # update_cumulative_avg(filtered_df, cols=[key])
    # df[f"{FirstCycleColumns.CUM_AVG.value}_{key}"] = filtered_df[
    #     f"{FirstCycleColumns.CUM_AVG.value}_{key}"
    # ]

    # calculate_z_score(df)

    return results


def update_percent_with_grp_start_close(**kwargs):
    required_keys = [
        "cycle_analysis",
        "group_start_close",
        "percent_col",
        "percent_key",
    ]
    if not all([key in kwargs for key in required_keys]):
        raise ValueError(
            f"Required keys missing. Required keys are {required_keys}"
        )

    cycle_analysis = kwargs["cycle_analysis"]
    group_start_close = kwargs["group_start_close"]
    percent_col = kwargs["percent_col"]
    percent_key = kwargs["percent_key"]

    cycle_analysis[percent_key] = make_round(
        (cycle_analysis[percent_col] / group_start_close) * 100
    )


def calculate_z_score(df):
    df[FirstCycleColumns.Z_SCORE.value] = make_round(
        (
            df[f"{FirstCycleColumns.TRAILLING_365_DAYS.value}_return"]
            - df[
                f"{FirstCycleColumns.CUM_AVG.value}_{FirstCycleColumns.TRAILLING_365_DAYS.value}_return"
            ]
        )
        / df[
            f"Cumulative Std Dev {FirstCycleColumns.TRAILLING_365_DAYS.value}_return"
        ]
    )


def update_trail_return(df, trail_dates):
    for key in trail_dates:
        df[f"{key}_return"] = make_round(
            (df["Close"] / df[f"{key}_Close"] - 1) * 100
        )

        # remove_cols(df, cols=[f"{key}_Close"])


def prev_weekday(datetime):
    pre_weekday = None
    if datetime.weekday() < 5:
        pre_weekday = datetime
    else:
        if datetime.weekday() == 5:
            pre_weekday = datetime - BDay(1)
        else:
            pre_weekday = datetime - BDay(2)
    return pre_weekday


def update_trail_date_close(df, time_frame, trail_dates):
    df_copy = df.copy()
    df_copy.set_index("dt", inplace=True)

    for key, value in trail_dates.items():
        update_trailing_date(
            df_copy, key, value + timedelta(minutes=int(time_frame))
        )

        df_copy[f"{key}_Close"] = df_copy[key].map(df_copy["Close"])

        # df_copy[f"{key}_Close"].fillna(0, inplace=True)

        df[f"{key}_date"] = df_copy[key].values

        df[f"{key}_Close"] = df_copy[f"{key}_Close"].values


def update_trailing_date(df_copy, col_name, time_delta):
    # Calculate the new date by subtracting time_delta from the index
    df_copy[col_name] = df_copy.index - time_delta

    # Find the previous index that is present in the DataFrame
    def find_previous_index(date):
        # Get all available indices that are before the given date
        valid_indices = df_copy.index[df_copy.index <= date]
        if valid_indices.empty:
            return pd.NaT
        else:
            return valid_indices[-1]

    # Apply the function to find the previous present index
    df_copy[col_name] = df_copy[col_name].apply(find_previous_index)

    # Any date that is not in the index will be converted to NaT
    df_copy[col_name] = df_copy[col_name].where(
        df_copy[col_name].isin(df_copy.index), pd.NaT
    )

    return df_copy


def update_rolling_averages(df, time_frame, rolling_avg):
    df_copy = df.copy()
    df_copy.set_index("dt", inplace=True)
    for key, value in rolling_avg.items():
        update_rolling_avg_for_CTC(
            df_copy,
            time_delta=value - timedelta(minutes=int(time_frame)),
            col_name=key,
        )

        df[key] = df_copy[key].values


def update_rolling_avg_for_CTC(df, time_delta, col_name, min_periods=0):

    df[col_name] = make_round(
        df[
            f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}"
        ]
        .rolling(time_delta, min_periods=min_periods)
        .mean()
    )


def update_cumulative_avg(df, cols):
    for col in cols:
        df[f"{FirstCycleColumns.CUM_AVG.value}_{col}"] = make_round(
            df[col].expanding().mean()
        )


def update_positive_negative(**kwargs):
    required_keys = ["cycle_analysis", "columns"]
    if not all([key in kwargs for key in required_keys]):
        raise ValueError(
            f"Required keys missing. Required keys are {required_keys}"
        )

    cycle_analysis = kwargs["cycle_analysis"]
    columns = kwargs["columns"]

    for col in columns:
        cycle_analysis[
            f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{col}"
        ] = (1 if cycle_analysis[col] > 0 else 0)


def update_cycle_duration(**kwargs):

    required_keys = ["cycle_analysis", "cycle_data", "cycle_duration_key"]
    if not all([key in kwargs for key in required_keys]):
        raise ValueError(f"Missing required keys: {required_keys}")

    cycle_analysis = kwargs["cycle_analysis"]
    cycle_data = kwargs["cycle_data"]
    cycle_duration_key = kwargs["cycle_duration_key"]

    cycle_analysis[cycle_duration_key] = format_duration(
        make_round(
            (
                cycle_data.iloc[-1]["dt"] - cycle_data.iloc[0]["dt"]
            ).total_seconds()
        )
    )


def update_signal_start_duration(**kwargs):
    # group_start_row, cycle_analysis, cycle_data, key
    required_keys = [
        "group_start_row",
        "cycle_analysis",
        "cycle_data",
        "signal_start_duration_key",
    ]

    if not all(key in kwargs for key in required_keys):
        raise ValueError(f"Missing required keys: {required_keys}")

    cycle_analysis = kwargs["cycle_analysis"]
    cycle_data = kwargs["cycle_data"]
    group_start_row = kwargs["group_start_row"]
    key = kwargs["signal_start_duration_key"]

    cycle_analysis[key] = format_duration(
        make_round(
            (cycle_data.iloc[0]["dt"] - group_start_row["dt"]).total_seconds()
        )
    )


def update_move(**kwargs):
    required_keys = [
        "group_start_row",
        "cycle_analysis",
        "cycle_data",
        "move_key",
    ]

    if not all([key in kwargs for key in required_keys]):
        raise ValueError(f"Missing required keys: {required_keys}")

    group_start_row = kwargs["group_start_row"]
    cycle_analysis = kwargs["cycle_analysis"]
    cycle_data = kwargs["cycle_data"]
    move_key = kwargs["move_key"]

    cycle_analysis[move_key] = make_positive(
        make_round(cycle_data.iloc[0]["Close"] - group_start_row["Close"])
    )


def update_move_percent(**kwargs):
    required_keys = [
        "group_start_row",
        "cycle_analysis",
        "move_percent_key",
        "move_key",
    ]

    if not all([key in kwargs for key in required_keys]):
        raise ValueError(f"Missing required keys: {required_keys}")

    group_start_row = kwargs["group_start_row"]
    cycle_analysis = kwargs["cycle_analysis"]
    move_key = kwargs["move_key"]
    move_percent_key = kwargs["move_percent_key"]

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


def update_cumulative_standard_dev(
    df: pd.DataFrame,
    key: str = f"{FirstCycleColumns.TRAILLING_365_DAYS.value}_return",
):
    """Calculate the cumulative standard deviation"""
    df[f"Cumulative Std Dev {key}"] = df[key].expanding().std()
