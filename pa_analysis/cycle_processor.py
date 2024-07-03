import pandas as pd

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


def process_cycles(**kwargs):
    # get the base df
    all_df = get_cycle_base_df(**kwargs)

    # process the data
    for time_frame, df in all_df.items():
        results = analyze_cycles(df, time_frame, kwargs)

        # max to min percent
        update_max_to_min_percent(df, kwargs)

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
            second_cycle_columns=secondary_cycle_columns,
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
            output_dir=PA_ANALYSIS_CYCLE_FOLDER,
            csv_filename=f"result_tf_{time_frame}.csv",
        )


def update_max_to_min_percent(df, kwargs):
    adj_close_max_to_min(df, kwargs)

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
                    f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{idx}_{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}": [],
                    f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{idx}_{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}": [],
                }
            )

        if cycle_count_col == SecondCycleIDColumns.CTC_CYCLE_ID.value:
            updates.update(
                {
                    f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{idx}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}": [],
                }
            )

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

                if cycle_count_col == SecondCycleIDColumns.MTM_CYCLE_ID.value:
                    update_positive_negative(
                        cycle_analysis=cycle_analysis,
                        columns=[
                            f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FROM_MAX.value}",
                            f"{idx}_{prefix}_{FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}",
                        ],
                    )

                if cycle_count_col == SecondCycleIDColumns.CTC_CYCLE_ID.value:
                    update_positive_negative(
                        cycle_analysis=cycle_analysis,
                        columns=[
                            f"{idx}_{prefix}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}",
                        ],
                    )

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
        PA_ANALYSIS_CYCLE_FOLDER,
        f"base_df_tf_{time_frame}.csv",
    )
    return results


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


def analyze_cycles(df, time_frame, kwargs):
    # Group by 'group_id'
    groups = list(df.groupby("group_id"))

    results = []
    updates = {col.value: [] for col in FirstCycleColumns}
    updates.update(
        {
            "index": [],
            "group_id": [],
            f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{FirstCycleColumns.CLOSE_TO_CLOSE.value}": [],
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

                update_positive_negative(
                    cycle_analysis,
                    columns=[FirstCycleColumns.CLOSE_TO_CLOSE.value],
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


def update_positive_negative(cycle_analysis, columns):
    for col in columns:
        cycle_analysis[
            f"{FirstCycleColumns.POSITIVE_NEGATIVE.value}_{col}"
        ] = (1 if cycle_analysis[col] > 0 else 0)


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
