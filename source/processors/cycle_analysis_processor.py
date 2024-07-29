import pandas as pd
from source.constants import (
    TARGET_PROFIT_FOLDER,
    FirstCycleColumns,
    MarketDirection,
    TargetProfitColumns,
)
from source.utils import make_round, write_dataframe_to_csv


def get_min_max_idx(
    cycle_data, group_start_row, group_id, cycle=None, is_last_cycle=False
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


def update_max_to_min(**kwargs):
    required_keys = [
        "max_key",
        "min_key",
        "max_to_min_key",
        "cycle_analysis",
        "is_last_cycle",
    ]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            f"Required keys are missing in the kwargs: {required_keys}"
        )

    max_key = kwargs.get("max_key")
    min_key = kwargs.get("min_key")
    max_to_min_key = kwargs.get("max_to_min_key")
    cycle_analysis = kwargs.get("cycle_analysis")
    is_last_cycle = kwargs.get("is_last_cycle", False)

    if cycle_analysis[max_key] is None or cycle_analysis[min_key] is None:
        cycle_analysis[max_to_min_key] = 0
        return
    value = make_round(cycle_analysis[max_key] - cycle_analysis[min_key])
    if is_last_cycle:
        value *= -1
    cycle_analysis[max_to_min_key] = value


def update_cycle_min_max(**kwargs):
    required_keys = [
        "cycle_analysis",
        "adjusted_cycle_data",
        "min_idx",
        "max_idx",
        "cycle_min",
        "cycle_max",
        "min_key",
        "max_key",
    ]

    if not all([key in kwargs for key in required_keys]):
        raise ValueError(
            f"Required keys are missing in the kwargs: {required_keys}"
        )

    max_idx = kwargs.get("max_idx")
    min_idx = kwargs.get("min_idx")
    cycle_min = kwargs.get("cycle_min")
    cycle_max = kwargs.get("cycle_max")
    cycle_analysis = kwargs.get("cycle_analysis")
    adjusted_cycle_data = kwargs.get("adjusted_cycle_data")
    min_key = kwargs.get("min_key")
    max_key = kwargs.get("max_key")

    if max_idx:
        cycle_analysis[max_key] = adjusted_cycle_data.loc[max_idx, "High"]
    else:
        cycle_analysis[max_key] = cycle_max

    if min_idx:
        cycle_analysis[min_key] = adjusted_cycle_data.loc[min_idx, "Low"]
    else:
        cycle_analysis[min_key] = cycle_min


def adj_close_max_to_min(df, kwargs):
    df["adjusted_close_for_max_to_min"] = df["Close"] * (
        kwargs.get("max_to_min_percent") / 100
    )


def update_close_to_close(**kwargs):

    required_keys = [
        "market_direction",
        "cycle_analysis",
        "close_to_close_key",
        "last_close",
        "first_close",
    ]
    if not all(key in kwargs for key in required_keys):
        raise ValueError(
            f"Required keys are missing in the kwargs: {required_keys}"
        )

    market_direction = kwargs.get("market_direction")
    cycle_analysis = kwargs.get("cycle_analysis")
    close_to_close_key = kwargs.get("close_to_close_key")
    last_close = kwargs.get("last_close")
    first_close = kwargs.get("first_close")

    value = pd.NA
    if market_direction == MarketDirection.LONG:
        value = make_round(last_close - first_close)
    else:
        value = make_round(first_close - last_close)

    cycle_analysis[close_to_close_key] = value


def update_MTM_CTC_cols(df, validated_data):

    # need adjusted_close_for_max_to_min column
    adj_close_max_to_min(df, validated_data)

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
                    is_last_cycle=False,
                )

                # close to close
                update_close_to_close(
                    market_direction,
                    cycle_analysis,
                    close_to_close_key=FirstCycleColumns.CLOSE_TO_CLOSE.value,
                    last_close=adjusted_cycle_data.iloc[-1]["Close"],
                    first_close=adjusted_cycle_data.iloc[0]["Close"],
                )

                results.append(cycle_analysis)

                for key, value in cycle_analysis.items():
                    updates[key].append(value)

    for col, values in updates.items():
        if col != "index" and values:
            df.loc[updates["index"], col] = values

    return results


def tp_method_1(df, tp_percent, cycle_col_name, close_col_name):
    # tp_close
    df[TargetProfitColumns.TP_CLOSE.value] = 0.0
    df[TargetProfitColumns.TP_CUM_AVG_CLOSE.value] = 0.0
    df[TargetProfitColumns.TP_CUM_AVG_CLOSE_PERCENT.value] = 0.0

    # groups = df.groupby("group_id")
    # for group_id, group_data in groups:

    df[TargetProfitColumns.TP_CLOSE.value] = df.loc[
        (df[cycle_col_name] > 1) & (df[close_col_name] == "YES"), "Close"
    ]
    # cum avg tp_close
    df[TargetProfitColumns.TP_CUM_AVG_CLOSE.value] = (
        df[TargetProfitColumns.TP_CLOSE.value].expanding().mean()
    )
    # tp_cum_avg_tp_close_percent (depends on market direction)
    df.loc[
        df["market_direction"] == MarketDirection.LONG,
        TargetProfitColumns.TP_CUM_AVG_CLOSE_PERCENT.value,
    ] = df[TargetProfitColumns.TP_CUM_AVG_CLOSE.value] * (
        1 + (tp_percent / 10)
    )

    # Update for 'SHORT' market direction
    df.loc[
        df["market_direction"] == MarketDirection.SHORT,
        TargetProfitColumns.TP_CUM_AVG_CLOSE_PERCENT.value,
    ] = df[TargetProfitColumns.TP_CUM_AVG_CLOSE.value] * (
        1 - (tp_percent / 10)
    )

    # tp_end(yes/no) (depends on market direction)
    df[TargetProfitColumns.TP_END.value] = "NO"
    df.loc[
        (df["market_direction"] == MarketDirection.LONG)
        & (
            df["High"] > df[TargetProfitColumns.TP_CUM_AVG_CLOSE_PERCENT.value]
        ),
        TargetProfitColumns.TP_END.value,
    ] = "YES"

    df.loc[
        (df["market_direction"] == MarketDirection.SHORT)
        & (df["Low"] < df[TargetProfitColumns.TP_CUM_AVG_CLOSE_PERCENT.value]),
        TargetProfitColumns.TP_END.value,
    ] = "YES"


def tp_method_2(df, tp_percent, cycle_col_name, close_col_name):
    df[TargetProfitColumns.TP_CUMMAX.value] = 0.0
    df[TargetProfitColumns.TP_CUMMIN.value] = 0.0
    df[TargetProfitColumns.TP_CLOSE_DIFF_PERCENT.value] = 0.0
    df[TargetProfitColumns.TP_END.value] = "NO"

    groups = df.groupby("group_id")
    for group_id, group_data in groups:
        group_start_row = group_data.iloc[0]
        # for market direction long find cummax
        min_idx, max_idx, cycle_min, cycle_max = get_min_max_idx(
            group_data, group_start_row, group_id
        )

        if group_start_row["market_direction"] == MarketDirection.LONG:

            df.loc[
                (df["group_id"] == group_id) & (df.index > min_idx),
                TargetProfitColumns.TP_CUMMAX.value,
            ] = df.loc[
                (df["group_id"] == group_id) & (df.index > min_idx), "Low"
            ].cummax()

            df.loc[
                (df["group_id"] == group_id) & (df.index > min_idx),
                TargetProfitColumns.TP_CLOSE_DIFF_PERCENT.value,
            ] = (
                df[TargetProfitColumns.TP_CUMMAX.value]
                / group_data.loc[min_idx]["Low"]
                - 1
            ) * 100

            # tp_end(yes/no) (depends on market direction)
            df.loc[
                (df["group_id"] == group_id)
                & (df.index > min_idx)
                & (
                    df[TargetProfitColumns.TP_CLOSE_DIFF_PERCENT.value]
                    > tp_percent
                ),
                TargetProfitColumns.TP_END.value,
            ] = "YES"

        elif group_start_row["market_direction"] == MarketDirection.SHORT:
            # find cummax from max_idx
            df.loc[
                (df["group_id"] == group_id) & (df.index > max_idx),
                TargetProfitColumns.TP_CUMMIN.value,
            ] = df.loc[df.index > max_idx, "High"].cummin()

            df.loc[
                (df["group_id"] == group_id) & (df.index > max_idx),
                TargetProfitColumns.TP_CLOSE_DIFF_PERCENT.value,
            ] = (
                df[TargetProfitColumns.TP_CUMMIN.value]
                / group_data.loc[max_idx]["High"]
                - 1
            ) * 100

            # tp_end(yes/no) (depends on market direction)
            df.loc[
                (df["group_id"] == group_id)
                & (df.index > max_idx)
                & (
                    df[TargetProfitColumns.TP_CLOSE_DIFF_PERCENT.value]
                    > tp_percent
                ),
                TargetProfitColumns.TP_END.value,
            ] = "YES"


def update_target_profit_analysis(
    df, tp_percent, tp_method, cycle_col_name, close_col_name
):
    methods = {"1": tp_method_1, "2": tp_method_2}
    methods[tp_method](df, tp_percent, cycle_col_name, close_col_name)
    write_dataframe_to_csv(df, TARGET_PROFIT_FOLDER, "base_df.csv")
