from functools import reduce
import operator
import pandas as pd
from source.utils import make_round
from volatile_analysis.constants import (
    AnalysisConstant,
    VolatileTag,
)


def cumulative_stddev(df, col, period):
    """Calculate the cumulative standard deviation of a list of numbers."""
    df[f"{period}_{AnalysisConstant.CUM_STD.value}"] = (
        df[col].expanding().std()
    )
    return df


def cumulutaive_avg_volatility(df, col, period):
    """Calculate the cumulative average volatility of a list of numbers."""
    df[f"{period}_{AnalysisConstant.CUM_AVG_VOLATILITY.value}"] = (
        df[col].expanding().mean()
    )
    return df


def z_score(
    df,
    col,
    period,
    cum_std_col=AnalysisConstant.CUM_STD.value,
    cum_avg_volatility_col=AnalysisConstant.CUM_AVG_VOLATILITY.value,
):
    """Calculate the z score of a list of numbers."""
    df[f"{period}_{AnalysisConstant.Z_SCORE.value}"] = make_round(
        (df[cum_avg_volatility_col] - df[col]) / df[cum_std_col]
    ).fillna(0)

    return df


def normalize_column(df, col, new_col, threshold=0.5):
    """Normalize the column of a dataframe."""
    df[new_col] = 0
    df.loc[df[col] > threshold, new_col] = 1
    return df


def trailing_window_sum(
    df, window_size, period, col=AnalysisConstant.NORM_Z_SCORE.value
):
    """Calculate the trailing window sum of a list of numbers."""
    df[f"{period}_{AnalysisConstant.TRAIL_WINDOW_SUM.value}"] = (
        df[col].rolling(window=window_size).sum()
    )
    return df


def trailing_window_avg(
    df, window_size, period, col=AnalysisConstant.TRAIL_WINDOW_SUM.value
):
    """Calculate the trailing window average of a list of numbers."""
    df[f"{period}_{AnalysisConstant.TRAIL_WINDOW_AVG.value}"] = (
        df[col].rolling(window=window_size).mean()
    )
    return df


def update_volatile_tag(
    df,
    lv_threshold,
    hv_threshold,
    col=AnalysisConstant.TRAIL_WINDOW_AVG.value,
    new_col=AnalysisConstant.VOLATILE_TAG.value,
):
    """Update the volatile tag of a dataframe."""

    df[new_col] = pd.NA
    df.loc[df[col] <= lv_threshold, new_col] = VolatileTag.LV.value
    df.loc[df[col] >= hv_threshold, new_col] = VolatileTag.HV.value
    df[new_col] = df[new_col].fillna(method="ffill")
    return df


def update_cycle_id(
    df,
    col=AnalysisConstant.VOLATILE_TAG.value,
    new_col=AnalysisConstant.CYCLE_ID.value,
):
    """Update the cycle id of a dataframe."""

    condition = (df[col] == VolatileTag.LV.value) & (
        df[col].shift(1) != VolatileTag.LV.value
    ) | (df[col] == VolatileTag.HV.value) & (
        df[col].shift(1) != VolatileTag.HV.value
    )

    df[new_col] = 0
    df.loc[condition, new_col] = 1
    df[new_col] = df[new_col].cumsum()
    return df


def update_cycle_id_multi_tag(
    df,
    cols,
    col=AnalysisConstant.VOLATILE_TAG.value,
    new_col=AnalysisConstant.CYCLE_ID.value,
):
    """Update the cycle id of a dataframe."""

    # make condition
    start_condition = reduce(
        operator.and_, (df[cols[0]] == df[col] for col in cols[1:])
    )

    end_condition = reduce(
        operator.or_, (df[cols[0]] != df[col] for col in cols[1:])
    )

    df_indices = df.index
    start_indices = df_indices[start_condition]
    end_indices = df_indices[end_condition]

    df = updated_cycle_id_by_start_end(start_indices, end_indices, df, new_col)
    return df


def updated_cycle_id_by_start_end(
    start_indices, end_indices, df, new_col, counter=0
):
    """Update the cycle id of a dataframe based on start and end conditions."""
    in_cycle = pd.Series(False, index=df.index)
    cycle_counter = pd.Series(0, index=df.index)

    for start_idx in start_indices:
        if not in_cycle[start_idx]:
            counter += 1
            end_idx = end_indices[end_indices > start_idx].min()
            if not pd.isna(end_idx):
                in_cycle.loc[start_idx:end_idx] = True
                cycle_counter.loc[start_idx:end_idx] = counter
            else:
                in_cycle.loc[start_idx:] = True
                cycle_counter.loc[start_idx:] = counter

    df[new_col] = cycle_counter
    return df


def get_first_tag(df, col):
    first_tag_index = (
        df[col].isin([VolatileTag.HV.value, VolatileTag.LV.value]).idxmax()
    )
    first_tag = df.loc[first_tag_index, col]
    return first_tag


def update_group_id(
    df,
    col=AnalysisConstant.VOLATILE_TAG.value,
    new_col=AnalysisConstant.GROUP_ID.value,
):
    pd.set_option("display.max_rows", None)

    # Define the condition for tagging group changes
    condition = (
        (df[col] == VolatileTag.HV.value)
        & (df[col].shift(1) == VolatileTag.LV.value)
    ) | (
        (df[col] == VolatileTag.LV.value)
        & (df[col].shift(1) == VolatileTag.HV.value)
    )

    # Skip initial NaN values
    first_valid_index = df[col].first_valid_index()

    # Initialize GROUP_ID with 0
    df[new_col] = 0

    if first_valid_index is not None:
        # Apply the condition starting from the first valid index
        condition = condition[first_valid_index:]

        # Mark the groups based on the condition
        df.loc[first_valid_index:, new_col] = condition.cumsum() + 1

    return df


def get_group_duration(group_data):
    """Calculate the cycle duration of a list of numbers."""
    duration = group_data.index[-1] - group_data.index[0]
    return duration


def close_to_close(data):
    """Calculate the close to close of a list of numbers."""
    pass


# pd.set_option("display.max_rows", None)
