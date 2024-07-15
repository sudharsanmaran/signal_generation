import pandas as pd
from source.utils import make_round
from volatile_analysis.constants import AnalysisConstant, VolatileTag


def cumulative_stddev(df, col):
    """Calculate the cumulative standard deviation of a list of numbers."""
    df[AnalysisConstant.CUM_STD.value] = df[col].expanding().std()
    return df


def cumulutaive_avg_volatility(df, col):
    """Calculate the cumulative average volatility of a list of numbers."""
    df[AnalysisConstant.CUM_AVG_VOLATILITY.value] = df[col].expanding().mean()

    return df


def z_score(
    df,
    col,
    cum_std_col=AnalysisConstant.CUM_STD.value,
    cum_avg_volatility_col=AnalysisConstant.CUM_AVG_VOLATILITY.value,
):
    """Calculate the z score of a list of numbers."""
    df[AnalysisConstant.Z_SCORE.value] = make_round(
        (df[cum_avg_volatility_col] - df[col]) / df[cum_std_col]
    ).fillna(0)

    return df


def normalize_column(df, col, new_col, threshold=0.5):
    """Normalize the column of a dataframe."""
    df[new_col] = 0
    df.loc[df[col] > threshold, new_col] = 1
    return df


def trailing_window_sum(
    df, window_size, col=AnalysisConstant.NORM_Z_SCORE.value
):
    """Calculate the trailing window sum of a list of numbers."""
    df[AnalysisConstant.TRAIL_WINDOW_SUM.value] = (
        df[col].rolling(window=window_size).sum()
    )
    return df


def trailing_window_avg(
    df, window_size, col=AnalysisConstant.TRAIL_WINDOW_SUM.value
):
    """Calculate the trailing window average of a list of numbers."""
    df[AnalysisConstant.TRAIL_WINDOW_AVG.value] = (
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

    pd.set_option("display.max_rows", None)

    first_tag_index = (
        df[col].isin([VolatileTag.HV.value, VolatileTag.LV.value]).idxmax()
    )
    first_tag = df.loc[first_tag_index, col]
    # make condition
    condition = (df[col] == first_tag) & (df[col].shift(1) != first_tag)

    df[new_col] = 0
    df.loc[condition, new_col] = 1
    df[new_col] = df[new_col].cumsum()
    return df


def cycle_duration(data):
    """Calculate the cycle duration of a list of numbers."""
    pass


def close_to_close(data):
    """Calculate the close to close of a list of numbers."""
    pass
