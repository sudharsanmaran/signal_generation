import os

import pandas as pd

from source.constants import MarketDirection
from source.data_reader import read_files


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
    # return the result
    pass


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
    cycle_base_df = []
    for df in all_df:
        cycle_base_df.append(update_base_df(df, rename_dict.values(), base_df))
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
    merged_df["signal_start_price"] = merged_df["Close"][0]

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

        # update cycle number
        cycle_codition = (merged_df[f"close_to_{col}"] == "YES") & (
            merged_df[f"close_to_{col}"].shift(1) == "NO"
        )

        counter = 2 if merged_df[f"close_to_{col}"][0] == "YES" else 1
        merged_df[f"cycle_no_{col}"] = cycle_codition.cumsum() + counter

    return merged_df
