import os
import pandas as pd


def read_data(
    instrument,
    portfolio_ids,
    strategy_ids,
    start_date,
    end_date,
    entry_fractal_file_number,
    exit_fractal_file_number,
    bb_file_number,
    bb_band_column,
    trail_bb_file_number,
    trail_bb_band_column,
):

    # Convert start and end dates to datetime
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")

    base_path = os.getenv("DB_PATH")

    strategy_dfs, is_close_read = [], False
    for portfolio_id, strategy_id in zip(portfolio_ids, strategy_ids):
        strategy_path = os.path.join(
            base_path, portfolio_id, instrument, f"{strategy_id}_result.csv"
        )
        columns = []
        if not is_close_read:
            columns = ["TIMESTAMP", "Close", f"TAG_{portfolio_id}"]
            is_close_read = True
        else:
            columns = ["TIMESTAMP", f"TAG_{portfolio_id}"]
        strategy_df = pd.read_csv(
            strategy_path,
            parse_dates=["TIMESTAMP"],
            date_format="%Y-%m-%d %H:%M:%S",
            usecols=columns,
            index_col="TIMESTAMP",
        )
        strategy_df.index = pd.to_datetime(strategy_df.index)
        strategy_df = strategy_df.loc[start_date:end_date]
        strategy_dfs.append(strategy_df)

    all_strategies_df = pd.concat(strategy_dfs, axis=1)

    entry_fractal_path = os.path.join(
        base_path, "Fractal", instrument, f"{entry_fractal_file_number}_result.csv"
    )
    exit_fractal_path = os.path.join(
        base_path, "Fractal", instrument, f"{exit_fractal_file_number}_result.csv"
    )
    bb_band_path = os.path.join(
        base_path, "BB Band", instrument, f"{bb_file_number}_result.csv"
    )
    trail_bb_band_path = os.path.join(
        base_path, "BB Band", instrument, f"{trail_bb_file_number}_result.csv"
    )

    entry_fractal_df = pd.read_csv(
        entry_fractal_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M",
        usecols=[
            "TIMESTAMP",
            "P_1_FRACTAL_LONG",
            "P_1_FRACTAL_SHORT",
            "P_1_FRACTAL_CONFIRMED_LONG",
            "P_1_FRACTAL_CONFIRMED_SHORT",
        ],
        dtype={
            "P_1_FRACTAL_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_SHORT": "boolean",
            "P_1_FRACTAL_SHORT": "boolean",
        },
        index_col="TIMESTAMP",
    )
    # convert dt to datetime
    entry_fractal_df.index = pd.to_datetime(entry_fractal_df.index)

    exit_fractal_df = pd.read_csv(
        exit_fractal_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M",
        usecols=[
            "TIMESTAMP",
            "P_1_FRACTAL_LONG",
            "P_1_FRACTAL_SHORT",
            "P_1_FRACTAL_CONFIRMED_LONG",
            "P_1_FRACTAL_CONFIRMED_SHORT",
        ],
        dtype={
            "P_1_FRACTAL_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_LONG": "boolean",
            "P_1_FRACTAL_CONFIRMED_SHORT": "boolean",
            "P_1_FRACTAL_SHORT": "boolean",
        },
        index_col="TIMESTAMP",
    )
    # convert dt to datetime
    exit_fractal_df.index = pd.to_datetime(entry_fractal_df.index)

    # Define the columns to read from BB band file based on bb_band_sd
    bb_band_cols = ["TIMESTAMP", bb_band_column]

    # Read the BB band file with date filtering, parsing, and indexing
    bb_band_df = pd.read_csv(
        bb_band_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=bb_band_cols,
        index_col="TIMESTAMP",
    )

    # Rename BB band columns for consistency
    bb_band_df.rename(
        columns={bb_band_column: f"bb_{bb_band_column}"},
        inplace=True,
    )

    # Define the columns to read from Trail BB band file based on bb_band_sd
    trail_bb_band_cols = ["TIMESTAMP", trail_bb_band_column]

    # Read the Trail BB band file with date filtering, parsing, and indexing
    trail_bb_band_df = pd.read_csv(
        trail_bb_band_path,
        parse_dates=["TIMESTAMP"],
        date_format="%Y-%m-%d %H:%M:%S",
        usecols=trail_bb_band_cols,
        index_col="TIMESTAMP",
    )

    trail_bb_band_df.rename(
        columns={trail_bb_band_column: f"trail_{trail_bb_band_column}"},
        inplace=True,
    )

    entry_fractal_df = entry_fractal_df[
        (entry_fractal_df.index >= start_date) & (entry_fractal_df.index <= end_date)
    ]
    exit_fractal_df = exit_fractal_df[
        (exit_fractal_df.index >= start_date) & (exit_fractal_df.index <= end_date)
    ]
    bb_band_df = bb_band_df[
        (bb_band_df.index >= start_date) & (bb_band_df.index <= end_date)
    ]
    trail_bb_band_df = trail_bb_band_df[
        (trail_bb_band_df.index >= start_date) & (trail_bb_band_df.index <= end_date)
    ]


    return (
        all_strategies_df,
        entry_fractal_df,
        exit_fractal_df,
        bb_band_df,
        trail_bb_band_df,
    )


def merge_data(
    strategy_df, entry_fractal_df, exit_fractal_df, bb_band_df, trail_bb_band_df
):
    merged_df = strategy_df.join(entry_fractal_df, how="left")
    merged_df = strategy_df.join(exit_fractal_df, how="left")
    merged_df = merged_df.join(bb_band_df, how="left")
    merged_df = merged_df.join(trail_bb_band_df, how="left")
    return merged_df
