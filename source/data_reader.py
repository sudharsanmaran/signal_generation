from functools import reduce
import os
import pandas as pd


def merge_all_df(all_dfs):
    merged_df = reduce(lambda x, y: x.join(y, how="left"), all_dfs)
    return merged_df


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
    read_entry_fractal,
    read_exit_fractal,
    read_bb_fractal,
    read_trail_bb_fractal,
):
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")
    base_path = os.getenv("DB_PATH")
    strategy_dfs = []

    is_close_read = False
    for portfolio_id, strategy_id in zip(portfolio_ids, strategy_ids):
        strategy_path = os.path.join(
            base_path, portfolio_id, instrument, f"{strategy_id}_result.csv"
        )
        columns = ["TIMESTAMP", f"TAG_{portfolio_id}"]
        if not is_close_read:
            columns.insert(1, "Close")
            is_close_read = True
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
    all_dfs = [all_strategies_df]

    fractal_columns = [
        "P_1_FRACTAL_LONG",
        "P_1_FRACTAL_SHORT",
        "P_1_FRACTAL_CONFIRMED_LONG",
        "P_1_FRACTAL_CONFIRMED_SHORT",
    ]
    index = "TIMESTAMP"

    file_details = {
        "entry_fractal": {
            "read": read_entry_fractal,
            "path": "Fractal",
            "file_number": entry_fractal_file_number,
            "cols": [index, *fractal_columns],
            "dtype": {col: "boolean" for col in fractal_columns},
            "rename": {col: f"entry_{col}" for col in fractal_columns},
        },
        "exit_fractal": {
            "read": read_exit_fractal,
            "path": "Fractal",
            "file_number": exit_fractal_file_number,
            "cols": [index, *fractal_columns],
            "dtype": {col: "boolean" for col in fractal_columns},
            "rename": {col: f"exit_{col}" for col in fractal_columns},
        },
        "bb_band": {
            "read": read_bb_fractal,
            "path": "BB Band",
            "file_number": bb_file_number,
            "cols": [index, bb_band_column],
            "rename": {bb_band_column: f"bb_{bb_band_column}"},
        },
        "trail_bb_band": {
            "read": read_trail_bb_fractal,
            "path": "BB Band",
            "file_number": trail_bb_file_number,
            "cols": [index, trail_bb_band_column],
            "rename": {trail_bb_band_column: f"trail_{trail_bb_band_column}"},
        },
    }

    for _, details in file_details.items():
        if details["read"]:
            file_path = os.path.join(
                base_path,
                details["path"],
                instrument,
                f"{details['file_number']}_result.csv",
            )
            df = pd.read_csv(
                file_path,
                parse_dates=["TIMESTAMP"],
                date_format="%Y-%m-%d %H:%M:%S",
                usecols=details["cols"],
                dtype=details.get("dtype", None),
                index_col="TIMESTAMP",
            )
            df.index = pd.to_datetime(df.index)
            df = df.loc[start_date:end_date]
            if "rename" in details:
                df.rename(columns=details["rename"], inplace=True)
            all_dfs.append(df)

    return all_dfs
