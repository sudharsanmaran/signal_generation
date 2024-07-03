"""
The `data_reader.py` module provided is responsible for reading and merging various data files related to trading strategies, fractals, and Bollinger Bands (BB). The module includes functions for merging dataframes and reading data from CSV files based on provided parameters. Below, I will add comments and docstrings to explain the purpose and functionality of each section of the code.

### Explanation:
- **Imports**: Importing necessary libraries and project-specific constants.
- **`merge_all_df` Function**: Merges a list of dataframes on their indices using a left join.
  - **Args**: `all_dfs` (list of pandas DataFrames).
  - **Returns**: Merged DataFrame.
- **`read_data` Function**: Reads data from various CSV files based on the provided parameters and returns a list of DataFrames.
  - **Args**: 
    - Various parameters specifying the instrument, portfolio and strategy IDs, date range, file numbers, column names, and flags indicating whether to read certain data.
  - **Returns**: List of DataFrames containing the read data.
  - **Function Details**:
    - Converts `start_date` and `end_date` to pandas datetime format.
    - Constructs the base path from an environment variable.
    - Reads strategy data files into DataFrames and filters them by date range.
    - Concatenates all strategy DataFrames into one DataFrame.
    - Defines a dictionary with details for reading additional data (entry fractals, exit fractals, Bollinger Bands, trailing Bollinger Bands).
    - Reads additional data files into DataFrames, renames columns if necessary, and appends them to the list of DataFrames.
    - Returns the list of DataFrames.
"""

from functools import reduce
import os
import pandas as pd

# Import project-specific constants
from source.constants import entry_fractal_columns, exit_fractal_columns


def merge_all_df(all_dfs):
    """
    Merge a list of dataframes on their indices using a left join.

    Args:
        all_dfs (list): List of pandas DataFrames to be merged.

    Returns:
        pandas.DataFrame: Merged DataFrame.
    """
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
    """
    Read data from various CSV files based on the provided parameters and return a list of DataFrames.

    Args:
        instrument (str): Instrument name.
        portfolio_ids (list): List of portfolio IDs.
        strategy_ids (list): List of strategy IDs.
        start_date (str): Start date in the format 'dd/mm/yyyy HH:MM:SS'.
        end_date (str): End date in the format 'dd/mm/yyyy HH:MM:SS'.
        entry_fractal_file_number (int): File number for entry fractal data.
        exit_fractal_file_number (int): File number for exit fractal data.
        bb_file_number (int): File number for Bollinger Bands data.
        bb_band_column (str): Column name for Bollinger Bands data.
        trail_bb_file_number (int): File number for trailing Bollinger Bands data.
        trail_bb_band_column (str): Column name for trailing Bollinger Bands data.
        read_entry_fractal (bool): Whether to read entry fractal data.
        read_exit_fractal (bool): Whether to read exit fractal data.
        read_bb_fractal (bool): Whether to read Bollinger Bands data.
        read_trail_bb_fractal (bool): Whether to read trailing Bollinger Bands data.

    Returns:
        list: List of DataFrames containing the read data.
    """
    # Convert start and end dates to pandas datetime format
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y %H:%M:%S")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y %H:%M:%S")

    # Get the base path from environment variables
    base_path = os.getenv("DB_PATH")

    # Loop through each portfolio and strategy ID pair
    all_dfs = load_strategy_data(
        instrument,
        portfolio_ids,
        strategy_ids,
        start_date,
        end_date,
        base_path,
    )

    # Index column name
    index = "TIMESTAMP"

    # Dictionary to store file details for reading additional data
    file_details = {
        "entry_fractal": update_entry_fractal_file(
            instrument,
            entry_fractal_file_number,
            read_entry_fractal,
            base_path,
            index,
        ),
        "exit_fractal": update_exit_fractal_file(
            instrument,
            exit_fractal_file_number,
            read_exit_fractal,
            base_path,
            index,
        ),
        "bb_band": {
            "read": read_bb_fractal,
            "file_path": os.path.join(
                base_path,
                "BB Band",
                instrument,
                f"{bb_file_number}_result.csv",
            ),
            "index_col": "TIMESTAMP",
            "cols": [index, bb_band_column],
            "rename": {bb_band_column: f"bb_{bb_band_column}"},
        },
        "trail_bb_band": {
            "read": read_trail_bb_fractal,
            "path": "BB Band",
            "file_number": trail_bb_file_number,
            "file_path": os.path.join(
                base_path,
                "BB Band",
                instrument,
                f"{trail_bb_file_number}_result.csv",
            ),
            "index_col": "TIMESTAMP",
            "cols": [index, trail_bb_band_column],
            "rename": {trail_bb_band_column: f"trail_{trail_bb_band_column}"},
        },
    }

    dfs = read_files(start_date, end_date, file_details)
    all_dfs.extend(dfs.values())
    return all_dfs


def update_exit_fractal_file(
    instrument, exit_fractal_file_number, read_exit_fractal, base_path, index
):
    return {
        "read": read_exit_fractal,
        "file_path": os.path.join(
            base_path,
            "Fractal",
            instrument,
            f"{exit_fractal_file_number}_result.csv",
        ),
        "index_col": "TIMESTAMP",
        "cols": [index, *exit_fractal_columns],
        "dtype": {col: "boolean" for col in entry_fractal_columns},
        "rename": {col: f"exit_{col}" for col in entry_fractal_columns},
    }


def update_entry_fractal_file(
    instrument, entry_fractal_file_number, read_entry_fractal, base_path, index
):
    return {
        "read": read_entry_fractal,
        "file_path": os.path.join(
            base_path,
            "Fractal",
            instrument,
            f"{entry_fractal_file_number}_result.csv",
        ),
        "index_col": "TIMESTAMP",
        "cols": [index, *entry_fractal_columns],
        "dtype": {col: "boolean" for col in entry_fractal_columns},
        "rename": {col: f"entry_{col}" for col in entry_fractal_columns},
    }


def read_files(
    start_date,
    end_date,
    file_details: dict,
):
    data_frames = {}
    # Loop through each file detail to read additional data
    for file_name, details in file_details.items():
        if details["read"]:
            # Read the data CSV file into a DataFrame
            df = pd.read_csv(
                details.get("file_path"),
                parse_dates=[details["index_col"]],
                date_format="%Y-%m-%d %H:%M:%S",
                usecols=details["cols"],
                dtype=details.get("dtype", None),
                index_col=details["index_col"],
            )
            df.index = pd.to_datetime(df.index)
            # Filter the DataFrame for the specified date range
            df = df.loc[start_date:end_date]
            # Rename columns if specified
            if "rename" in details:
                df.rename(columns=details["rename"], inplace=True)
            data_frames[file_name] = df
    return data_frames


def load_strategy_data(
    instrument,
    portfolio_ids,
    strategy_ids,
    start_date,
    end_date,
    base_path,
):
    """
    Load strategy data from CSV files for the specified instrument, portfolio, and strategy IDs.
    """

    is_close_read, strategy_dfs = False, []
    for portfolio_id, strategy_id in zip(portfolio_ids, strategy_ids):
        # Construct the path to the strategy CSV file
        strategy_path = os.path.join(
            base_path, portfolio_id, instrument, f"{strategy_id}_result.csv"
        )
        # Define the columns to be read from the CSV file
        columns = ["TIMESTAMP", f"TAG_{portfolio_id}"]
        if not is_close_read:
            columns.insert(1, "Close")
            is_close_read = True

        try:
            # Read the strategy CSV file into a DataFrame
            strategy_df = pd.read_csv(
                strategy_path,
                parse_dates=["TIMESTAMP"],
                date_format="%Y-%m-%d %H:%M:%S",
                usecols=columns,
                index_col="TIMESTAMP",
            )
        except Exception as e:
            print(f"Error reading {strategy_path}: {e}")
            raise e
        strategy_df.index = pd.to_datetime(strategy_df.index)
        # Filter the DataFrame for the specified date range
        strategy_df = strategy_df.loc[start_date:end_date]
        strategy_dfs.append(strategy_df)

    # Concatenate all strategy DataFrames along the columns
    all_strategies_df = pd.concat(strategy_dfs, axis=1)
    all_dfs = [all_strategies_df]
    return all_dfs
