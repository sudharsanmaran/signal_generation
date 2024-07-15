from datetime import timedelta
import os


from source.constants import VOLATILE_OUTPUT_FOLDER
from source.data_reader import read_files
from source.utils import write_dataframe_to_csv
from volatile_analysis.analysis import (
    cumulative_stddev,
    cumulutaive_avg_volatility,
    normalize_column,
    trailing_window_avg,
    trailing_window_sum,
    update_cycle_id,
    update_volatile_tag,
    z_score,
)
from volatile_analysis.constants import AnalysisConstant


def get_files_data(validated_data):
    base_path = os.getenv("VOLATILE_DB_PATH")
    time_frame = validated_data["time_frame"]
    parameter_id = validated_data["parameter_id"]
    period = validated_data["period"]
    cols = [
        "h",
        "l",
        "c",
        "calculate_change_1",
        f"calculate_stdv_{parameter_id}_{period}",
        f"calculate_annualized_volatility_{parameter_id}",
        f"calculate_avg_volatility_{parameter_id}_{period}",
    ]
    index = "dt"
    instrument = validated_data["instrument"]

    files_to_read = {
        time_frame: {
            "read": True,
            "file_path": os.path.join(
                base_path,
                f"{time_frame}_{instrument}.csv",
            ),
            "index_col": index,
            "cols": [index, *cols],
        }
    }

    return files_to_read


def process_volatile(validated_data):

    df = get_base_df(validated_data)
    df = analyse_volatile(df)

    write_dataframe_to_csv(df, VOLATILE_OUTPUT_FOLDER, "volatile_analysis.csv")
    return


def analyse_volatile(df):
    return df


def get_base_df(validated_data):
    files_to_read = get_files_data(validated_data)

    start_date = validated_data["start_date"]
    end_date = validated_data["end_date"]

    dfs = read_files(start_date, end_date, files_to_read)

    df = dfs[validated_data.get("time_frame")]

    update_z_score(
        df,
        f"calculate_stdv_{validated_data['parameter_id']}_{validated_data['period']}",
    )

    normalize_column(
        df,
        AnalysisConstant.Z_SCORE.value,
        AnalysisConstant.NORM_Z_SCORE.value,
        validated_data["z_score_threshold"],
    )

    trailing_window_sum(
        df,
        validated_data["sum_window_size"],
    )

    trailing_window_avg(
        df,
        validated_data["avg_window_size"],
    )

    update_volatile_tag(df, validated_data["lv_tag"], validated_data["hv_tag"])
    update_cycle_id(df)
    return df


def update_z_score(df, col_name):
    cumulative_stddev(df, col_name)
    cumulutaive_avg_volatility(df, col_name)
    z_score(df, col_name)
    return df
