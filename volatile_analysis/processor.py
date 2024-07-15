from collections import defaultdict
import os

import pandas as pd


from source.constants import VOLATILE_OUTPUT_FOLDER
from source.data_reader import read_files
from source.utils import make_positive, make_round, write_dataframe_to_csv
from volatile_analysis.analysis import (
    cumulative_stddev,
    cumulutaive_avg_volatility,
    get_group_duration,
    normalize_column,
    trailing_window_avg,
    trailing_window_sum,
    update_cycle_id,
    update_group_id,
    update_volatile_tag,
    z_score,
)
from volatile_analysis.constants import AnalysisColumn, AnalysisConstant


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
    groups = df.groupby(AnalysisConstant.GROUP_ID.value)
    analysis = defaultdict(list)
    for group_id, group_data in groups:
        group_analysis = {}
        if group_id < 1:
            continue

        group_analysis["index"] = group_data.index[-1]

        next_row = get_next_group_first_row(group_id=group_id, df=df)
        if next_row is not None:
            adjusted_group_data = pd.concat(
                [group_data, next_row.to_frame().T]
            )
        else:
            adjusted_group_data = group_data

        group_analysis[AnalysisColumn.CYCLE_DURATION.value] = (
            get_group_duration(adjusted_group_data)
        )

        max, min = get_max_min(adjusted_group_data)
        group_analysis[AnalysisColumn.CYCLE_MAX_1.value] = max["h"]
        group_analysis[AnalysisColumn.CYCLE_MIN_1.value] = min["l"]

        group_analysis[AnalysisColumn.MAX_TO_MIN.value] = make_round(
            make_positive(max["h"] - min["l"])
        )

        group_analysis[AnalysisColumn.MAX_TO_MIN_DURATION.value] = (
            min.name - max.name
        )

        group_analysis[AnalysisColumn.MAX_TO_MIN_TO_CLOSE.value] = make_round(
            make_positive(
                (
                    group_analysis[AnalysisColumn.MAX_TO_MIN.value]
                    / group_data.iloc[0]["c"]
                )
                - 1
            )
            * 100
        )

        max2, min2 = get_min_max(adjusted_group_data)
        group_analysis[AnalysisColumn.CYCLE_MAX_2.value] = max2["h"]
        group_analysis[AnalysisColumn.CYCLE_MIN_2.value] = min2["l"]

        group_analysis[AnalysisColumn.MIN_TO_MAX.value] = make_round(
            make_positive(max2["h"] - min2["l"])
        )

        group_analysis[AnalysisColumn.MIN_TO_MAX_DURATION.value] = (
            max2.name - min2.name
        )

        group_analysis[AnalysisColumn.MIN_TO_MAX_TO_CLOSE.value] = make_round(
            make_positive(
                (
                    group_analysis[AnalysisColumn.MIN_TO_MAX.value]
                    / group_data.iloc[0]["c"]
                )
                - 1
            )
            * 100
        )

        group_analysis[AnalysisColumn.CTC.value] = make_round(
            group_data.iloc[-1]["c"] - group_data.iloc[0]["c"]
        )

        for key, value in group_analysis.items():
            analysis[key].append(value)

    for col, values in analysis.items():
        if col != "index" and values:
            df.loc[analysis["index"], col] = values

    return df


def get_max_min(group_data):
    max_id = group_data["h"].idxmax()
    min_id = group_data[max_id:]["l"].idxmin()
    return group_data.loc[max_id], group_data.loc[min_id]


def get_min_max(group_data):
    min_id = group_data["l"].idxmin()
    max_id = group_data[min_id:]["h"].idxmax()
    return group_data.loc[max_id], group_data.loc[min_id]


def get_next_group_first_row(group_id, df):
    next_group = df[df[AnalysisConstant.GROUP_ID.value] == group_id + 1]
    if next_group.empty:
        return None
    return next_group.iloc[0]


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
    update_group_id(df)
    return df


def update_z_score(df, col_name):
    cumulative_stddev(df, col_name)
    cumulutaive_avg_volatility(df, col_name)
    z_score(df, col_name)
    return df
