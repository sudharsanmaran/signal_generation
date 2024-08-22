import os

import pandas as pd

from source.constants import VOLUME_OUTPUT_FOLDER
from source.utils import make_round, write_dataframe_to_csv
from volatile_analysis.analysis import updated_cycle_id_by_start_end
from volatile_analysis.processor import analyse_volatile


AVG_ZSCORE_SUM_THRESHOLD = "avg_zscore_sum_threshold"
VOLUME_DB_PATH = os.getenv("VOLUME_DB_PATH")
VOLUME_QUATRE_DB_PATH = os.getenv("VOLUME_QUATRE_DB_PATH")
CYCLE_DURATION = "cycle_duration"
WEIGHTED_AVERAGE_PRICE = "Weighted Average Price"
CUM_AVG_WEIGHTED_AVERAGE_PRICE = "Cum Avg Weighted Average Price"
CUM_AVG_WEIGHTED_AVERAGE_PRICE_TO_C = "Cum Avg Weighted Avg Price to C"
AVG_ZSCORE = "Average Z score"
RANK_ON_Z_SCORE = "Rank on Z Score"

C = "c"
DT = "dt"
COUNT = "Count"
DURATION = "duration"
CYCLE_ID = "cycle_id"
FILTERED_V = "filtered_v"
CATEGORY = "category"


def read_file(file_path: str, validated_data) -> dict:
    start_date = validated_data["start_date"]
    end_date = validated_data["end_date"]
    parameter_id = validated_data["parameter_id"]
    period = validated_data["period"]
    time_frame = validated_data["time_frame"]
    instrument = validated_data["instrument"]

    columns_to_read = [
        "dt",
        "o",
        "h",
        "l",
        "c",
        "v",
    ]
    dependent_cols = [
        f"calculate_volume_stdv_{parameter_id}",
        f"calculate_avg_volume_{parameter_id}",
        f"calculate_sum_zscores_{parameter_id}_{period}",
        f"calculate_avg_zscore_sums_{parameter_id}_{period}",
    ]
    df = pd.read_csv(
        os.path.join(
            file_path,
            f"{instrument}_TF_{time_frame}.csv",
        ),
        usecols=[*columns_to_read, *dependent_cols],
        dtype={col: float for col in dependent_cols},
    )
    df["dt"] = pd.to_datetime(df["dt"])
    df = df[(df["dt"] >= start_date) & (df["dt"] <= end_date)]
    return df


def process(validated_data: dict):

    CALCULATE_AVG_ZSCORE_SUMS = f"calculate_avg_zscore_sums_{validated_data['parameter_id']}_{validated_data['period']}"
    # Load data
    df = read_file(
        VOLUME_DB_PATH,
        validated_data,
    )

    # volume_quatre_df = pd.read_csv(
    #     filepath_or_buffer=VOLUME_QUATRE_DB_PATH, index_col="INSTRUMENT"
    # )

    # instrument_quatre = volume_quatre_df.loc[validated_data["instrument"]]

    # df["quatre_shares"] = df.apply(
    #     lambda row: determine_quarter(row, instrument_quatre), axis=1
    # )

    pd.set_option("display.max_rows", None)

    # Filter values based on threshold
    df[FILTERED_V] = df["v"].where(
        df[CALCULATE_AVG_ZSCORE_SUMS]
        > validated_data[AVG_ZSCORE_SUM_THRESHOLD]
    )

    # Create markers for groupings
    df["marker"] = (
        df[FILTERED_V].notna() != df[FILTERED_V].shift().notna()
    ).cumsum()
    df["marker"] = df[FILTERED_V].notna() * df["marker"]

    # Calculate average Z score and ranking
    cumulative_group_data = pd.DataFrame()
    rank = []
    rank_index = []

    for group_id, group_data in df.groupby("marker"):
        if group_id > 0:
            df.at[group_data.index[-1], AVG_ZSCORE] = group_data[
                CALCULATE_AVG_ZSCORE_SUMS
            ].mean()
            cumulative_group_data = pd.concat(
                [cumulative_group_data, group_data]
            )
            cumulative_group_data[f"{group_id}_rank"] = df[AVG_ZSCORE].rank(
                method="min", ascending=False
            )
            n_cum_df = cumulative_group_data[
                cumulative_group_data[f"{group_id}_rank"].notna()
            ]
            rank.append(n_cum_df[f"{group_id}_rank"].iloc[-1])
            rank_index.append(n_cum_df.index[-1])

    df.loc[rank_index, RANK_ON_Z_SCORE] = rank

    # Calculate Weighted Average Price
    for group_id, group_data in df.groupby("marker"):
        if group_id > 0:
            group_data["temp"] = group_data[C] * group_data[FILTERED_V]
            weighted_avg_price = (
                group_data["temp"].sum() / group_data[FILTERED_V].sum()
            )
            df.at[group_data.index[-1], WEIGHTED_AVERAGE_PRICE] = (
                weighted_avg_price
            )

    # Calculate Count and Duration
    df[COUNT] = df[WEIGHTED_AVERAGE_PRICE].notna().cumsum()
    df.loc[df[WEIGHTED_AVERAGE_PRICE].isna(), COUNT] = pd.NA

    filtered_df = df[df[COUNT].notna()]
    filtered_df[DURATION] = filtered_df[DT].diff().dt.days

    df[DURATION] = filtered_df[DURATION]

    # Identify cycles
    update_cycle_id(validated_data, df, filtered_df)

    # category
    df[CATEGORY] = "CV"
    df.loc[df[CYCLE_ID] % 2 == 0, CATEGORY] = "NCV"
    df.loc[df[CYCLE_ID].isna(), CATEGORY] = pd.NA

    df["calculate_change_1"] = df[C].pct_change()

    for group_id, group_data in df.groupby(CYCLE_ID):

        df.loc[group_data.index, CUM_AVG_WEIGHTED_AVERAGE_PRICE] = (
            group_data[WEIGHTED_AVERAGE_PRICE].expanding().mean()
        )

    df[CUM_AVG_WEIGHTED_AVERAGE_PRICE_TO_C] = make_round(
        (df[C] / df[CUM_AVG_WEIGHTED_AVERAGE_PRICE] - 1) * 100
    )

    df = update_sub_cycle_id(df, validated_data)

    # make dt as index
    df.set_index(DT, inplace=True)

    analyse_volatile(
        df,
        validate_data=validated_data,
        group_by_col=CYCLE_ID,
        include_next_first_row=True,
        prefix="1",
    )

    analyse_volatile(
        df,
        validate_data=validated_data,
        group_by_col="sub_cycle_id",
        include_next_first_row=True,
        prefix="2",
    )

    write_dataframe_to_csv(
        df,
        VOLUME_OUTPUT_FOLDER,
        f"{validated_data['instrument']}_ZS_{validated_data[AVG_ZSCORE_SUM_THRESHOLD]}_CD_{validated_data[CYCLE_DURATION]}_{df.index[0]}_{df.index[-1]}.csv".replace(":","-"),
    )


def update_sub_cycle_id(df, validated_data):
    interval = validated_data["sub_cycle_interval"]
    for group_id, group_data in df.groupby(CYCLE_ID):
        if group_id > 0:
            pass

        lower_threshold = validated_data["sub_cycle_lower_threshold"]
        upper_threshold = validated_data["sub_cycle_upper_threshold"]
        # itter over the group data to get the start and end index
        start_index, end_index, possible_start_index = [], [], []
        for idx, row in enumerate(group_data.iterrows()):
            index, value = row
            if value[CUM_AVG_WEIGHTED_AVERAGE_PRICE_TO_C] < lower_threshold:
                possible_start_index.append(idx)

            if (
                len(possible_start_index) > len(end_index)
                and value[CUM_AVG_WEIGHTED_AVERAGE_PRICE_TO_C]
                > upper_threshold
            ):
                start_index.append(possible_start_index[0])
                possible_start_index.clear()
                end_index.append(idx)
                lower_threshold += interval
                upper_threshold += interval

        if len(end_index) == 0:
            end_index.append(len(group_data) - 1)

        start_index = group_data.index[start_index]
        end_index = group_data.index[end_index]

        return updated_cycle_id_by_start_end(
            start_index, end_index, df, "sub_cycle_id"
        )


def update_cycle_id(validated_data, df, filtered_df):
    filtered_df = filtered_df[filtered_df[DURATION].notna()]
    filtered_df[CYCLE_ID] = (
        filtered_df[DURATION].le(validated_data[CYCLE_DURATION]).astype(int)
    )

    filtered_df[CYCLE_ID].fillna(0, inplace=True)
    filtered_df["cycle_increment_marker"] = filtered_df[CYCLE_ID] == 0

    df["cycle_increment_marker"] = filtered_df["cycle_increment_marker"]

    indices_list = filtered_df[
        (filtered_df[CYCLE_ID] > 0) & (filtered_df[CYCLE_ID].shift(-1) == 0)
    ].index

    shifted_dates = pd.Series(
        df.loc[indices_list, DT]
        + pd.Timedelta(days=validated_data["cycle_duration"])
    )

    def adjust_to_business_day(date):
        if date.weekday() >= 5:
            date += pd.offsets.BDay(6 - date.weekday())
        return date

    shifted_dates = shifted_dates.apply(adjust_to_business_day)

    # Create a list to store target indices
    target_indices = []

    # Iterate over shifted_dates to find exact matches or the closest next date
    for target_date in shifted_dates:
        # Check for exact match
        exact_match = df[df["dt"] == target_date].index
        if not exact_match.empty:
            target_indices.extend(exact_match)
        else:
            # Find the closest future date
            future_dates = df[df["dt"] > target_date]
            if not future_dates.empty:
                closest_index = future_dates.index[0]
                target_indices.append(closest_index)

    if len(target_indices) != len(indices_list):
        raise ValueError(
            "Number of target indices and indices list are not equal"
        )

    df.loc[target_indices, "cycle_increment_marker"] = True
    # cycle start at count 2
    df.loc[df[COUNT] == 2, "cycle_increment_marker"] = True
    df[CYCLE_ID] = df["cycle_increment_marker"].cumsum()
    df[CYCLE_ID].ffill(inplace=True)


def determine_quarter(row, instrument_quatre):
    month = row["dt"].month
    year = row["dt"].year

    # Determine the quarter
    if month in [1, 2, 3]:
        return instrument_quatre[f"DEC-{year-1}"]
    elif month in [4, 5, 6]:
        return instrument_quatre[f"MAR-{year}"]
    elif month in [7, 8, 9]:
        return instrument_quatre[f"JUN-{year}"]
    elif month in [10, 11, 12]:
        return instrument_quatre[f"SEP-{year}"]
