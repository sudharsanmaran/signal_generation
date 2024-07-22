import os

import pandas as pd


cycle_id = 1


def read_file(file_path: str) -> dict:
    columns_to_read = [
        "dt",
        "o",
        "h",
        "l",
        "c",
        "v",
        "calculate_volume_stdv_1",
        "calculate_avg_volume_1",
        "calculate_sum_zscores_1_5",
        "calculate_avg_zscore_sums_1_5",
    ]
    df = pd.read_csv(file_path, usecols=columns_to_read)
    df["dt"] = pd.to_datetime(df["dt"])
    return df


def process(validated_data: dict):
    file_path = os.getenv("FINAL_DB_PATH")
    df = read_file(file_path)

    pd.set_option("display.max_rows", None)

    df.loc[
        df["calculate_avg_zscore_sums_1_5"]
        > validated_data["avg_zscore_sum_threshold"],
        "filtered_v",
    ] = df["v"]

    df["marker"] = (
        df["filtered_v"].notna() != df["filtered_v"].shift().notna()
    ).cumsum()
    df["marker"] = df["filtered_v"].notna() * df["marker"]

    cumulative_group_data, rank, rank_index = pd.DataFrame(), [], []
    for group_id, group_data in df.groupby("marker"):
        if group_id > 0:
            df.loc[group_data.index[0], "Average Z score"] = group_data[
                "calculate_avg_zscore_sums_1_5"
            ].mean()

            cumulative_group_data = pd.concat(
                [cumulative_group_data, group_data]
            )

            cumulative_group_data[f"{group_id}_rank"] = df[
                "Average Z score"
            ].rank(method="min", ascending=False)

            n_cum_df = cumulative_group_data[
                cumulative_group_data[f"{group_id}_rank"].notna()
            ]
            rank.append(n_cum_df[f"{group_id}_rank"].iloc[-1])
            rank_index.append(n_cum_df.index[-1])

    df.loc[rank_index, "Rank on Z Score"] = rank

    for group_id, group_data in df.groupby("marker"):
        if group_id > 0:
            group_data["temp"] = group_data["c"] * group_data["filtered_v"]
            df.loc[group_data.index[0], "Weighted Average Price"] = (
                group_data["temp"].sum() / group_data["filtered_v"].sum()
            )

    df["Count"] = df["Weighted Average Price"].notna().cumsum()
    df.loc[df["Weighted Average Price"].isna(), "Count"] = pd.NA

    filtered_df = df[df["Count"].notna()]
    filtered_df["duration"] = filtered_df["dt"].diff().dt.days

    df["duration"] = filtered_df["duration"]

    filtered_df = filtered_df[filtered_df["duration"].notna()]
    filtered_df.loc[
        filtered_df["duration"] <= validated_data["cycle_duration"], "cycle_id"
    ] = 1

    filtered_df["cycle_id"].fillna(
        0,
        inplace=True,
    )

    filtered_df.loc[filtered_df["cycle_id"] == 0, "cycle_increment_marker"] = (
        True
    )

    df["cycle_increment_marker"] = filtered_df["cycle_increment_marker"]

    indices_list = filtered_df[
        (
            (filtered_df["cycle_id"] > 0)
            & (filtered_df["cycle_id"].shift(-1) == 0)
        )
    ].index

    shifted_dates = pd.Series(
        df.loc[indices_list, "dt"] + pd.Timedelta(days=100)
    )

    def adjust_to_business_day(date):
        if date.weekday() >= 5:  # If it's Saturday (5) or Sunday (6)
            date += pd.offsets.BDay(7 - date.weekday())  # Add 1 business day
        return date

    shifted_dates = shifted_dates.apply(adjust_to_business_day)

    target_indices = df[df["dt"].isin(shifted_dates)].index
    df.loc[target_indices, "cycle_increment_marker"] = True
    df.loc[df["Count"] == 2, "cycle_increment_marker"] = True
    df["cycle_id"] = df["cycle_increment_marker"].cumsum()
    df["cycle_id"].ffill(inplace=True)

    a = 20


def update_cycle_id(df, cycle_duration):
    global cycle_id
    for i in range(1, len(df)):
        current_date = df.loc[i, "dt"]
        previous_date = df.loc[i - 1, "dt"]
        if pd.notnull(df.loc[i, "Count"]):
            if (current_date - previous_date).days > cycle_duration:
                cycle_id += 1
                cycle_duration = 0
            else:
                cycle_duration = (current_date - previous_date).days
        df.loc[i, "cycle_id"] = cycle_id
    return df
