import os

import pandas as pd


from source.constants import VOLATILE_OUTPUT_FOLDER
from source.data_reader import read_files
from source.utils import (
    make_negative,
    make_positive,
    make_round,
    make_round_series,
    write_dataframe_to_csv,
)
from volatile_analysis.analysis import (
    cumulative_stddev,
    cumulutaive_avg_volatility,
    get_group_duration,
    normalize_column,
    trailing_window_avg,
    trailing_window_sum,
    update_cycle_id,
    update_cycle_id_multi_tag,
    update_volatile_tag,
    z_score,
)
from volatile_analysis.constants import (
    AnalysisColumn,
    AnalysisConstant,
    PosNegConstant,
    VolatileTag,
)


def get_files_data(validated_data):
    base_path = os.getenv("VOLATILE_DB_PATH")
    time_frames = validated_data["time_frames"]
    parameter_id = validated_data["parameter_id"]
    cols = [
        "h",
        "l",
        "c",
        "calculate_change_1",
        *[
            f"calculate_annualized_volatility_{value}"
            for _, value in parameter_id.items()
        ],
        *[
            f"calculate_stdv_{parameter_id}_{period}"
            for period, parameter_id in parameter_id.items()
        ],
        *[
            f"calculate_avg_volatility_{parameter_id}_{period}"
            for period, parameter_id in parameter_id.items()
        ],
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
        for time_frame in time_frames
    }

    return files_to_read


def process_volatile(validated_data):

    dfs = get_base_df(validated_data)
    df, include_next_first_row = update_volatile_cycle_id(validated_data, dfs)
    df = analyse_volatile(
        df,
        tagcol=f"{validated_data['time_frames'][0]}_{validated_data['periods'][0]}_{AnalysisConstant.VOLATILE_TAG.value}",
        validate_data=validated_data,
        group_by_col=AnalysisConstant.CYCLE_ID.value,
        include_next_first_row=include_next_first_row,
        analyze=validated_data["analyze"],
    )

    write_dataframe_to_csv(df, VOLATILE_OUTPUT_FOLDER, "volatile_analysis.csv")
    return


def update_volatile_cycle_id(validated_data, dfs):
    def get_columns(timeframes, periods):
        return {
            timeframe: [
                f"{timeframe}_{period}_{AnalysisConstant.VOLATILE_TAG.value}"
                for period in periods
            ]
            for timeframe in timeframes
        }

    def process_single_dataframe(df, periods, timeframes):
        if len(periods) == 1:
            period = periods[0]
            timeframe = timeframes[0]
            col = f"{timeframe}_{period}_{AnalysisConstant.VOLATILE_TAG.value}"
            update_cycle_id(
                df, col=col, new_col=AnalysisConstant.CYCLE_ID.value
            )
            return df, True
        else:
            cols = list(get_columns(timeframes, periods).values())[0]
            update_cycle_id_multi_tag(
                df, cols=cols, new_col=AnalysisConstant.CYCLE_ID.value
            )
            return df, False

    def merge_dataframes(dfs, cols):
        iter_dfs = iter(dfs.items())
        _, merged_df = next(iter_dfs)
        for tf, df in iter_dfs:
            df = df[[*cols[tf]]]
            merged_df = merged_df.merge(df, on="dt", how="outer").ffill()
        return merged_df

    def process_multiple_dataframes(dfs, periods, timeframes):
        cols = get_columns(timeframes, periods)
        merged_df = merge_dataframes(dfs, cols)
        flat_cols = []
        for col in cols.values():
            flat_cols.extend(col)

        update_cycle_id_multi_tag(
            merged_df, cols=flat_cols, new_col=AnalysisConstant.CYCLE_ID.value
        )
        return merged_df, False

    if len(dfs) == 1:
        timeframe = validated_data["time_frames"]
        df = dfs[timeframe[0]]
        return process_single_dataframe(
            df, validated_data["periods"], timeframe
        )
    else:
        return process_multiple_dataframes(
            dfs, validated_data["periods"], validated_data["time_frames"]
        )


def analyse_volatile(
    df,
    group_by_col,
    validate_data,
    tagcol=AnalysisConstant.VOLATILE_TAG.value,
    include_next_first_row=False,
    analyze=VolatileTag.ALL.value,
):
    def process_group(group_id, group_data):
        if group_id < 1:
            return

        if (
            analyze != VolatileTag.ALL.value
            and group_data[tagcol].iloc[0] != analyze
        ):
            return

        last_index = group_data.index[-1]

        adjusted_group_data = (
            get_adjusted_group_data(group_id, group_data)
            if include_next_first_row
            else group_data
        )

        df.loc[last_index, AnalysisColumn.CYCLE_DURATION.value] = (
            get_group_duration(adjusted_group_data)
        )

        max, min = get_max_min(adjusted_group_data)
        df.loc[last_index, AnalysisColumn.CYCLE_MAX_1.value] = max["h"]
        df.loc[last_index, AnalysisColumn.CYCLE_MIN_1.value] = min["l"]
        df.loc[last_index, AnalysisColumn.MAX_TO_MIN.value] = make_round(
            make_positive(max["h"] - min["l"])
        )
        df.loc[last_index, AnalysisColumn.MAX_TO_MIN_DURATION.value] = (
            min.name - max.name
        )
        df.loc[last_index, AnalysisColumn.MAX_TO_MIN_TO_CLOSE.value] = (
            calculate_max_to_min_to_close(df, last_index, group_data)
        )

        max2, min2 = get_min_max(adjusted_group_data)
        df.loc[last_index, AnalysisColumn.CYCLE_MAX_2.value] = max2["h"]
        df.loc[last_index, AnalysisColumn.CYCLE_MIN_2.value] = min2["l"]
        df.loc[last_index, AnalysisColumn.MIN_TO_MAX.value] = make_round(
            make_positive(max2["h"] - min2["l"])
        )
        df.loc[last_index, AnalysisColumn.MIN_TO_MAX_DURATION.value] = (
            max2.name - min2.name
        )
        df.loc[last_index, AnalysisColumn.MIN_TO_MAX_TO_CLOSE.value] = (
            calculate_min_to_max_to_close(df, last_index, group_data)
        )

        df.loc[last_index, AnalysisColumn.CTC.value] = make_round(
            group_data.iloc[-1]["c"] - group_data.iloc[0]["c"]
        )

        update_capital_and_capital_o_s(df, group_data)

        df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_MAX.value] = df.loc[
            group_data.index, AnalysisColumn.CAPITAL_O_S.value
        ].max()
        df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_MIN.value] = df.loc[
            group_data.index, AnalysisColumn.CAPITAL_O_S.value
        ].min()
        df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_CLOSE.value] = df.loc[
            group_data.index[-1], AnalysisColumn.CAPITAL_O_S.value
        ]
        df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_TO_CLOSE.value] = (
            calculate_cycle_capital_to_close(df, last_index)
        )

        df.loc[last_index, AnalysisColumn.POSITIVE_NEGATIVE.value] = (
            get_direction(
                df.loc[
                    last_index, AnalysisColumn.CYCLE_CAPITAL_TO_CLOSE.value
                ],
                validate_data["capital_upper_threshold"],
                validate_data["capital_lower_threshold"],
            )
        )

        update_positive_negative_metrics(df, last_index, group_data)

        df.loc[last_index, AnalysisColumn.RISK_REWARD_MAX.value] = (
            calculate_risk_reward(
                df, last_index, AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value
            )
        )
        df.loc[last_index, AnalysisColumn.RISK_REWARD_CTC.value] = (
            calculate_risk_reward(
                df, last_index, AnalysisColumn.CYCLE_CAPITAL_TO_CLOSE.value
            )
        )

    def update_capital_and_capital_o_s(df, group_data):

        df.loc[group_data.index[0], AnalysisColumn.CAPITAL.value] = 100
        df.loc[group_data.index[0], AnalysisColumn.CAPITAL_O_S.value] = df.loc[
            group_data.index[0], AnalysisColumn.CAPITAL.value
        ] + (
            df.loc[group_data.index[0], AnalysisColumn.CAPITAL.value]
            * group_data.loc[group_data.index[0], "calculate_change_1"]
        )

        for i in range(1, len(group_data)):
            df.loc[group_data.index[i], AnalysisColumn.CAPITAL.value] = df.loc[
                group_data.index[i - 1], AnalysisColumn.CAPITAL_O_S.value
            ]

            df.loc[
                group_data.index[i], AnalysisColumn.CAPITAL_O_S.value
            ] = df.loc[group_data.index[i], AnalysisColumn.CAPITAL.value] + (
                df.loc[group_data.index[i], AnalysisColumn.CAPITAL.value]
                * group_data.loc[group_data.index[i], "calculate_change_1"]
            )

        df[AnalysisColumn.CAPITAL.value] = make_round_series(
            df[AnalysisColumn.CAPITAL.value]
        )

        df[AnalysisColumn.CAPITAL_O_S.value] = make_round_series(
            df[AnalysisColumn.CAPITAL_O_S.value]
        )

    def get_adjusted_group_data(group_id, group_data):
        next_row = get_next_group_first_row(
            group_id=group_id, df=df, group_by_col=group_by_col
        )
        return (
            pd.concat([group_data, next_row.to_frame().T])
            if next_row is not None
            else group_data
        )

    def calculate_max_to_min_to_close(df, last_index, group_data):
        return make_round(
            make_positive(
                (
                    df.loc[last_index, AnalysisColumn.MAX_TO_MIN.value]
                    / group_data.iloc[0]["c"]
                    - 1
                )
                * 100
            )
        )

    def calculate_min_to_max_to_close(df, last_index, group_data):
        return make_round(
            make_positive(
                (
                    df.loc[last_index, AnalysisColumn.MIN_TO_MAX.value]
                    / group_data.iloc[0]["c"]
                    - 1
                )
                * 100
            )
        )

    def calculate_cycle_capital_to_close(df, last_index):
        return make_round(
            (
                (
                    df.loc[
                        last_index, AnalysisColumn.CYCLE_CAPITAL_CLOSE.value
                    ]
                    / df.loc[last_index, AnalysisColumn.CAPITAL.value]
                )
                * 100
            )
            - 100
        )

    def update_positive_negative_metrics(df, last_index, group_data):
        if df.loc[last_index, AnalysisColumn.POSITIVE_NEGATIVE.value] in {
            PosNegConstant.POSITIVE.value,
            PosNegConstant.POSITIVE_MINUS.value,
        }:
            df.loc[
                last_index, AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value
            ] = calculate_cycle_capital_pos_neg_max(df, last_index)
            df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_DD.value] = (
                calculate_cycle_capital_dd(df, last_index)
            )
            df.loc[last_index, AnalysisColumn.MIN_MAX_TO_CLOSE.value] = (
                calculate_min_max_to_close(df, last_index)
            )
        elif df.loc[last_index, AnalysisColumn.POSITIVE_NEGATIVE.value] in {
            PosNegConstant.NEGATIVE.value,
            PosNegConstant.NEGATIVE_PLUS.value,
        }:
            df.loc[
                last_index, AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value
            ] = calculate_cycle_capital_pos_neg_max(
                df, last_index, is_positive=False
            )
            df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_DD.value] = (
                calculate_cycle_capital_dd(df, last_index, is_positive=False)
            )
            df.loc[last_index, AnalysisColumn.MIN_MAX_TO_CLOSE.value] = (
                calculate_min_max_to_close(df, last_index, is_positive=False)
            )

        df.loc[last_index, AnalysisColumn.PROBABILITY.value] = (
            1
            if df.loc[last_index, AnalysisColumn.POSITIVE_NEGATIVE.value]
            in {PosNegConstant.NEGATIVE.value, PosNegConstant.POSITIVE.value}
            else 0
        )

    def calculate_cycle_capital_pos_neg_max(df, last_index, is_positive=True):
        return make_round(
            (
                (
                    df.loc[
                        last_index,
                        (
                            AnalysisColumn.CYCLE_CAPITAL_MIN.value
                            if not is_positive
                            else AnalysisColumn.CYCLE_CAPITAL_MAX.value
                        ),
                    ]
                    / df.loc[last_index, AnalysisColumn.CAPITAL.value]
                )
                * 100
            )
            - 100
        )

    def calculate_cycle_capital_dd(df, last_index, is_positive=True):
        return make_round(
            make_negative(
                (
                    (
                        df.loc[
                            last_index,
                            (
                                AnalysisColumn.CYCLE_CAPITAL_MAX.value
                                if not is_positive
                                else AnalysisColumn.CYCLE_CAPITAL_MIN.value
                            ),
                        ]
                        / df.loc[last_index, AnalysisColumn.CAPITAL.value]
                    )
                    * 100
                )
                - 100
            )
        )

    def calculate_min_max_to_close(df, last_index, is_positive=True):
        return make_round(
            make_positive(
                (
                    (
                        df.loc[
                            last_index,
                            (
                                AnalysisColumn.CYCLE_CAPITAL_MIN.value
                                if not is_positive
                                else AnalysisColumn.CYCLE_CAPITAL_MAX.value
                            ),
                        ]
                        - df.loc[last_index, AnalysisColumn.CAPITAL.value]
                    )
                    - (
                        df.loc[
                            last_index,
                            AnalysisColumn.CYCLE_CAPITAL_CLOSE.value,
                        ]
                        - df.loc[last_index, AnalysisColumn.CAPITAL.value]
                    )
                )
                / df.loc[last_index, AnalysisColumn.CAPITAL.value]
            )
        )

    def calculate_risk_reward(df, last_index, col):
        return make_round(
            make_positive(
                df.loc[last_index, col]
                / df.loc[last_index, AnalysisColumn.CYCLE_CAPITAL_DD.value]
            )
        )

    groups = df.groupby(group_by_col)
    for group_id, group_data in groups:
        process_group(group_id, group_data)

    return df


def get_direction(value, upper_threshold, lower_threshold):
    direction = None
    if value > 0:
        if value < upper_threshold:
            direction = PosNegConstant.POSITIVE_MINUS.value
        else:
            direction = PosNegConstant.POSITIVE.value
    elif value > lower_threshold:
        direction = PosNegConstant.NEGATIVE_PLUS.value
    else:
        direction = PosNegConstant.NEGATIVE.value

    return direction


def get_max_min(group_data):
    max_id = group_data["h"].idxmax()
    min_id = group_data[max_id:]["l"].idxmin()
    return group_data.loc[max_id], group_data.loc[min_id]


def get_min_max(group_data):
    min_id = group_data["l"].idxmin()
    max_id = group_data[min_id:]["h"].idxmax()
    return group_data.loc[max_id], group_data.loc[min_id]


def get_next_group_first_row(group_id, df, group_by_col):
    next_group = df[df[group_by_col] == group_id + 1]
    if next_group.empty:
        return None
    return next_group.iloc[0]


def get_base_df(validated_data):
    files_to_read = get_files_data(validated_data)

    start_date = validated_data["start_date"]
    end_date = validated_data["end_date"]

    dfs = read_files(start_date, end_date, files_to_read)

    for time_frame, df in dfs.items():
        for period, parameter_id in validated_data["parameter_id"].items():
            update_z_score(
                df,
                f"calculate_stdv_{parameter_id}_{period}",
                period,
            )

            normalize_column(
                df,
                f"{period}_{AnalysisConstant.Z_SCORE.value}",
                f"{period}_{AnalysisConstant.NORM_Z_SCORE.value}",
                validated_data["z_score_threshold"],
            )

            trailing_window_sum(
                df,
                validated_data["sum_window_size"],
                period,
                col=f"{period}_{AnalysisConstant.NORM_Z_SCORE.value}",
            )

            trailing_window_avg(
                df,
                validated_data["avg_window_size"],
                period,
                col=f"{period}_{AnalysisConstant.TRAIL_WINDOW_SUM.value}",
            )

            update_volatile_tag(
                df,
                validated_data["lv_tag"],
                validated_data["hv_tag"],
                col=f"{period}_{AnalysisConstant.TRAIL_WINDOW_AVG.value}",
                new_col=f"{time_frame}_{period}_{AnalysisConstant.VOLATILE_TAG.value}",
            )

    return dfs


def update_z_score(df, col_name, period):
    cumulative_stddev(df, col_name, period)
    cumulutaive_avg_volatility(df, col_name, period)
    z_score(
        df,
        col_name,
        period,
        cum_std_col=f"{period}_{AnalysisConstant.CUM_STD.value}",
        cum_avg_volatility_col=f"{period}_{AnalysisConstant.CUM_AVG_VOLATILITY.value}",
    )
    return df
