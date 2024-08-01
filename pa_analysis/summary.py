import pandas as pd

from pa_analysis.constants import (
    FirstCycleSummaryColumns,
    OutputHeader,
    SummaryColumns,
)
from source.constants import (
    PA_ANALYSIS_CYCLE_FOLDER,
    PA_ANALYSIS_FOLDER,
    PA_ANALYSIS_SUMMARY_FOLDER,
    FirstCycleColumns,
    GroupAnalytics,
    MarketDirection,
)
from source.utils import make_round, write_dataframe_to_csv


def process_summaries(files: list[str]):

    dfs = read_files(files)
    for df, file in zip(dfs, files):
        process_summary(df, file)

    return


def extract_enum(value):
    try:
        # Split on dot and get the part after the dot
        key = value.split(".")[-1]
        return MarketDirection[key]
    except KeyError:
        return MarketDirection.UNKNOWN


def read_files(files: list[str]) -> dict[str, pd.DataFrame]:
    dfs = []
    for file in files:
        df = pd.read_csv(
            f"{PA_ANALYSIS_CYCLE_FOLDER}/{file}",
            index_col="dt",
            parse_dates=True,
            dtype={
                GroupAnalytics.CLOSE_TO_MIN_MAX_POINTS.value: float,
                GroupAnalytics.CLOSE_TO_MIN_MAX_PERCENT.value: float,
            },
        )
        df["market_direction"] = df["market_direction"].apply(extract_enum)
        df[GroupAnalytics.DURATION.value] = pd.to_timedelta(
            df[GroupAnalytics.DURATION.value]
        )
        df[FirstCycleColumns.CYCLE_DURATION.value] = pd.to_timedelta(
            df[FirstCycleColumns.CYCLE_DURATION.value]
        )
        dfs.append(df)
    return dfs


def get_masks(df: pd.DataFrame):

    group_CTC_sums = df.groupby("group_id")[
        FirstCycleColumns.CLOSE_TO_CLOSE.value
    ].sum()

    pos = group_CTC_sums[group_CTC_sums > 0].index
    neg = group_CTC_sums[group_CTC_sums < 0].index

    return (
        df["market_direction"].isin(
            [MarketDirection.LONG, MarketDirection.SHORT]
        ),
        df["market_direction"] == MarketDirection.LONG,
        df["market_direction"] == MarketDirection.SHORT,
    ), (
        df["group_id"].isin(pos),
        df["group_id"].isin(neg),
    )


def create_common_props(df, terms):
    return {
        SummaryColumns.INSTRUMENT.value: terms[0],
        SummaryColumns.START_DATE.value: df.index[0],
        SummaryColumns.END_DATE.value: df.index[-1],
        SummaryColumns.DURATION.value: make_round(
            (df.index[-1] - df.index[0]).days / 365, decimal=3
        ),
    }


def create_category_result(
    common_props: dict,
    category: str,
    df: pd.DataFrame,
    mask: pd.Series,
    basic_result: pd.DataFrame,
    basic_result_index_map: dict,
) -> dict:
    result = {**common_props, SummaryColumns.CATEGORY.value: category}

    metrics = {
        GroupAnalytics.CLOSE_TO_MIN_MAX_POINTS.value: SummaryColumns.PRICE_MOVEMENT.value,
        GroupAnalytics.CLOSE_TO_MIN_MAX_PERCENT.value: SummaryColumns.PRICE_MOVEMENT_PERCENT.value,
        GroupAnalytics.DURATION.value: SummaryColumns.PRICE_MOVEMENT_DURATION.value,
    }

    for key, column in metrics.items():
        if df[mask].empty:
            result[f"avg_{column}"] = 0
            result[f"median_{column}"] = 0
        else:
            result[f"avg_{column}"] = make_round(df[mask][key].mean())
            result[f"median_{column}"] = make_round(df[mask][key].median())

    if (category,) not in basic_result_index_map:
        return result

    result[SummaryColumns.PROBABILITY.value] = basic_result[
        OutputHeader.PROBABILITY.value
    ].iloc[basic_result_index_map[(category,)]]

    if (category, "net") not in basic_result_index_map:
        return result

    result[SummaryColumns.RISK_REWARD.value] = basic_result[
        OutputHeader.RISK_REWARD.value
    ].iloc[basic_result_index_map[(category, "net")]]

    return result


def read_basic_result(file):
    df = pd.read_csv(
        f"{PA_ANALYSIS_FOLDER}/{file}",
        dtype={
            OutputHeader.SIGNAL.value: float,
            OutputHeader.POINTS.value: float,
            OutputHeader.POINTS_PERCENT.value: float,
            OutputHeader.PROBABILITY.value: float,
            OutputHeader.POINTS_PER_SIGNAL: float,
            OutputHeader.POINTS_PER_SIGNAL_PERCENT: float,
            OutputHeader.RISK_REWARD: float,
            OutputHeader.SIGNAL_DURATION.value: float,
            OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value: float,
        },
    )
    return df


def create_pos_neg_result(
    df,
    adj_mask: pd.Series,
    pos_neg: str,
    category: str,
    basic_result: pd.DataFrame,
    index_map: dict,
) -> dict:

    pos_neg_res = {}
    pos_neg_res[f"{pos_neg}_{SummaryColumns.GROUP_COUNT.value}"] = df[
        adj_mask
    ]["group_id"].nunique()

    if (category, pos_neg) not in index_map:
        return pos_neg_res

    pos_neg_res[
        f"{pos_neg}_{SummaryColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION.value}"
    ] = basic_result[OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value].iloc[
        index_map[(category, pos_neg)]
    ]

    pos_neg_res[f"{pos_neg}_{SummaryColumns.NET_POINTS_PER_GROUP.value}"] = (
        basic_result[OutputHeader.POINTS_PER_SIGNAL.value].iloc[
            index_map[(category, pos_neg)]
        ]
    )

    pos_neg_res[
        f"{pos_neg}_{SummaryColumns.NET_POINTS_PERCENT_PER_GROUP.value}"
    ] = basic_result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value].iloc[
        index_map[(category, pos_neg)]
    ]

    return pos_neg_res


def process_summary(df: pd.DataFrame, file: str):
    terms = file.split("_")
    common_props = create_common_props(df, terms)
    direction_masks, pos_neg_masks = get_masks(df)

    update_basic_analysis_summary(
        df, common_props, file, pos_neg_masks, direction_masks
    )

    update_first_cycle_summary(df, common_props, direction_masks, file)

    return


def update_first_cycle_summary(df, common_props, direction_masks, file):
    result = []
    for category, mask in zip(["overall", "long", "short"], direction_masks):
        if df[mask].shape[0] < 1:
            continue
        res = {**common_props}
        res[SummaryColumns.CATEGORY.value] = category
        res[FirstCycleSummaryColumns.GROUP_COUNT.value] = df[mask][
            "group_id"
        ].nunique()

        grouped = df[mask].groupby("group_id")
        first_cycle_col = next(
            (
                col
                for col in df.columns
                if "cycle_no" in col and "MTM" not in col and "CTC" not in col
            ),
            None,
        )

        res[FirstCycleSummaryColumns.AVG_NO_OF_CYCLES_PER_GROUP.value] = (
            make_round(grouped[first_cycle_col].nunique().mean())
        )

        res[FirstCycleSummaryColumns.AVG_CYCLES_DURATION_PER_GROUP.value] = (
            make_round(
                grouped[FirstCycleColumns.CYCLE_DURATION.value].mean().mean()
            )
        )

        res[f"sum_{FirstCycleSummaryColumns.CTC_POINT.value}"] = make_round(
            grouped[FirstCycleColumns.CLOSE_TO_CLOSE.value].sum().sum()
        )

        res[f"avg_{FirstCycleSummaryColumns.CTC_POINT.value}"] = make_round(
            grouped[FirstCycleColumns.CLOSE_TO_CLOSE.value].mean().sum()
        )

        res[f"sum_{FirstCycleSummaryColumns.CTC_POINT_PERCENT.value}"] = (
            grouped[FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value]
            .sum()
            .sum()
        )

        res[f"avg_{FirstCycleSummaryColumns.CTC_POINT_PERCENT.value}"] = (
            make_round(
                grouped[
                    FirstCycleColumns.CLOSE_TO_CLOSE_TO_CLOSE_PERCENT.value
                ]
                .mean()
                .sum()
            )
        )

        res[
            f"sum_{FirstCycleSummaryColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
        ] = make_round(
            grouped[FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value]
            .sum()
            .sum()
        )

        res[
            f"avg_{FirstCycleSummaryColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value}"
        ] = make_round(
            grouped[FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN.value]
            .mean()
            .sum()
        )

        res[
            f"sum_{FirstCycleSummaryColumns.POINTS_FRM_AVG_TILL_MIN_TO_MAX_PERCENT.value}"
        ] = make_round(
            grouped[
                FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value
            ]
            .sum()
            .sum()
        )

        res[
            f"avg_{FirstCycleSummaryColumns.POINTS_FRM_AVG_TILL_MIN_TO_MAX_PERCENT.value}"
        ] = make_round(
            grouped[
                FirstCycleColumns.POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT.value
            ]
            .mean()
            .sum()
        )

        update_MTM_crossed_count(df, res, mask)

        result.append(res)

    result_df = pd.DataFrame(result)
    write_dataframe_to_csv(
        result_df,
        f"{PA_ANALYSIS_SUMMARY_FOLDER}/first_cycle_summary",
        f"{file}_first_cycle_summary.csv",
    )
    return result


def update_MTM_crossed_count(df, res, mask):
    mtm_cols = [col for col in df.columns if "IS_MTM Crossed" in col]
    for col in mtm_cols:
        res[col] = df[mask][df[mask][col] == "YES"][col].count()
    return


def update_basic_analysis_summary(
    df, common_props, file, pos_neg_masks, direction_masks
):
    result = []
    basic_result = read_basic_result(file)
    basic_result_index_map = {
        ("long", "pos"): 0,
        ("long", "neg"): 1,
        ("short", "pos"): 3,
        ("short", "neg"): 4,
        ("long",): 9,
        ("short",): 10,
        ("long", "net"): 2,
        ("short", "net"): 5,
    }
    for category, mask in zip(["overall", "long", "short"], direction_masks):
        if df[mask].shape[0] < 1:
            continue

        cat_res = create_category_result(
            common_props,
            category,
            df,
            mask,
            basic_result,
            basic_result_index_map,
        )
        pos_neg_res = {}
        for pos_neg, pos_neg_mask in zip(["pos", "neg"], pos_neg_masks):
            adj_mask = mask & pos_neg_mask
            pos_neg_res.update(
                create_pos_neg_result(
                    df,
                    adj_mask,
                    pos_neg,
                    category,
                    basic_result,
                    basic_result_index_map,
                )
            )
        all_res = {**cat_res, **pos_neg_res}
        result.append(all_res)

    result_df = pd.DataFrame(result)
    write_dataframe_to_csv(
        result_df,
        f"{PA_ANALYSIS_SUMMARY_FOLDER}/basic_analysis_summary",
        f"{file}_summary.csv",
    )
    return
