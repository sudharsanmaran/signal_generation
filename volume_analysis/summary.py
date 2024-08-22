from typing import List

import pandas as pd

from source.constants import VOLUME_OUTPUT_FOLDER, VOLUME_OUTPUT_SUMMARY_FOLDER
from source.utils import make_round, write_dataframe_to_csv
from volatile_analysis.constants import AnalysisColumn, PosNegConstant
from volatile_analysis.summary import update_weighted_avg
from volume_analysis.constants import Operations, SummaryColumn
from volume_analysis.processor import (
    AVG_ZSCORE,
    CATEGORY,
    COUNT,
    CYCLE_ID,
    DURATION,
    RANK_ON_Z_SCORE,
)


def read_files(files: list[str]):
    dfs = []
    for file in files:
        df = pd.read_csv(
            f"{VOLUME_OUTPUT_FOLDER}/{file}",
            index_col="dt",
            parse_dates=True,
        )
        df[DURATION] = pd.to_timedelta(df[DURATION])
        df[f"1{AnalysisColumn.CYCLE_DURATION.value}"] = pd.to_timedelta(
            df[f"1{AnalysisColumn.CYCLE_DURATION.value}"]
        )
        dfs.append(df)
    return dfs


def process_summaries(files: List[str]):
    dfs = read_files(files)
    result = []

    name = ""
    for df, file in zip(dfs, files):
        result.extend(process_summary(df, file))
    result_df = pd.DataFrame(result)

    result_df.columns = get_multi_index()

    write_dataframe_to_csv(
        result_df, VOLUME_OUTPUT_SUMMARY_FOLDER, f"{name}summary.csv"
    )
    return


def get_multi_index():
    return pd.MultiIndex.from_tuples(
        [
            (SummaryColumn.STRATEGY_ID.value, "", ""),
            (SummaryColumn.INSTRUMENT.value, "", ""),
            (SummaryColumn.DURATION.value, "", ""),
            (SummaryColumn.DURATION_IN_YEARS.value, "", ""),
            (SummaryColumn.NO_OF_CYCLES.value, "", ""),
            (SummaryColumn.NO_OF_COUNTS.value, "", ""),
            (SummaryColumn.CATEGORY.value, "", ""),
            (SummaryColumn.AVG_CYCLE_DURATION.value, "Positive", "average"),
            (SummaryColumn.AVG_CYCLE_DURATION.value, "Negative", "average"),
            (SummaryColumn.AVG_COUNT_DURATION.value, "Positive", "average"),
            (SummaryColumn.AVG_COUNT_DURATION.value, "Negative", "average"),
            (SummaryColumn.AVG_VOLUME_TRADED.value, "Positive", "average"),
            (SummaryColumn.AVG_VOLUME_TRADED.value, "Negative", "average"),
            (SummaryColumn.AVG_ZSCORE.value, "Positive", "average"),
            (SummaryColumn.AVG_ZSCORE.value, "Negative", "average"),
            (SummaryColumn.AVG_ZSCORE_RANK.value, "Positive", "average"),
            (SummaryColumn.AVG_ZSCORE_RANK.value, "Negative", "average"),
            (SummaryColumn.CTC.value, "Positive", "average"),
            (SummaryColumn.CTC.value, "Positive", "sum"),
            (SummaryColumn.CTC.value, "Positive", "median"),
            (SummaryColumn.CTC.value, "Negative", "average"),
            (SummaryColumn.CTC.value, "Negative", "sum"),
            (SummaryColumn.CTC.value, "Negative", "median"),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                "Positive",
                "average",
            ),
            (SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value, "Positive", "sum"),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                "Positive",
                "median",
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                "Negative",
                "average",
            ),
            (SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value, "Negative", "sum"),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                "Negative",
                "median",
            ),
            (SummaryColumn.MIN_MAX_TO_CLOSE.value, "Positive", "average"),
            (SummaryColumn.MIN_MAX_TO_CLOSE.value, "Positive", "median"),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                "Positive",
                "weighted_average",
            ),
            (SummaryColumn.MIN_MAX_TO_CLOSE.value, "Negative", "average"),
            (SummaryColumn.MIN_MAX_TO_CLOSE.value, "Negative", "median"),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                "Negative",
                "weighted_average",
            ),
            (SummaryColumn.RISK_REWARD_MAX.value, "Positive", "average"),
            (SummaryColumn.RISK_REWARD_MAX.value, "Positive", "median"),
            (SummaryColumn.RISK_REWARD_MAX.value, "Negative", "average"),
            (SummaryColumn.RISK_REWARD_MAX.value, "Negative", "median"),
            (SummaryColumn.RISK_REWARD_CTC.value, "Positive", "average"),
            (SummaryColumn.RISK_REWARD_CTC.value, "Positive", "median"),
            (SummaryColumn.RISK_REWARD_CTC.value, "Negative", "average"),
            (SummaryColumn.RISK_REWARD_CTC.value, "Negative", "median"),
        ]
    )


def create_common_props(df: pd.DataFrame, terms: list):
    df_start, df_end = df.index[0], df.index[-1]
    return {
        SummaryColumn.STRATEGY_ID.value: "_".join(terms[:-2]),
        SummaryColumn.INSTRUMENT.value: terms[0],
        SummaryColumn.DURATION.value: (
            df_start.strftime("%Y-%m-%d %H:%M:%S"),
            df_end.strftime("%Y-%m-%d %H:%M:%S"),
        ),
        SummaryColumn.DURATION_IN_YEARS.value: make_round(
            (df_end - df_start).days / 365
        ),
        SummaryColumn.NO_OF_CYCLES.value: df[CYCLE_ID].nunique(),
        SummaryColumn.NO_OF_COUNTS.value: df[COUNT].nunique(),
    }


def process_summary(df: pd.DataFrame, file: str):
    terms = file.split("_")
    common_props = create_common_props(df, terms)
    result = []

    OPERATIONS = {
        Operations.AVERAGE: pd.Series.mean,
        Operations.MEDIAN: pd.Series.median,
        Operations.SUM: pd.Series.sum,
        Operations.MAX: pd.Series.max,
        Operations.WEIGHTED_AVERAGE_PRICE: update_weighted_avg,
    }
    cols = {
        SummaryColumn.AVG_CYCLE_DURATION.value: (
            f"1{AnalysisColumn.CYCLE_DURATION.value}",
            (Operations.AVERAGE,),
        ),
        SummaryColumn.AVG_COUNT_DURATION.value: (
            DURATION,
            (Operations.AVERAGE,),
        ),
        SummaryColumn.AVG_VOLUME_TRADED.value: ("v", (Operations.AVERAGE,)),
        SummaryColumn.AVG_ZSCORE.value: (AVG_ZSCORE, (Operations.AVERAGE,)),
        SummaryColumn.AVG_ZSCORE_RANK.value: (
            RANK_ON_Z_SCORE,
            (Operations.AVERAGE,),
        ),
        SummaryColumn.CTC.value: (
            f"1{AnalysisColumn.CTC.value}",
            (Operations.AVERAGE, Operations.SUM, Operations.MEDIAN),
        ),
        SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value: (
            f"1{AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value}",
            (Operations.AVERAGE, Operations.SUM, Operations.MEDIAN),
        ),
        SummaryColumn.MIN_MAX_TO_CLOSE.value: (
            f"1{AnalysisColumn.MIN_MAX_TO_CLOSE.value}",
            (
                Operations.AVERAGE,
                Operations.MEDIAN,
                Operations.WEIGHTED_AVERAGE_PRICE,
            ),
        ),
        SummaryColumn.RISK_REWARD_MAX.value: (
            f"1{AnalysisColumn.RISK_REWARD_MAX.value}",
            (Operations.AVERAGE, Operations.MEDIAN),
        ),
        SummaryColumn.RISK_REWARD_CTC.value: (
            f"1{AnalysisColumn.RISK_REWARD_CTC.value}",
            (Operations.AVERAGE, Operations.MEDIAN),
        ),
    }

    category_mask, pos_neg_mask = get_mask(df)
    for category, mask in zip(["cv", "non-cv"], category_mask):
        res = {**common_props, SummaryColumn.CATEGORY.value: category}
        for col, (key, operations) in cols.items():
            for operation in operations:
                func = OPERATIONS[operation]
                for sign, mask_2 in zip(["pos", "neg"], pos_neg_mask):
                    adjusted_mask = mask & mask_2
                    handle_operation(
                        operation, func, sign, adjusted_mask, df, key, col, res
                    )

        result.append(res)

    return result


def handle_operation(operation, func, sign, mask, df, key, col, res):
    if operation == Operations.WEIGHTED_AVERAGE_PRICE:
        # Special case for weighted average price
        func(
            sign,
            res,
            df,
            mask,
            f"1{AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value}",  # col1
            key,  # col2 (Replace with actual column name)
            col,  # new_col
        )
    else:
        # Generic case
        res[f"{operation.value}_{sign}_{col}"] = func(df.loc[mask, key])


def get_mask(df: pd.DataFrame):
    return (
        (df[CATEGORY] == "cv", df[CATEGORY] == "non-cv"),
        (
            df[f"1{AnalysisColumn.POSITIVE_NEGATIVE.value}"].isin(
                {
                    PosNegConstant.POSITIVE.value,
                    PosNegConstant.POSITIVE_MINUS.value,
                }
            ),
            df[f"1{AnalysisColumn.POSITIVE_NEGATIVE.value}"].isin(
                {
                    PosNegConstant.NEGATIVE.value,
                    PosNegConstant.NEGATIVE_PLUS.value,
                }
            ),
        ),
    )
