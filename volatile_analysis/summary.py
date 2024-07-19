import pandas as pd
from source.constants import (
    VOLATILE_OUTPUT_FOLDER,
    VOLATILE_OUTPUT_SUMMARY_FOLDER,
)
from source.utils import make_round, write_dataframe_to_csv
from volatile_analysis.constants import (
    AnalysisColumn,
    AnalysisConstant,
    Operation,
    PosNegConstant,
    SummaryColumn,
)


def process_summaries(files: list[str]):
    dfs = read_files(files)
    result = []

    for df, file in zip(dfs, files):
        result.extend(process_summary(df, file))
    result_df = pd.DataFrame(result)

    result_df.columns = get_multi_index()

    write_dataframe_to_csv(
        result_df, VOLATILE_OUTPUT_SUMMARY_FOLDER, "summary.csv"
    )
    return


def get_multi_index():

    return pd.MultiIndex.from_tuples(
        [
            (SummaryColumn.STRATEGY_ID.value, "", ""),
            (SummaryColumn.TIME_FRAME.value, "", ""),
            (SummaryColumn.INSTRUMENT.value, "", ""),
            (SummaryColumn.VOLATILE_COMBINATIONS.value, "", ""),
            (SummaryColumn.CAPITAL_RANGE.value, "", ""),
            (SummaryColumn.DURATION.value, "", ""),
            (SummaryColumn.DURATION_IN_YEARS.value, "", ""),
            (SummaryColumn.CATEGORY.value, "", ""),
            (SummaryColumn.VOLATILE_TAG.value, "", ""),
            (SummaryColumn.NO_OF_CYCLES.value, "", ""),
            (
                SummaryColumn.CYCLE_DURATION.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CYCLE_DURATION.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.ANNUAL_VOLATILITY_1.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.ANNUAL_VOLATILITY_1.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MAX_TO_MIN_TO_FIRST_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MAX_TO_MIN_TO_FIRST_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MIN_TO_MAX_TO_FIRST_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MIN_TO_MAX_TO_FIRST_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.CLOSE_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.POSITIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.POSITIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.NEGATIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
                PosNegConstant.NEGATIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.POSITIVE.value,
                Operation.WEIGHTED_AVG.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.SUM.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.MIN_MAX_TO_CLOSE.value,
                PosNegConstant.NEGATIVE.value,
                Operation.WEIGHTED_AVG.value,
            ),
            (
                SummaryColumn.RISK_REWARD_MAX.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.RISK_REWARD_MAX.value,
                PosNegConstant.POSITIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.RISK_REWARD_MAX.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.RISK_REWARD_MAX.value,
                PosNegConstant.NEGATIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.RISK_REWARD_CTC.value,
                PosNegConstant.POSITIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.RISK_REWARD_CTC.value,
                PosNegConstant.POSITIVE.value,
                Operation.MEDIAN.value,
            ),
            (
                SummaryColumn.RISK_REWARD_CTC.value,
                PosNegConstant.NEGATIVE.value,
                Operation.AVG.value,
            ),
            (
                SummaryColumn.RISK_REWARD_CTC.value,
                PosNegConstant.NEGATIVE.value,
                Operation.MEDIAN.value,
            ),
        ]
    )


def parse_file_terms(file: str):
    terms = file.split("_")
    return terms


def parse_timeframes_and_periods(terms: list):
    tfs = terms[0][2:].split("-")
    periods = terms[1][2:].split("-")
    return tfs, periods


def create_common_props(df: pd.DataFrame, terms: list):
    df_start, df_end = df.index[0], df.index[-1]
    return {
        SummaryColumn.STRATEGY_ID.value: "_".join(terms[:2]),
        SummaryColumn.TIME_FRAME.value: terms[0][2:],
        SummaryColumn.INSTRUMENT.value: terms[2],
        SummaryColumn.VOLATILE_COMBINATIONS.value: (
            df.iloc[0]["lv_tag"],
            df.iloc[0]["hv_tag"],
        ),
        SummaryColumn.CAPITAL_RANGE.value: (
            df.iloc[0]["capital_lower_threshold"],
            df.iloc[0]["capital_upper_threshold"],
        ),
        SummaryColumn.DURATION.value: (
            df_start.strftime("%Y-%m-%d %H:%M:%S"),
            df_end.strftime("%Y-%m-%d %H:%M:%S"),
        ),
        SummaryColumn.DURATION_IN_YEARS.value: make_round(
            (df_end - df_start).days / 365
        ),
    }


def create_category_result(common_props: dict, idx: int):
    cat_res = {**common_props}
    cat_res[SummaryColumn.CATEGORY.value] = get_category(idx)
    return cat_res


def create_volatility_result(
    df: pd.DataFrame, adj_mask: pd.Series, cat_res: dict, volatila_tag_col: str
):
    vol_res = {**cat_res}
    vol_res[SummaryColumn.VOLATILE_TAG.value] = df[adj_mask].iloc[-1][
        volatila_tag_col
    ]
    vol_res[SummaryColumn.NO_OF_CYCLES.value] = get_no_of_valid_cycles(
        df, adj_mask
    )
    return vol_res


def process_summary(df: pd.DataFrame, file: str):
    pd.set_option("display.max_rows", None)

    terms = parse_file_terms(file)
    tfs, periods = parse_timeframes_and_periods(terms)
    common_props = create_common_props(df, terms)
    volatila_tag_col = (
        f"{tfs[0]}_{periods[0]}_{AnalysisConstant.VOLATILE_TAG.value}"
    )

    (
        lv_mask,
        hv_mask,
        pos_neg_mask,
        pos_neg_plus_minus,
        positive_mask,
        negative_mask,
    ) = get_masks(df, volatila_tag_col)

    result = []
    for idx, mask in enumerate((pos_neg_mask, pos_neg_plus_minus)):
        if df[mask].shape[0] < 1:
            continue

        cat_res = create_category_result(common_props, idx)
        for adj_mask in (mask & lv_mask, mask & hv_mask):
            if df[adj_mask].shape[0] < 1:
                continue

            vol_res = create_volatility_result(
                df, adj_mask, cat_res, volatila_tag_col
            )
            update_columns(
                df,
                adj_mask,
                vol_res,
                positive_mask,
                negative_mask,
            )
            result.append(vol_res)

    return result


def update_columns(
    df: pd.DataFrame,
    adj_mask: pd.Series,
    vol_res: dict,
    positive_mask: pd.Series,
    negative_mask: pd.Series,
):

    column_operations = {
        (
            AnalysisColumn.CYCLE_DURATION.value,
            SummaryColumn.CYCLE_DURATION.value,
        ): [Operation.AVG.value],
        (
            "calculate_annualized_volatility_1",
            SummaryColumn.ANNUAL_VOLATILITY_1.value,
        ): [Operation.AVG.value],
        (AnalysisColumn.CTC.value, SummaryColumn.CLOSE_TO_CLOSE.value): [
            Operation.AVG.value,
            Operation.SUM.value,
            Operation.MEDIAN.value,
        ],
        (
            AnalysisColumn.MAX_TO_MIN_TO_CLOSE.value,
            SummaryColumn.MAX_TO_MIN_TO_FIRST_CLOSE.value,
        ): [
            Operation.AVG.value,
        ],
        (
            AnalysisColumn.MIN_TO_MAX_TO_CLOSE.value,
            SummaryColumn.MIN_TO_MAX_TO_FIRST_CLOSE.value,
        ): [
            Operation.AVG.value,
        ],
        (
            AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
            SummaryColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
        ): [
            Operation.AVG.value,
            Operation.SUM.value,
            Operation.MEDIAN.value,
        ],
        (
            AnalysisColumn.MAX_TO_MIN_TO_CLOSE.value,
            SummaryColumn.MIN_MAX_TO_CLOSE.value,
        ): [
            Operation.AVG.value,
            Operation.SUM.value,
            Operation.MEDIAN.value,
            Operation.WEIGHTED_AVG.value,
        ],
        (
            AnalysisColumn.RISK_REWARD_MAX.value,
            SummaryColumn.RISK_REWARD_MAX.value,
        ): [
            Operation.AVG.value,
            Operation.MEDIAN.value,
        ],
        (
            AnalysisColumn.RISK_REWARD_CTC.value,
            SummaryColumn.RISK_REWARD_CTC.value,
        ): [
            Operation.AVG.value,
            Operation.MEDIAN.value,
        ],
    }

    for cols, operations in column_operations.items():
        analysis_col, summary_col = cols
        update_pos_neg_columns(
            df,
            adj_mask,
            vol_res,
            positive_mask,
            negative_mask,
            analysis_col,
            summary_col,
            operations,
        )


def update_pos_neg_columns(
    df: pd.DataFrame,
    adj_mask: pd.Series,
    vol_res: dict,
    positive_mask: pd.Series,
    negative_mask: pd.Series,
    analysis_col: str,
    summary_col: str,
    operations: list,
):
    for sign, mask in [
        (PosNegConstant.POSITIVE.value, positive_mask),
        (PosNegConstant.NEGATIVE.value, negative_mask),
    ]:
        update_generic_columns(
            df,
            adj_mask & mask,
            vol_res,
            sign,
            analysis_col,
            summary_col,
            operations,
        )


def update_generic_columns(
    df: pd.DataFrame,
    adj_mask: pd.Series,
    vol_res: dict,
    sign: str,
    analysis_col: str,
    summary_col: str,
    operations: list,
):
    if df[adj_mask].shape[0] < 1:
        if Operation.AVG.value in operations:
            vol_res[f"{sign}_{Operation.AVG.value}_{summary_col}"] = 0.0
        if Operation.SUM.value in operations:
            vol_res[f"{sign}_{Operation.SUM.value}_{summary_col}"] = 0.0
        if Operation.MEDIAN.value in operations:
            vol_res[f"{sign}_{Operation.MEDIAN.value}_{summary_col}"] = 0.0
        if Operation.WEIGHTED_AVG.value in operations:
            vol_res[f"{sign}_{Operation.WEIGHTED_AVG.value}_{summary_col}"] = (
                0.0
            )
    else:
        if Operation.AVG.value in operations:
            vol_res[f"{sign}_{Operation.AVG.value}_{summary_col}"] = df[
                adj_mask
            ][analysis_col].mean()

        if Operation.SUM.value in operations:
            vol_res[f"{sign}_{Operation.SUM.value}_{summary_col}"] = (
                make_round(df[adj_mask][analysis_col].sum())
            )
        if Operation.MEDIAN.value in operations:
            vol_res[f"{sign}_{Operation.MEDIAN.value}_{summary_col}"] = (
                make_round(df[adj_mask][analysis_col].median())
            )

    if (
        summary_col == SummaryColumn.MIN_MAX_TO_CLOSE.value
        and Operation.WEIGHTED_AVG.value in operations
    ):
        update_weighted_avg(
            sign,
            vol_res,
            df,
            adj_mask,
            AnalysisColumn.CYCLE_CAPITAL_POS_NEG_MAX.value,
            AnalysisColumn.MIN_MAX_TO_CLOSE.value,
            SummaryColumn.MIN_MAX_TO_CLOSE.value,
        )


def update_weighted_avg(sign, vol_res, df, adj_mask, col1, col2, new_col_name):

    vol_res[f"{sign}_{Operation.WEIGHTED_AVG.value}_{new_col_name}"] = (
        make_round(
            (df[adj_mask][col1] * df[adj_mask][col2]).sum()
            / df[adj_mask][col2].sum()
        )
    )


def get_masks(df, volatila_tag_col):
    return (
        df[volatila_tag_col] == "LV",
        df[volatila_tag_col] == "HV",
        df[AnalysisColumn.POSITIVE_NEGATIVE.value].isin(
            {PosNegConstant.POSITIVE.value, PosNegConstant.NEGATIVE.value}
        ),
        df[AnalysisColumn.POSITIVE_NEGATIVE.value].isin(
            {
                PosNegConstant.POSITIVE_MINUS.value,
                PosNegConstant.NEGATIVE_PLUS.value,
            }
        ),
        df[AnalysisColumn.POSITIVE_NEGATIVE.value].isin(
            {
                PosNegConstant.POSITIVE.value,
                PosNegConstant.POSITIVE_MINUS.value,
            }
        ),
        df[AnalysisColumn.POSITIVE_NEGATIVE.value].isin(
            {
                PosNegConstant.NEGATIVE.value,
                PosNegConstant.NEGATIVE_PLUS.value,
            }
        ),
    )


def get_no_of_valid_cycles(df, adj_mask):
    return df[adj_mask][df[adj_mask][AnalysisConstant.CYCLE_ID.value] > 0][
        AnalysisConstant.CYCLE_ID.value
    ].nunique()


def get_category(idx):
    category = None
    if idx == 0:
        category = PosNegConstant.POSITIVE_NEGATIVE.value
    else:
        category = PosNegConstant.POSITIVE_NEGATIVE_PLUS_MINUS.value

    return category


def read_files(files: list[str]):
    dfs = []
    for file in files:
        df = pd.read_csv(
            f"{VOLATILE_OUTPUT_FOLDER}/{file}",
            index_col="dt",
            parse_dates=True,
        )
        df[AnalysisColumn.CYCLE_DURATION.value] = pd.to_timedelta(
            df[AnalysisColumn.CYCLE_DURATION.value]
        )
        dfs.append(df)
    return dfs
