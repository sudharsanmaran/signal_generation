from collections import defaultdict
import logging
from typing import Dict, List

import pandas as pd
from portfolio.constants import PNLSummaryCols
from portfolio.data_reader import read_signal_gen_file
from portfolio.validation import Configs, InputData
from source.constants import (
    PORTFOLIO_COMPANY_OUTPUT_FOLDER,
    PORTFOLIO_PNL_OUTPUT_FOLDER,
    PORTFOLIO_SUMMARY_OUTPUT_FOLDER,
    OutputColumn,
    TradeExitType,
)
from source.utils import make_round, write_dataframe_to_csv


logger = logging.getLogger(__name__)


def process_portfolio(validated_data: InputData) -> None:

    update_company_base_df(validated_data.companies_df, validated_data.configs)
    company_sg_df_map = construct_company_signal_dictionary(validated_data)
    pnl_df = formulate_PNL_df(validated_data, company_sg_df_map)
    summary_df = formulate_daily_pnl_summary(validated_data, pnl_df)
    update_company_summary(validated_data, summary_df)

    write_dataframe_to_csv(
        pnl_df,
        PORTFOLIO_PNL_OUTPUT_FOLDER,
        f"{validated_data.companies_data.segment}_{validated_data.companies_data.parameter_id}_pnl.csv",
    )

    write_dataframe_to_csv(
        summary_df,
        PORTFOLIO_SUMMARY_OUTPUT_FOLDER,
        f"{validated_data.companies_data.segment}_{validated_data.companies_data.parameter_id}_day_summary.csv",
    )

    write_dataframe_to_csv(
        validated_data.companies_df,
        PORTFOLIO_COMPANY_OUTPUT_FOLDER,
        f"{validated_data.companies_data.segment}_{validated_data.companies_data.parameter_id}_company_base.csv",
    )


def update_company_summary(
    validated_data: InputData, summary_df: pd.DataFrame
) -> None:

    def populate_company_entry_data() -> None:

        if entry_row.empty:
            logger.error(
                f"{update_company_summary.__name__}: no entry found for {company} on {company_row['Date']}"
            )
            return

        validated_data.companies_df.loc[
            name, PNLSummaryCols.OPEN_EXPOSURE_COST.value
        ] = entry_row[PNLSummaryCols.OPEN_EXPOSURE_COST.value].iloc[0]

        validated_data.companies_df.loc[
            name, PNLSummaryCols.OPEN_EXPOSURE_PERCENT.value
        ] = entry_row[PNLSummaryCols.OPEN_EXPOSURE_PERCENT.value].iloc[0]

        validated_data.companies_df.loc[
            name, PNLSummaryCols.PORTFOLIO_VALUE.value
        ] = entry_row[PNLSummaryCols.PORTFOLIO_VALUE.value].iloc[0]

        validated_data.companies_df.loc[
            name, PNLSummaryCols.MTM_UNREALIZED
        ] = entry_row[PNLSummaryCols.MTM_UNREALIZED.value].iloc[0]

        validated_data.companies_df.loc[
            name, PNLSummaryCols.MTM_UNREALIZED_BY_CAPITAL.value
        ] = entry_row[PNLSummaryCols.MTM_UNREALIZED_BY_CAPITAL.value].iloc[0]

    def populate_company_exit_data() -> None:

        if exit_row.empty:
            logger.error(
                f"{update_company_summary.__name__}: no exit found for {company} on {company_row['Date']}"
            )
            return
        validated_data.companies_df.loc[
            name, PNLSummaryCols.MTM_REALIZED.value
        ] = exit_row[PNLSummaryCols.MTM_REALIZED.value].iloc[0]

        validated_data.companies_df.loc[
            name, PNLSummaryCols.MTM_REALIZED_BY_CAPITAL.value
        ] = exit_row[PNLSummaryCols.MTM_REALIZED_BY_CAPITAL.value].iloc[0]

    def populate_daily_summary_for_all_companies() -> None:
        for _, group in validated_data.companies_df.groupby("Date"):
            start_index = group.index[0]
            mtm_sum = group[PNLSummaryCols.MTM_REALIZED.value].sum()
            validated_data.companies_df.loc[start_index, "Realised Gain"] = (
                mtm_sum
            )
            validated_data.companies_df.loc[start_index, "Closing Capital"] = (
                mtm_sum + validated_data.configs.capital
            )

    for name, company_row in validated_data.companies_df.iterrows():
        company = company_row["Name of Company"]
        ticker = fetch_ticker(validated_data.company_tickers, company)
        company_summary = summary_df[
            (summary_df["Company"] == ticker)
            & (summary_df["Date"] == company_row["Date"])
        ]

        entry_row = company_summary.loc[
            company_summary[PNLSummaryCols.INITIATED_POSITION.value] == "YES"
        ]
        exit_row = company_summary.loc[
            company_summary[PNLSummaryCols.EXITS.value] == "YES"
        ]
        populate_company_entry_data()
        populate_company_exit_data()

    populate_daily_summary_for_all_companies()


def formulate_PNL_df(validated_data, company_sg_df_map):

    pnl_dict = defaultdict(list)
    out_of_list, prev_day_companies = set(), set()
    prev_day_companies_pnl = {}
    prev_group_df = pd.DataFrame()
    for group_id, group_data in validated_data.companies_df.groupby("Date"):
        group_data.index = group_data["Name of Company"]

        if len(prev_day_companies) > 0:
            out_of_list = prev_day_companies - set(
                group_data["Name of Company"]
            )

        prev_day_companies = set(group_data["Name of Company"])

        for company in out_of_list:
            company_row = prev_group_df.loc[company]

            ticker = fetch_ticker(validated_data.company_tickers, company)

            update_common_record(
                pnl_dict,
                company_row,
                pd.Timestamp(f"{group_id} 09:15:00"),
                ticker,
            )
            process_out_of_list_exit(
                company,
                pnl_dict,
                cum_value=prev_day_companies_pnl.get(company, 0),
            )

        prev_group_df = group_data

        for company in group_data["Name of Company"].unique().tolist():
            sg_df = company_sg_df_map.get(company, pd.DataFrame())
            if sg_df.empty:
                logger.error(
                    f"{process_portfolio.__name__}: no signal gen file found for {company}"
                )
                continue

            # filter the data based on the group_id
            sg_df = sg_df[sg_df["DATETIME"].dt.date == group_id.date()]
            if sg_df.empty:
                logger.error(
                    f"{process_portfolio.__name__}: empty signal gen df for {company} for {group_id}"
                )
                continue

            company_row = group_data.loc[company]
            entry_id = 0
            for name, row in sg_df.iterrows():

                update_common_record(
                    pnl_dict,
                    company_row,
                    row["DATETIME"],
                    row[OutputColumn.INSTRUMENT.value],
                )

                if row["DATETIME_TYPE"] == OutputColumn.ENTRY_DATETIME.value:
                    entry_id += 1
                    process_entry(
                        name,
                        row,
                        pnl_dict,
                        validated_data.configs,
                        company_row=company_row,
                        entry_id=entry_id,
                    )
                else:
                    entry_id = 0
                    process_exit(name, row, pnl_dict, validated_data.configs)

                if sg_df.index[-1] == name:
                    prev_day_companies_pnl[company] = pnl_dict["CUM_VALUE"][-1]

    pnl_df = pd.DataFrame(pnl_dict)
    return pnl_df


def fetch_ticker(company_tickers, company):
    try:
        ticker = company_tickers.loc[company]["Ticker Symbol"]
    except KeyError:
        ticker = "NA"
    return ticker


def formulate_daily_pnl_summary(
    validated_data: InputData, pnl_df
) -> pd.DataFrame:

    def update_common_summary_cols() -> None:
        summary_dict[PNLSummaryCols.DATE.value].append(date)
        summary_dict[PNLSummaryCols.COMPANY.value].append(ticker)
        summary_dict[PNLSummaryCols.CLOSING_PRICE.value].append(1234)

    def default_exit_data() -> Dict:
        return {
            PNLSummaryCols.EXITS.value: "NO",
            PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: 0,
            PNLSummaryCols.TOTAL_VOLUME.value: 0,
            PNLSummaryCols.TOTAL_AMOUNT.value: 0,
            PNLSummaryCols.MTM_REALIZED.value: 0,
            PNLSummaryCols.MTM_REALIZED_BY_CAPITAL.value: 0,
            PNLSummaryCols.MTM_REALIZED_BY_EXPOSURE.value: 0,
        }

    def default_entry_data() -> Dict:
        return {
            PNLSummaryCols.INITIATED_POSITION.value: "NO",
            PNLSummaryCols.NO_OF_ENTRIES_FOR_THE_DAY.value: 0,
            PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: 0,
            PNLSummaryCols.TOTAL_VOLUME.value: 0,
            PNLSummaryCols.TOTAL_AMOUNT.value: 0,
            PNLSummaryCols.OPEN_VOLUME.value: 0,
            f"OP_{PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value}": 0,
            PNLSummaryCols.OPEN_EXPOSURE_COST.value: 0,
            PNLSummaryCols.OPEN_EXPOSURE_PERCENT.value: 0,
            PNLSummaryCols.PORTFOLIO_VALUE.value: 0,
            PNLSummaryCols.MTM_UNREALIZED.value: 0,
            PNLSummaryCols.PERCENT_OF_OPEN_POSITION.value: 0,
            PNLSummaryCols.MTM_UNREALIZED_BY_CAPITAL.value: 0,
            PNLSummaryCols.MTM_UNREALIZED_BY_EXPOSURE.value: 0,
        }

    def populate_entry_data(group: pd.DataFrame) -> None:
        summary_values = default_exit_data()
        if group.empty:
            summary_values.update(default_entry_data())
        else:
            open_exposure = group["CUM_VALUE"].iloc[-1]
            open_volume = group["CUM_VOLUME"].iloc[-1]
            portfolio_value = (
                open_exposure
                * summary_dict[PNLSummaryCols.CLOSING_PRICE.value][-1]
            )
            summary_values.update(
                {
                    PNLSummaryCols.INITIATED_POSITION.value: "YES",
                    PNLSummaryCols.NO_OF_ENTRIES_FOR_THE_DAY.value: group[
                        "ENTRY_ID"
                    ]
                    .ne("")
                    .sum(),
                    PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: group[
                        "PURCHASE_PRICE"
                    ].iloc[-1],
                    PNLSummaryCols.TOTAL_VOLUME.value: group["VOLUME"].iloc[
                        -1
                    ],
                    PNLSummaryCols.TOTAL_AMOUNT.value: group[
                        "VOLUME_TO_CONSIDER"
                    ].iloc[-1],
                    PNLSummaryCols.OPEN_VOLUME.value: open_volume,
                    f"OP_{PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value}": group[
                        "WEIGHTED_AVG"
                    ].iloc[-1],
                    PNLSummaryCols.OPEN_EXPOSURE_COST.value: open_exposure,
                    PNLSummaryCols.OPEN_EXPOSURE_PERCENT.value: make_round(
                        (open_exposure / validated_data.configs.capital) * 100
                    ),
                    PNLSummaryCols.PORTFOLIO_VALUE.value: portfolio_value,
                    PNLSummaryCols.MTM_UNREALIZED.value: (
                        portfolio_value - open_exposure
                    ),
                    PNLSummaryCols.PERCENT_OF_OPEN_POSITION.value: make_round(
                        (portfolio_value / validated_data.configs.capital)
                        * 100
                    ),
                    PNLSummaryCols.MTM_UNREALIZED_BY_CAPITAL.value: make_round(
                        (portfolio_value / validated_data.configs.capital)
                        * 100
                    ),
                    PNLSummaryCols.MTM_UNREALIZED_BY_EXPOSURE.value: make_round(
                        (portfolio_value / allowed_exposure) * 100
                    ),
                }
            )
        populate_summary_dict(summary_values)

    def populate_exit_data(group: pd.DataFrame) -> None:
        summary_values = default_entry_data()
        if group.empty:
            summary_values.update(default_exit_data())
        else:
            weight_avg = group["SELL_PRICE"].iloc[-1]
            total_volume = group["TP_VOLUME_TO_SOLD"].iloc[-1]
            summary_values.update(
                {
                    PNLSummaryCols.EXITS.value: "YES",
                    PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: weight_avg,
                    PNLSummaryCols.TOTAL_VOLUME.value: total_volume,
                    PNLSummaryCols.TOTAL_AMOUNT.value: weight_avg
                    * total_volume,
                    PNLSummaryCols.MTM_REALIZED.value: group[
                        "PROFIT_LOSS"
                    ].iloc[-1],
                    PNLSummaryCols.MTM_REALIZED_BY_CAPITAL.value: make_round(
                        (
                            group["PROFIT_LOSS"].iloc[-1]
                            / validated_data.configs.capital
                        )
                        * 100
                    ),
                    PNLSummaryCols.MTM_REALIZED_BY_EXPOSURE.value: make_round(
                        (group["PROFIT_LOSS"].iloc[-1] / allowed_exposure)
                        * 100
                    ),
                }
            )
        populate_summary_dict(summary_values)

    def populate_summary_dict(summary_values: Dict) -> None:
        for key, value in summary_values.items():
            summary_dict[key].append(value)

    company_df = validated_data.companies_df
    company_tickers = validated_data.company_tickers
    pnl_df["Date"] = pnl_df["DATETIME"].dt.date
    pnl_df.set_index("Date", inplace=True)

    summary_dict = defaultdict(list)
    for date, group in company_df.groupby("Date"):
        group.index = group["Name of Company"]
        for company in group.index:
            ticker = fetch_ticker(company_tickers, company)
            pnl_mask = (pnl_df.index == date.date()) & (
                pnl_df["COMPANY"] == ticker
            )
            pnl = pnl_df.loc[pnl_mask]

            if pnl.empty:
                logger.error(
                    f"{formulate_daily_pnl_summary.__name__}: no pnl found for {company} on {date}"
                )
                continue

            allowed_exposure = group.loc[company, "allowed_exposure"]

            for type, group in pnl.groupby("TYPE"):
                update_common_summary_cols()
                if type == "ENTRY":
                    populate_entry_data(group)
                elif type == "EXIT":
                    populate_exit_data(group)

    return pd.DataFrame(summary_dict)


def update_common_record(pnl_dict, company_row, date_time, instrument):
    pnl_dict["DATETIME"].append(date_time)
    pnl_dict["COMPANY"].append(instrument)
    pnl_dict["UNIQUE_ID"].append(company_row["Unique ID"])


def construct_company_signal_dictionary(validated_data: InputData) -> Dict:
    company_sg_df_map = {}
    for company in validated_data.company_lists:
        if company not in validated_data.company_sg_map:
            logger.error(
                f"{process_portfolio.__name__}: no signal gen file found for {company}"
            )
            continue

        sg_df = read_signal_gen_file(validated_data.company_sg_map[company])
        if sg_df.empty:
            continue

        # Reshape the DataFrame to long format
        df_long = pd.melt(
            sg_df,
            id_vars=[
                OutputColumn.ENTRY_PRICE.value,
                OutputColumn.EXIT_PRICE.value,
                OutputColumn.ENTRY_TYPE.value,
                OutputColumn.EXIT_TYPE.value,
                OutputColumn.ENTRY_ID.value,
                OutputColumn.EXIT_ID.value,
                OutputColumn.INSTRUMENT.value,
            ],
            value_vars=[
                OutputColumn.ENTRY_DATETIME.value,
                OutputColumn.EXIT_DATETIME.value,
            ],
            var_name="DATETIME_TYPE",
            value_name="DATETIME",
        )

        # Separate the DataFrame into Entry and Exit types
        df_entry = df_long[
            df_long["DATETIME_TYPE"] == OutputColumn.ENTRY_DATETIME.value
        ]
        df_exit = df_long[
            df_long["DATETIME_TYPE"] == OutputColumn.EXIT_DATETIME.value
        ]

        # Drop duplicates within each type
        df_entry = df_entry.drop_duplicates(subset=["DATETIME"])
        df_exit = df_exit.drop_duplicates(subset=["DATETIME"])

        # Concatenate the entry and exit DataFrames back together
        df_cleaned = (
            pd.concat([df_exit, df_entry])
            .sort_values(by="DATETIME")
            .reset_index(drop=True)
        )

        company_sg_df_map[company] = df_cleaned
    return company_sg_df_map


def process_out_of_list_exit(company, pnl_dict, cum_value):
    logger.info(
        f"{process_out_of_list_exit.__name__}: Processing out of list exit for {company}"
    )

    entry_cols = [
        "ENTRY_ID",
        "ENTRY_TYPE",
        "PURCHASE_PRICE",
        "PURCHASE_VALUE",
        "VOLUME",
        "VOLUME_TO_CONSIDER",
        "ALLOWED_EXPOSURE",
        "PRICE_EXEDED",
    ]
    for col in entry_cols:
        pnl_dict[col].append("")

    day_close = 1234
    pnl_dict["TYPE"].append("EXIT")
    pnl_dict["EXIT_ID"].append(1)  # todo: check
    pnl_dict["EXIT_TYPE"].append(TradeExitType.OUT_OF_LIST.value)
    pnl_dict["SELL_PRICE"].append(day_close)
    pnl_dict["VOLUME_TO_SOLD"].append(cum_value)
    pnl_dict["SELL_VALUE"].append(pnl_dict["VOLUME_TO_SOLD"][-1] * day_close)
    pnl_dict["PROFIT_LOSS"].append(pnl_dict["SELL_VALUE"][-1] - cum_value)

    # carry forward the values for future calculations
    try:
        cum_value = pnl_dict["CUM_VALUE"][-1]
    except IndexError:
        cum_value = 0

    try:
        cum_volume = pnl_dict["CUM_VOLUME"][-1]
    except IndexError:
        cum_volume = 0

    pnl_dict["WEIGHTED_AVG"].append(0)
    pnl_dict["CUM_VALUE"].append(cum_value)
    pnl_dict["CUM_VOLUME"].append(cum_volume)

    pnl_dict["TP_VOLUME_TO_SOLD"].append(0)
    pnl_dict["TP_REMAINING_VOLUME"].append(0)
    pnl_dict["TP_NEED_TO_SELL"].append(0)
    pnl_dict["TP_VOLUME_SOLDED"].append(0)
    pnl_dict["TP_NEED_TO_SELL_PRICE"].append(0)


def process_entry(
    name,
    row,
    pnl_dict: Dict[str, List],
    configs: Configs,
    company_row,
    entry_id,
):
    logger.info(
        f"{process_entry.__name__}: Processing entry for {name} at {row['DATETIME']}"
    )

    pnl_dict["TYPE"].append("ENTRY")

    exit_cols = [
        "EXIT_ID",
        "EXIT_TYPE",
        "SELL_PRICE",
        "VOLUME_TO_SOLD",
        "SELL_VALUE",
        "PROFIT_LOSS",
    ]
    for col in exit_cols:
        pnl_dict[col].append("")

    pnl_dict["ENTRY_ID"].append(entry_id)
    pnl_dict["ENTRY_TYPE"].append(row[OutputColumn.ENTRY_TYPE.value])
    pnl_dict["PURCHASE_PRICE"].append(row[OutputColumn.ENTRY_PRICE.value])
    pnl_dict["PURCHASE_VALUE"].append(
        configs.capital * configs.cash_percent * configs.risk_per_entry_fractal
    )
    pnl_dict["ALLOWED_EXPOSURE"].append(company_row["allowed_exposure"])
    pnl_dict["VOLUME"].append(
        int(
            pnl_dict["PURCHASE_VALUE"][-1]
            / row[OutputColumn.ENTRY_PRICE.value]
        )
    )

    volume_portfolio_metrics(pnl_dict, method=1)

    pnl_dict["PRICE_EXEDED"].append(
        "YES"
        if pnl_dict["CUM_VALUE"][-1] > pnl_dict["ALLOWED_EXPOSURE"][-1]
        else "NO"
    )

    if pnl_dict["PRICE_EXEDED"][-1] == "YES":

        pnl_dict["VOLUME"][-1] = max(
            (
                pnl_dict["VOLUME"][-1]
                - (
                    int(
                        (
                            pnl_dict["CUM_VALUE"][-1]
                            - pnl_dict["ALLOWED_EXPOSURE"][-1]
                        )
                        / row[OutputColumn.ENTRY_PRICE.value]
                    )
                    + 1
                )
            ),
            0,
        )

        volume_portfolio_metrics(pnl_dict, method=2)

    tp_cols = [
        "TP_VOLUME_TO_SOLD",
        "TP_REMAINING_VOLUME",
        "TP_NEED_TO_SELL",
        "TP_VOLUME_SOLDED",
        "TP_NEED_TO_SELL_PRICE",
    ]
    for col in tp_cols:
        pnl_dict[col].append(0)


def volume_portfolio_metrics(pnl_dict, method=1):

    if method == 1:
        pnl_dict["VOLUME_TO_CONSIDER"].append(
            pnl_dict["VOLUME"][-1] * pnl_dict["PURCHASE_PRICE"][-1]
        )

        if "CUM_VALUE" not in pnl_dict:
            pnl_dict["CUM_VALUE"].append(pnl_dict["VOLUME_TO_CONSIDER"][-1])
        else:
            pnl_dict["CUM_VALUE"].append(
                pnl_dict["CUM_VALUE"][-1] + pnl_dict["VOLUME_TO_CONSIDER"][-1]
            )

        if "CUM_VOLUME" not in pnl_dict:
            pnl_dict["CUM_VOLUME"].append(pnl_dict["VOLUME"][-1])
        else:
            pnl_dict["CUM_VOLUME"].append(
                pnl_dict["CUM_VOLUME"][-1] + pnl_dict["VOLUME"][-1]
            )

        pnl_dict["WEIGHTED_AVG"].append(
            pnl_dict["CUM_VALUE"][-1] / pnl_dict["CUM_VOLUME"][-1]
        )

    elif method == 2:

        pnl_dict["VOLUME_TO_CONSIDER"][-1] = (
            pnl_dict["VOLUME"][-1] * pnl_dict["PURCHASE_PRICE"][-1]
        )

        pnl_dict["CUM_VALUE"][-1] = (
            pnl_dict["CUM_VALUE"][-2] + pnl_dict["VOLUME_TO_CONSIDER"][-1]
        )

        pnl_dict["CUM_VOLUME"][-1] = (
            pnl_dict["CUM_VOLUME"][-2] + pnl_dict["VOLUME"][-1]
        )

        pnl_dict["WEIGHTED_AVG"][-1] = (
            pnl_dict["CUM_VALUE"][-1] / pnl_dict["CUM_VOLUME"][-1]
        )


def process_exit(name, row, pnl_dict: dict[List], configs: Configs):
    logger.info(
        f"{process_exit.__name__}: Processing exit for {name} at {row['DATETIME']}"
    )

    pnl_dict["TYPE"].append("EXIT")

    entry_cols = [
        "ENTRY_ID",
        "ENTRY_TYPE",
        "PURCHASE_PRICE",
        "PURCHASE_VALUE",
        "VOLUME",
        "VOLUME_TO_CONSIDER",
        "ALLOWED_EXPOSURE",
        "PRICE_EXEDED",
    ]
    for col in entry_cols:
        pnl_dict[col].append(0)

    try:
        cum_value = pnl_dict["CUM_VALUE"][-1]
    except IndexError:
        cum_value = 0

    pnl_dict["EXIT_ID"].append(row[OutputColumn.EXIT_ID.value])
    pnl_dict["EXIT_TYPE"].append(row[OutputColumn.EXIT_TYPE.value])
    pnl_dict["SELL_PRICE"].append(row[OutputColumn.EXIT_PRICE.value])
    pnl_dict["VOLUME_TO_SOLD"].append(cum_value)
    pnl_dict["SELL_VALUE"].append(
        pnl_dict["VOLUME_TO_SOLD"][-1] * row[OutputColumn.EXIT_PRICE.value]
    )
    pnl_dict["PROFIT_LOSS"].append(
        pnl_dict["SELL_VALUE"][-1] - cum_value
    )

    tp_cols = [
        "TP_VOLUME_TO_SOLD",
        "TP_REMAINING_VOLUME",
        "TP_NEED_TO_SELL",
        "TP_VOLUME_SOLDED",
        "TP_NEED_TO_SELL_PRICE",
    ]
    for col in tp_cols:
        pnl_dict[col].append(0)

    try:
        cum_value = pnl_dict["CUM_VALUE"][-1]
    except IndexError:
        cum_value = 0

    try:
        cum_volume = pnl_dict["CUM_VOLUME"][-1]
    except IndexError:
        cum_volume = 0

    pnl_dict["WEIGHTED_AVG"].append(0)
    pnl_dict["CUM_VALUE"].append(cum_value)
    pnl_dict["CUM_VOLUME"].append(cum_volume)

    if pnl_dict["EXIT_TYPE"][-1] == TradeExitType.TP.value:

        volume_to_sold = pnl_dict["CUM_VOLUME"][-2] * (
            configs.open_volume_percent / 100
        )

        idx = -2
        while (
            pnl_dict["EXIT_ID"][idx] == "" and pnl_dict["ENTRY_ID"][idx] != 1
        ):
            idx -= 1

        rem_volume, rem_price = 0, 0
        while pnl_dict["EXIT_TYPE"][idx] != TradeExitType.TP.value:

            if pnl_dict["ENTRY_ID"][idx] == 1:
                pnl_dict["TP_VOLUME_TO_SOLD"][idx] = volume_to_sold
            else:
                pnl_dict["TP_VOLUME_TO_SOLD"][idx] = pnl_dict[
                    "TP_REMAINING_VOLUME"
                ][idx - 1]

            pnl_dict["TP_REMAINING_VOLUME"][idx] = (
                pnl_dict["TP_VOLUME_TO_SOLD"][idx] - pnl_dict["VOLUME"][idx]
            )

            pnl_dict["TP_NEED_TO_SELL"][idx] = (
                pnl_dict["TP_REMAINING_VOLUME"][idx] * -1
                if pnl_dict["TP_REMAINING_VOLUME"][idx] < 0
                else 0
            )

            if pnl_dict["TP_NEED_TO_SELL"][idx] > 0:
                rem_volume += pnl_dict["TP_NEED_TO_SELL"][idx]
                rem_price += pnl_dict["WEIGHTED_AVG"][idx]

            pnl_dict["TP_VOLUME_SOLDED"][idx] = pnl_dict["VOLUME"][idx]

            pnl_dict["TP_NEED_TO_SELL_PRICE"][idx] = (
                pnl_dict["WEIGHTED_AVG"][idx]
                * pnl_dict["TP_NEED_TO_SELL"][idx]
            )

            idx += 1

        if rem_volume > 0 and rem_price > 0:
            pnl_dict["WEIGHTED_AVG"][-1] = rem_price
            pnl_dict["CUM_VALUE"][-1] = rem_volume * rem_price
            pnl_dict["CUM_VOLUME"][-1] = rem_volume
        else:
            pnl_dict["WEIGHTED_AVG"][-1] = pnl_dict["WEIGHTED_AVG"][-2]
            pnl_dict["CUM_VALUE"][-1] = pnl_dict["CUM_VALUE"][-2]
            pnl_dict["CUM_VOLUME"][-1] = pnl_dict["CUM_VOLUME"][-2]


def update_company_base_df(company_df: pd.DataFrame, configs: Configs):
    """Update the company base DataFrame with the required columns"""
    assign_unique_ids(company_df)
    update_risk_per_stock(company_df)
    update_category_risk_total(company_df)
    update_allowed_exposure(company_df, configs)
    logger.info("Company base df updated")
    write_dataframe_to_csv(
        company_df, PORTFOLIO_COMPANY_OUTPUT_FOLDER, "company_base.csv"
    )


def update_allowed_exposure(company_df, configs: Configs):
    company_df["allowed_exposure"] = (
        configs.capital * configs.cash_percent * company_df["risk_per_stock"]
    )


def update_category_risk_total(company_df):
    company_df["category_risk_total"] = 0
    for _, group in company_df.groupby(["Date", "Category"]):
        company_df.loc[group.index, "category_risk_total"] = group[
            "risk_per_stock"
        ].sum()


def update_risk_per_stock(company_df):
    for _, group in company_df.groupby("Date"):
        company_df.loc[group.index, "risk_per_stock"] = round(
            group["Name of Company"].nunique() / 100, 2
        )


def assign_unique_ids(company_df: pd.DataFrame):
    company_id_map = {}
    unique_id = 1
    prev_date = None
    unique_ids = []

    for date, group in company_df.groupby("Date"):
        company_id_map[date] = {}
        current_ids = []
        for company in group["Name of Company"]:
            if not prev_date or company not in company_id_map[prev_date]:
                company_id_map[date][company] = unique_id
                unique_id += 1
            else:
                company_id_map[date][company] = company_id_map[prev_date][
                    company
                ]

            current_ids.append(company_id_map[date][company])
        unique_ids.extend(current_ids)

        # free up memory
        if prev_date:
            del company_id_map[prev_date]
        prev_date = date

    company_df["Unique ID"] = unique_ids
