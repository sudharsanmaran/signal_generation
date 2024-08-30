from collections import defaultdict
import logging
from typing import Dict, List

import pandas as pd
from portfolio.constants import PNLSummaryCols
from portfolio.data_reader import read_signal_gen_file
from portfolio.validation import Configs, InputData
from source.constants import (
    PORTFOLIO_COMPANY_OUTPUT_FOLDER,
    PORTFOLIO_PNL_OUTPUT_FOLDER, OutputColumn, TradeExitType
)
from source.utils import write_dataframe_to_csv


logger = logging.getLogger(__name__)


def process_portfolio(validated_data: InputData):

    pd.set_option('display.max_rows', None)

    update_company_base_df(
        validated_data.companies_df,
        validated_data.configs
    )

    company_sg_df_map = construct_company_signal_dictionary(validated_data)

    pnl_dict = defaultdict(list)
    out_of_list, prev_day_companies = set(), set()
    prev_day_companies_pnl = {}
    prev_group_df = pd.DataFrame()
    for group_id, group_data in validated_data.companies_df.groupby("Date"):
        group_data.index = group_data["Name of Company"]

        if len(prev_day_companies) > 0:
            out_of_list = prev_day_companies - set(
                group_data["Name of Company"])

        prev_day_companies = set(group_data["Name of Company"])

        for company in out_of_list:
            company_row = prev_group_df.loc[company]

            ticker = fetch_ticker(validated_data.company_tickers, company)

            update_common_record(
                pnl_dict, company_row,
                pd.Timestamp(f"{group_id} 09:15:00"), ticker
            )
            process_out_of_list_exit(
                company, pnl_dict,
                cum_value=prev_day_companies_pnl.get(company, 0)
            )

        prev_group_df = group_data

        for company in group_data["Name of Company"].unique().tolist():
            sg_df = company_sg_df_map.get(company, pd.DataFrame())
            if sg_df.empty:
                logger.error(
                    f"{process_portfolio.__name__}: no signal gen file found for {company}")
                continue

            # filter the data based on the group_id
            sg_df = sg_df[sg_df['DATETIME'].dt.date == group_id.date()]
            if sg_df.empty:
                logger.error(
                    f"{process_portfolio.__name__}: empty signal gen df for {company} for {group_id}")
                continue

            company_row = group_data.loc[company]
            entry_id = 0
            for name, row in sg_df.iterrows():

                if row["DATETIME"] < pd.Timestamp("2023-03-17 13:20:00"):
                    a = 1

                update_common_record(
                    pnl_dict, company_row, row['DATETIME'],
                    row[OutputColumn.INSTRUMENT.value]
                )

                if row['DATETIME_TYPE'] == OutputColumn.ENTRY_DATETIME.value:
                    entry_id += 1
                    process_entry(name, row, pnl_dict, validated_data.configs,
                                  company_row=company_row, entry_id=entry_id)
                else:
                    entry_id = 0
                    process_exit(name, row, pnl_dict, validated_data.configs)

                if sg_df.index[-1] == name:
                    prev_day_companies_pnl[company] = pnl_dict["CUM_VALUE"][-1]

    # find diff length in pnl_dict list
    for key in pnl_dict:
        if len(pnl_dict[key]) != len(pnl_dict['DATETIME']):
            a = 1

    pnl_df = pd.DataFrame(pnl_dict)

    # sort based on datetime
    # pnl_df = pnl_df.sort_values(by='DATETIME').reset_index(drop=True)

    # pnl_df.set_index('DATETIME', inplace=True)

    summary_df = formulate_daily_pnl_summary(
        validated_data.companies_df, pnl_df, validated_data.company_tickers
    )

    write_dataframe_to_csv(
        pnl_df, PORTFOLIO_PNL_OUTPUT_FOLDER,
        f"{validated_data.companies_data.segment}_{
            validated_data.companies_data.parameter_id}_pnl.csv"
    )


def fetch_ticker(company_tickers, company):
    try:
        ticker = company_tickers.loc[company]["Ticker Symbol"]
    except KeyError:
        ticker = "NA"
    return ticker


def formulate_daily_pnl_summary(company_df, pnl_df, company_tickers):

    pnl_df['Date'] = pnl_df['DATETIME'].dt.date
    pnl_df.set_index('Date', inplace=True)

    summary_dict = defaultdict(list)
    for date, group in company_df.groupby('Date'):
        group.index = group['Name of Company']
        for company in group.index:
            ticker = fetch_ticker(company_tickers, company)
            pnl_mask = (pnl_df.index == date.date()) & (
                pnl_df['COMPANY'] == ticker)
            pnl = pnl_df.loc[pnl_mask]

            if pnl.empty:
                logger.error(
                    f"{formulate_daily_pnl_summary.__name__}: no pnl found for {company} on {date}")

            summary_dict[PNLSummaryCols.DATE.value].append(date)
            summary_dict[PNLSummaryCols.COMPANY.value].append(ticker)

            for type, group in pnl.groupby('TYPE'):
                if type == "ENTRY":
                    populate_entry_data(summary_dict, group)
                elif type == "EXIT":
                    pass

    return pd.DataFrame(summary_dict)


def populate_exit_data(summary_dict, group: pd.DataFrame):
    if group.empty:
        summary_values = {
            PNLSummaryCols.EXITS.value: "NO",
            PNLSummaryCols.OPEN_POSITION.value: "NO",
            PNLSummaryCols.TRADE_ID.value: 0,
            PNLSummaryCols.OPEN_VOLUME.value: 0,
            PNLSummaryCols.OPEN_EXPOSURE_COST.value: 0,
            PNLSummaryCols.OPEN_EXPOSURE_PERCENT.value: 0,
        }


def populate_entry_data(summary_dict, group: pd.DataFrame):
    if group.empty:
        summary_values = default_entry_data()
    else:
        summary_values = {
            PNLSummaryCols.INITIATED_POSITION.value: "YES",
            PNLSummaryCols.NO_OF_ENTRIES_FOR_THE_DAY.value: group["ENTRY_ID"].ne("").sum(),
            PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: group['WEIGHTED_AVG'].iloc[-1],
            PNLSummaryCols.TOTAL_VOLUME.value: group['CUM_VOLUME'].iloc[-1],
            PNLSummaryCols.TOTAL_AMOUNT.value: group['CUM_VALUE'].iloc[-1],
        }

    for key, value in summary_values.items():
        summary_dict[key].append(value)


def default_entry_data():
    return {
        PNLSummaryCols.INITIATED_POSITION.value: "NO",
        PNLSummaryCols.NO_OF_ENTRIES_FOR_THE_DAY.value: 0,
        PNLSummaryCols.WEIGHTED_AVERAGE_PRICE.value: 0,
        PNLSummaryCols.TOTAL_VOLUME.value: 0,
        PNLSummaryCols.TOTAL_AMOUNT.value: 0,
    }


def update_common_record(pnl_dict, company_row, date_time, instrument):
    pnl_dict["DATETIME"].append(date_time)
    pnl_dict["COMPANY"].append(instrument)
    pnl_dict["UNIQUE_ID"].append(
        company_row['Unique ID'])


def construct_company_signal_dictionary(validated_data):
    company_sg_df_map = {}
    for company in validated_data.company_lists:
        if company not in validated_data.company_sg_map:
            logger.error(
                f"{process_portfolio.__name__}: no signal gen file found for {company}")
            continue

        sg_df = read_signal_gen_file(
            validated_data.company_sg_map[company]
        )
        if sg_df.empty:
            continue

        # Reshape the DataFrame to long format
        df_long = pd.melt(sg_df,
                          id_vars=[
                              OutputColumn.ENTRY_PRICE.value,
                              OutputColumn.EXIT_PRICE.value,
                              OutputColumn.ENTRY_TYPE.value,
                              OutputColumn.EXIT_TYPE.value,
                              OutputColumn.ENTRY_ID.value,
                              OutputColumn.EXIT_ID.value,
                              OutputColumn.INSTRUMENT.value
                          ],
                          value_vars=[
                              OutputColumn.ENTRY_DATETIME.value,
                              OutputColumn.EXIT_DATETIME.value
                          ],
                          var_name='DATETIME_TYPE',
                          value_name='DATETIME')

        # Separate the DataFrame into Entry and Exit types
        df_entry = df_long[df_long['DATETIME_TYPE']
                           == OutputColumn.ENTRY_DATETIME.value]
        df_exit = df_long[df_long['DATETIME_TYPE']
                          == OutputColumn.EXIT_DATETIME.value]

        # Drop duplicates within each type
        df_entry = df_entry.drop_duplicates(subset=['DATETIME'])
        df_exit = df_exit.drop_duplicates(subset=['DATETIME'])

        # Concatenate the entry and exit DataFrames back together
        df_cleaned = pd.concat([df_exit, df_entry]).sort_values(
            by='DATETIME').reset_index(drop=True)

        company_sg_df_map[company] = df_cleaned
    return company_sg_df_map


def process_out_of_list_exit(company, pnl_dict, cum_value):
    logger.info(f"{process_out_of_list_exit.__name__}: Processing out of list exit for {
                company}")

    entry_cols = [
        'ENTRY_ID', 'ENTRY_TYPE', 'PURCHASE_PRICE',
        'PURCHASE_VALUE', 'VOLUME', 'VOLUME_TO_CONSIDER',
        'ALLOWED_EXPOSURE', 'PRICE_EXEDED',
    ]
    for col in entry_cols:
        pnl_dict[col].append("")

    day_close = 1234
    pnl_dict['EXIT_ID'].append(1)  # todo: check
    pnl_dict['EXIT_TYPE'].append(TradeExitType.OUT_OF_LIST.value)
    pnl_dict["SELL_PRICE"].append(day_close)
    pnl_dict["VOLUME_TO_SOLD"].append(cum_value)
    pnl_dict["SELL_VALUE"].append(
        pnl_dict["VOLUME_TO_SOLD"][-1] * day_close
    )

    try:
        cum_value = pnl_dict["CUM_VALUE"][-1]
    except IndexError:
        cum_value = 0

    try:
        cum_volume = pnl_dict["CUM_VOLUME"][-1]
    except IndexError:
        cum_volume = 0

    pnl_dict["PROFIT_LOSS"].append(
        pnl_dict["SELL_VALUE"][-1] - cum_value)

    pnl_dict["WEIGHTED_AVG"].append(0)
    pnl_dict["CUM_VALUE"].append(cum_value)
    pnl_dict["CUM_VOLUME"].append(cum_volume)

    pnl_dict["TP_VOLUME_TO_SOLD"].append(0)
    pnl_dict["TP_REMAINING_VOLUME"].append(0)
    pnl_dict["TP_NEED_TO_SELL"].append(0)
    pnl_dict["TP_VOLUME_SOLDED"].append(0)
    pnl_dict["TP_NEED_TO_SELL_PRICE"].append(0)


def process_entry(
    name, row, pnl_dict: Dict[str, List],
    configs: Configs, company_row, entry_id
):
    logger.info(f"{process_entry.__name__}: Processing entry for {
                name} at {row['DATETIME']}")

    pnl_dict["TYPE"] = "ENTRY"

    exit_cols = [
        'EXIT_ID', 'EXIT_TYPE', 'SELL_PRICE',
        'VOLUME_TO_SOLD', 'SELL_VALUE', 'PROFIT_LOSS'
    ]
    for col in exit_cols:
        pnl_dict[col].append("")

    pnl_dict['ENTRY_ID'].append(entry_id)
    pnl_dict['ENTRY_TYPE'].append(row[OutputColumn.ENTRY_TYPE.value])
    pnl_dict["PURCHASE_PRICE"].append(row[OutputColumn.ENTRY_PRICE.value])
    pnl_dict["PURCHASE_VALUE"].append(
        configs.capital * configs.cash_percent * configs.risk_per_entry_fractal
    )
    pnl_dict["ALLOWED_EXPOSURE"].append(
        company_row['allowed_exposure']
    )
    pnl_dict["VOLUME"].append(
        int(pnl_dict["PURCHASE_VALUE"][-1] / row[OutputColumn.ENTRY_PRICE.value]))

    volume_portfolio_metrics(pnl_dict, method=1)

    pnl_dict["PRICE_EXEDED"].append(
        "YES" if pnl_dict["CUM_VALUE"][-1] > pnl_dict["ALLOWED_EXPOSURE"][-1] else "NO")

    if pnl_dict["PRICE_EXEDED"][-1] == "YES":

        pnl_dict["VOLUME"][-1] = max((
            pnl_dict["VOLUME"][-1] - (
                int(
                    (
                        pnl_dict["CUM_VALUE"][-1] -
                        pnl_dict["ALLOWED_EXPOSURE"][-1]
                    ) / row[OutputColumn.ENTRY_PRICE.value]
                ) + 1
            )
        ), 0)

        volume_portfolio_metrics(pnl_dict, method=2)

    tp_cols = [
        'TP_VOLUME_TO_SOLD', 'TP_REMAINING_VOLUME',
        'TP_NEED_TO_SELL', 'TP_VOLUME_SOLDED',
        'TP_NEED_TO_SELL_PRICE'
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
                pnl_dict["CUM_VALUE"][-1] + pnl_dict["VOLUME_TO_CONSIDER"][-1])

        if "CUM_VOLUME" not in pnl_dict:
            pnl_dict["CUM_VOLUME"].append(pnl_dict["VOLUME"][-1])
        else:
            pnl_dict["CUM_VOLUME"].append(
                pnl_dict["CUM_VOLUME"][-1] + pnl_dict["VOLUME"][-1])

        pnl_dict["WEIGHTED_AVG"].append(
            pnl_dict["CUM_VALUE"][-1] / pnl_dict["CUM_VOLUME"][-1])

    elif method == 2:

        pnl_dict["VOLUME_TO_CONSIDER"][-1] = (
            pnl_dict["VOLUME"][-1] * pnl_dict["PURCHASE_PRICE"][-1]
        )

        pnl_dict["CUM_VALUE"][-1] = (
            pnl_dict["CUM_VALUE"][-2] +
            pnl_dict["VOLUME_TO_CONSIDER"][-1]
        )

        pnl_dict["CUM_VOLUME"][-1] = (
            pnl_dict["CUM_VOLUME"]
            [-2] + pnl_dict["VOLUME"][-1]
        )

        pnl_dict["WEIGHTED_AVG"][-1] = (
            pnl_dict["CUM_VALUE"][-1] / pnl_dict["CUM_VOLUME"][-1]
        )


def process_exit(name, row, pnl_dict, configs: Configs):
    logger.info(f"{process_exit.__name__}: Processing exit for {
                name} at {row['DATETIME']}")

    pnl_dict["TYPE"] = "EXIT"

    entry_cols = [
        'ENTRY_ID', 'ENTRY_TYPE', 'PURCHASE_PRICE',
        'PURCHASE_VALUE', 'VOLUME', 'VOLUME_TO_CONSIDER',
        'ALLOWED_EXPOSURE', 'PRICE_EXEDED',
    ]
    for col in entry_cols:
        pnl_dict[col].append("")

    pnl_dict['EXIT_ID'].append(row[OutputColumn.EXIT_ID.value])
    pnl_dict['EXIT_TYPE'].append(row[OutputColumn.EXIT_TYPE.value])
    pnl_dict["SELL_PRICE"].append(row[OutputColumn.EXIT_PRICE.value])
    pnl_dict["VOLUME_TO_SOLD"].append(
        pnl_dict["CUM_VOLUME"][-1])
    pnl_dict["SELL_VALUE"].append(
        pnl_dict["VOLUME_TO_SOLD"][-1] * row[OutputColumn.EXIT_PRICE.value])
    pnl_dict["PROFIT_LOSS"].append(
        pnl_dict["SELL_VALUE"][-1] - pnl_dict["CUM_VALUE"][-1])

    tp_cols = [
        'TP_VOLUME_TO_SOLD', 'TP_REMAINING_VOLUME',
        'TP_NEED_TO_SELL', 'TP_VOLUME_SOLDED',
        'TP_NEED_TO_SELL_PRICE'
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

        volume_to_sold = (
            pnl_dict["CUM_VOLUME"][-2] * (configs.open_volume_percent / 100)
        )

        idx = -2
        while pnl_dict["EXIT_ID"][idx] == "" and pnl_dict["ENTRY_ID"][idx] != 1:
            idx -= 1

        rem_volume, rem_price = 0, 0
        while pnl_dict["EXIT_TYPE"][idx] != TradeExitType.TP.value:

            if pnl_dict["ENTRY_ID"][idx] == 1:
                pnl_dict["TP_VOLUME_TO_SOLD"][idx] = volume_to_sold
            else:
                pnl_dict["TP_VOLUME_TO_SOLD"][idx] = (
                    pnl_dict["TP_REMAINING_VOLUME"][idx - 1]
                )

            try:
                pnl_dict["TP_REMAINING_VOLUME"][idx] = (
                    pnl_dict["TP_VOLUME_TO_SOLD"][idx] -
                    pnl_dict["VOLUME"][idx]
                )
            except Exception:
                a = 1

            pnl_dict["TP_NEED_TO_SELL"][idx] = (
                pnl_dict["TP_REMAINING_VOLUME"][idx] * -
                1 if pnl_dict["TP_REMAINING_VOLUME"][idx] < 0 else 0
            )

            if pnl_dict["TP_NEED_TO_SELL"][idx] > 0:
                rem_volume += pnl_dict["TP_NEED_TO_SELL"][idx]
                rem_price += pnl_dict["WEIGHTED_AVG"][idx]

            pnl_dict["TP_VOLUME_SOLDED"][idx] = pnl_dict["VOLUME"][idx]

            pnl_dict["TP_NEED_TO_SELL_PRICE"][idx] = (
                pnl_dict["WEIGHTED_AVG"][idx] *
                pnl_dict["TP_NEED_TO_SELL"][idx]
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


def update_company_base_df(company_df, configs: Configs):
    assign_unique_ids(company_df)
    update_risk_per_stock(company_df)
    update_category_risk_total(company_df)
    update_allowed_exposure(company_df, configs)
    logger.info("Company base df updated")
    write_dataframe_to_csv(
        company_df, PORTFOLIO_COMPANY_OUTPUT_FOLDER, "company_base.csv")


def update_allowed_exposure(company_df, configs: Configs):
    company_df["allowed_exposure"] = (
        configs.capital *
        configs.cash_percent * company_df["risk_per_stock"]
    )


def update_category_risk_total(company_df):
    company_df["category_risk_total"] = 0
    for _, group in company_df.groupby(['Date', 'Category']):
        company_df.loc[group.index,
                       'category_risk_total'] = group['risk_per_stock'].sum()


def update_risk_per_stock(company_df):
    for _, group in company_df.groupby('Date'):
        company_df.loc[group.index, "risk_per_stock"] = round(
            group["Name of Company"].nunique() / 100, 2)


def assign_unique_ids(company_df: pd.DataFrame):
    company_id_map = {}
    unique_id = 1
    prev_date = None
    unique_ids = []

    for date, group in company_df.groupby('Date'):
        company_id_map[date] = {}
        current_ids = []
        for company in group['Name of Company']:
            if not prev_date or company not in company_id_map[prev_date]:
                company_id_map[date][company] = unique_id
                unique_id += 1
            else:
                company_id_map[date][company] = company_id_map[prev_date][company]

            current_ids.append(company_id_map[date][company])
        unique_ids.extend(current_ids)

        # free up memory
        if prev_date:
            del company_id_map[prev_date]
        prev_date = date

    company_df['Unique ID'] = unique_ids
