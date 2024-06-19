import os
import time
from datetime import datetime

import pandas as pd
import numpy as np
from tradesheet.constants import DATE, InputCols, FUTURE_FILE_PREFIX, FUTURE_FILE_PATH, ExpiryCols, EXPIRY_FILE, \
    ONLY_DATE, DTE_COL, EXPIRY_COL, StrikeDiffCols, LOT_FILE, LOT_SIZE, InputFileCols, OutputCols, HedgeCols, \
    OUTPUT_PATH
from tradesheet.src.base import TradeSheetGenerator
from tradesheet.src.cash import CashSegment
from tradesheet.src.option import OptionSegment
from tradesheet.utils import int_to_roman, get_bool


class FutureSegment(TradeSheetGenerator):
    dir_path = FUTURE_FILE_PATH
    output_file_name = f"{OUTPUT_PATH}future_output"

    def __init__(self, input_data, ee_df, strategy_pair="", instrument=""):
        super().__init__(input_data, ee_df, strategy_pair, instrument)
        self.delay_exit = True
        self.cash_db_df = CashSegment(input_data, ee_df, strategy_pair, instrument).read_csv_files_in_date_range()
        self.cash_db_df[DATE] = pd.to_datetime(self.cash_db_df['Date'] + ' ' + self.cash_db_df['Time']).dt.floor('min')

        # self.hedge_calculation(input_data)

        if not self.expiry:
            raise Exception("Please provide expiry date")

        self.expiry_column = f"{ExpiryCols.EXPIRY_PREFIX}{str(self.expiry).zfill(2)}"
        self.next_expiry_column = None
        if self.is_next_expiry and self.expiry != self.next_expiry:
            self.next_expiry_column = f"{ExpiryCols.EXPIRY_PREFIX}{str(self.next_expiry).zfill(2)}"

        self.expiry_df = self.read_expiry_data(ExpiryCols, EXPIRY_FILE)
        self.lot_df = self.read_expiry_data(StrikeDiffCols, LOT_FILE)

        self.ee_df[ONLY_DATE] = pd.to_datetime(self.ee_df[InputCols.ENTRY_DT]).dt.date
        df_merged = pd.merge(self.ee_df, self.expiry_df, left_on=ONLY_DATE, right_on=ExpiryCols.DATE, how="left")
        df_merged = pd.merge(df_merged, self.lot_df, left_on=ONLY_DATE, right_on=StrikeDiffCols.DATE, how="left")

        # Calculate DTE
        # column_x for expiry df and column_y for lot size df
        df_merged[DTE_COL] = (df_merged[f'{self.expiry_column}_x'].dt.date - df_merged[InputCols.ENTRY_DT].dt.date).apply(lambda x: x.days)

        # Rename columns of expiry and lot size
        if not self.is_next_expiry:
            df_merged.rename(columns={f'{self.expiry_column}_x': EXPIRY_COL, f'{self.expiry_column}_y': LOT_SIZE}, inplace=True)
        else:
            df_merged[EXPIRY_COL] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), df_merged[f'{self.expiry_column}_x'], df_merged[f'{self.next_expiry_column}_x'])
            df_merged[LOT_SIZE] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), df_merged[f'{self.expiry_column}_y'], df_merged[f'{self.next_expiry_column}_y'])

        # Filter df by DTE
        if self.dte_based_trading and self.dte:
            df_merged = df_merged[df_merged[DTE_COL].isin(self.dte)]

        # Drop extra columns
        drop_columns_list = [
            f'{self.expiry_column}_x',
            f'{self.next_expiry_column}_x',
            f'{self.expiry_column}_y',
            f'{self.next_expiry_column}_y',
            ExpiryCols.DATE,
            ExpiryCols.SYMBOL,
            StrikeDiffCols.DATE,
            StrikeDiffCols.SYMBOL,
            ONLY_DATE
        ]
        df_merged.drop(columns=drop_columns_list, errors='ignore', inplace=True)
        self.ee_df = df_merged

    def iterate_dir_month_wise(self, month_dir):
        df_list = []
        # Iterate through the files in the month directory
        for date_dir in os.listdir(month_dir):
            try:
                file_date = datetime.strptime(date_dir, '%d%m%Y')
            except ValueError:
                continue
            # Check if the file date is within the specified range
            if self.start_date <= file_date <= self.end_date:
                file_name = FUTURE_FILE_PREFIX.format(self.symbol, int_to_roman(self.expiry), date_dir)
                file_path = os.path.join(month_dir, date_dir, file_name)
                df_list.append(pd.read_csv(file_path))
        return df_list

    def hedge_calculation(self, input_data):
        self.is_hedge = get_bool(input_data.pop("Hedge", False))
        self.hadge_df = None
        if self.is_hedge:
            hedge_expiry = input_data["Hedge Expiry"]
            hedge_strike = input_data["Hedge Strike"]
            hedge_delay_exit = get_bool(input_data["Hedge Delayed Exit"])
            hedge_input = {
                **input_data,
                InputFileCols.EXPIRY: hedge_expiry,
                InputFileCols.STRIKE: hedge_strike,
                InputFileCols.DELAYED_EXIT: hedge_delay_exit,
                InputFileCols.PREMIUM: False,
                InputFileCols.VOLUME: False,
                InputFileCols.CAPITAL: None
            }
            self.hedge_df = OptionSegment(hedge_input, hedge=True).generate_result_df()

            rename_cols = {
                OutputCols.TICKER: HedgeCols.TICKER,
                OutputCols.ENTRY_PRICE: HedgeCols.ENTRY_PRICE,
                OutputCols.EXIT_PRICE: HedgeCols.EXIT_PRICE,
                OutputCols.RE_AD_ENTRY_PRICE: HedgeCols.RE_ENTRY_PRICE,
                OutputCols.RE_AD_EXIT_PRICE: HedgeCols.RE_EXIT_PRICE,
            }
            self.hedge_df = self.hedge_df[[*rename_cols.keys(), InputCols.ENTRY_DT]]
            self.hedge_df[HedgeCols.P_L] = (self.hedge_df[OutputCols.EXIT_PRICE] - self.hedge_df[OutputCols.ENTRY_PRICE])
            self.hedge_df[HedgeCols.RE_P_L] = (self.hedge_df[OutputCols.RE_AD_EXIT_PRICE] - self.hedge_df[OutputCols.RE_AD_ENTRY_PRICE])
            self.hedge_df.rename(columns=rename_cols, inplace=True)

    def generate_trade_sheet(self):
        future_db_df = self.read_csv_files_in_date_range()
        if future_db_df is not None:
            future_db_df[DATE] = pd.to_datetime(future_db_df['Date'] + ' ' + future_db_df['Time']).dt.floor('min')
            results = []
            for index, row in self.ee_df.iterrows():
                output = {**self.result}
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                filtered_df = future_db_df[(future_db_df[DATE] >= entry_dt) & (future_db_df[DATE] <= exit_dt)]
                filtered_df = filtered_df.reset_index(drop=True)
                output, future_db_df = self.iterate_signal(future_db_df, filtered_df, row, output, entry_dt, exit_dt, lot_size=int(row[LOT_SIZE]))
                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            # if self.hedge_df is not None:
            #     result_df = pd.merge(result_df, self.hedge_df, on=InputCols.ENTRY_DT)
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)

