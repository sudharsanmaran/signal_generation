import datetime
import os
import time

import pandas as pd
import numpy as np

from tradesheet.constants import ExpiryCols, EXPIRY_FILE, STOCKS_EXPIRY_FILE, LOT_FILE, STOCKS_LOT_FILE, StrikeDiffCols, \
    STRIKE_FILE, STOCKS_STRIKE_FILE, \
    ONLY_DATE, EXIT_DATE, InputCols, EXIT_EXPIRY, DTE_COL, EXPIRY_COL, LOT_SIZE, STRIKE_DIFF, EXPIRY_NUMBER_COL, \
    OPTION_FILE_NAME, DATE, CashCols
from tradesheet.src.cash import CashSegment


class OptionMixin:

    def __init__(self, input_data, ee_df, strategy_pair="", instrument=""):
        super().__init__(input_data, ee_df, strategy_pair, instrument)
        missing_expiry_df = pd.read_csv(self.missing_file_path)
        missing_expiry_df = missing_expiry_df[missing_expiry_df["Ticker"] == instrument]
        missing_dates = pd.to_datetime(missing_expiry_df["Missing Date"]).dt.date
        self.ee_df = self.ee_df[~(self.ee_df[InputCols.ENTRY_DT].dt.date.isin(missing_dates))]
        self.date_expiry_tracker = {}
        self.hedge_date_expiry_tracker = {}
        self.expiry_data = {}  # to store expiry, strike and lot size data
        from tradesheet.src.option import OptionSegment
        self.cash_db_df = CashSegment(input_data, ee_df, strategy_pair, instrument).read_csv_files_in_date_range()
        self.is_option = isinstance(self, OptionSegment)

        if not self.expiry:
            raise Exception("Please provide expiry date")

        self.expiry_column = self.get_expiry_column_name(self.expiry)
        self.next_expiry_column = None
        if self.is_next_expiry and self.expiry != self.next_expiry:
            self.next_expiry_column = self.get_expiry_column_name(self.next_expiry)

        self.expiry_df = self.read_expiry_data(ExpiryCols, [EXPIRY_FILE, STOCKS_EXPIRY_FILE], parse_date=True)
        self.lot_df = self.read_expiry_data(StrikeDiffCols, [LOT_FILE, STOCKS_LOT_FILE])
        if self.is_hedge or self.is_option:
            self.strike_df = self.read_expiry_data(StrikeDiffCols, [STRIKE_FILE, STOCKS_STRIKE_FILE]) 

        self.ee_df[ONLY_DATE] = pd.to_datetime(self.ee_df[InputCols.ENTRY_DT]).dt.date
        self.ee_df[EXIT_DATE] = pd.to_datetime(self.ee_df[InputCols.EXIT_DT]).dt.date
        df_merged = pd.merge(self.ee_df, self.expiry_df[[ExpiryCols.DATE, self.expiry_column]], left_on=EXIT_DATE,
                             right_on=ExpiryCols.DATE, how="inner")
        df_merged.rename(columns={f'{self.expiry_column}': EXIT_EXPIRY}, inplace=True)
        df_merged = pd.merge(df_merged, self.expiry_df[self.use_cols(ExpiryCols)], left_on=ONLY_DATE,
                             right_on=ExpiryCols.DATE, how="inner")
        df_merged = pd.merge(df_merged, self.lot_df[self.use_cols(StrikeDiffCols)], left_on=ONLY_DATE,
                             right_on=StrikeDiffCols.DATE, how="inner")

        if self.is_option:
            df_merged = pd.merge(df_merged, self.strike_df[self.use_cols(StrikeDiffCols)], left_on=ONLY_DATE,
                                 right_on=StrikeDiffCols.DATE, how="inner")

        # Calculate DTE
        # column_x for expiry df and column_y for lot size df
        df_merged[DTE_COL] = (df_merged[f'{self.expiry_column}_x'] - df_merged[InputCols.ENTRY_DT].dt.date).apply(
            lambda x: x.days)

        # Rename columns of expiry and lot size
        if not self.is_next_expiry:
            rename_cols = {f'{self.expiry_column}_x': EXPIRY_COL, f'{self.expiry_column}_y': LOT_SIZE}
            if self.is_option:
                rename_cols[self.expiry_column] = STRIKE_DIFF
            df_merged.rename(columns=rename_cols, inplace=True)
        else:
            df_merged[EXPIRY_NUMBER_COL] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), self.expiry,
                                                    self.next_expiry)
            df_merged[EXPIRY_COL] = np.where(df_merged[DTE_COL] > int(self.from_which_dte),
                                             df_merged[f'{self.expiry_column}_x'],
                                             df_merged[f'{self.next_expiry_column}_x'])
            df_merged[LOT_SIZE] = np.where(df_merged[DTE_COL] > int(self.from_which_dte),
                                           df_merged[f'{self.expiry_column}_y'],
                                           df_merged[f'{self.next_expiry_column}_y'])
            if self.is_option:
                df_merged[STRIKE_DIFF] = np.where(df_merged[DTE_COL] > int(self.from_which_dte),
                                                  df_merged[f'{self.expiry_column}'],
                                                  df_merged[f'{self.next_expiry_column}'])

        # Filter df by DTE
        if self.dte_based_trading and self.dte:
            df_merged = df_merged[df_merged[DTE_COL].isin(self.dte)]

        # Drop extra columns
        drop_columns_list = [
            f'{self.expiry_column}_x',
            f'{self.next_expiry_column}_x',
            f'{self.expiry_column}_y',
            f'{self.next_expiry_column}_y',
            f'{self.expiry_column}',
            f'{self.next_expiry_column}',
            f"{ExpiryCols.DATE}_x",
            f"{ExpiryCols.DATE}_y",
            f"{ExpiryCols.SYMBOL}_x",
            f"{ExpiryCols.SYMBOL}_y",
            f"{StrikeDiffCols.DATE}_x",
            f"{StrikeDiffCols.SYMBOL}_x",
            f"{StrikeDiffCols.DATE}_y",
            f"{StrikeDiffCols.SYMBOL}_y",
            EXIT_DATE,
        ]

        df_merged.drop(columns=drop_columns_list, errors='ignore', inplace=True)
        self.ee_df = df_merged

    def remove_previous_date_data(self, current_date, previous_date):
        if previous_date and previous_date < current_date and not self.segment_df.empty:
            s_t = time.time()
            self.segment_df = self.segment_df[self.segment_df[DATE].dt.date > previous_date]
            self.cash_db_df = self.cash_db_df[self.cash_db_df[DATE].dt.date > previous_date]
            if hasattr(self, "hedge_df") and not self.hedge_df.empty:
                self.hedge_df = self.hedge_df[self.hedge_df[DATE].dt.date > previous_date]
            self.date_expiry_tracker.pop(previous_date, None)
            self.expiry_data.pop(previous_date, None)
            # print("REMOVING", time.time() - s_t)

    # def set_expiry_data(self, current_date):
    #     # E: Expiry, L: Lot size, S: Strike
    #     if current_date not in self.expiry_data:
    #         # TODO: use update method
    #         self.expiry_data.setdefault(current_date, {})
    #         self.expiry_data[current_date]["E"] = self.expiry_df[self.expiry_df[ExpiryCols.DATE] == current_date].to_dict(orient="records")[0]
    #         self.expiry_data[current_date]["L"] = self.lot_df[self.lot_df[StrikeDiffCols.DATE] == current_date].to_dict(orient="records")[0]
    #         if self.is_option or self.is_hedge:
    #             self.expiry_data[current_date]["S"] = self.strike_df[self.strike_df[StrikeDiffCols.DATE] == current_date].to_dict(orient="records")[0]

    def set_expiry_data(self, current_date):
        # E: Expiry, L: Lot size, S: Strike
        if current_date not in self.expiry_data:
            # TODO: use update method
            data = {
                "E": self.expiry_df[self.expiry_df[ExpiryCols.DATE] == current_date].to_dict(orient="records")[0],
                "L": self.lot_df[self.lot_df[StrikeDiffCols.DATE] == current_date].to_dict(orient="records")[0]
            }
            if self.is_option or self.is_hedge:
                data.update({"S": self.strike_df[self.strike_df[StrikeDiffCols.DATE] == current_date].to_dict(orient="records")[0]})
            self.expiry_data.update({current_date: data})

    def get_option_file_name(self, file_date: datetime.date, expiry_date: datetime.date) -> tuple:
        """
        :param file_date: date object of date file name e.g. 01112022(1/11/2022)
        :param expiry_date: expiry date or None
        :return: file name

        If expiry date is given then take it otherwise take expiry date from column
        """
        if not expiry_date:
            expiry_date = self.ee_df[self.ee_df[ONLY_DATE] == file_date][EXPIRY_COL].to_list()[0]
        if expiry_date:
            return OPTION_FILE_NAME.format(self.symbol, expiry_date.strftime("%d%b%y").upper()), expiry_date
        return None, None

    def iterate_option_dir_month_wise(self, month_dir, start_date, end_date, expiry_dt=None, **kwargs):
        df_list = []
        dr = kwargs.get("date_range")
        date_list = [d.strftime('%d%m%Y') for d in sorted(dr)] if dr else os.listdir(month_dir)
        # this is different from self.is_hedge. THis variable is to check whether file
        # is being read for option segment or hedge in future segment
        is_hedge = kwargs.get("hedge", False)
        # Iterate through the files in the month directory
        for date_dir in date_list:
            try:
                file_date = pd.to_datetime(date_dir, format='%d%m%Y').date()
                # Check if the file date is within the specified range
                if not (expiry_date := expiry_dt):
                    expiry_date = self.ee_df[self.ee_df[ONLY_DATE] == file_date][EXPIRY_COL].to_list()[0]
                if start_date <= file_date <= end_date and expiry_date:
                    expiry_file_name = OPTION_FILE_NAME.format(self.symbol, expiry_date.strftime("%d%b%y").upper())
                    file_path = os.path.join(month_dir, date_dir, expiry_file_name)
                    df = pd.read_csv(file_path)
                    df_list.append(df)
                    if is_hedge:
                        self.hedge_date_expiry_tracker.setdefault(file_date, [])
                        self.hedge_date_expiry_tracker[file_date].append(expiry_date)
                    else:
                        self.date_expiry_tracker.setdefault(file_date, [])
                        self.date_expiry_tracker[file_date].append(expiry_date)
            except:
                continue
        return df_list

    def get_delayed_price(self, current_date, expiry_date, find_str, expiry_str, expiry_to_consider):
        """
        Delayed Exit: To check for the first candle after signal end till expiry date.
        SO if signal from 1/11/2024 9:15 to 1/11/2024 9:34 and expiry date is 3/11/2024, Then
        if candle not found at signal exit i.e. at 1/11/2024 9:34 then we will check for delayed exit
        on first record starting from 1/11/2024 9:35 to last timestamp of 3/11/2024.
        """
        # Filter df after last candle of signal for the same date.
        new_df = self.segment_df[(self.segment_df[DATE] > current_date) & (
                    self.segment_df[DATE].dt.date == current_date.date()) & (
                                     self.segment_df[CashCols.TICKER].str.contains(find_str))]

        date_idx = 1
        date_ranges = pd.date_range(current_date.date(), expiry_date).date
        exit_record = None
        while date_idx <= len(date_ranges):
            if not new_df.empty:
                # else exit on first candle of filtered df.
                exit_record = new_df.iloc[0]
                break
            try:
                next_date = date_ranges[date_idx]
            except:
                break

            if expiry_to_consider not in self.date_expiry_tracker.get(next_date, []):
                dt_format = next_date.strftime('%d%m%Y')
                file_path = self.get_file_path(next_date, expiry_str, dt_format)
                if os.path.exists(file_path):
                    new_df = pd.read_csv(file_path)
                    new_df[DATE] = pd.to_datetime(new_df['Date'] + ' ' + new_df['Time'],
                                                  format=self.db_date_format).dt.floor('min')
                    self.date_expiry_tracker.setdefault(next_date, [])
                    self.date_expiry_tracker[next_date].append(expiry_to_consider)
                    self.segment_df = pd.concat([self.segment_df, new_df], ignore_index=True)
                    new_df = new_df[(new_df[DATE].dt.date == next_date) &
                                    (new_df[CashCols.TICKER].str.contains(find_str))]
            else:
                new_df = self.segment_df[(self.segment_df[DATE].dt.date == next_date) &
                                         (self.segment_df[CashCols.TICKER].str.contains(find_str))]

            date_idx += 1
        return exit_record