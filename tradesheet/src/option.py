import os
import time

import pandas as pd
import numpy as np
from tradesheet.constants import DATE, InputCols, CashCols, EXPIRY_FILE, STRIKE_FILE, OPTION_FILE_NAME, \
    OPTION_FILE_PATH, ONLY_DATE, \
    STRIKE_DIFF, EXPIRY_COL, DTE_COL, ExpiryCols, StrikeDiffCols, EXIT_DATE, EXIT_EXPIRY, LOT_SIZE, LOT_FILE, \
    InputFileCols, OUTPUT_PATH
from tradesheet.src.base import TradeSheetGenerator
from tradesheet.src.cash import CashSegment


class OptionSegment(TradeSheetGenerator):
    dir_path = OPTION_FILE_PATH
    output_file_name = f"{OUTPUT_PATH}option_output"
    STRIKE_POSTFIX = {
        InputCols.GREEN: "CE",
        InputCols.RED: "PE",
    }

    def __init__(self, input_data, ee_df, strategy_pair="", instrument="", hedge=False):
        super().__init__(input_data, ee_df, strategy_pair, instrument)
        self.hedge = hedge
        self.delay_exit = input_data.get(InputFileCols.DELAYED_EXIT, True)
        self.cash_db_df = CashSegment(input_data, ee_df, strategy_pair, instrument).read_csv_files_in_date_range()
        self.cash_db_df[DATE] = pd.to_datetime(self.cash_db_df['Date'] + ' ' + self.cash_db_df['Time']).dt.floor('min')

        if not self.expiry:
            raise Exception("Please provide expiry date")

        self.expiry_column = f"{ExpiryCols.EXPIRY_PREFIX}{str(self.expiry).zfill(2)}"
        self.next_expiry_column = None
        if self.is_next_expiry and self.expiry != self.next_expiry:
            self.next_expiry_column = f"{ExpiryCols.EXPIRY_PREFIX}{str(self.next_expiry).zfill(2)}"

        self.expiry_df = self.read_expiry_data(ExpiryCols, EXPIRY_FILE)
        self.strike_df = self.read_expiry_data(StrikeDiffCols, STRIKE_FILE)
        self.lot_df = self.read_expiry_data(StrikeDiffCols, LOT_FILE)

        self.ee_df[ONLY_DATE] = pd.to_datetime(self.ee_df[InputCols.ENTRY_DT]).dt.date
        self.ee_df[EXIT_DATE] = pd.to_datetime(self.ee_df[InputCols.EXIT_DT]).dt.date

        df_merged = pd.merge(self.ee_df, self.expiry_df[[ExpiryCols.DATE, self.expiry_column]],  left_on=EXIT_DATE, right_on=ExpiryCols.DATE, how="left")
        df_merged.rename(columns={f'{self.expiry_column}': EXIT_EXPIRY}, inplace=True)
        df_merged = pd.merge(df_merged, self.expiry_df, left_on=ONLY_DATE, right_on=ExpiryCols.DATE, how="left")
        df_merged = pd.merge(df_merged, self.lot_df, left_on=ONLY_DATE, right_on=StrikeDiffCols.DATE, how="left")
        df_merged = pd.merge(df_merged, self.strike_df, left_on=ONLY_DATE, right_on=StrikeDiffCols.DATE, how="left")

        # Calculate DTE
        # column_x for expiry df and column_y for lot size df and column_z for strik df
        df_merged[DTE_COL] = (df_merged[f'{self.expiry_column}_x'].dt.date - df_merged[InputCols.ENTRY_DT].dt.date).apply(lambda x: x.days)

        # Rename columns of expiry and lot size
        if not self.is_next_expiry:
            df_merged.rename(columns={f'{self.expiry_column}_x': EXPIRY_COL, f'{self.expiry_column}_y': STRIKE_DIFF, f'{self.expiry_column}': LOT_SIZE}, inplace=True)
        else:
            df_merged[EXPIRY_COL] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), df_merged[f'{self.expiry_column}_x'], df_merged[f'{self.next_expiry_column}_x'])
            df_merged[LOT_SIZE] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), df_merged[f'{self.expiry_column}_y'], df_merged[f'{self.next_expiry_column}_y'])
            df_merged[STRIKE_DIFF] = np.where(df_merged[DTE_COL] > int(self.from_which_dte), df_merged[f'{self.expiry_column}'], df_merged[f'{self.next_expiry_column}'])

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
        print(df_merged.columns)
        self.ee_df = df_merged

    @staticmethod
    def get_atm_strike(price, strike_diff):
        try:
            n = price
            d = strike_diff
            # Smaller multiple
            a = (n // d) * d
            # Larger multiple
            b = a + d
            # Return of closest of two
            temp = (b if n - a > b - n else a)
            diff = temp - int(temp)
            if not diff == 0:
                return temp
            return int(temp)
        except Exception as ex:
            print("SIMULATION.SIMULATION.get_atm_strike", ex)

    def iterate_dir_month_wise(self, month_dir):
        df_list = []
        # Iterate through the files in the month directory

        for date_dir in os.listdir(month_dir):
            try:
                file_date = pd.to_datetime(date_dir, format='%d%m%Y')
                # Check if the file date is within the specified range
                if self.start_date <= file_date < self.end_date:
                    expiry_date = self.ee_df[self.ee_df[ONLY_DATE] == pd.to_datetime(date_dir, format='%d%m%Y').date()][EXPIRY_COL].to_list()
                    if expiry_date:
                        expiry_date = expiry_date[0]
                        expiry_file_name = OPTION_FILE_NAME.format(self.symbol, expiry_date.strftime("%d%b%y").upper())
                        file_path = os.path.join(month_dir, date_dir, expiry_file_name)
                        df = pd.read_csv(file_path)
                        df_list.append(df)
            except ValueError:
                continue
        return df_list

    def get_itm_or_otm(self, strike_diff, tag, atm):
        move_range = abs(self.strike) * strike_diff
        if (not self.hedge and tag == InputCols.GREEN) or (self.hedge and tag == InputCols.RED):
            return atm - move_range if self.strike > 0 else atm + move_range

        elif (not self.hedge and tag == InputCols.RED) or (self.hedge and tag == InputCols.GREEN):
            return atm + move_range if self.strike > 0 else atm - move_range

        return atm

    def generate_trade_sheet(self):
        result_df = self.generate_result_df()
        if result_df is not None:
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)

    def generate_result_df(self):
        option_db_df = self.read_csv_files_in_date_range()
        if option_db_df is not None:
            option_db_df[DATE] = pd.to_datetime(option_db_df['Date'] + ' ' + option_db_df['Time'], format="%d/%m/%Y %H:%M:%S").dt.floor('min')

            results = []
            for index, row in self.ee_df.iterrows():
                expiry_in_ticker = row[EXPIRY_COL].date().strftime("%d%b%y").upper()
                tag = row[InputCols.TAG]
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                print(entry_dt)
                # Check entry == exit.
                if entry_dt.date() < exit_dt.date() and row[EXPIRY_COL] != row[EXIT_EXPIRY]:
                    is_exists = option_db_df[(option_db_df[DATE].dt.date == exit_dt.date()) & (option_db_df[CashCols.TICKER].str.contains(expiry_in_ticker))]
                    if is_exists.empty:
                        file_path = f"{self.dir_path}\\{self.symbol.upper()}\\{exit_dt.year}\\{exit_dt.strftime('%b').upper()}\\{exit_dt.strftime('%d%m%Y')}\\{OPTION_FILE_NAME.format(self.symbol,expiry_in_ticker)}"
                        if os.path.exists(file_path):
                            new_df = pd.read_csv(file_path)
                            new_df[DATE] = pd.to_datetime(new_df['Date'] + ' ' + new_df['Time'],
                                                                format="%d/%m/%Y %H:%M:%S").dt.floor('min')

                            option_db_df = pd.concat([option_db_df, new_df], ignore_index=True)

                entry_close = row[InputCols.ENTRY_CLOSE]
                strike_diff = int(row[STRIKE_DIFF])
                atm = strike_price = self.get_atm_strike(entry_close, strike_diff)
                if self.strike:
                    strike_price = self.get_itm_or_otm(strike_diff, tag, atm)

                while True:
                    find_str = f"{expiry_in_ticker}{int(strike_price)}{self.STRIKE_POSTFIX.get(tag, '')}"
                    filtered_df = option_db_df[(option_db_df[DATE] >= entry_dt) & (option_db_df[DATE] <= exit_dt) & (option_db_df[CashCols.TICKER].str.contains(find_str))]
                    if filtered_df.empty:
                        break
                    filtered_df = filtered_df.reset_index(drop=True)
                    tracking_price = filtered_df.iloc[0][CashCols.CLOSE]
                    price_diff = abs(strike_price - entry_close)

                    if self.premium and self.strike >= 0 and tracking_price < price_diff:
                        strike_price = strike_price - strike_diff if tag == InputCols.GREEN else strike_price + strike_diff
                    else:
                        break

                def get_delayed_exit():
                    return self.get_delayed_price(option_db_df, filtered_df.iloc[0][DATE], row[EXPIRY_COL], find_str, expiry_in_ticker)

                output = {**self.result}
                output, option_db_df = self.iterate_signal(option_db_df, filtered_df, row, output, entry_dt, exit_dt,
                                                           lot_size=int(row[LOT_SIZE]),
                                                           delayed_function=get_delayed_exit if self.delay_exit else None,
                                                           expiry_date=row[EXPIRY_COL])
                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            return result_df

    def get_delayed_price(self, option_db_df, current_date, expiry_date, find_str, expiry_in_ticker):
        """
        Delayed Exit: To check for the first candle after signal end till expiry date.
        SO if signal from 1/11/2024 9:15 to 1/11/2024 9:34 and expiry date is 3/11/2024, Then
        if candle not found at signal exit i.e. at 1/11/2024 9:34 then we will check for delayed exit
        on first record starting from 1/11/2024 9:35 to last timestamp of 3/11/2024.
        """
        # Filter df after last candle of signal for the same date.
        new_df = option_db_df[(option_db_df[DATE] > current_date) & (
                    option_db_df[DATE].dt.date == current_date.date()) & (
                                     option_db_df[CashCols.TICKER].str.contains(find_str))]
        date_idx = 1
        date_ranges = pd.date_range(current_date.date(), expiry_date.date())
        exit_record = None
        while date_idx < len(date_ranges):
            if new_df.empty:
                # if new filtered df is empty then check for the next date and if that date data exists in
                # option df then fetch otherwise read from database.
                date = date_ranges[date_idx]
                new_df = option_db_df[(option_db_df[DATE].dt.date == date.date())]
                if new_df.empty:
                    file_path = f"{self.dir_path}\\{self.symbol.upper()}\\{date.year}\\{date.strftime('%b').upper()}\\{date.strftime('%d%m%Y')}\\{OPTION_FILE_NAME.format(self.symbol, expiry_in_ticker)}"
                    if os.path.exists(file_path):

                        print("Fetched from Database", current_date)
                        new_df = pd.read_csv(file_path)
                        new_df[DATE] = pd.to_datetime(new_df['Date'] + ' ' + new_df['Time'],
                                                      format="%d/%m/%Y %H:%M:%S").dt.floor('min')

                        option_db_df = pd.concat([option_db_df, new_df], ignore_index=True)
                new_df = new_df[(option_db_df[CashCols.TICKER].str.contains(find_str))]
            else:
                # else exit on first candle of filtered df.
                exit_record = new_df.iloc[0]
                break
            date_idx += 1
        return option_db_df, exit_record
    