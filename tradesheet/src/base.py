import os
import time
from datetime import datetime

import pandas as pd
from tradesheet.constants import ENTRY_EXIT_FILE, DATE, InputCols, InputValues, CHECK_AD, \
    ENTRY, EXIT, CashCols, RESULT_DICT, OutputCols, ExitTypes, InputFileCols, DTE_COL
from tradesheet.utils import percentage, clean_int, get_bool


class TradeSheetGenerator:
    def __init__(self, input_data, ee_df, strategy_pair="", instrument=""):
        self.output_file_name = f"{self.output_file_name}_{instrument}_{strategy_pair}.csv"
        self.start_date = input_data[InputFileCols.START_DATE]
        self.end_date = input_data.get(InputFileCols.END_DATE)
        self.symbol = instrument or input_data.get(InputFileCols.INSTRUMENT) 
        self.segment = input_data[InputFileCols.SEGMENT]
        self.expiry = input_data[InputFileCols.EXPIRY]
        self.strike = clean_int(input_data[InputFileCols.STRIKE])
        self.dte_based_trading = input_data.get(InputFileCols.DTE_BASED_TESTING)
        self.dte = self.filter_value(input_data.get(InputFileCols.DTE_FROM))
        self.ad_based_entry = input_data.get(InputFileCols.AD_BASED_ENTRY, False)
        self.ad = input_data.get(InputFileCols.AD)
        self.ad_percent = input_data.get(InputFileCols.AD_PERCENTAGE)
        self.target = input_data.get(InputFileCols.TP_TRADING)
        self.tp_percent = input_data.get(InputFileCols.TP_PERCENTAGE)
        self.sl_trading = input_data.get(InputFileCols.SL_TRADING)
        self.sl_percent = input_data.get(InputFileCols.SL_PERCENTAGE)
        self.redeploy = input_data.get(InputFileCols.IS_REDEPLOY)
        self.ad_based_redeploy = input_data.get(InputFileCols.RE_AD_BASED_ENTRY)
        self.ad_redeploy = input_data.get(InputFileCols.RE_AD)
        self.ad_redeploy_percent = input_data.get(InputFileCols.RE_AD_PERCENTAGE)
        self.is_next_expiry = input_data.get(InputFileCols.IS_NEXT_EXPIRY)
        self.from_which_dte = input_data[InputFileCols.NEXT_DTE_FROM]
        self.next_expiry = input_data[InputFileCols.NEXT_EXPIRY]
        self.premium = input_data[InputFileCols.PREMIUM]
        self.volume = input_data.get(InputFileCols.VOLUME)
        self.no_of_mins = input_data.get(InputFileCols.VOLUME_MIN)
        self.capital = clean_int(input_data.get(InputFileCols.CAPITAL))
        self.risk = input_data.get(InputFileCols.RISK)
        self.leverage = input_data.get(InputFileCols.LEVERAGE)

        self.dte_based_exit = input_data.get(InputFileCols.DTE_BASED_EXIT)
        self.exit_dte_no = clean_int(input_data.get(InputFileCols.EXIT_DTE_NUMBER))
        self.exit_dte_time = input_data.get(InputFileCols.EXIT_DTE_TIME)
        try:
            self.exit_dte_time = pd.to_datetime(self.exit_dte_time, format='%H:%M:%S').time() if self.dte_based_exit else None
        except:
            raise Exception("INVALID DTE EXIT TIME FORMATE")
        self.rollover_candle = clean_int(input_data.get(InputFileCols.ROLLOVER_CANDLE))

        self.risk_at_play = percentage(self.capital, self.risk)
        self.delay_exit = False
        self.result = {**RESULT_DICT}
        self.result.update({
            OutputCols.AD: self.ad,
            OutputCols.AD_PERCENT: self.ad_percent,
            OutputCols.TARGET_PROFIT: self.tp_percent,
            OutputCols.SL_PERCENT: self.sl_percent,
            OutputCols.RE_AD: self.ad_redeploy,
            OutputCols.RE_AD_PERCENT: self.ad_redeploy_percent,
            OutputCols.CAPITAL: self.capital,
            OutputCols.VOLUME_MIN: self.no_of_mins,
        })
        self.ee_df = ee_df[(ee_df[InputCols.ENTRY_DT] >= self.start_date) & (ee_df[InputCols.ENTRY_DT] <= self.end_date)]

        if self.ad_based_entry and self.ad not in [InputValues.APPRECIATION, InputValues.DEPRECIATION] and not self.ad_percent:
            raise Exception("Please provide appreciation values")

    @staticmethod
    def generator(df):
        for item in df.to_dict(orient="records"):
            yield item

    @staticmethod
    def filter_value(value):
        if isinstance(value, str):
            if "-" in value:
                start, end = value.split("-")
                value_ids = list(range(int(start), int(end) + 1))
            else:
                ids = value.split(",")
                value_ids = [int(sid) for sid in ids if sid]
        else:
            value_ids = [value]
        return value_ids

    def generate_trade_sheet(self):
        raise NotImplementedError

    def iterate_dir_month_wise(self, month_dir):
        raise NotImplementedError

    def read_expiry_data(self, col_class, file_path):
        expiry_date_columns = [col_class.DATE, self.expiry_column]
        if self.next_expiry_column:
            expiry_date_columns.append(self.next_expiry_column)
        use_cols = [col_class.SYMBOL] + expiry_date_columns
        df = pd.read_csv(file_path, usecols=use_cols, parse_dates=expiry_date_columns,
                         date_format=col_class.DATE_FORMAT)
        df[col_class.DATE] = pd.to_datetime(df[col_class.DATE]).dt.date
        return df[
            (df[col_class.SYMBOL] == self.symbol) & (df[col_class.DATE] >= self.start_date.date()) & (
                    df[col_class.DATE] <= self.end_date.date())]

    def sum_of_volume(self, df, filtered_df, idx):
        try:
            date = filtered_df.iloc[idx][DATE]
            df = df[df[DATE].dt.date == date.date()].reset_index(drop=True)
            start_idx = df[df[DATE] == date].index.to_list()[0]
            sliced_df = df[start_idx+1:start_idx+1+self.no_of_mins]
            return sliced_df[CashCols.VOLUME].sum()
        except Exception as e:
            print(e)

    @staticmethod
    def check_ad(high, low, ad_price, ad):
        return high > ad_price if ad == InputValues.APPRECIATION else low < ad_price

    def cal_capital_management(self, entry_price, exit_price, tag, lot_size):
        try:
            qty = (self.risk_at_play * (self.leverage if self.leverage else 1)) / percentage(entry_price, self.sl_percent)
            if tag == InputCols.GREEN:
                diff = exit_price - entry_price
            else:
                diff = entry_price - exit_price
            roi = (diff * qty)/self.capital
            probability = 1 if roi > 0 else 0

            revised_qty = round(qty / lot_size) * lot_size if lot_size else None
            return qty, roi, probability, revised_qty
        except Exception as e:
            print(e)

    def read_csv_files_in_date_range(self):
        # Initialize an empty list to store DataFrames
        df_list = []
        # Iterate through the years within the range
        for year in range(self.start_date.year, self.end_date.year + 1):
            year_dir = os.path.join(f"{self.dir_path}\\{self.symbol.upper()}", str(year))

            if not os.path.exists(year_dir):
                continue

            # Determine the start and end month for the current year
            start_month = 1 if year > self.start_date.year else self.start_date.month
            end_month = 12 if year < self.end_date.year else self.end_date.month

            # Iterate through the months within the range for the current year
            for month in range(start_month, end_month + 1):
                month_name = datetime(year, month, 1).strftime('%b').upper()
                month_dir = os.path.join(year_dir, month_name)

                if not os.path.exists(month_dir):
                    continue

                df_list.extend(self.iterate_dir_month_wise(month_dir))

        # Concatenate all DataFrames in the list into a single DataFrame
        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            print("Data not found")
            return None

    def find_entry_exit(self, filtered_df, is_ad, ad, ad_percent, tracking_price, exit_dt, expiry_date, dte=None, start_index=0):
        # CHeck expiry after 3:20 at expiry date
        expiry_date = expiry_date.replace(hour=15, minute=20, second=0) if expiry_date else None
        entry_price = exit_price = target_price = sl_price = ad_price = None
        ad_time = exit_time = exit_type = None
        step = CHECK_AD if is_ad else ENTRY
        idx = 0
        entry_idx = None

        for idx, cash_record in filtered_df.iloc[start_index:].iterrows():
            if step == CHECK_AD:
                percent = percentage(tracking_price, ad_percent)
                ad_price = tracking_price + percent if ad == InputValues.APPRECIATION else tracking_price - percent
                step = ENTRY
            elif step == ENTRY:
                if not is_ad:
                    entry_price = tracking_price
                elif self.check_ad(cash_record[CashCols.HIGH], cash_record[CashCols.LOW], ad_price, ad):
                    entry_price = ad_price
                    ad_time = cash_record[DATE]
                if entry_price:
                    target_price = (entry_price + percentage(entry_price, self.tp_percent)) if self.target else None
                    sl_price = (entry_price - percentage(entry_price, self.sl_percent)) if self.sl_trading else None
                    step = EXIT
                    entry_idx = idx
            if step == EXIT and idx != start_index:
                if expiry_date and cash_record[DATE].date() > expiry_date.date():
                    break
                elif start_index == 0 and self.target and target_price < cash_record[CashCols.HIGH]:
                    exit_price = target_price
                    exit_type = ExitTypes.TARGET_EXIT
                elif self.sl_trading and sl_price > cash_record[CashCols.LOW]:
                    exit_price = sl_price
                    exit_type = ExitTypes.SL_EXIT
                elif expiry_date and cash_record[DATE] > expiry_date:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.EXPIRY_EXIT
                elif self.dte_based_exit and dte == self.exit_dte_no and cash_record[DATE].time() == self.exit_dte_time:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.DTE_BASED_EXIT
                elif idx == len(filtered_df) - 1 and cash_record[DATE] == exit_dt:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.SIGNAL_EXIT
                if exit_price:
                    exit_time = cash_record[DATE]
                    break
        return entry_price, exit_price, ad_price, ad_time, exit_time, exit_type, entry_idx, idx

    @staticmethod
    def get_ad_price_level(cash_df, ad_time):
        close = cash_df[cash_df[DATE] == ad_time][CashCols.CLOSE].to_list()
        return close[0] if close else None

    @staticmethod
    def get_tracking_price(df, tracking_time, col=CashCols.CLOSE):
        close = df[df[DATE] == tracking_time][col].to_list()
        return close[0] if close else None

    def iterate_signal(self, final_df, filtered_df, row, output, entry_dt, exit_dt, lot_size=None, delayed_function=None, expiry_date=None):
        if self.__class__.__name__ == "CashSegment":
            cash_df = filtered_df
            dte = None
        else:
            cash_df = self.cash_db_df
            dte = row[DTE_COL]

        if not filtered_df.empty:
            tracking_time = filtered_df.iloc[0][DATE]
            tracking_price = self.get_tracking_price(filtered_df, tracking_time)
            if entry_dt != tracking_time:
                output[OutputCols.TRACKING_PRICE_REVISED_TIME] = tracking_time
            output[OutputCols.TRACKING_PRICE] = tracking_price
            output[OutputCols.TRACKING_PRICE_TIME] = row[InputCols.ENTRY_DT]
            entry_price, exit_price, ad_price, ad_time, exit_time, exit_type, entry_idx, exit_idx = self.find_entry_exit(
                filtered_df,
                self.ad_based_entry,
                self.ad,
                self.ad_percent,
                tracking_price,
                exit_dt,
                expiry_date,
                dte=dte)
            output[OutputCols.TICKER] = filtered_df.iloc[exit_idx][CashCols.TICKER]
            output[OutputCols.AD_PRICE] = ad_price
            output[OutputCols.AD_TIME] = ad_time
            output[OutputCols.AD_PRICE_LEVEL] = self.get_ad_price_level(cash_df, ad_time)
            output[OutputCols.MAX_P] = filtered_df['High'].max()
            output[OutputCols.MIN_P] = filtered_df['Low'].min()
            output[OutputCols.ENTRY_PRICE] = entry_price
            if entry_price and not exit_price and delayed_function:
                final_df, exit_record = delayed_function()
                if exit_record is not None:
                    exit_price = exit_record[CashCols.OPEN]
                    exit_time = exit_record[DATE]
                    exit_type = ExitTypes.DELAYED_EXIT

            if entry_price and exit_price:
                tag = row[InputCols.TAG]
                output[OutputCols.EXIT_TIME] = exit_time
                output[OutputCols.EXIT_PRICE] = exit_price
                output[OutputCols.EXIT_TYPE] = exit_type

                if ad_time:
                    ad_df = filtered_df[(filtered_df[DATE] >= tracking_time) & (filtered_df[DATE] <= ad_time)]
                    output[OutputCols.MAX_AD_P] = ad_df['High'].max()
                    output[OutputCols.MIN_AD_P] = ad_df['Low'].min()
                    exit_df = filtered_df[(filtered_df[DATE] >= ad_time) & (filtered_df[DATE] <= exit_time)]
                    output[OutputCols.MAX_EXIT_P] = exit_df['High'].max()
                    output[OutputCols.MIN_EXIT_P] = exit_df['Low'].min()
                if self.sl_trading and self.risk_at_play:
                    output[OutputCols.QTY], output[OutputCols.ROI], output[OutputCols.PROBABILITY], output[
                        OutputCols.REVISED_QTY] = self.cal_capital_management(entry_price, exit_price, tag, lot_size)
                if self.volume and self.no_of_mins:
                    output[OutputCols.ENTRY_VOLUME] = self.sum_of_volume(final_df, filtered_df, entry_idx)
                    output[OutputCols.EXIT_VOLUME] = self.sum_of_volume(final_df, filtered_df, exit_idx)

                # Check for redeploy condition
                if self.redeploy and exit_type in [ExitTypes.TARGET_EXIT, ExitTypes.SL_EXIT, ExitTypes.DTE_BASED_EXIT] and exit_idx < len(
                        filtered_df) - 1:
                    re_tracking_price = self.get_tracking_price(filtered_df, filtered_df.iloc[exit_idx][DATE], CashCols.OPEN)
                    re_entry_price, re_exit_price, re_ad_price, re_ad_time, re_exit_time, re_exit_type, re_entry_idx, re_exit_idx = self.find_entry_exit(
                        filtered_df, self.ad_based_redeploy, self.ad_redeploy, self.ad_redeploy_percent,
                        re_tracking_price, exit_dt, expiry_date, start_index=exit_idx, dte=row[DTE_COL])
                    output[OutputCols.RE_AD_ENTRY_PRICE] = re_entry_price
                    output[OutputCols.RE_AD_PRICE] = re_ad_price
                    output[OutputCols.RE_AD_TIME] = re_ad_time
                    output[OutputCols.RE_AD_PRICE_LEVEL] = self.get_ad_price_level(cash_df, re_ad_time)
                    output[OutputCols.RE_AD_EXIT_PRICE] = re_exit_price
                    output[OutputCols.RE_EXIT_TYPE] = re_exit_type
                    output[OutputCols.RE_EXIT_TIME] = re_exit_time
                    if re_ad_time and re_exit_time:
                        exit_df = filtered_df[(filtered_df[DATE] >= re_ad_time) & (filtered_df[DATE] <= re_exit_time)]
                        output[OutputCols.MAX_RE_EXIT_P] = exit_df['High'].max()
                        output[OutputCols.MIN_RE_EXIT_P] = exit_df['Low'].min()
                    if self.sl_trading and self.risk_at_play and re_entry_price and re_exit_price:
                        output[OutputCols.RE_QTY], output[OutputCols.RE_ROI], output[
                            OutputCols.RE_PROBABILITY], output[OutputCols.REVISED_RE_QTY] = self.cal_capital_management(re_entry_price, re_exit_price, tag, lot_size)

        return output, final_df
