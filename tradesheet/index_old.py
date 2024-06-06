import os
import time
from datetime import datetime

import pandas as pd

from tradesheet.constants import INPUT_FILE, ENTRY_EXIT_FILE, DATE, InputCols, InputValues, CHECK_AD, \
    ENTRY, EXIT, CashCols, RESULT_DICT, OutputCols, ExitTypes, EXPIRY_FILE, CASH_FILE_PATH, \
    CASH_FILE_PREFIX, FUTURE_FILE_PREFIX, FUTURE_FILE_PATH, STRIKE_FILE, OPTION_FILE_NAME, OPTION_FILE_PATH
from tradesheet.utils import percentage, int_to_roman, clean_int


def get_bool(val):
    true = ['true', 't', '1', 'y', 'yes', 'enabled', 'enable', 'on']
    false = ['false', 'f', '0', 'n', 'no', 'disabled', 'disable', 'off', '', None]
    if val.lower() in true:
        return True
    elif val.lower() in false:
        return False
    else:
        raise ValueError('The value \'{}\' cannot be mapped to boolean.'
                         .format(val))


class TradeSheetGenerator:

    def __init__(self, input_data):
        ee_df = pd.read_csv(ENTRY_EXIT_FILE, parse_dates=[InputCols.ENTRY_DT])
        ee_df[DATE] = pd.to_datetime(ee_df[InputCols.DATE])
        ee_df.drop(columns=["Unnamed: 0"], inplace=True)
        self.start_date = input_data["Start Date"]
        self.end_date = input_data["End Date"]
        self.symbol = input_data["INSTRUMENT"]
        self.segment = input_data["Segment"]
        self.expiry = input_data["Expiry"]
        self.strike = int(input_data["Strike"])
        self.dte_based_trading = input_data["Days to expiry (DTE) - Based testing"]
        self.dte = self.filter_value(input_data["DTE"])
        self.ad_based_entry = get_bool(input_data.get("Appreciation/Depreciation based entry", False))
        self.ad = input_data.get("Appreciation/Depreciation")
        self.ad_percent = input_data.get("Appreciation/ Depreciation %")
        self.target = get_bool(input_data.get("TARGET"))
        self.tp_percent = input_data.get("TARGET Profit %")
        self.sl_trading = get_bool(input_data.get("SL Trading"))
        self.sl_percent = input_data.get("SL %")
        self.redeploy = get_bool(input_data.get("Re-deployment"))
        self.ad_based_redeploy = get_bool(input_data.get("RE_Appreciation/Depreciation based entry"))
        self.ad_redeploy = input_data.get("RE_Appreciation/Depreciation")
        self.ad_redeploy_percent = input_data.get("RE_Appreciation/ Depreciation %")
        self.volume = get_bool(input_data.get("Volume feature on/off"))
        self.no_of_mins = input_data.get("Number of minutes")
        self.capital = clean_int(input_data.get("Capital"))
        self.risk = input_data.get("Risk")
        self.risk_at_play = percentage(self.capital, self.risk)
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

    def cal_capital_management(self, entry_price, exit_price, tag):
        try:
            qty = (self.risk_at_play/ percentage(entry_price, self.sl_percent))
            if tag == InputCols.GREEN:
                diff = exit_price - entry_price
            else:
                diff = entry_price - exit_price
            roi = (diff * qty)/self.capital
            probability = 1 if roi > 0 else 0
            return qty, roi, probability
        except:
            breakpoint()

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

    def find_entry_exit(self, filtered_df, is_ad, ad, ad_percent, tracking_price, start_index=0):
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
                if start_index == 0 and self.target and target_price < cash_record[CashCols.HIGH]:
                    exit_price = target_price
                    exit_type = ExitTypes.TARGET_EXIT
                elif self.sl_trading and sl_price > cash_record[CashCols.LOW]:
                    exit_price = sl_price
                    exit_type = ExitTypes.SL_EXIT
                elif idx == len(filtered_df) - 1:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.SIGNAL_EXIT
                if exit_price:
                    exit_time = cash_record[DATE]
                    break
        return entry_price, exit_price, ad_price, ad_time, exit_time, exit_type, entry_idx, idx

    def iterate_signal(self, final_df, filtered_df, row, output):
        if not filtered_df.empty:
            tracking_price = filtered_df.iloc[0][CashCols.CLOSE]
            tracking_time = filtered_df.iloc[0][DATE]

            entry_price, exit_price, ad_price, ad_time, exit_time, exit_type, entry_idx, exit_idx = self.find_entry_exit(
                filtered_df,
                self.ad_based_entry,
                self.ad,
                self.ad_percent,
                tracking_price)
            if entry_price and exit_price:
                tag = row[InputCols.TAG]
                output[OutputCols.TICKER] = filtered_df.iloc[exit_idx][CashCols.TICKER]
                output[OutputCols.TRACKING_PRICE] = tracking_price
                output[OutputCols.TRACKING_PRICE_TIME] = row[InputCols.ENTRY_DT]
                output[OutputCols.AD_PRICE] = ad_price
                output[OutputCols.AD_TIME] = ad_time
                output[OutputCols.EXIT_TIME] = exit_time
                output[OutputCols.EXIT_PRICE] = exit_price
                output[OutputCols.EXIT_TYPE] = exit_type
                output[OutputCols.MAX_P] = filtered_df['High'].max()
                output[OutputCols.MIN_P] = filtered_df['Low'].min()
                if ad_time:
                    ad_df = filtered_df[(filtered_df[DATE] >= tracking_time) & (filtered_df[DATE] <= ad_time)]
                    output[OutputCols.MAX_AD_P] = ad_df['High'].max()
                    output[OutputCols.MIN_AD_P] = ad_df['Low'].min()
                    exit_df = filtered_df[(filtered_df[DATE] >= ad_time) & (filtered_df[DATE] <= exit_time)]
                    output[OutputCols.MAX_EXIT_P] = exit_df['High'].max()
                    output[OutputCols.MIN_EXIT_P] = exit_df['Low'].min()
                if self.sl_trading and self.risk_at_play:
                    output[OutputCols.QTY], output[OutputCols.ROI], output[OutputCols.PROBABILITY] = self.cal_capital_management(entry_price, exit_price, tag)
                if self.volume and self.no_of_mins:
                    output[OutputCols.ENTRY_VOLUME] = self.sum_of_volume(final_df, filtered_df, entry_idx)
                    output[OutputCols.EXIT_VOLUME] = self.sum_of_volume(final_df, filtered_df, exit_idx)
                if self.redeploy and exit_type in [ExitTypes.TARGET_EXIT, ExitTypes.SL_EXIT] and exit_idx < len(
                        filtered_df) - 1:
                    re_tracking_price = filtered_df.iloc[exit_idx][CashCols.OPEN]
                    re_entry_price, re_exit_price, re_ad_price, re_ad_time, re_exit_time, re_exit_type, re_entry_idx, re_exit_idx = self.find_entry_exit(
                        filtered_df, self.ad_based_redeploy, self.ad_redeploy, self.ad_redeploy_percent,
                        re_tracking_price, exit_idx)
                    output[OutputCols.RE_AD_PRICE] = re_ad_price
                    output[OutputCols.RE_AD_TIME] = re_ad_time
                    output[OutputCols.RE_AD_EXIT_PRICE] = re_exit_price
                    output[OutputCols.RE_EXIT_TYPE] = re_exit_type
                    output[OutputCols.RE_EXIT_TIME] = re_exit_time
                    if re_ad_time:
                        exit_df = filtered_df[(filtered_df[DATE] >= re_ad_time) & (filtered_df[DATE] <= re_exit_time)]
                        output[OutputCols.MAX_RE_EXIT_P] = exit_df['High'].max()
                        output[OutputCols.MIN_RE_EXIT_P] = exit_df['Low'].min()
                    if self.sl_trading and self.risk_at_play and re_entry_price and re_exit_price:
                        output[OutputCols.RE_QTY], output[OutputCols.RE_ROI], output[
                            OutputCols.RE_PROBABILITY] = self.cal_capital_management(re_entry_price, re_exit_price,
                                                                              tag)
        return output


class CashSegment(TradeSheetGenerator):
    dir_path = CASH_FILE_PATH

    def iterate_dir_month_wise(self, month_dir):
        df_list = []
        # Iterate through the files in the month directory
        for file_name in os.listdir(month_dir):
            if file_name.endswith('.csv'):
                # Extract the date from the file name
                file_date_str = file_name.split('.')[0].split(CASH_FILE_PREFIX.format(self.symbol))[-1]
                try:
                    file_date = datetime.strptime(file_date_str, '%d%m%Y')
                except ValueError:
                    continue

                # Check if the file date is within the specified range
                if self.start_date <= file_date <= self.end_date:
                    file_path = os.path.join(month_dir, file_name)
                    df_list.append(pd.read_csv(file_path))
        return df_list

    def generate_trade_sheet(self):
        self.ee_df = self.ee_df[self.ee_df[InputCols.TAG] == InputCols.GREEN]
        # breakpoint()
        s_t = time.time()
        cash_db_df = self.read_csv_files_in_date_range()
        print(time.time() - s_t)
        if cash_db_df is not None:
            cash_db_df[DATE] = pd.to_datetime(cash_db_df['Date'] + ' ' + cash_db_df['Time']).dt.floor('min')
            results = []
            for index, row in self.ee_df.iterrows():
                output = {**self.result}
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                filtered_df = cash_db_df[(cash_db_df[DATE] >= entry_dt) & (cash_db_df[DATE] <= exit_dt)]
                filtered_df = filtered_df.reset_index(drop=True)
                output = self.iterate_signal(cash_db_df, filtered_df, row, output)

                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv("cash_output.csv", index=False)
            os.chmod('cash_output.csv', 0o600)


class FutureSegment(TradeSheetGenerator):
    dir_path = FUTURE_FILE_PATH

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

    def generate_trade_sheet(self):
        # expiry_df = pd.read_csv(EXPIRY_FILE, index_col=None, parse_dates=["Date"])
        s_t = time.time()
        future_db_df = self.read_csv_files_in_date_range()
        print(time.time() - s_t)
        if future_db_df is not None:
            future_db_df[DATE] = pd.to_datetime(future_db_df['Date'] + ' ' + future_db_df['Time']).dt.floor('min')
            results = []
            for index, row in self.ee_df.iterrows():
                output = {**self.result}
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                # expiry_dt = expiry_df[(expiry_df["Date"].dt.date == entry_dt.date()) & (expiry_df["Symbol"] == "NIFTY")]["EXPIRY_01"].to_list()[0]
                filtered_df = future_db_df[(future_db_df[DATE] >= entry_dt) & (future_db_df[DATE] <= exit_dt)]
                filtered_df = filtered_df.reset_index(drop=True)
                output = self.iterate_signal(future_db_df, filtered_df, row, output)
                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv("future_output.csv", index=False)
            os.chmod('future_output.csv', 0o600)


class OptionSegment(TradeSheetGenerator):
    dir_path = OPTION_FILE_PATH
    STRIKE_POSTFIX = {
        InputCols.GREEN: "CE",
        InputCols.RED: "PE",
    }

    def __init__(self, input_data):
        super().__init__(input_data)
        if not self.expiry:
            raise Exception("Please provide expiry date")

        self.expiry_column = f"EXPIRY_{str(self.expiry).zfill(2)}"
        expiry_df = pd.read_csv(EXPIRY_FILE, usecols=["Date", "Symbol", self.expiry_column], parse_dates=["Date", self.expiry_column], date_format="%m/%d/%Y")
        self.expiry_df = expiry_df[(expiry_df["Symbol"] == self.symbol) & (expiry_df["Date"] >= self.start_date) & (expiry_df["Date"] <= self.end_date)]

        strike_df = pd.read_csv(STRIKE_FILE, usecols=["TIMESTAMP", "SYMBOL", self.expiry_column], parse_dates=["TIMESTAMP", self.expiry_column], date_format="%m/%d/%Y")
        self.strike_df = strike_df[(strike_df["SYMBOL"] == self.symbol) & (strike_df["TIMESTAMP"] >= self.start_date) & (
                strike_df["TIMESTAMP"] <= self.end_date)]

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
                # breakpoint()
                if self.start_date <= file_date <= self.end_date:
                    expiry_date = self.expiry_df[self.expiry_df["Date"] == pd.to_datetime(date_dir, format='%d%m%Y')][self.expiry_column].to_list()[0]
                    expiry_file_name = OPTION_FILE_NAME.format(self.symbol, expiry_date.strftime("%d%b%y").upper())
                    file_path = os.path.join(month_dir, date_dir, expiry_file_name)
                    dte = (expiry_date - file_date).days
                    df = pd.read_csv(file_path)
                    if self.dte_based_trading:
                        df["DTE"] = dte
                    df["expiry_date"] = expiry_date
                    df_list.append(df)
            except ValueError:
                continue
        return df_list

    def get_itm_or_otm(self, strike_diff, tag, atm):
        move_range = abs(self.strike) * strike_diff
        if tag == InputCols.GREEN:
            return (atm - move_range if self.strike > 0 else atm + move_range)

        elif tag == InputCols.RED:
            return atm + move_range if self.strike > 0 else atm - move_range

        return atm

    def generate_trade_sheet(self):
        s_t = time.time()
        option_db_df = self.read_csv_files_in_date_range()
        print(time.time() - s_t)
        if option_db_df is not None:
            option_db_df[DATE] = pd.to_datetime(option_db_df['Date'] + ' ' + option_db_df['Time'], format="%d/%m/%Y %H:%M:%S").dt.floor('min')
            if self.dte_based_trading and self.dte:
                option_db_df = option_db_df[option_db_df["DTE"].isin(self.dte)]
            results = []
            for index, row in self.ee_df.iterrows():
                entry_close = row[InputCols.ENTRY_CLOSE]
                tag = row[InputCols.TAG]
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                strike_diff = self.strike_df[self.strike_df["TIMESTAMP"].dt.date == entry_dt.date()][self.expiry_column].to_list()
                if not strike_diff:
                    continue
                # breakpoint()
                strike_diff = int(strike_diff[0])
                atm = strike_price = self.get_atm_strike(entry_close, strike_diff)
                if self.strike:
                    strike_price = self.get_itm_or_otm(strike_diff, tag, atm)
                find_str = f"{int(strike_price)}{self.STRIKE_POSTFIX.get(tag, '')}"
                output = {**self.result}
                filtered_df = option_db_df[(option_db_df[DATE] >= entry_dt) & (option_db_df[DATE] <= exit_dt) & (option_db_df[CashCols.TICKER].str.contains(find_str))]
                filtered_df = filtered_df.reset_index(drop=True)
                output = self.iterate_signal(option_db_df, filtered_df, row, output)
                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv("option_output.csv", index=False)
            os.chmod('option_output.csv', 0o600)


if __name__ == '__main__':
    st = time.time()
    SEGMENT_CLASS = {
        "cash": CashSegment,
        "future": FutureSegment,
        "option": OptionSegment,
    }
    input_data = pd.read_csv(INPUT_FILE, parse_dates=["Start Date", "End Date"], keep_default_na=False, dayfirst=True).to_dict(orient="records")[0]
    segment_class = SEGMENT_CLASS.get(input_data["Segment"].lower(), None)
    print(segment_class)
    instance = segment_class(input_data)
    instance.generate_trade_sheet()
    print(time.time()-st)
