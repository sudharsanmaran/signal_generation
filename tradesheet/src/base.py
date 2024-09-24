import os
import time
from datetime import datetime, time as dtime, timedelta

import pandas as pd
from tradesheet.constants import DATE, InputCols, InputValues, \
    ENTRY, EXIT, CashCols, RESULT_DICT, OutputCols, ExitTypes, InputFileCols, ExpiryCols, TradeType, \
    PRE_EXIT, OPTION_DATE_FORMAT, EXPIRY_EXIT_TIME
from tradesheet.utils import percentage, clean_int


class TradeSheetGenerator:
    db_date_format = "%Y-%m-%d %H:%M:%S"
    STRIKE_POSTFIX = {
        InputCols.GREEN: "CE",
        InputCols.RED: "PE",
    }

    def __init__(self, input_data, ee_df, strategy_pair="", instrument=""):
        self.output_file_name = f"{self.output_file_name}_{input_data['file_name']}.csv"
        self.start_date = input_data[InputFileCols.START_DATE].date()
        self.end_date = input_data.get(InputFileCols.END_DATE).date()
        self.symbol = instrument or input_data.get(InputFileCols.INSTRUMENT) 
        self.segment = input_data[InputFileCols.SEGMENT]
        self.expiry = input_data[InputFileCols.EXPIRY]
        self.strike = input_data[InputFileCols.STRIKE]
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

        self.is_hedge = input_data.get(InputFileCols.HEDGE)
        self.hedge_expiry = input_data.get(InputFileCols.HEDGE_EXPIRY)
        self.hedge_strike = input_data.get(InputFileCols.HEDGE_STRIKE)
        self.hedge_delay_exit = input_data.get(InputFileCols.HEDGE_DELAYED_EXIT)

        try:
            self.exit_dte_time = pd.to_datetime(self.exit_dte_time, format='%H:%M:%S').time() if self.dte_based_exit else None
        except:
            raise Exception("INVALID DTE EXIT TIME FORMATE")
        self.rollover_candle = input_data.get(InputFileCols.ROLLOVER_CANDLE)

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
        self.ee_df = ee_df[(ee_df[InputCols.ENTRY_DT].dt.date >= self.start_date) & (
                    ee_df[InputCols.ENTRY_DT].dt.date <= self.end_date)]

        if self.ad_based_entry and self.ad not in [InputValues.APPRECIATION, InputValues.DEPRECIATION] and not self.ad_percent:
            raise Exception("Please provide appreciation values")

    @staticmethod
    def get_expiry_column_name(expiry_number, col_class=ExpiryCols):
        return f"{col_class.EXPIRY_PREFIX}{str(expiry_number).zfill(2)}"

    @staticmethod
    def filter_value(value: str) -> list:
        """
        Function will filter string in list
        :param value: str or int.
        :return: list
        "1-4" -> [1,2,3,4]
        "1,4,7" -> [1,4,7]
        5 -> [5]
        """
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

    def iterate_dir_month_wise(self, month_dir, start_date, end_date, **kwargs):
        raise NotImplementedError

    def use_cols(self, col_class):
        """Return a list of columns to read from dataframe.
            NOT USED.
        """
        expiry_date_columns = [col_class.DATE, self.expiry_column]
        if self.next_expiry_column:
            expiry_date_columns.append(self.next_expiry_column)
        return [col_class.SYMBOL] + expiry_date_columns

    def read_expiry_data(self, col_class: object, file_path_lst: list, parse_date: bool = False) -> pd.DataFrame:
        """Function will read file and convert date column to date type and return df
            This function is used to read expiry, strike and lot size file.
        """
        final_df = pd.DataFrame()
        for file_path in file_path_lst:
            df = pd.read_csv(file_path, date_format=col_class.DATE_FORMAT)
            df[col_class.DATE] = pd.to_datetime(df[col_class.DATE]).dt.date
            if parse_date:
                for col in df.columns.to_list():
                    if ExpiryCols.EXPIRY_PREFIX in col:
                        df[col] = pd.to_datetime(df[col]).dt.date

            df = df[
                (df[col_class.SYMBOL] == self.symbol) & (df[col_class.DATE] >= self.start_date) & (
                        df[col_class.DATE] <= self.ee_df[InputCols.EXIT_DT].max().date())]
            final_df = pd.concat([final_df, df])
        return final_df

    def sum_of_volume(self, date: datetime, ticker=None) -> int:
        """function is used to find sum of volume of entry and exit time.
        for e.g. start time i.e. date is 1/11/2022 9:20 and self.no_of_mins is 5 minutes then
        sum of volume from 1/11/2022 9:20 to 1/11/2022 9:24 will be returned
        """
        try:
            after_volume_min_date = date + timedelta(minutes=self.no_of_mins)
            date_mask = (self.segment_df[DATE] >= date) & (self.segment_df[DATE] < after_volume_min_date)
            if ticker:
                date_mask &= (self.segment_df[CashCols.TICKER] == ticker)
            return self.segment_df.loc[date_mask, CashCols.VOLUME].sum()
        except Exception as e:
            print(e)

    @staticmethod
    def check_ad(high: float, low: float, ad_price: float, ad: str) -> bool:
        return high > ad_price if ad == InputValues.APPRECIATION else low < ad_price

    def cal_capital_management(self, entry_price: float, exit_price: float, tag: str, lot_size: int) -> tuple:
        """
        Formulas of Capital management:
        here entry price is Appreciation Price or Tracking Price whichever is applicable
        risk_at_play = Capital Input * Risk
        qty = (risk_at_play * leverage) / entry_price * stop_loss %
        roi (For long) = ((exit_price - entry_price) x qty)/ Capital Input
        roi (For short) = ((entry_price - exit_price) x qty)/ Capital Input
        probability = If ROI is positive then 1 else 0
        revised_qty = round(qty/lot_size) * lot_size
        """
        try:
            qty = (self.risk_at_play * (self.leverage if self.leverage else 1)) / percentage(entry_price, self.sl_percent)
            if tag == InputCols.GREEN:
                diff = exit_price - entry_price
            else:
                diff = entry_price - exit_price
            roi = (diff * qty) / self.capital
            probability = 1 if roi > 0 else 0

            revised_qty = (round(qty / lot_size) * lot_size) if lot_size else None
            return qty, roi, probability, revised_qty
        except Exception as e:
            print(e)

    def read_csv_files_in_date_range(self, start_date=None, end_date=None, **kwargs):
        """Function will read data from Database and create Dataframe.
        Folder path: <segment_name>/<instrument>/<year>/<month>/File
        CASH/NIFTY/2022/NOV/<file_name>
        """
        # In case of hedge, we need to pass dir_path in kwargs.
        if not (dir_path := kwargs.get("dir_path")):
            dir_path = self.dir_path
        df_list = []
        if not start_date:
            start_date = self.start_date
        if not end_date:
            end_date = self.end_date

        # Iterate through the years within the range
        for year in range(start_date.year, end_date.year + 1):
            year_dir = os.path.join(f"{dir_path}\\{self.symbol.upper()}", str(year))

            if not os.path.exists(year_dir):
                continue

            # Determine the start and end month for the current year
            start_month = 1 if year > start_date.year else start_date.month
            end_month = 12 if year < end_date.year else end_date.month

            # Iterate through the months within the range for the current year
            for month in range(start_month, end_month + 1):
                month_name = datetime(year, month, 1).strftime('%b').upper()
                month_dir = os.path.join(year_dir, month_name)

                if not os.path.exists(month_dir):
                    continue

                df_list.extend(self.iterate_dir_month_wise(month_dir, start_date, end_date, **kwargs))

        # Concatenate all DataFrames in the list into a single DataFrame
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            df[DATE] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format=self.db_date_format).dt.floor('min')
            return df
        else:
            # print("Data not found")
            return None

    def find_entry_exit(self,
                        filtered_df: pd.DataFrame,
                        is_ad: bool,
                        ad: str | None,
                        ad_percent: float | None,
                        tracking_price: float,
                        tracking_time: datetime,
                        entry_dt: datetime,
                        exit_dt: datetime,
                        expiry_date: datetime | None,
                        trade_type: str | None = None):
        """
        Function will find Entry and exit time/price.
        :param filtered_df: df of data from database  from signal start time to end time. for e.g. if signal start
        from 15/11/2023 9:15 to 15/11/2023 9:34 then this contains data from respected segment database from these time.
        :param is_ad: whether to consider Appreciation/Depreciation
        :param ad: String Appreciation or Depreciation(A/D)
        :param ad_percent: percentage for A/D
        :param tracking_price: price based on which ad_price and exit will be found
        :param tracking_time:
        :param entry_dt: signal start date
        :param exit_dt: signal end date
        :param expiry_date: In case of CASH segment, it will be none. for FUTURE/OPTION, it is a date get from
        EXPIRY FILE based on expiry number.
        :param trade_type: it is a string which indicates, whether this is main trade, redeployment trade or rollover.
        For rollover, we do not need to consider A/D regardless A/D is true or not. So in case of rollover, we make
        is_ad false based on this param.
        :return: tuple

        Formula to calculate ad_price:
        Appreciation: Tracking Price + ad_percent% on tracking price
        Depreciation: Tracking Price - ad_percent% on tracking price
        """
        # CHeck expiry exit after 3:20 at expiry date
        if expiry_date:
            dte = (expiry_date - entry_dt.date()).days
            expiry_date = datetime.combine(expiry_date, EXPIRY_EXIT_TIME)
        else:
            dte = None
        dte_exit_time = datetime.combine(entry_dt.date(), self.exit_dte_time) if self.exit_dte_time else None
        entry_price = exit_price = target_price = sl_price = ad_price = None
        ad_time = exit_time = exit_type = entry_time = None
        idx = 0
        # If Trade type is rollover then do not consider AD and make is_ad False.
        is_ad = is_ad and trade_type != TradeType.ROLLOVER

        if is_ad:
            percent = percentage(tracking_price, ad_percent)
            ad_price = tracking_price + percent if ad == InputValues.APPRECIATION else tracking_price - percent
            step = ENTRY
        else:
            entry_price = tracking_price
            entry_time = tracking_time
            step = PRE_EXIT
        for idx, cash_record in filtered_df[filtered_df[DATE] > tracking_time].iterrows():

            if step == PRE_EXIT and entry_price:
                target_price = (entry_price + percentage(entry_price, self.tp_percent)) if self.target else None
                sl_price = (entry_price - percentage(entry_price, self.sl_percent)) if self.sl_trading else None
                step = EXIT
            elif step == ENTRY and is_ad and self.check_ad(cash_record[CashCols.HIGH], cash_record[CashCols.LOW],
                                                           ad_price, ad):
                entry_price = ad_price
                ad_time = cash_record[DATE]
                entry_time = ad_time
                step = PRE_EXIT
            if step == EXIT:
                if expiry_date and cash_record[DATE].date() > expiry_date.date():
                    # Need to find exit until expiry date.
                    break
                elif trade_type != TradeType.REDEPLOYMENT and self.target and target_price < cash_record[CashCols.HIGH]:
                    # No check for TP exit in case of redeployment.
                    # In date is same as entry date
                    if cash_record[DATE] == entry_time:
                        exit_price = target_price
                    else:
                        exit_price = target_price if target_price > cash_record[CashCols.OPEN] else cash_record[CashCols.OPEN]
                    exit_type = ExitTypes.TARGET_EXIT
                elif self.sl_trading and sl_price > cash_record[CashCols.LOW]:
                    exit_price = sl_price
                    exit_type = ExitTypes.SL_EXIT
                elif expiry_date and cash_record[DATE] > expiry_date:
                    # take exit on any candle after expiry date(time: 3:20)
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.EXPIRY_EXIT
                elif self.dte_based_exit and dte == self.exit_dte_no and cash_record[DATE] == dte_exit_time:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.DTE_BASED_EXIT
                elif idx == len(filtered_df) - 1 and cash_record[DATE] == exit_dt:
                    exit_price = cash_record[CashCols.CLOSE]
                    exit_type = ExitTypes.SIGNAL_EXIT
                if exit_price:
                    exit_time = cash_record[DATE]
                    break

        return entry_price, exit_price, ad_price, ad_time, entry_time, exit_time, exit_type

    @staticmethod
    def get_ad_price_level(cash_df: pd.DataFrame, ad_time: datetime) -> float | None:
        """AD Price Level is the price in cash db at ad_time"""
        result = cash_df.loc[cash_df[DATE] == ad_time, CashCols.CLOSE]
        return result.iloc[0] if not result.empty else None

    @staticmethod
    def get_tracking_price(df: pd.DataFrame, tracking_time: datetime, exit_dt: datetime, col=CashCols.CLOSE) -> tuple:
        close = df[(df[DATE] >= tracking_time) & (df[DATE] <= exit_dt)]
        return (close.iloc[0][col], close.iloc[0][DATE]) if not close.empty else (None, None)

    @staticmethod
    def get_atm_strike(price: float, strike_diff: int) -> float:
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

    def get_itm_or_otm(self, strike, strike_diff, tag, atm) -> float:
        """Calculation of Itm and OTM.
        ===============================================
        STRIKE	CALL	strike_price	PUT	    STRIKE
        3	    ITM	        2560	    OTM	    -3
        2	    ITM	        2580	    OTM	    -2
        1	    ITM	        2600	    OTM	    -1
                ATM	        2620	    ATM
        -1	    OTM	        2640	    ITM	    1
        -2	    OTM	        2660	    ITM	    2
        -3	    OTM	        2680	    ITM	    3

        ================================================
        CALL = GREEN, PUT = RED
        For e.g.
        atm = 2620,
        strike_diff = 20  (Will get from STrike diff file)
        strike = 1 (Given in input file, by default 0)
        tag = GREEN
        so, in this case, check 1 strike in CAll which is called ITM
        and ATM - strike_diff i.e. 2620 - 20 = 2600

        If tag is RED then it will be reverse case
        check 1 strike in PUT which is called ITM
        and ATM + strike_diff i.e. 2620 - 20 = 2640.

        In Case of hedge we will check reverse case. i.e. in case of GREEN, we buy PUT
        for RED, we buy CALL.
        """
        move_range = abs(strike) * strike_diff
        if (not self.is_hedge and tag == "GREEN") or (self.is_hedge and tag == "RED"):
            return atm - move_range if strike > 0 else atm + move_range

        elif (not self.is_hedge and tag == "RED") or (self.is_hedge and tag == "GREEN"):
            return atm + move_range if strike > 0 else atm - move_range
        return atm

    @staticmethod
    def get_max_min_high_low(df:pd.DataFrame, sd=None, ed=None, find=False) -> tuple:
        if find:
            date_mask = (df[DATE] > sd) & (df[DATE] < ed)
            aggregated_result = df.loc[date_mask].agg({'High': 'max', 'Low': 'min'})
            return aggregated_result['High'], aggregated_result["Low"]
        else:
            return df['High'].max(), df['Low'].min()

    def iterate_signal(self, filtered_df, row, entry_dt, exit_dt, lot_size=None, delayed_exit=False, expiry_date=None,
                       rid=0, **kwargs):
        s_time = time.time()
        output = {**row, **self.result}
        last_exit_type = last_exit_time = None

        if not filtered_df.empty:
            trade_type = TradeType.ROLLOVER if rid > 0 else None
            # Tracking price to be taken from the segment db only
            tracking_price, tracking_time = filtered_df.iloc[0][CashCols.CLOSE], filtered_df.iloc[0][DATE]
            if entry_dt != tracking_time:
                output[OutputCols.TRACKING_PRICE_REVISED_TIME] = tracking_time
            output[OutputCols.TRACKING_PRICE] = tracking_price
            output[OutputCols.TRACKING_PRICE_TIME] = entry_dt
            entry_price, exit_price, ad_price, ad_time, entry_time, exit_time, exit_type = self.find_entry_exit(
                filtered_df,
                self.ad_based_entry,
                self.ad,
                self.ad_percent,
                tracking_price,
                tracking_time,
                entry_dt,
                exit_dt,
                expiry_date,
                trade_type=trade_type)
            last_exit_type = exit_type
            last_exit_time = exit_time
            output[OutputCols.TICKER] = filtered_df.iloc[-1][CashCols.TICKER]
            output[OutputCols.AD_PRICE] = ad_price
            output[OutputCols.AD_TIME] = ad_time
            if self.__class__.__name__ != "CashSegment":
                # Ad level price is, price at ad_time in cash df.
                output[OutputCols.AD_PRICE_LEVEL] = self.get_ad_price_level(self.cash_db_df, ad_time)
            output[OutputCols.MAX_P], output[OutputCols.MIN_P] = self.get_max_min_high_low(filtered_df,
                                                                                           entry_time or entry_dt ,
                                                                                           exit_time or exit_dt,
                                                                                           find=True)
            output[OutputCols.ENTRY_TIME] = entry_time
            if entry_price and not exit_price and delayed_exit:
                exit_record = self.get_delayed_price(filtered_df.iloc[-1][DATE], expiry_date, **kwargs)
                if exit_record is not None:
                    exit_price = exit_record[CashCols.OPEN]
                    exit_time = exit_record[DATE]
                    exit_type = ExitTypes.DELAYED_EXIT

            if entry_price and exit_price:
                tag = row[InputCols.TAG]
                output[OutputCols.EXIT_TIME] = exit_time
                output[OutputCols.EXIT_PRICE] = exit_price
                output[OutputCols.EXIT_TYPE] = exit_type
                if self.__class__.__name__ == "FutureSegment" and tag == InputCols.RED:
                    output[OutputCols.NET_POINTS] = entry_price - exit_price
                else:
                    output[OutputCols.NET_POINTS] = exit_price - entry_price

                # Find Min Max
                if ad_time:
                    # ad_df = filtered_df[(filtered_df[DATE] >= tracking_time) & (filtered_df[DATE] <= ad_time)]
                    output[OutputCols.MAX_AD_P], output[OutputCols.MIN_AD_P] = self.get_max_min_high_low(filtered_df,
                                                                                                         tracking_time,
                                                                                                         ad_time,
                                                                                                         find=True)
                    if exit_time:
                        # exit_df = filtered_df[(filtered_df[DATE] >= ad_time) & (filtered_df[DATE] <= exit_time)]
                        output[OutputCols.MAX_EXIT_P], output[OutputCols.MIN_EXIT_P] = self.get_max_min_high_low(
                            filtered_df, ad_time, exit_time, find=True)

                # Capital Management
                if self.sl_trading and self.risk_at_play:
                    output[OutputCols.QTY], output[OutputCols.ROI], output[OutputCols.PROBABILITY], output[
                        OutputCols.REVISED_QTY] = self.cal_capital_management(entry_price, exit_price, tag, lot_size)

                # Calculate Volume
                if self.volume and self.no_of_mins and self.__class__.__name__ != "CashSegment":
                    output[OutputCols.ENTRY_VOLUME] = self.sum_of_volume(entry_time, kwargs.get("find_str"))
                    output[OutputCols.EXIT_VOLUME] = self.sum_of_volume(exit_time, kwargs.get("find_str"))

                # Check for redeploy condition only if TP or SL exit
                if self.redeploy and exit_type in [ExitTypes.TARGET_EXIT, ExitTypes.SL_EXIT] and exit_time < \
                        filtered_df.iloc[-1][DATE]:
                    # Open of next candle will be new tracking price for redeployment
                    re_tracking_price, re_tracking_time = self.get_tracking_price(filtered_df,
                                                                                  exit_time + timedelta(minutes=1),
                                                                                  exit_dt, CashCols.OPEN)
                    if re_tracking_price:
                        re_entry_price, re_exit_price, re_ad_price, re_ad_time, re_entry_time, re_exit_time, re_exit_type = self.find_entry_exit(
                            filtered_df,
                            self.ad_based_redeploy,
                            self.ad_redeploy,
                            self.ad_redeploy_percent,
                            re_tracking_price,
                            re_tracking_time,
                            entry_dt,
                            exit_dt,
                            expiry_date,
                            trade_type=TradeType.REDEPLOYMENT)
                        last_exit_type = re_exit_type
                        last_exit_time = re_exit_time
                        output[OutputCols.RE_AD_ENTRY_TIME] = re_entry_time
                        output[OutputCols.RE_AD_PRICE] = re_ad_price
                        output[OutputCols.RE_AD_TIME] = re_ad_time
                        if self.__class__.__name__ != "CashSegment":
                            output[OutputCols.RE_AD_PRICE_LEVEL] = self.get_ad_price_level(self.cash_db_df, re_ad_time)

                        if self.__class__.__name__ == "FutureSegment" and tag == InputCols.RED:
                            output[OutputCols.NET_POINTS] = re_entry_price - re_exit_price
                        else:
                            output[OutputCols.NET_POINTS] = re_exit_price - re_entry_price
                        output[OutputCols.RE_AD_EXIT_PRICE] = re_exit_price
                        output[OutputCols.RE_EXIT_TYPE] = re_exit_type
                        output[OutputCols.RE_EXIT_TIME] = re_exit_time
                        if re_ad_time and re_exit_time:
                            output[OutputCols.MAX_RE_EXIT_P], output[
                                OutputCols.MIN_RE_EXIT_P] = self.get_max_min_high_low(filtered_df, re_ad_time,
                                                                                      re_exit_time, find=True)

                        # Capital Management
                        if self.sl_trading and self.risk_at_play and re_entry_price and re_exit_price:
                            output[OutputCols.RE_QTY], output[OutputCols.RE_ROI], output[
                                OutputCols.RE_PROBABILITY], output[
                                OutputCols.REVISED_RE_QTY] = self.cal_capital_management(re_entry_price, re_exit_price,
                                                                                         tag, lot_size)
        return output, last_exit_type, last_exit_time
