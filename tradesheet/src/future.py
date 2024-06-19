import os
from datetime import datetime, time
import time as tm
import pandas as pd
from tradesheet.constants import DATE, InputCols, FUTURE_FILE_PREFIX, FUTURE_FILE_PATH, OutputCols, \
    EXPIRY_NUMBER_COL, CashCols, ExitTypes, OPTION_FILE_PATH, OUTPUT_PATH
from tradesheet.src.base import TradeSheetGenerator
from tradesheet.src.mixin import OptionMixin
from tradesheet.utils import int_to_roman


HEDGE_DATE_COLUMNS = [OutputCols.ENTRY_TIME, OutputCols.EXIT_TIME, OutputCols.RE_AD_ENTRY_TIME, OutputCols.RE_EXIT_TIME]


class FutureSegment(OptionMixin, TradeSheetGenerator):
    dir_path = FUTURE_FILE_PATH
    output_file_name = f"{OUTPUT_PATH}future_output"

    def get_expiry_for_file(self, file_date):
        if self.next_expiry_column:
            expiry_number = self.ee_df[self.ee_df[InputCols.ENTRY_DT].dt.date == file_date][EXPIRY_NUMBER_COL].to_list()
            return expiry_number[0] if expiry_number else None
        return self.expiry

    def iterate_dir_month_wise(self, month_dir, start_date, end_date, expiry_number=None, **kwargs):
        if kwargs.get("hedge"):
            return self.iterate_option_dir_month_wise(month_dir, start_date, end_date, **kwargs)
        dr = kwargs.get("date_range")
        date_list = [d.strftime('%d%m%Y') for d in sorted(dr)] if dr else os.listdir(month_dir)

        df_list = []
        # Iterate through the files in the month directory
        for date_dir in date_list:
            try:
                file_date = pd.to_datetime(date_dir, format='%d%m%Y').date()
                expiry_no = self.get_expiry_for_file(file_date) if not expiry_number else expiry_number
                # file_name, expiry_no = self.get_future_file_name(file_date, date_dir, expiry_number)
                if start_date <= file_date <= end_date and expiry_no:
                    file_name = FUTURE_FILE_PREFIX.format(self.symbol, int_to_roman(expiry_no), date_dir)
                    file_path = os.path.join(month_dir, date_dir, file_name)
                    df_list.append(pd.read_csv(file_path))
                    self.date_expiry_tracker.setdefault(file_date, [])
                    self.date_expiry_tracker[file_date].append(expiry_no)
            except:
                continue
        return df_list

    def get_price(self, filtered_hedge_df, dtime: datetime, exit_time: datetime) -> float:
        if dtime:
            if self.hedge_delay_exit:
                condition = (filtered_hedge_df[DATE] >= dtime)
                if exit_time:
                    condition &= (filtered_hedge_df[DATE] < exit_time)
            else:
                condition = filtered_hedge_df[DATE] == dtime
            exists = filtered_hedge_df[condition]
            return exists.iloc[0][CashCols.CLOSE] if not exists.empty else None

    @staticmethod
    def find_p_and_l(exit_price, entry_price):
        pl = None
        price_na = None
        if exit_price and entry_price:
            pl = exit_price - entry_price
        elif exit_price and not entry_price:
            price_na = "EN_NA"
        elif entry_price and not exit_price:
            price_na = "EX_NA"
        else:
            price_na = "B_NA"
        return pl, price_na

    def hedge_calculation(self, output, ro_entry_dt, exit_dt, tag, hedge_expiry_number, current_expiry_data):
        """Hedge Calculation"""
        col_name = self.get_expiry_column_name(hedge_expiry_number)
        hedge_expiry = current_expiry_data["E"][col_name]
        strike_diff = current_expiry_data["S"][col_name]
        cash_tracking_price, _ = self.get_tracking_price(self.cash_db_df, ro_entry_dt, exit_dt)
        atm = strike_price = self.get_atm_strike(cash_tracking_price, strike_diff)
        if self.hedge_strike:
            strike_price = self.get_itm_or_otm(self.hedge_strike, strike_diff, tag, atm)
        expiry_in_ticker = hedge_expiry.strftime("%d%b%y").upper()
        find_str = f"{expiry_in_ticker}{int(strike_price)}{self.STRIKE_POSTFIX.get(tag, '')}.NFO"
        # s_d = output.get(OutputCols.ENTRY_TIME)
        # e_d = output.get(OutputCols.RE_EXIT_TIME) or output.get(OutputCols.EXIT_TIME) or hedge_expiry
        date_range = {d.date() for col in HEDGE_DATE_COLUMNS if (d := output.get(col))}
        if date_range:
            date_range.add(hedge_expiry)
            # we don't need to check weekend condition here as for hedge, we are getting data of date
            # from entry - exit time, so no can be weekend date.
            missing_dates = [single_date for single_date in date_range if
                             hedge_expiry not in self.hedge_date_expiry_tracker.get(single_date, [])]

            if missing_dates:
                self.hedge_df = pd.concat(
                    [self.hedge_df, self.read_csv_files_in_date_range(start_date=min(missing_dates),
                                                                      end_date=max(missing_dates),
                                                                      dir_path=OPTION_FILE_PATH,
                                                                      expiry_dt=hedge_expiry,
                                                                      date_range=missing_dates,
                                                                      hedge=True)])

            # self.hedge_df = self.read_csv_files_in_date_range(s_d.date(), e_d.date(), dir_path=OPTION_FILE_PATH,
            #                                                   expiry_dt=hedge_expiry.date(), option=True)
            filtered_hedge_df = self.hedge_df[self.hedge_df[CashCols.TICKER].str.contains(find_str)]
            hedge_expiry = datetime.combine(hedge_expiry, time(15, 20)) if hedge_expiry else None
            output[OutputCols.H_TICKER] = filtered_hedge_df.iloc[0][
                CashCols.TICKER] if not filtered_hedge_df.empty else None
            output[OutputCols.H_ENTRY_PRICE] = self.get_price(filtered_hedge_df, output.get(OutputCols.ENTRY_TIME),
                                                              output.get(OutputCols.EXIT_TIME))
            output[OutputCols.H_EXIT_PRICE] = self.get_price(filtered_hedge_df, output.get(OutputCols.EXIT_TIME),
                                                             hedge_expiry)
            output[OutputCols.H_P_L], output[OutputCols.H_PRICE_NA] = self.find_p_and_l(
                output[OutputCols.H_EXIT_PRICE], output[OutputCols.H_ENTRY_PRICE])

            output[OutputCols.H_RE_ENTRY_PRICE] = self.get_price(filtered_hedge_df,
                                                                 output.get(OutputCols.RE_AD_ENTRY_TIME),
                                                                 output.get(OutputCols.RE_EXIT_TIME))
            output[OutputCols.H_RE_EXIT_PRICE] = self.get_price(filtered_hedge_df, output.get(OutputCols.RE_EXIT_TIME),
                                                                hedge_expiry)
            output[OutputCols.H_RE_P_L], output[OutputCols.H_RE_PRICE_NA] = self.find_p_and_l(
                output[OutputCols.H_RE_EXIT_PRICE], output[OutputCols.H_RE_ENTRY_PRICE])

            # after getting hedge values
        hedge_expiry_number += 1
        return output, hedge_expiry_number

    def generate_trade_sheet(self):
        self.segment_df = pd.DataFrame()
        self.hedge_df = pd.DataFrame()

        if self.segment_df is not None:
            results = []
            previous_date = None
            for index, row in self.ee_df.iterrows():
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                current_date = entry_dt.date()

                # remove previous date data from segment df so that filter can be optimized
                self.remove_previous_date_data(current_date, previous_date)

                # find expiry, lot_size and strike data and assign to self.expiry_data for further use.
                self.set_expiry_data(current_date)

                # Start logic for trade along with rollover
                rid = 0  # Rollover Id
                # consider column value if next_expiry on otherwise expiry number will be same for all i.e. self.expiry
                signal_expiry_number = row[EXPIRY_NUMBER_COL] if self.is_next_expiry else self.expiry
                ro_entry_dt = entry_dt  # Rollover entry date
                hedge_expiry_number = self.hedge_expiry
                current_date_expiry = self.expiry_data[current_date]
                while True:
                    try:
                        # this is used to find particular expiry data from Ticker column. Ticker columns has data
                        # like NIFTY-II.NFO
                        col_name = self.get_expiry_column_name(signal_expiry_number)
                        # For future, expiry in ticker is based on expiry number
                        expiry_in_ticker = f"{self.symbol}-{int_to_roman(signal_expiry_number)}.NFO"
                        expiry_date = current_date_expiry["E"][col_name]
                        lot_size = current_date_expiry["L"][col_name]

                        # first we check whether, our segment_df contains data from ro_entry_date to exit date of expiry
                        # number signal_expiry_number. If yes then just filter data otherwise, read data for missing
                        # date only if that date is weekday and update segment_df.
                        s_t = tm.time()
                        date_range = set(pd.date_range(start=ro_entry_dt, end=exit_dt).date)
                        missing_dates = [single_date for single_date in date_range if
                                         signal_expiry_number not in self.date_expiry_tracker.get(single_date,
                                                                                                  []) and single_date.weekday() not in [
                                             5, 6]]

                        if missing_dates:
                            self.segment_df = pd.concat(
                                [self.segment_df, self.read_csv_files_in_date_range(start_date=min(missing_dates),
                                                                                    end_date=max(missing_dates),
                                                                                    expiry_number=signal_expiry_number,
                                                                                    date_range=missing_dates)])

                        filtered_df = self.segment_df[
                            (self.segment_df[DATE] >= ro_entry_dt) & (self.segment_df[DATE] <= exit_dt)
                            & (self.segment_df[CashCols.TICKER] == expiry_in_ticker)].reset_index(drop=True)

                        output, last_exit_type, last_exit_time = self.iterate_signal(
                            filtered_df,
                            row,
                            ro_entry_dt,
                            exit_dt,
                            lot_size=lot_size,
                            expiry_date=expiry_date,
                            delayed_exit=True,
                            rid=rid,
                            find_str=expiry_in_ticker,
                            expiry_str=signal_expiry_number
                        )

                        output[OutputCols.TRADE_ID] = index + 1
                        output[OutputCols.ROLLOVER_ID] = rid

                        # Hedge calculation
                        if self.is_hedge:
                            output, hedge_expiry_number = self.hedge_calculation(output,
                                                                                 ro_entry_dt,
                                                                                 exit_dt,
                                                                                 row[InputCols.TAG],
                                                                                 hedge_expiry_number,
                                                                                 current_date_expiry)
                        results.append(output)
                        if not (self.dte_based_exit and self.rollover_candle not in ['', None] and last_exit_type == ExitTypes.DTE_BASED_EXIT):
                            break

                        rid += 1
                        signal_expiry_number += 1
                        ro_entry_dt = filtered_df.loc[filtered_df[DATE] >= last_exit_time].iloc[self.rollover_candle][
                            DATE]
                    except Exception as e:
                        raise
                        print(f"Error: {e}")
                        break
                previous_date = current_date

            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)

    def get_delayed_price(self, current_date, expiry_date, find_str, expiry_str):
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
        while date_idx < len(date_ranges):
            if not new_df.empty:
                # else exit on first candle of filtered df.
                exit_record = new_df.iloc[0]
                break
            next_date = date_ranges[date_idx]

            if expiry_str not in self.date_expiry_tracker.get(next_date, []):
                file_name = FUTURE_FILE_PREFIX.format(self.symbol, int_to_roman(expiry_str), next_date)
                file_path = f"{self.dir_path}\\{self.symbol.upper()}\\{next_date.year}\\{next_date.strftime('%b').upper()}\\{next_date.strftime('%d%m%Y')}\\{file_name}"
                if os.path.exists(file_path):
                    new_df = pd.read_csv(file_path)
                    new_df[DATE] = pd.to_datetime(new_df['Date'] + ' ' + new_df['Time'],
                                                  format=self.db_date_format).dt.floor('min')
                    self.date_expiry_tracker.setdefault(next_date, [])
                    self.date_expiry_tracker[next_date].append(expiry_str)
                    self.segment_df = pd.concat([self.segment_df, new_df], ignore_index=True)
            else:
                new_df = self.segment_df[(self.segment_df[DATE].dt.date == next_date) &
                                         (self.segment_df[CashCols.TICKER].str.contains(find_str))]

            date_idx += 1
        return exit_record
