import os
from datetime import timedelta

import pandas as pd
from tradesheet.constants import DATE, InputCols, CashCols, OPTION_FILE_NAME, \
    OPTION_FILE_PATH, EXPIRY_NUMBER_COL, OutputCols, OUTPUT_PATH
from tradesheet.src.base import TradeSheetGenerator
from tradesheet.src.mixin import OptionMixin


class OptionSegment(OptionMixin, TradeSheetGenerator):
    dir_path = OPTION_FILE_PATH
    # db_date_format = OPTION_DATE_FORMAT

    output_file_name = f"{OUTPUT_PATH}option_output"
    STRIKE_POSTFIX = {
        InputCols.GREEN: "CE",
        InputCols.RED: "PE",
    }

    def iterate_dir_month_wise(self, month_dir, start_date, end_date, **kwargs):
        return self.iterate_option_dir_month_wise(month_dir, start_date, end_date, **kwargs)

    def generate_trade_sheet(self):
        self.segment_df = pd.DataFrame()
        if self.segment_df is not None:
            results = []
            previous_date = None
            for index, row in self.ee_df.iterrows():
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                tag = row[InputCols.TAG]
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
                current_date_expiry = self.expiry_data[current_date]

                while True:
                    try:
                        col_name = self.get_expiry_column_name(signal_expiry_number)
                        expiry_date = current_date_expiry["E"][col_name]
                        lot_size = current_date_expiry["L"][col_name]
                        strike_diff = current_date_expiry["S"][col_name]
                        # For future, expiry in ticker is based on expiry date
                        expiry_in_ticker = expiry_date.strftime("%d%b%y").upper()

                        # - first we check whether, our segment_df contains data from ro_entry_date to exit date of expiry
                        # date. If yes then just filter data otherwise, read data for missing
                        # date only if that date is weekday and update segment_df.
                        # - when we pass datetime object in date_range function, it does not consider end date in range,
                        # that is why we pass one day extra in end date.
                        date_range = set(pd.date_range(start=ro_entry_dt, end=exit_dt+timedelta(days=1)).date)
                        missing_dates = [single_date for single_date in date_range if
                                         single_date.weekday() not in [5, 6] and
                                         expiry_date not in self.date_expiry_tracker.get(single_date, [])]

                        if missing_dates:
                            self.segment_df = pd.concat(
                                [self.segment_df, self.read_csv_files_in_date_range(start_date=min(missing_dates),
                                                                                    end_date=max(missing_dates),
                                                                                    expiry_dt=expiry_date,
                                                                                    date_range=missing_dates)])

                        # first we find time, and close price in cash db at ro_entry_dt
                        # for e.g. ro_entry_dt is 28/11/2022 9:15 then we find close of this date in Cash db
                        # which becomes cash_tracking_price.

                        cash_tracking_price, cash_tracking_time = self.get_tracking_price(self.cash_db_df, ro_entry_dt,
                                                                                          exit_dt)

                        # based on cash_tracking_price and strike_diff, find ATM and strike price
                        atm = strike_price = self.get_atm_strike(cash_tracking_price, strike_diff)
                        if self.strike:
                            # find strike_price base on strike if given.
                            strike_price = self.get_itm_or_otm(self.strike, strike_diff, tag, atm)

                        # Fetching strike based on Premium feature
                        while True:
                            find_str = f"{expiry_in_ticker}{int(strike_price)}{self.STRIKE_POSTFIX.get(tag, '')}.NFO"
                            filtered_df = self.segment_df.loc[
                                (self.segment_df[DATE] >= ro_entry_dt) & (self.segment_df[DATE] <= exit_dt) &
                                (self.segment_df[CashCols.TICKER].str.contains(find_str))].reset_index(drop=True)
                            # Premium will be checked only if ITM strike is given i.e. strike is positive.
                            check_premium = self.premium and self.strike >= 0
                            if filtered_df.empty or not check_premium:
                                break

                            if check_premium:
                                # for premium feature we find Difference between strike_price and cash_tracking_price.
                                # Tracking price will be first candle price in option db.
                                # If tracking_price is "lower than" difference then select a
                                # higher strike (check doc string of get_itm_or_otm function for more info.)
                                # If it is higher, then continue with the same strike
                                tracking_price = filtered_df.iloc[0][CashCols.CLOSE]
                                price_diff = abs(strike_price - cash_tracking_price)
                                if tracking_price < price_diff:
                                    strike_price = strike_price - strike_diff if tag == InputCols.GREEN else strike_price + strike_diff
                                else:
                                    break

                        output, last_exit_type, last_exit_time = self.iterate_signal(
                            filtered_df,
                            row,
                            ro_entry_dt,
                            exit_dt,
                            lot_size=lot_size,
                            expiry_date=expiry_date,
                            delayed_exit=True,
                            rid=rid,
                            find_str=find_str,
                            expiry_str=expiry_in_ticker
                        )
                        output[OutputCols.TRADE_ID] = index + 1
                        output[OutputCols.ROLLOVER_ID] = rid
                        results.append(output)
                        if not (self.dte_based_exit and self.rollover_candle not in ['', None] and last_exit_type is not None): #== ExitTypes.DTE_BASED_EXIT):
                            break
                        rid += 1
                        signal_expiry_number += 1
                        ro_entry_dt = filtered_df.loc[filtered_df[DATE] >= last_exit_time].iloc[self.rollover_candle][
                            DATE]
                    except Exception as e:
                        print(f"Error: {e}")
                        break
                previous_date = current_date
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)

    def get_file_path(self, next_date, expiry_str):
        file_path = f"{self.dir_path}\\{self.symbol.upper()}\\{next_date.year}\\{next_date.strftime('%b').upper()}\\{next_date.strftime('%d%m%Y')}\\{OPTION_FILE_NAME.format(self.symbol, expiry_str)}"

    def get_delayed_price(self, current_date, expiry_date, find_str, expiry_str, **kwargs):
        """
        Delayed Exit: To check for the first candle after signal end till expiry date.
        SO if signal from 1/11/2024 9:15 to 1/11/2024 9:34 and expiry date is 3/11/2024, Then
        if candle not found at signal exit i.e. at 1/11/2024 9:34 then we will check for delayed exit
        on first record starting from 1/11/2024 9:35 to last timestamp of 3/11/2024.
        """
        return super().get_delayed_price(current_date, expiry_date, find_str, expiry_str, expiry_date)

        # # Filter df after last candle of signal for the same date.
        # new_df = self.segment_df[(self.segment_df[DATE] > current_date) & (
        #         self.segment_df[DATE].dt.date == current_date.date()) & (
        #                              self.segment_df[CashCols.TICKER].str.contains(find_str))]
        # date_idx = 1
        # date_ranges = pd.date_range(current_date.date(), expiry_date).date
        # exit_record = None
        # while date_idx <= len(date_ranges):
        #     if not new_df.empty:
        #         # else exit on first candle of filtered df.
        #         exit_record = new_df.iloc[0]
        #         break
        #
        #     next_date = date_ranges[date_idx]
        #     if expiry_date not in self.date_expiry_tracker.get(next_date, []):
        #         file_path = f"{self.dir_path}\\{self.symbol.upper()}\\{next_date.year}\\{next_date.strftime('%b').upper()}\\{next_date.strftime('%d%m%Y')}\\{OPTION_FILE_NAME.format(self.symbol, expiry_str)}"
        #         if os.path.exists(file_path):
        #             # print("Fetched from Database", current_date)
        #             new_df = pd.read_csv(file_path)
        #             new_df[DATE] = pd.to_datetime(new_df['Date'] + ' ' + new_df['Time'],).dt.floor('min')
        #             self.date_expiry_tracker.setdefault(next_date, [])
        #             self.date_expiry_tracker[next_date].append(expiry_date)
        #             self.segment_df = pd.concat([self.segment_df, new_df], ignore_index=True)
        #             new_df = new_df[(new_df[DATE].dt.date == next_date) &
        #                                      (new_df[CashCols.TICKER].str.contains(find_str))]
        #     else:
        #         new_df = self.segment_df[(self.segment_df[DATE].dt.date == next_date) &
        #                                  (self.segment_df[CashCols.TICKER].str.contains(find_str))]
        #     date_idx += 1
        # return exit_record
