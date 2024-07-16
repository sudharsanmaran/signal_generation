
import os
import time
from datetime import datetime

import pandas as pd
from tradesheet.constants import DATE, InputCols, CASH_FILE_PATH, CASH_FILE_PREFIX, OUTPUT_PATH
from tradesheet.src.base import TradeSheetGenerator


class CashSegment(TradeSheetGenerator):
    dir_path = CASH_FILE_PATH
    output_file_name = f"{OUTPUT_PATH}cash_output"

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
        cash_db_df = self.read_csv_files_in_date_range()
        if cash_db_df is not None:
            cash_db_df[DATE] = pd.to_datetime(cash_db_df['Date'] + ' ' + cash_db_df['Time']).dt.floor('min')
            results = []
            for index, row in self.ee_df.iterrows():
                output = {**self.result}
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                filtered_df = cash_db_df[(cash_db_df[DATE] >= entry_dt) & (cash_db_df[DATE] <= exit_dt)]
                filtered_df = filtered_df.reset_index(drop=True)
                output, cash_db_df = self.iterate_signal(cash_db_df, filtered_df, row, output, entry_dt, exit_dt)

                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)
