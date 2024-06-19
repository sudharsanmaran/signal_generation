
import os
from datetime import datetime

import pandas as pd
from tradesheet.constants import DATE, InputCols, CASH_FILE_PATH, CASH_FILE_PREFIX, OUTPUT_PATH, OutputCols
from tradesheet.src.base import TradeSheetGenerator


class CashSegment(TradeSheetGenerator):
    dir_path = CASH_FILE_PATH
    output_file_name = f"{OUTPUT_PATH}cash_output"

    def iterate_dir_month_wise(self, month_dir, start_date, end_date, **kwargs):
        df_list = []
        # Iterate through the files in the month directory
        for file_name in os.listdir(month_dir):
            if file_name.endswith('.csv'):
                # Extract the date from the file name
                file_date_str = file_name.split('.')[0].split(CASH_FILE_PREFIX.format(self.symbol))[-1]
                try:
                    file_date = datetime.strptime(file_date_str, '%d%m%Y').date()
                except ValueError:
                    continue
                # Check if the file date is within the specified range
                if start_date <= file_date <= end_date:
                    file_path = os.path.join(month_dir, file_name)
                    df_list.append(pd.read_csv(file_path))
        return df_list

    def generate_trade_sheet(self):
        self.ee_df = self.ee_df[self.ee_df[InputCols.TAG] == InputCols.GREEN]
        self.segment_df = self.read_csv_files_in_date_range()
        previous_date = None
        if self.segment_df is not None:
            results = []
            for index, row in self.ee_df.iterrows():
                entry_dt, exit_dt = row[InputCols.ENTRY_DT], row[InputCols.EXIT_DT]
                if previous_date and previous_date < entry_dt.date():
                    self.segment_df = self.segment_df[self.segment_df[DATE].dt.date > previous_date]
                filtered_df = self.segment_df.loc[(self.segment_df[DATE] >= entry_dt) & (self.segment_df[DATE] <= exit_dt)].reset_index(drop=True)
                output, _, _ = self.iterate_signal(filtered_df, row, entry_dt, exit_dt)
                results.append(output)
                previous_date = entry_dt.date()
                results.append({**row, **output})
            result_df = pd.DataFrame(results, columns=[*self.ee_df.columns.to_list(), *self.result.keys()])
            result_df.drop([OutputCols.TRADE_ID, OutputCols.ROLLOVER_ID], axis=1, inplace=True)
            result_df.to_csv(self.output_file_name, index=False)
            os.chmod(self.output_file_name, 0o600)
