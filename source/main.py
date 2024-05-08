import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class Trade:
    entry_id_counter = 0
    max_exits = float('inf')
    consider_all_exits = False
    instrument = None
    strategy_id = None

    def __init__(self, entry_signal, entry_datetime, entry_price):
        Trade.entry_id_counter += 1
        self.entry_id = Trade.entry_id_counter

        self.entry_signal = entry_signal
        self.entry_datetime = entry_datetime
        self.entry_price = entry_price
        self.exits = []
        self.trade_closed = False
        self.exit_id_counter = 0

    def calculate_pnl(self, exit_price):
        pnl = 0
        if self.entry_signal == 'long':
            pnl = exit_price - self.entry_price
        else:
            pnl = self.entry_price - exit_price
        return pnl

    def add_exit(self, exit_datetime, exit_price, exit_type):
        if not self.trade_closed:
            self.exit_id_counter += 1
            self.exits.append(
                {
                    'exit_id': self.exit_id_counter,
                    'exit_datetime': exit_datetime,
                    'exit_price': exit_price,
                    'exit_type': exit_type,
                    'pnl': self.calculate_pnl(exit_price)
                }
            )

            # Check for trade closure based on max_exits and exit_type
            if ((self.exit_id_counter >= Trade.max_exits)
                    or exit_type == 'Tag Change Exit'):
                self.trade_closed = True

    def is_trade_closed(self):
        return self.trade_closed

    def formulate_output(self):
        return [
            {
                'Instrument': Trade.instrument,
                'Strategy ID': Trade.strategy_id,
                'Signal': self.entry_signal,
                'Entry Datetime': self.entry_datetime,
                'Entry ID': self.entry_id,
                'Exit ID': exit['exit_id'],
                'Exit Datetime': exit['exit_datetime'],
                'Exit Type': exit['exit_type'],
                'Entry Price': self.entry_price,
                'Exit Price': exit['exit_price'],
                'Profit/Loss': exit['pnl']
            }
            for exit in self.exits
        ]


def validate_input(instrument, strategy_id, start_date, end_date, fractal_file_number, bb_file_number, bb_band_sd):
    # Validate the input parameters
    # Return True if all parameters are valid, otherwise False
    pass


def read_data(
    instrument, strategy_id, start_date, end_date,
    fractal_file_number, bb_file_number, bb_band_sd
):
    # Define the hardcoded paths to the files
    strategy_path = f'~/Downloads/Test case Database/Strategy/F13/{instrument}/{strategy_id}_result.csv'
    fractal_path = f'~/Downloads/Test case Database/Entry & Exit/Fractal/{instrument}/combined_{fractal_file_number}.csv'
    bb_band_path = f'~/Downloads/Test case Database/Entry & Exit/BB/{instrument}/combined_{bb_file_number}.csv'

    # Read the strategy file with date filtering, parsing, and indexing
    strategy_df = pd.read_csv(
        strategy_path, parse_dates=['dt'], date_format='%Y-%m-%d %H:%M:%S',
        usecols=['dt', 'Close',
                 'Strategy Number'],
        dtype={'Strategy Number': int},
        index_col='dt')

    # Read the fractal file with date filtering, parsing, and indexing
    fractal_df = pd.read_csv(
        fractal_path, parse_dates=['dt'], date_format='%Y-%m-%d %H:%M',
        usecols=['dt', 'P_1_FRACTAL_LONG', 'P_1_FRACTAL_SHORT',
                 'P_1_FRACTAL_CONFIRMED_LONG', 'P_1_FRACTAL_CONFIRMED_SHORT'],
        dtype={'P_1_FRACTAL_LONG': 'boolean',
               'P_1_FRACTAL_CONFIRMED_LONG': 'boolean',
               'P_1_FRACTAL_CONFIRMED_SHORT': 'boolean',
               'P_1_FRACTAL_SHORT': 'boolean'},
        index_col='dt')
    # convert dt to datetime
    fractal_df.index = pd.to_datetime(fractal_df.index)

    # Define the columns to read from BB band file based on bb_band_sd

    bb_band_cols = [
        'DT', f'P_1_MEAN_BAND_{bb_band_sd}',
        f'P_1_UPPER_BAND_{bb_band_sd}', f'P_1_LOWER_BAND_{bb_band_sd}',
        f'P_1_TAG_{bb_band_sd}']

    # Read the BB band file with date filtering, parsing, and indexing
    bb_band_df = pd.read_csv(
        bb_band_path, parse_dates=['DT'], date_format='%Y-%m-%d %H:%M:%S',
        usecols=bb_band_cols,
        index_col='DT')

    # Rename BB band columns for consistency
    bb_band_df.rename(columns={
        f'CLOSE_{bb_band_sd}': 'close',
        f'P_1_MEAN_BAND_{bb_band_sd}': 'mean_band',
        f'P_1_UPPER_BAND_{bb_band_sd}': 'upper_band',
        f'P_1_LOWER_BAND_{bb_band_sd}': 'lower_band',
        f'P_1_TAG_{bb_band_sd}': 'tag',
    }, inplace=True)

    # Convert start and end dates to datetime
    start_date = pd.to_datetime(start_date, format='%d/%m/%Y %H:%M:%S')
    end_date = pd.to_datetime(end_date, format='%d/%m/%Y %H:%M:%S')

    # Filter data by date range
    strategy_df = strategy_df[(strategy_df.index >= start_date) & (
        strategy_df.index <= end_date)]
    fractal_df = fractal_df[(fractal_df.index >= start_date)
                            & (fractal_df.index <= end_date)]
    bb_band_df = bb_band_df[(bb_band_df.index >= start_date)
                            & (bb_band_df.index <= end_date)]

    strategy_df = strategy_df.dropna(axis=0)
    fractal_df = fractal_df.dropna(axis=0)
    bb_band_df = bb_band_df.dropna(axis=0)

    return strategy_df, fractal_df, bb_band_df


def merge_data(strategy_df, fractal_df, bb_band_df):
    # Join the strategy and fractal dataframes on their index (datetime)
    merged_df = strategy_df.join(fractal_df, how='left')

    # Join the resulting dataframe with the BB band dataframe on the index (datetime)
    merged_df = merged_df.join(bb_band_df, how='left')

    return merged_df


def merge_data_without_duplicates(strategy_df, fractal_df, bb_band_df):
    # Concatenate DataFrames with how='outer' to keep all rows from any DataFrame
    merged_df = pd.concat(
        [strategy_df, fractal_df, bb_band_df], axis=1, join='outer')
    # Forward fill missing values to propagate non-NaN values from previous rows
    merged_df.fillna(method='ffill', inplace=True)
    return merged_df


def check_entry_conditions(row, last_fractal):
    market_direction = 'long' if row['tag'] == 'GREEN' else 'short' if row['tag'] == 'RED' else None

    # Reset the last fractal for the opposite direction when the market direction changes
    if market_direction == 'long':
        last_fractal['short'] = None
    elif market_direction == 'short':
        last_fractal['long'] = None

    # Check for new fractals
    if market_direction == 'long' and row['P_1_FRACTAL_LONG']:
        last_fractal['long'] = (row.name, row['Close'])
    elif market_direction == 'short' and row['P_1_FRACTAL_SHORT']:
        last_fractal['short'] = (row.name, row['Close'])

    # Check for confirmed entry based on last_fractal, price, and mean band
    if (
            market_direction == 'long' and
            row['P_1_FRACTAL_CONFIRMED_LONG'] and
            last_fractal['long'] and
            row['Close'] > row['mean_band']
    ):
        return True

    elif (
            market_direction == 'short' and
            row['P_1_FRACTAL_CONFIRMED_SHORT'] and
            last_fractal['short'] and
            row['Close'] < row['mean_band']
    ):
        return True

    return False


def identify_exit_signals(row, last_fractal):
    market_direction = row["tag"]
    exit_type, is_fractal_change_exit = None, False
    if market_direction == "GREEN" and row["P_1_FRACTAL_CONFIRMED_SHORT"]:
        exit_type = "Fractal Change Exit"
        is_fractal_change_exit = True
    elif market_direction == "RED" and row["P_1_FRACTAL_CONFIRMED_LONG"]:
        exit_type = "Fractal Change Exit"
        is_fractal_change_exit = True

    previous_direction = last_fractal.get("previous_direction")
    if not previous_direction and not is_fractal_change_exit:
        return False, None

    if market_direction != previous_direction:
        exit_type = "Tag Change Exit"
        return True, exit_type

    return is_fractal_change_exit, exit_type


pd.set_option('display.max_rows', None)  # None means show all rows
pd.set_option('display.max_columns', None)  # None means show all columns
pd.set_option('display.width', 500)  # Adjust the width to your preference
pd.set_option('display.max_colwidth', None)


def main():
    instrument = "BANKNIFTY"
    strategy_id = 1
    start_date = "3/1/2019 9:35:00"
    end_date = "3/1/2019 11:00:00"
    fractal_file_number = 136
    fractal_exit = "2"  # or 1 or 2 or 3 etc.
    bb_file_number = 1
    bb_band_sd = 2.0  # version number (2.0, 2.25, 2.5, 2.75, 3)

    # set the class variables
    Trade.instrument = instrument
    Trade.strategy_id = strategy_id
    # upadate the max exits if fractal_exit is numeric identify by try block
    try:
        Trade.max_exits = int(fractal_exit)
    except ValueError:
        Trade.consider_all_exits = True

    # Read and filter data
    strategy_df, fractal_df, bb_band_df = read_data(
        instrument, strategy_id, start_date, end_date,
        fractal_file_number, bb_file_number, bb_band_sd
    )

    # Merge data
    merged_df = merge_data(strategy_df, fractal_df, bb_band_df)

    # Dictionary to track last fractals for both directions
    last_fractal = {'long': None, 'short': None, "previous_direction": None}
    active_trades, completed_trades = [], []
    for index, row in merged_df.iterrows():
        is_entry = check_entry_conditions(row, last_fractal)
        is_exit, exit_type = identify_exit_signals(row, last_fractal)
        # Check for entry using check_entry_conditions
        if is_entry:
            trade = Trade(
                entry_signal=row['tag'],
                entry_datetime=index,
                entry_price=row['Close'],
            )
            active_trades.append(trade)

        if is_exit:
            for trade in active_trades[:]:
                trade.add_exit(row.name, row['Close'], exit_type)
                if trade.is_trade_closed():
                    completed_trades.append(trade)
                    active_trades.remove(trade)

        last_fractal["previous_direction"] = row["tag"]

    trade_outputs = []
    for trade in completed_trades:
        trade_outputs.extend(trade.formulate_output())

    output_df = pd.DataFrame(trade_outputs)

    print(output_df)
    # write the output to csv
    output_df.to_csv("output.csv", index=False)


if __name__ == "__main__":
    main()
