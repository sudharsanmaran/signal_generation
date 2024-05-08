from source.trade import Trade
from source.utils import check_entry_conditions, merge_data_without_duplicates, read_data, merge_data
import pandas as pd
pd.set_option('display.max_rows', None)  # None means show all rows
pd.set_option('display.max_columns', None)  # None means show all columns
pd.set_option('display.width', 500)  # Adjust the width to your preference
pd.set_option('display.max_colwidth', None)


def main():
    instrument = "BANKNIFTY"
    strategy_id = 1
    start_date = "3/1/2019 9:35:00"
    end_date = "3/1/2019 10:00:00"
    fractal_file_number = 136
    fractal_exit = "ALL"  # or 1, 2, 3, etc.
    bb_file_number = 1
    bb_band_sd = 2.0  # version number (2.0, 2.25, 2.5, 2.75, 3)

    # Read and filter data
    strategy_df, fractal_df, bb_band_df = read_data(
        instrument, strategy_id, start_date, end_date,
        fractal_file_number, bb_file_number, bb_band_sd
    )

    # Merge data
    merged_df = merge_data(strategy_df, fractal_df, bb_band_df)

    market_direction = None
    # Dictionary to track last fractals for both directions
    last_fractal = {'long': None, 'short': None}

    active_trades, completed_trades = [], []

    for index, row in merged_df.iterrows():

        # Check for entry using check_entry_conditions
        if check_entry_conditions(row, last_fractal):
            trade = Trade(
                entry_signal=row['tag'],
                entry_datetime=row.name,
                entry_price=row['Close'],
                max_exits=fractal_exit
            )
            active_trades.append(trade)
    #     # Check for trade exits
#         exit = check_exit_conditions(row, trade, market_direction)
    #     for trade in active_trades[:]:  # Create a copy of the list to modify it during iteration
    #         if exit:
    #             trade.add_exit(*exit)
    #             if trade_is_completed(trade, exit_condition):  # Define trade_is_completed based on your exit logic
    #                 active_trades.remove(trade)
    #                 completed_trades.append(trade)

    # # Calculate P&L for completed trades
    # for trade in completed_trades:
    #     trade_pnl = trade.calculate_pnl()
    #     # Store or print P&L information
    #     a = 12


if __name__ == "__main__":
    main()
