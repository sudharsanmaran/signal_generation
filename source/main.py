import time
import pandas as pd

from source.constants import MarketDirection, TradeType
from source.trade import Trade
from source.trade_processor import process_trade
from source.validations import validate_input


def main():
    start = time.time()
    instrument = "BANKNIFTY"
    portfolio_ids = "F13, F13_1"
    strategy_ids = "1, 4 | 2, 3 "
    long_entry_signals = "GREEN, GREEN"
    long_exit_signals = "GREEN, RED | RED, RED"
    short_entry_signals = "RED, RED"
    short_exit_signals = "GREEN, RED |GREEN, GREEN"
    start_date = "3/01/2019 09:15:00"
    end_date = "3/04/2019 16:00:00"
    entry_fractal_file_number = "1"
    exit_fractal_file_number = "2"
    fractal_exit_count = "6"  # or 1 or 2 or 3 etc.
    bb_file_number = "1"
    trail_bb_file_number = "1"
    bb_band_sd = 2.0  # standard deviations (2.0, 2.25, 2.5, 2.75, 3.0)
    trail_bb_band_sd = 2.0  # standard deviations (2.0, 2.25, 2.5, 2.75, 3.0)
    bb_band_column = "mean"  # (mean, upper, lower)
    trail_bb_band_column = "mean"
    trade_start_time = "13:15:00"
    trade_end_time = "15:20:00"
    check_fractal = True
    check_bb_band = False
    check_trail_bb_band = False
    trail_bb_band_direction = "higher"  # or "lower"
    trade_type = TradeType.POSITIONAL
    allowed_direction = MarketDirection.ALL

    # validated_input = validate_input(
    #     instrument=instrument,
    #     strategy_ids=strategy_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    #     entry_fractal_file_number=entry_fractal_file_number,
    #     exit_fractal_file_number=exit_fractal_file_number,
    #     bb_file_number=bb_file_number,
    #     trail_bb_file_number=trail_bb_file_number,
    #     bb_band_sd=bb_band_sd,
    #     trail_bb_band_sd=trail_bb_band_sd,
    #     bb_band_column=bb_band_column,
    #     trail_bb_band_column=trail_bb_band_column,
    #     trade_start_time=trade_start_time,
    #     trade_end_time=trade_end_time,
    #     check_fractal=check_fractal,
    #     check_bb_band=check_bb_band,
    #     check_trail_bb_band=check_trail_bb_band,
    #     trail_bb_band_direction=trail_bb_band_direction,
    #     trade_type=trade_type,
    #     allowed_direction=allowed_direction,
    #     fractal_exit_count=fractal_exit_count,
    #     long_entry_signals=long_entry_signals,
    #     long_exit_signals=long_exit_signals,
    #     short_entry_signals=short_entry_signals,
    #     short_exit_signals=short_exit_signals,
    #     portfolio_ids=portfolio_ids,
    # )

    # todo
    # Validate the input parameters
    portfolio_ids = [id.strip() for id in portfolio_ids.split(",")]

    def parse_signals(signals):
        return [signal.strip() for signal in signals]

    strategy_ids = [parse_signals(id.split(",")) for id in strategy_ids.split("|")]

    conditions = {
        "entry": {
            MarketDirection.LONG: [
                parse_signals(cond.split(",")) for cond in long_entry_signals.split("|")
            ],
            MarketDirection.SHORT: [
                parse_signals(cond.split(","))
                for cond in short_entry_signals.split("|")
            ],
        },
        "exit": {
            MarketDirection.LONG: [
                parse_signals(cond.split(",")) for cond in long_exit_signals.split("|")
            ],
            MarketDirection.SHORT: [
                parse_signals(cond.split(",")) for cond in short_exit_signals.split("|")
            ],
        },
    }

    # set the class variables
    Trade.strategy_ids = strategy_ids
    Trade.instrument = instrument
    Trade.trade_start_time = pd.to_datetime(trade_start_time).time()
    Trade.trade_end_time = pd.to_datetime(trade_end_time).time()
    Trade.check_fractal = check_fractal
    Trade.check_bb_band = check_bb_band
    Trade.check_trail_bb_band = check_trail_bb_band
    Trade.type = trade_type
    Trade.market_direction_conditions = conditions
    Trade.bb_band_column = f"P_1_{bb_band_column.upper()}_BAND_{bb_band_sd}"
    Trade.trail_bb_band_column = (
        f"P_1_{trail_bb_band_column.upper()}_BAND_{trail_bb_band_sd}"
    )
    Trade.allowed_direction = allowed_direction
    Trade.signal_columns = [f"TAG_{id}" for id in portfolio_ids]

    if trail_bb_band_direction == "higher":
        Trade.trail_compare_func = lambda a, b: a > b
        Trade.trail_opposite_compare_func = lambda a, b: a < b
    else:
        Trade.trail_compare_func = lambda a, b: a < b
        Trade.trail_opposite_compare_func = lambda a, b: a > b

    try:
        Trade.fractal_exit_count = int(fractal_exit_count)
    except ValueError:
        pass

    process_trade(
        portfolio_ids,
        start_date,
        end_date,
        entry_fractal_file_number,
        exit_fractal_file_number,
        bb_file_number,
        trail_bb_file_number,
    )
    stop = time.time()
    print(f"Time taken: {stop-start} seconds")


if __name__ == "__main__":
    main()
