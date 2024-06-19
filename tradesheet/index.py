import time

import pandas as pd
from tradesheet.constants import INPUT_FILE, ENTRY_EXIT_FILE, InputCols, InputFileCols
from tradesheet.src.cash import CashSegment
from tradesheet.src.future import FutureSegment
from tradesheet.src.option import OptionSegment


SEGMENT_CLASS = {
    "cash": CashSegment,
    "future": FutureSegment,
    "options": OptionSegment,
}


def generate_tradesheet(
    input_data, signal_generation_df, strategy_pair="", instrument=""
):
    st = time.time()
    segment_class = SEGMENT_CLASS.get(input_data["segment"].lower(), None)
    if segment_class:
        instance = segment_class(input_data, signal_generation_df, instrument=instrument, strategy_pair=strategy_pair)
        instance.generate_trade_sheet()
    else:
        print("Provided Segment is incorrect")
    print(f"{instrument}_{strategy_pair}:   {time.time() - st}")


if __name__ == "__main__":
    input_data = pd.read_csv(
        INPUT_FILE,
        parse_dates=["Start Date", "End Date"],
        keep_default_na=False,
        dayfirst=True,
    ).to_dict(orient="records")[0]
    ee_df = pd.read_csv(
        ENTRY_EXIT_FILE, parse_dates=[InputCols.ENTRY_DT, InputCols.EXIT_DT]
    )
    ee_df.drop(columns=["Unnamed: 0"], inplace=True)
    generate_tradesheet(input_data, ee_df)
