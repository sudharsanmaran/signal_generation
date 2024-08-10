from collections import defaultdict, deque
import os
import pandas as pd

from source.constants import (
    PA_ANALYSIS_CYCLE_FOLDER,
    SG_CYCLE_OUTPUT_FOLDER,
    CycleType,
    MarketDirection,
    SecondCycleIDColumns,
    TargetProfitColumns,
)
from source.data_reader import (
    merge_all_df,
    read_files,
    update_entry_fractal_file,
    update_exit_fractal_file,
)
from source.processors.cycle_analysis_processor import (
    update_target_profit_analysis,
)
from source.processors.cycle_trade_processor import (
    check_cycle_entry_condition,
    check_cycle_exit_signals,
)
from source.processors.signal_trade_processor import (
    get_market_direction,
    process_trade,
)
from source.trade import Trade, initialize
from source.utils import write_dataframe_to_csv


def process_pa_output(validated_data, *args):

    pa_df = pd.read_csv(
        f"{PA_ANALYSIS_CYCLE_FOLDER}/{validated_data['pa_file']}",
        index_col="dt",
        parse_dates=True,
    )
    terms = validated_data["pa_file"].split("_")
    instrument = terms[0]

    index = "TIMESTAMP"
    base_path = os.getenv("DB_PATH")
    file_details = {
        "entry_fractal": update_entry_fractal_file(
            instrument,
            validated_data["entry_fractal_file_number"],
            validated_data["check_entry_fractal"],
            base_path,
            index,
        ),
        "exit_fractal": update_exit_fractal_file(
            instrument,
            validated_data["exit_fractal_file_number"],
            validated_data["check_exit_fractal"],
            base_path,
            index,
        ),
        "bb_band": {
            "read": validated_data["check_bb_band"],
            "file_path": os.path.join(
                base_path,
                "BB Band",
                instrument,
                f"{validated_data['bb_file_number']}_result.csv",
            ),
            "index_col": "TIMESTAMP",
            "cols": [
                index,
                f'P_1_{validated_data["bb_band_column"].upper()}_BAND_{validated_data["bb_band_sd"]}',
            ],
            "rename": {
                f'P_1_{validated_data["bb_band_column"].upper()}_BAND_{validated_data["bb_band_sd"]}': f"bb_{validated_data['bb_band_column']}"
            },
        },
    }

    cycle_cols = get_cycle_columns(pa_df)

    update_target_profit_analysis(
        pa_df,
        validated_data.get("tp_percentage"),
        validated_data.get("tp_method"),
        cycle_col_name=cycle_cols[CycleType.MTM_CYCLE],
        close_col_name=cycle_cols[CycleType.FIRST_CYCLE].replace(
            "cycle_no", "close_to"
        ),
    )

    cols = [
        "Open",
        "High",
        "Low",
        "Close",
        "market_direction",
        "exit_market_direction",
        "group_id",
        cycle_cols[validated_data["cycle_to_consider"]],
        TargetProfitColumns.TP_END.value,
    ]

    merged_df = pa_df[cols]

    dfs = read_files(
        pa_df.index[0],
        pa_df.index[-1],
        file_details,
    )

    merged_df = merge_all_df([merged_df, *dfs.values()])

    merged_df["previous_cycle_id"] = merged_df[
        cycle_cols[validated_data["cycle_to_consider"]]
    ].shift(1)

    write_dataframe_to_csv(merged_df, SG_CYCLE_OUTPUT_FOLDER, "merged_df.csv")

    initialize(validated_data)

    Trade.current_cycle = cycle_cols[validated_data["cycle_to_consider"]]

    file_name = validated_data["pa_file"]

    entry_state = defaultdict(deque)

    exit_state = {
        MarketDirection.PREVIOUS: None,
        "signal_count": 1,
    }
    output_df = process_trade(
        instrument,
        "",
        "",
        merged_df,
        entry_state,
        exit_state,
        entry_func=check_cycle_entry_condition,
        exit_func=check_cycle_exit_signals,
    )

    write_dataframe_to_csv(output_df, SG_CYCLE_OUTPUT_FOLDER, file_name)


def get_cycle_columns(merged_df):
    cycle_cols = defaultdict(list)

    cycle_cols[CycleType.FIRST_CYCLE] = [
        col for col in merged_df.columns if "cycle_no_" in col
    ][0]

    cycle_cols[CycleType.MTM_CYCLE] = [
        col
        for col in merged_df.columns
        if SecondCycleIDColumns.MTM_CYCLE_ID.value in col
    ][0]

    cycle_cols[CycleType.CTC_CYCLE] = [
        col
        for col in merged_df.columns
        if SecondCycleIDColumns.CTC_CYCLE_ID.value in col
    ][0]

    return cycle_cols
