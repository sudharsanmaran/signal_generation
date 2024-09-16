from collections import defaultdict, deque
import os
import pandas as pd

from source.constants import (
    PA_ANALYSIS_CYCLE_FOLDER,
    SG_CYCLE_OUTPUT_FOLDER,
    SG_FRACTAL_ANALYSIS_OUTPUT_FOLDER,
    CycleType,
    MarketDirection,
    SecondCycleIDColumns,
    TargetProfitColumns,
)
from source.data_reader import (
    merge_all_df,
    read_files,
    update_entry_fractal_file_with_period,
    update_exit_fractal_file_with_period,
)
from source.processors.cycle_analysis_processor import (
    update_target_profit_analysis,
)
from source.processors.cycle_trade_processor import (
    check_cycle_entry_condition,
    check_cycle_exit_signals,
)
from source.processors.signal_trade_processor import (
    process_trade,
)
from source.trade import Trade, initialize
from source.utils import write_dataframe_to_csv
from tradesheet.index import generate_tradesheet


def process_pa_output(validated_data, *args):

    pa_df = pd.read_csv(
        f"{PA_ANALYSIS_CYCLE_FOLDER}/{validated_data['pa_file']}",
        index_col="dt",
        parse_dates=True,
    )
    terms = validated_data["pa_file"].split("_")
    instrument = terms[0]

    index = "dt"
    fractal_path = os.getenv("SIGNAL_FRACTAL_DB_PATH")
    bb_path = os.getenv("BB_DB_PATH")

    file_details = {
        "entry_fractal": update_entry_fractal_file_with_period(
            instrument,
            validated_data["entry_fractal_file_number"],
            validated_data["check_entry_fractal"],
            fractal_path,
            index,
            period=validated_data["entry_fractal_period"],
        ),
        "exit_fractal": update_exit_fractal_file_with_period(
            instrument,
            validated_data["exit_fractal_file_number"],
            validated_data["check_exit_fractal"],
            fractal_path,
            index,
            period=validated_data["exit_fractal_period"],
        ),
    }

    if validated_data["check_bb_band"]:
        file_details.update(
            {
                "bb_band": {
                    "read": validated_data["check_bb_band"],
                    "file_path": os.path.join(
                        bb_path,
                        instrument,
                        f"{instrument}_TF_{validated_data['bb_file_number']}.csv",
                    ),
                    "index_col": "dt",
                    "cols": [
                        index,
                        f'P_{validated_data["parameter_id"]}_{validated_data["bb_band_column"].upper()}_BAND_{validated_data["period"]}_{validated_data["bb_band_sd"]}',
                    ],
                    "rename": {
                        f'P_{validated_data["parameter_id"]}_{validated_data["bb_band_column"].upper()}_BAND_{validated_data["period"]}_{validated_data["bb_band_sd"]}': f"bb_{validated_data['bb_band_column']}"
                    },
                },
            }
        )

    cycle_cols = get_cycle_columns(pa_df)

    cols = [
        "Open",
        "High",
        "Low",
        "Close",
        "market_direction",
        "exit_market_direction",
        "group_id",
        "signal_category",
        *[cycle_cols[cycle] for cycle in validated_data["cycle_to_consider"]],
    ]

    # covert market direction to MarketDirection enum
    market_direction_map = {
        "MarketDirection.LONG": MarketDirection.LONG,
        "MarketDirection.SHORT": MarketDirection.SHORT,
    }

    pa_df["market_direction"] = pa_df["market_direction"].map(
        market_direction_map
    )
    pa_df["exit_market_direction"] = pa_df["exit_market_direction"].map(
        market_direction_map
    )

    if validated_data["calculate_tp"]:
        update_target_profit_analysis(
            pa_df,
            validated_data.get("tp_percentage"),
            validated_data.get("tp_method"),
            cycle_col_name=cycle_cols[CycleType.MTM_CYCLE],
            close_col_name=cycle_cols[CycleType.FIRST_CYCLE].replace(
                "cycle_no", "close_to"
            ),
        )
        cols.append(TargetProfitColumns.TP_END.value)

    merged_df = pa_df[cols]

    start_date, end_date = pa_df.index[0], pa_df.index[-1]
    validated_data["start_date"] = start_date
    validated_data["end_date"] = end_date

    dfs = read_files(
        start_date,
        end_date,
        file_details,
    )

    merged_df = merge_all_df([merged_df, *dfs.values()])

    file_name = "_".join(validated_data["pa_file"].split("_")[:-2]) + ".csv"
    validated_data["file_name"] = file_name

    if validated_data["calculate_fractal_analysis"]:
        fractal_analysis = {}
        fractal_analysis["Strategy"] = validated_data["pa_file"]
        fractal_analysis["Instrument"] = instrument

        cols = [
            "signal_category",
            "market_direction",
            "group_id",
            cycle_cols[validated_data["cycle_to_consider"]],
            "entry_FRACTAL_CONFIRMED_LONG",
            "entry_FRACTAL_CONFIRMED_SHORT",
        ]
        fractal_analysis_df = merged_df[cols]

        # time diffrence between entry_FRACTAL_CONFIRMED_LONG and entry_FRACTAL_CONFIRMED_SHORT true
        long_true_mask = (
            fractal_analysis_df["entry_FRACTAL_CONFIRMED_LONG"] == True
        )
        short_true_mask = (
            fractal_analysis_df["entry_FRACTAL_CONFIRMED_SHORT"] == True
        )

        fractal_analysis_df.loc[long_true_mask, "time_diff_long"] = (
            fractal_analysis_df.index[long_true_mask].to_series().diff()
        )
        fractal_analysis_df.loc[short_true_mask, "time_diff_short"] = (
            fractal_analysis_df.index[short_true_mask].to_series().diff()
        )

        for _, group_data in fractal_analysis_df.groupby(
            ["group_id", cycle_cols[validated_data["cycle_to_consider"]]]
        ):
            first_index, last_index = group_data.index[0], group_data.index[-1]
            if (
                group_data.loc[first_index, "market_direction"]
                == "MarketDirection.LONG"
            ):
                fractal_analysis_df.loc[last_index, "fractal_count"] = (
                    group_data["entry_FRACTAL_CONFIRMED_LONG"].sum()
                )
                fractal_analysis_df.loc[last_index, "avg_time_diff"] = (
                    group_data["time_diff_long"].mean()
                )
                fractal_analysis_df.loc[last_index, "median_time_diff"] = (
                    group_data["time_diff_long"].median()
                )
            else:
                fractal_analysis_df.loc[last_index, "fractal_count"] = (
                    group_data["entry_FRACTAL_CONFIRMED_SHORT"].sum()
                )

                fractal_analysis_df.loc[last_index, "avg_time_diff"] = (
                    group_data["time_diff_short"].mean()
                )

                fractal_analysis_df.loc[last_index, "median_time_diff"] = (
                    group_data["time_diff_short"].median()
                )

        start_index = fractal_analysis_df.index[0]
        fractal_analysis_df.loc[start_index, "avg no of fractals"] = (
            fractal_analysis_df["fractal_count"].mean()
        )
        fractal_analysis_df.loc[start_index, "median no of fractals"] = (
            fractal_analysis_df["fractal_count"].median()
        )
        
        for key, value in fractal_analysis.items():
            fractal_analysis_df.loc[start_index, key] = value

        write_dataframe_to_csv(
            fractal_analysis_df,
            SG_FRACTAL_ANALYSIS_OUTPUT_FOLDER,
            file_name,
        )

    for cycle in validated_data["cycle_to_consider"]:

        merged_df["previous_cycle_id"] = merged_df[cycle_cols[cycle]].shift(1)

        name = file_name.split(".")[0]
        write_dataframe_to_csv(
            merged_df,
            SG_CYCLE_OUTPUT_FOLDER,
            f"merged_df_{name}_{cycle.value}.csv",
        )

        initialize(validated_data)

        Trade.current_cycle = cycle_cols[cycle]

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
        write_dataframe_to_csv(
            output_df, SG_CYCLE_OUTPUT_FOLDER, f"{name}_{cycle.value}.csv"
        )

        if Trade.trigger_trade_management:
            print("Trade Management Triggered")
            generate_tradesheet(validated_data, output_df, "PA DB", instrument)


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
