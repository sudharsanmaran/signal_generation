import os
import time
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from pa_analysis.analysis_processor import process
from pa_analysis.constants import PERIOD_OPTIONS, SD_OPTIONS, TIMEFRAME_OPTIONS
from source.streamlit import (
    get_flag_combinations,
    get_portfolio_flags,
    get_portfolio_strategies,
    get_strategy_id_combinations,
    load_input_from_json,
    select_all_options,
)
from source.constants import INSTRUMENTS
from pa_analysis.validation import validate


load_dotenv(override=True)


def main():
    """
    Main function to run the Streamlit app.
    """
    streamlit_inputs, saved_inputs = {}, {}
    use_saved_input = st.checkbox("Use Saved Inputs", value=False)
    if use_saved_input:
        all_user_inputs = load_input_from_json()
        if all_user_inputs:
            search_term = st.text_input("Search Notes")

            filtered_notes = [
                note
                for note in all_user_inputs.keys()
                if search_term.lower() in note.lower()
            ]

            selected_note = st.selectbox(
                "Select a note to view details", filtered_notes
            )
            saved_inputs = all_user_inputs[selected_note]

            # json does not support tuple, so converting to tuple
            saved_inputs["long_entry_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["long_entry_signals"])
            )

            saved_inputs["short_entry_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["short_entry_signals"])
            )

            saved_inputs["strategy_pairs"] = list(
                map(lambda x: tuple(x), saved_inputs["strategy_pairs"])
            )

        else:
            st.warning("saved data not found")

    st.header("PA Analysis")
    portfolio_ids_input = st.text_input(
        "Portfolio IDs (comma-separated, e.g., 1, 2, 3)",
        value=saved_inputs.get("portfolio_ids_input", "F13"),
    )
    streamlit_inputs["portfolio_ids_input"] = portfolio_ids_input

    instruments = st.multiselect(
        "INDICES",
        options=INSTRUMENTS,
        default=saved_inputs.get("instruments", ["BANKNIFTY"]),
    )
    streamlit_inputs["instruments"] = instruments

    if portfolio_ids_input:
        portfolio_ids = tuple(
            map(lambda a: a.strip(), portfolio_ids_input.split(","))
        )
        streamlit_inputs["portfolio_ids"] = portfolio_ids
        possible_flags_per_portfolio = get_portfolio_flags(
            portfolio_ids, streamlit_inputs, saved_inputs
        )
        filtered_flag_combinations = get_flag_combinations(
            portfolio_ids, possible_flags_per_portfolio
        )
        all_flag_combinations = ["ALL"] + filtered_flag_combinations

        long_entry_signals = st.multiselect(
            "Long Signals",
            all_flag_combinations,
            default=saved_inputs.get("long_entry_signals", "GREEN"),
            key="long_entry_signals",
            on_change=select_all_options,
            args=("long_entry_signals", filtered_flag_combinations),
        )

        short_entry_signals = st.multiselect(
            "Short Signals",
            [
                combination
                for combination in all_flag_combinations
                if combination not in long_entry_signals
            ],
            default=saved_inputs.get("short_entry_signals", "RED"),
            key="short_entry_signals",
            on_change=select_all_options,
            args=(
                "short_entry_signals",
                filtered_flag_combinations,
            ),
        )

        streamlit_inputs.update(
            {
                "long_entry_signals": long_entry_signals,
                "short_entry_signals": short_entry_signals,
            }
        )

        strategy_ids_per_portfolio = get_portfolio_strategies(
            portfolio_ids, streamlit_inputs, saved_inputs
        )
        filtered_strategy_id_combinations = get_strategy_id_combinations(
            portfolio_ids, strategy_ids_per_portfolio
        )
        all_filtered_strategy_id_combinations = [
            "ALL"
        ] + filtered_strategy_id_combinations
        strategy_pairs = st.multiselect(
            "Strategy Pairs",
            all_filtered_strategy_id_combinations,
            default=saved_inputs.get("strategy_pairs", None),
            key="Strategy Pairs",
            on_change=select_all_options,
            args=("Strategy Pairs", filtered_strategy_id_combinations),
        )
        streamlit_inputs["strategy_pairs"] = strategy_pairs

        start_date = st.text_input(
            "Start Date (format: dd/mm/yyyy hh:mm:ss)",
            value=saved_inputs.get("start_date", "3/01/2019 09:00:00"),
        )
        end_date = st.text_input(
            "End Date (format: dd/mm/yyyy hh:mm:ss)",
            value=saved_inputs.get("end_date", "3/04/2019 16:00:00"),
        )
        streamlit_inputs.update(
            {"start_date": start_date, "end_date": end_date}
        )
        calculate_cycles = st.checkbox(
            "Calculate Cycles",
            value=saved_inputs.get("calculate_cycles", True),
        )
        streamlit_inputs["calculate_cycles"] = calculate_cycles
        if calculate_cycles:
            time_frames = st.multiselect(
                "Time Frame",
                TIMEFRAME_OPTIONS,
                default=saved_inputs.get("time_frame", ["2", "3"]),
            )
            st.text("BB Band 1 inputs:")
            periods = st.multiselect(
                "Periods",
                PERIOD_OPTIONS,
                default=saved_inputs.get("period", ["20", "40"]),
            )
            sds = st.multiselect(
                "Standard Deviations",
                SD_OPTIONS,
                default=saved_inputs.get("sd", ["2"]),
            )
            streamlit_inputs.update(
                {"time_frames": time_frames, "periods": periods, "sds": sds}
            )

            st.text("BB Band 2 inputs:")
            check_bb_2 = st.checkbox(
                "Check BB 2",
                value=saved_inputs.get("calculate_cycles", True),
            )
            streamlit_inputs["check_bb_2"] = check_bb_2
            if check_bb_2:
                bb_2_periods = st.multiselect(
                    "BB 2 Periods",
                    PERIOD_OPTIONS,
                    default=saved_inputs.get("period", ["40"]),
                )
                bb_2_sds = st.multiselect(
                    "BB 2 Standard Deviations",
                    SD_OPTIONS,
                    default=saved_inputs.get("sd", ["2"]),
                )
                streamlit_inputs.update(
                    {"bb_2_periods": bb_2_periods, "bb_2_sds": bb_2_sds}
                )

    # Check if all required fields are filled
    all_fields_filled = (
        portfolio_ids_input
        and instruments
        and long_entry_signals
        and short_entry_signals
        and strategy_pairs
        and start_date
        and end_date
        and check_cycles_inputs(streamlit_inputs)
    )
    if all_fields_filled:
        if st.button("Submit"):
            start = time.time()
            try:
                validated_data = validate(streamlit_inputs)
            except Exception as e:
                st.write(f"Error: {e}")
                return
            st.session_state.result_dfs = process(validated_data)
            et = time.time()
            st.write(f"Time taken: {et - start} seconds")

        # Check if there are any results to display
        if (
            "result_dfs" in st.session_state
            and st.session_state.result_dfs is not None
        ):
            st.header("Trading Strategy Analytics")

            # Create a selectbox with the keys from the result_dfs dictionary
            selected_value = st.selectbox(
                "Select a strategy", list(st.session_state.result_dfs.keys())
            )

            # Display the DataFrame associated with the selected strategy
            if selected_value:
                for key, result in st.session_state.result_dfs[
                    selected_value
                ].items():
                    if isinstance(result, dict) or isinstance(
                        result, pd.DataFrame
                    ):
                        st.subheader(key)
                        st.table(result)

    else:
        st.write("Please fill in all the required fields.")


def check_cycles_inputs(input) -> bool:
    if input["calculate_cycles"] and input["check_bb_2"]:
        return input["bb_2_periods"] and input["bb_2_sds"]
    elif input["calculate_cycles"]:
        return input["time_frames"] and input["periods"] and input["sds"]
    return True


if __name__ == "__main__":
    main()
