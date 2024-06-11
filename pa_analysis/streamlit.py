import time
import streamlit as st
from dotenv import load_dotenv

from pa_analysis.analysis_processor import process
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
from source.trade_processor import multiple_process


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
        value=saved_inputs.get("portfolio_ids_input", "F13,F13_1"),
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
            default=saved_inputs.get("long_entry_signals", None),
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
            default=saved_inputs.get("short_entry_signals", None),
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

    # Check if all required fields are filled
    if (
        portfolio_ids_input
        and instruments
        and long_entry_signals
        and short_entry_signals
        and strategy_pairs
        and start_date
        and end_date
    ):
        if st.button("Submit"):
            start = time.time()
            validated_data = validate(streamlit_inputs)
            multiple_process(validated_data, process)
            et = time.time()
            st.write(f"Time taken: {et - start} seconds")
    else:
        st.write("Please fill in all the required fields.")


if __name__ == "__main__":
    main()
