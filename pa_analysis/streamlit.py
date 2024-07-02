import time
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from pa_analysis.analysis_processor import process
from source.streamlit import (
    check_cycles_inputs,
    format_set_portfolio_ids,
    set_allowed_direction,
    set_cycle_configs,
    set_entry_exit_signals,
    set_instrument,
    set_portfolio_flags,
    set_portfolio_ids,
    set_portfolio_strategies,
    load_input_from_json,
    set_start_end_datetime,
    set_strategy_pair,
)
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

            saved_inputs["long_exit_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["long_exit_signals"])
            )

            saved_inputs["short_entry_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["short_entry_signals"])
            )

            saved_inputs["short_exit_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["short_exit_signals"])
            )

            saved_inputs["strategy_pairs"] = list(
                map(lambda x: tuple(x), saved_inputs["strategy_pairs"])
            )

        else:
            st.warning("saved data not found")

    st.header("PA Analysis")
    portfolio_ids_input = set_portfolio_ids(streamlit_inputs, saved_inputs)
    set_allowed_direction(streamlit_inputs, saved_inputs)
    set_instrument(streamlit_inputs, saved_inputs)

    if portfolio_ids_input:
        portfolio_ids = format_set_portfolio_ids(
            streamlit_inputs, portfolio_ids_input
        )
        possible_flags_per_portfolio = set_portfolio_flags(
            portfolio_ids, streamlit_inputs, saved_inputs
        )
        set_entry_exit_signals(
            streamlit_inputs,
            saved_inputs,
            portfolio_ids,
            possible_flags_per_portfolio,
        )
        strategy_ids_per_portfolio = set_portfolio_strategies(
            portfolio_ids, streamlit_inputs, saved_inputs
        )
        set_strategy_pair(
            streamlit_inputs,
            saved_inputs,
            portfolio_ids,
            strategy_ids_per_portfolio,
        )
        set_start_end_datetime(streamlit_inputs, saved_inputs)
        set_cycle_configs(streamlit_inputs, saved_inputs)

    # Check if all required fields are filled
    required_fields = [
        streamlit_inputs["portfolio_ids_input"],
        streamlit_inputs["instruments"],
        streamlit_inputs["portfolio_ids"],
        streamlit_inputs["possible_flags_input"],
        streamlit_inputs["possible_strategies_input"],
        streamlit_inputs["strategy_pairs"],
        streamlit_inputs["start_date"],
        streamlit_inputs["end_date"],
        streamlit_inputs["long_entry_signals"],
        streamlit_inputs["short_entry_signals"],
        check_cycles_inputs(streamlit_inputs),
    ]
    all_fields_filled = all(required_fields)
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


if __name__ == "__main__":
    main()
