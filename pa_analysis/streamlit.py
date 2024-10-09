from datetime import datetime
from itertools import combinations, product
from pathlib import Path
import time
import pandas as pd
import streamlit as st

from pa_analysis.analysis_processor import process
from pa_analysis.summary import process_summaries
from pa_analysis.utils import categorize_signal
from source.constants import (
    PA_ANALYSIS_CYCLE_FOLDER,
    MarketDirection,
)
from source.streamlit import (
    add_tp_fields,
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
    update_volume_and_volatile_files,
    write_user_inputs,
)
from pa_analysis.validation import validate


file_name = "pa_analysis_user_inputs.json"


def main_1():

    global file_name

    saved_inputs = {}
    use_saved_input = st.checkbox("Use Saved Inputs", value=False)
    if use_saved_input:

        all_user_inputs = load_input_from_json(file_name)
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

    expander_option = st.selectbox(
        "Select Expander", ["Single Analysis", "Summary"], index=0
    )
    if expander_option == "Single Analysis":
        streamlit_inputs = {}
        st.header("PA Analysis")
        update_volume_and_volatile_files(streamlit_inputs, saved_inputs)
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
            strategy_ids_per_portfolio = set_portfolio_strategies(
                portfolio_ids, streamlit_inputs, saved_inputs
            )

            combination_length = st.number_input(
                "Combination Length",
                value=saved_inputs.get("combination_length", 2),
            )
            streamlit_inputs["combination_length"] = combination_length

            options = [
                (portfolio_id, strategy_id)
                for portfolio_id, strategy_ids in strategy_ids_per_portfolio.items()
                for strategy_id in strategy_ids
            ]
            all_combination = list(combinations(options, combination_length))

            saved_strategy_pairs = saved_inputs.get("strategy_pairs", [])
            saved_strategy_pairs = [
                tuple(tuple(item) for item in pair)
                for pair in saved_strategy_pairs
            ]
            strategy_pairs = st.multiselect(
                "Strategy Combination",
                options=all_combination,
                default=[
                    item
                    for item in saved_strategy_pairs
                    if item in all_combination
                ],
            )
            streamlit_inputs["strategy_pairs"] = strategy_pairs

            filtered_flag_combinations = get_flag_combinations(
                portfolio_ids, possible_flags_per_portfolio, combination_length
            )
            categories = categorize_signal(filtered_flag_combinations)

            categories_options = list(categories.keys())
            selected_category = st.selectbox(
                "Select Category",
                categories_options,
                index=categories_options.index(
                    saved_inputs.get("category", categories_options[0])
                ),
            )

            streamlit_inputs["category"] = selected_category

            all_flag_combinations = [*categories[selected_category]]

            if streamlit_inputs["allowed_direction"] in (
                MarketDirection.LONG.value,
                MarketDirection.ALL.value,
            ):
                long_entry_signals = st.multiselect(
                    "Long Entry Signals",
                    all_flag_combinations,
                    default=[
                        tuple(signal)
                        for signal in saved_inputs.get(
                            "long_entry_signals", []
                        )
                    ],
                )
            else:
                long_entry_signals = []

            if streamlit_inputs["allowed_direction"] in (
                MarketDirection.SHORT.value,
                MarketDirection.ALL.value,
            ):
                short_entry_signals = st.multiselect(
                    "Short Entry Signals",
                    [
                        combination
                        for combination in all_flag_combinations
                        if combination not in long_entry_signals
                    ],
                    default=[
                        tuple(signal)
                        for signal in saved_inputs.get(
                            "short_entry_signals", None
                        )
                    ],
                )
            else:
                short_entry_signals = []

            if streamlit_inputs["allowed_direction"] in (
                MarketDirection.LONG.value,
                MarketDirection.ALL.value,
            ):
                long_exit_signals = st.multiselect(
                    "Long Exit Signals",
                    set(filtered_flag_combinations) - set(long_entry_signals),
                    default=set(
                        [
                            *[
                                tuple(signal)
                                for signal in saved_inputs.get(
                                    "long_exit_signals", []
                                )
                            ],
                            *short_entry_signals,
                        ]
                    ),
                )
            else:
                long_exit_signals = []

            if streamlit_inputs["allowed_direction"] in (
                MarketDirection.SHORT.value,
                MarketDirection.ALL.value,
            ):
                short_exit_signals = st.multiselect(
                    "Short Exit Signals",
                    set(filtered_flag_combinations)
                    - set(short_entry_signals)
                    - set(long_exit_signals),
                    default=set(
                        [
                            *[
                                tuple(signal)
                                for signal in saved_inputs.get(
                                    "short_exit_signals", []
                                )
                            ],
                            *long_entry_signals,
                        ]
                    ),
                )
            else:
                short_exit_signals = []

            streamlit_inputs.update(
                {
                    "long_entry_signals": long_entry_signals,
                    "short_entry_signals": short_entry_signals,
                    "long_exit_signals": long_exit_signals,
                    "short_exit_signals": short_exit_signals,
                }
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
            add_tp_fields(streamlit_inputs, required_fields)
            all_fields_filled = all(required_fields)
            if all_fields_filled:
                notes = st.text_input(
                    "Notes", value=saved_inputs.get("notes", "")
                )
                save = st.checkbox(
                    "Save Inputs", value=saved_inputs.get("save", True)
                )
                if st.button("Submit"):
                    start = time.time()
                    try:
                        validated_data = validate(streamlit_inputs)
                        if validated_data:
                            if save:
                                temp = {
                                    "timestamp": datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                    "notes": notes,
                                }
                                temp.update(streamlit_inputs)
                                write_user_inputs(temp, file_name)
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
                        "Select a strategy",
                        list(st.session_state.result_dfs.keys()),
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

    elif expander_option == "Summary":
        # multi select for all names of output of volatile outputs
        folder = Path(PA_ANALYSIS_CYCLE_FOLDER)
        files = [f.name for f in folder.iterdir() if f.is_file()]
        selected_files = st.multiselect("Files", files)

        if selected_files and st.button("Submit"):
            process_summaries(selected_files)


def get_flag_combinations(
    portfolio_ids, possible_flags_per_portfolio, combination_length=2
):
    """
    Generate all possible combinations of flags for the given portfolios.

    Args:
        portfolio_ids (list): List of portfolio IDs.
        possible_flags_per_portfolio (dict): Dictionary of possible flags for each portfolio.

    Returns:
        list: List of valid flag combinations.
    """
    all_flags = set(
        flag
        for flags in possible_flags_per_portfolio.values()
        for flag in flags
    )
    flag_combinations = list(product(all_flags, repeat=combination_length))
    return flag_combinations


def main():
    """
    Main function to run the Streamlit app.
    """
    expander_option = st.selectbox(
        "Select Expander", ["Single Analysis", "Summary"]
    )
    if expander_option == "Single Analysis":
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
                    map(
                        lambda x: tuple(x), saved_inputs["short_entry_signals"]
                    )
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
        update_volume_and_volatile_files(streamlit_inputs)
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
            need_categorization = st.checkbox(
                "Need Categorization", value=False
            )
            if need_categorization:
                set_entry_exit_signals_1(
                    streamlit_inputs,
                    saved_inputs,
                    portfolio_ids,
                    possible_flags_per_portfolio,
                )
            else:
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
            need_additional_strategy_file = st.checkbox(
                "Need Additional Strategy File",
                value=saved_inputs.get("need_additional_strategy_file", False),
            )
            streamlit_inputs["need_additional_strategy_file"] = (
                need_additional_strategy_file
            )
            if need_additional_strategy_file:

                portfolio_id = st.text_input(
                    "Portfolio ID",
                    value=saved_inputs.get("portfolio_id", "F13"),
                )
                strategy_file = st.text_input(
                    "Strategy File",
                    value=saved_inputs.get("strategy_file", "1"),
                )
                long_entry_signal = st.text_input(
                    "Long Entry Signal",
                    value=saved_inputs.get("long_entry_signal", "GREEN"),
                )

                short_entry_signal = st.text_input(
                    "Short Entry Signal",
                    value=saved_inputs.get("short_entry_signal", "RED"),
                )

                streamlit_inputs["portfolio_ids"] = (
                    *streamlit_inputs["portfolio_ids"],
                    portfolio_id.strip(),
                )
                streamlit_inputs["strategy_pairs"] = [
                    (*pair, int(strategy_file.strip()))
                    for pair in streamlit_inputs["strategy_pairs"]
                ]

                streamlit_inputs["long_entry_signals"] = [
                    (*signal, long_entry_signal.strip())
                    for signal in streamlit_inputs["long_entry_signals"]
                ]
                streamlit_inputs["short_entry_signals"] = [
                    (*signal, short_entry_signal.strip())
                    for signal in streamlit_inputs["short_entry_signals"]
                ]
                streamlit_inputs["long_exit_signals"] = [
                    (*signal, short_entry_signal.strip())
                    for signal in streamlit_inputs["long_exit_signals"]
                ]
                streamlit_inputs["short_exit_signals"] = [
                    (*signal, long_entry_signal.strip())
                    for signal in streamlit_inputs["short_exit_signals"]
                ]

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
        add_tp_fields(streamlit_inputs, required_fields)
        all_fields_filled = all(required_fields)
        if all_fields_filled:
            notes = st.text_input("Notes", value=saved_inputs.get("notes", ""))
            save = st.checkbox(
                "Save Inputs", value=saved_inputs.get("save", True)
            )
            if st.button("Submit"):
                start = time.time()
                try:
                    validated_data = validate(streamlit_inputs)
                    if validated_data:
                        if save:
                            temp = {
                                "timestamp": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "notes": notes,
                            }
                            temp.update(streamlit_inputs)
                            write_user_inputs(temp, file_name)
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
                    "Select a strategy",
                    list(st.session_state.result_dfs.keys()),
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

    elif expander_option == "Summary":
        # multi select for all names of output of volatile outputs
        folder = Path(PA_ANALYSIS_CYCLE_FOLDER)
        files = [f.name for f in folder.iterdir() if f.is_file()]
        selected_files = st.multiselect("Files", files)

        if selected_files and st.button("Submit"):
            process_summaries(selected_files)


def set_entry_exit_signals_1(
    streamlit_inputs, saved_inputs, portfolio_ids, possible_flags_per_portfolio
):
    filtered_flag_combinations = get_flag_combinations(
        portfolio_ids, possible_flags_per_portfolio
    )

    categories = categorize_signal(filtered_flag_combinations)

    selected_category = st.selectbox(
        "Select Category",
        categories.keys(),
    )

    all_flag_combinations = [*categories[selected_category]]

    if streamlit_inputs["allowed_direction"] in (
        MarketDirection.LONG.value,
        MarketDirection.ALL.value,
    ):
        long_entry_signals = st.multiselect(
            "Long Entry Signals",
            all_flag_combinations,
            default=saved_inputs.get("long_entry_signals", None),
        )
    else:
        long_entry_signals = []

    if streamlit_inputs["allowed_direction"] in (
        MarketDirection.SHORT.value,
        MarketDirection.ALL.value,
    ):
        short_entry_signals = st.multiselect(
            "Short Entry Signals",
            [
                combination
                for combination in all_flag_combinations
                if combination not in long_entry_signals
            ],
            default=saved_inputs.get("short_entry_signals", None),
        )
    else:
        short_entry_signals = []

    if streamlit_inputs["allowed_direction"] in (
        MarketDirection.LONG.value,
        MarketDirection.ALL.value,
    ):
        long_exit_signals = st.multiselect(
            "Long Exit Signals",
            set(filtered_flag_combinations) - set(long_entry_signals),
            default=set(
                [
                    *saved_inputs.get("long_exit_signals", []),
                    *short_entry_signals,
                ]
            ),
        )
    else:
        long_exit_signals = []

    if streamlit_inputs["allowed_direction"] in (
        MarketDirection.SHORT.value,
        MarketDirection.ALL.value,
    ):
        short_exit_signals = st.multiselect(
            "Short Exit Signals",
            set(filtered_flag_combinations)
            - set(short_entry_signals)
            - set(long_exit_signals),
            default=set(
                [
                    *saved_inputs.get("short_exit_signals", []),
                    *long_entry_signals,
                ]
            ),
        )
    else:
        short_exit_signals = []

    streamlit_inputs.update(
        {
            "long_entry_signals": long_entry_signals,
            "short_entry_signals": short_entry_signals,
            "long_exit_signals": long_exit_signals,
            "short_exit_signals": short_exit_signals,
        }
    )


if __name__ == "__main__":
    main_1()
