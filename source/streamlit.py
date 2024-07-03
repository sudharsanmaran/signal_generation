"""
To document and comment the provided code in the `streamlit.py` file, I will add comments and docstrings to explain the purpose and functionality of each section. Here is the commented code:

### Explanation:
- **Imports**: Importing necessary libraries and modules at the beginning of the script.
- **`load_dotenv`**: Loads environment variables from a `.env` file.
- **Functions**:
  - **`select_all_options`**: This function sets all options for a given key in the Streamlit session state.
  - **`parse_strategy_ids`**: This function parses a string of strategy IDs into a list of integers.
  - **`get_portfolio_flags`**: This function retrieves possible flags for each portfolio based on user input.
  - **`get_flag_combinations`**: This function generates all possible combinations of flags for the given portfolios.
  - **`get_strategy_id_combinations`**: This function generates all possible combinations of strategy IDs for the given portfolios.
  - **`validate`**: This function validates the input data using Pydantic validators.
  - **`main`**: This is the main function that runs the Streamlit app, collecting user input and triggering the trade processing logic.
  - **`write_user_inputs`**: This function writes validated user inputs to a CSV file.
- **Streamlit Application**:
  - The Streamlit app collects various inputs related to trading systems from the user.
  - The inputs include portfolio IDs, entry/exit signals, strategy IDs, trade timings, and other trading parameters.
  - It validates the inputs, writes them to a CSV file, and initiates the trade processing functions.
"""

# Import necessary libraries
from datetime import datetime
import json
import os
import time
from pydantic import ValidationError
import streamlit as st
from itertools import product
from dotenv import load_dotenv

# Import project-specific modules
from source.constants import (
    INSTRUMENTS,
    PERIOD_OPTIONS,
    POSSIBLE_STRATEGY_IDS,
    SD_OPTIONS,
    STOCKS_FNO,
    STOCKS_NON_FNO,
    TIMEFRAME_OPTIONS,
    CycleType,
    MarketDirection,
    TradeType,
)
from source.processors.cycle_trade_processor import process_cycle
from source.processors.signal_trade_processor import (
    multiple_process,
    process_strategy,
)
from source.validation.cycle_validation import validate_cycle_input
from source.validation.signal_validations import validate_signal_input

# Load environment variables from a .env file
load_dotenv(override=True)


def select_all_options(key, combinations):
    """
    Select all options for a given key in Streamlit session state.

    Args:
        key (str): The key for the Streamlit session state.
        combinations (list): List of combinations to be set.
    """
    if "ALL" in st.session_state[key]:
        if key == "short_entry_signals":
            st.session_state["long_entry_signals"] = []
        st.session_state[key] = combinations


def parse_strategy_ids(input_str):
    """
    Parse a string of strategy IDs into a list of integers.

    Args:
        input_str (str): The input string containing strategy IDs.

    Returns:
        list: List of parsed strategy IDs.
    """
    ids = []
    parts = input_str.split(",")
    for part in parts:
        part = part.strip()
        if "-" in part:
            start, end = map(int, part.split("-"))
            ids.extend(range(start, end + 1))
        elif part.isdigit():
            ids.append(int(part))
        elif part.upper() == "ALL":
            ids.extend(POSSIBLE_STRATEGY_IDS)
    return ids


def set_portfolio_flags(portfolio_ids, streamlit_inputs, saved_inputs):
    """
    Get possible flags for each portfolio from user input.

    Args:
        portfolio_ids (list): List of portfolio IDs.
        streamlit_inputs (dict): Dictionary to store current Streamlit inputs.
        saved_inputs (dict): Dictionary of saved inputs to pre-populate fields.

    Returns:
        dict: Dictionary of possible flags for each portfolio.
    """
    possible_flags_per_portfolio = {}
    for portfolio_id in portfolio_ids:
        # Check if 'possible_flags_input' exists in saved_inputs, if not, use an empty dict
        saved_flags = saved_inputs.get("possible_flags_input", {}).get(
            portfolio_id, "RED, GREEN"
        )

        # Get the current input from the user, pre-populated with saved flags
        possible_flags_input = st.text_input(
            f"Possible Flags for portfolio {portfolio_id} (comma-separated, e.g., r, g, y)",
            value=saved_flags,
        )

        # Update the possible flags for the current portfolio ID
        possible_flags_per_portfolio[portfolio_id] = set(
            flag.strip() for flag in possible_flags_input.split(",")
        )

        # Ensure 'possible_flags_input' key exists in streamlit_inputs
        if "possible_flags_input" not in streamlit_inputs:
            streamlit_inputs["possible_flags_input"] = {}
        if (
            saved_inputs.get("possible_flags_input", {}).get(portfolio_id)
            != possible_flags_input
        ):
            saved_inputs["long_entry_signals"] = []
            saved_inputs["long_exit_signals"] = []
            saved_inputs["short_entry_signals"] = []
            saved_inputs["short_exit_signals"] = []

        # Update the 'possible_flags_input' dictionary with the current input for the portfolio ID
        streamlit_inputs["possible_flags_input"][
            portfolio_id
        ] = possible_flags_input

    return possible_flags_per_portfolio


def set_portfolio_strategies(portfolio_ids, streamlit_inputs, saved_inputs):
    """
    Get possible strategies for each portfolio from user input.

    Args:
        portfolio_ids (list): List of portfolio IDs.

    Returns:
        dict: Dictionary of possible strategies for each portfolio.
    """
    possible_strategies_per_portfolio = {}
    for portfolio_id in portfolio_ids:
        possible_strategies_input = st.text_input(
            f"Possible Strategies for portfolio {portfolio_id} (e.g., ALL, 2-10, 7)",
            value=saved_inputs.get("possible_strategies_input", {}).get(
                portfolio_id, "1"
            ),
        )
        possible_strategies_per_portfolio[portfolio_id] = parse_strategy_ids(
            possible_strategies_input
        )
        if "possible_strategies_input" not in streamlit_inputs:
            streamlit_inputs["possible_strategies_input"] = {}

        if (
            saved_inputs.get("possible_strategies_input", {}).get(portfolio_id)
            != possible_strategies_input
        ):
            saved_inputs["strategy_pairs"] = []

        streamlit_inputs["possible_strategies_input"][
            portfolio_id
        ] = possible_strategies_input

    return possible_strategies_per_portfolio


def get_flag_combinations(portfolio_ids, possible_flags_per_portfolio):
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
    flag_combinations = list(product(all_flags, repeat=len(portfolio_ids)))
    return [
        flag_pair
        for flag_pair in flag_combinations
        if all(
            flag_pair[i] in possible_flags_per_portfolio[portfolio_ids[i]]
            for i in range(len(portfolio_ids))
        )
    ]


def get_strategy_id_combinations(portfolio_ids, strategy_ids_per_portfolio):
    """
    Generate all possible combinations of strategy IDs for the given portfolios.

    Args:
        portfolio_ids (list): List of portfolio IDs.
        strategy_ids_per_portfolio (dict): Dictionary of strategy IDs for each portfolio.

    Returns:
        list: List of valid strategy ID combinations.
    """
    all_strategy_ids = set(
        id for ids in strategy_ids_per_portfolio.values() for id in ids
    )
    strategy_id_combinations = list(
        product(all_strategy_ids, repeat=len(portfolio_ids))
    )
    return [
        strategy_pair
        for strategy_pair in strategy_id_combinations
        if all(
            strategy_pair[i] in strategy_ids_per_portfolio[portfolio_ids[i]]
            for i in range(len(portfolio_ids))
        )
    ]


def validate(input_data, key: callable):
    """
    Validate the input data using Pydantic validators.

    Args:
        input_data (dict): The input data to be validated.

    Returns:
        dict: Validated input data or None if validation fails.
    """
    validated_input = None
    try:
        validated_input = key(input_data)
    except ValidationError as e:
        error_messages = [
            f"{err['loc'][0]}: {err['msg']}" for err in e.errors()
        ]
        st.error("\n,".join(error_messages))
    return validated_input


def main():
    """
    Main function to run the Streamlit app.
    """

    errors, streamlit_inputs, saved_inputs = [], {}, {}
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
            if "long_exit_signals" in saved_inputs:
                saved_inputs["long_exit_signals"] = list(
                    map(lambda x: tuple(x), saved_inputs["long_exit_signals"])
                )
            saved_inputs["short_entry_signals"] = list(
                map(lambda x: tuple(x), saved_inputs["short_entry_signals"])
            )
            if "short_exit_signals" in saved_inputs:
                saved_inputs["short_exit_signals"] = list(
                    map(lambda x: tuple(x), saved_inputs["short_exit_signals"])
                )
            saved_inputs["strategy_pairs"] = list(
                map(lambda x: tuple(x), saved_inputs["strategy_pairs"])
            )

        else:
            st.warning("saved data not found")

    st.header("Signal Generation")

    expander_option = st.selectbox("Select Expander", ["Signal", "Cycle"])

    if expander_option == "Signal":

        with st.expander("Configuration", expanded=False):
            portfolio_ids_input = set_portfolio_ids(
                streamlit_inputs, saved_inputs
            )

            set_allowed_direction(streamlit_inputs, saved_inputs)
            set_trade_type(streamlit_inputs, saved_inputs)

            set_instrument(streamlit_inputs, saved_inputs)

            stocks_fno = st.multiselect(
                "Stocks-FNO",
                options=STOCKS_FNO,
                default=saved_inputs.get("stocks_fno", ["HDFCBANK"]),
            )
            streamlit_inputs["stocks_fno"] = stocks_fno

            stocks_non_fno = st.multiselect(
                "Stocks-NONFNO",
                options=STOCKS_NON_FNO,
                default=saved_inputs.get("stocks_non_fno", ["YESBANK"]),
            )
            streamlit_inputs["stocks_non_fno"] = stocks_non_fno

            if portfolio_ids_input and streamlit_inputs["allowed_direction"]:
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
                strategy_pairs = set_strategy_pair(
                    streamlit_inputs,
                    saved_inputs,
                    portfolio_ids,
                    strategy_ids_per_portfolio,
                )

                set_start_end_datetime(streamlit_inputs, saved_inputs)

                st.text("Entry conditions: ")
                check_entry_based = st.checkbox(
                    "Check Entry Based",
                    value=saved_inputs.get("check_entry_based", False),
                )
                streamlit_inputs["check_entry_based"] = check_entry_based
                if check_entry_based:
                    number_of_entries = st.number_input(
                        "Number of Entries",
                        min_value=0,
                        value=saved_inputs.get("number_of_entries", 10),
                        step=1,
                    )
                    streamlit_inputs["number_of_entries"] = number_of_entries
                    steps_to_skip = st.number_input(
                        "Steps to Skip",
                        min_value=0,
                        value=saved_inputs.get("steps_to_skip", 3),
                        step=1,
                    )
                    streamlit_inputs["steps_to_skip"] = steps_to_skip

                # Entry Fractal Inputs (conditionally displayed)
                check_entry_fractal = st.checkbox(
                    "Check Entry Fractal",
                    value=saved_inputs.get("check_entry_fractal", False),
                )
                streamlit_inputs["check_entry_fractal"] = check_entry_fractal
                if check_entry_fractal:
                    entry_fractal_file_number = st.text_input(
                        "Entry Fractal File Number",
                        value=saved_inputs.get(
                            "entry_fractal_file_number", "1"
                        ),
                    )
                    streamlit_inputs["entry_fractal_file_number"] = (
                        entry_fractal_file_number
                    )

                    # Bollinger Band Inputs (conditionally displayed)
                    check_bb_band = st.checkbox(
                        "Check BB Band",
                        value=saved_inputs.get("check_bb_band", False),
                    )
                    streamlit_inputs["check_bb_band"] = check_bb_band
                    if check_bb_band:
                        bb_file_number = st.text_input(
                            "BB File Number",
                            value=saved_inputs.get("bb_file_number", "1"),
                        )

                        options = [2.0, 2.25, 2.5, 2.75, 3.0]
                        bb_band_sd = st.selectbox(
                            "BB Band Standard Deviations",
                            options=options,
                            index=options.index(
                                saved_inputs.get("bb_band_sd", 2.0)
                            ),
                        )

                        options = ["mean", "upper", "lower"]
                        bb_band_column = st.selectbox(
                            "BB Band Column",
                            options=options,
                            index=options.index(
                                saved_inputs.get("bb_band_column", "mean")
                            ),
                        )

                        streamlit_inputs.update(
                            {
                                "bb_file_number": bb_file_number,
                                "bb_band_sd": bb_band_sd,
                                "bb_band_column": bb_band_column,
                            }
                        )
                skip_rows = st.checkbox(
                    "Skip Rows", value=saved_inputs.get("skip_rows", False)
                )
                streamlit_inputs["skip_rows"] = skip_rows
                if skip_rows:
                    no_of_rows_to_skip = st.number_input(
                        "No Of Rows To Skip",
                        min_value=0,
                        value=saved_inputs.get("no_of_rows_to_skip", 0),
                        step=1,
                    )
                    streamlit_inputs["no_of_rows_to_skip"] = no_of_rows_to_skip

                if check_entry_based and check_entry_fractal:
                    check_entry_based = False
                    check_entry_fractal = False

                    error_mssg = "Please select either 'Check Entry Based' or 'Check Entry Fractal', not both."
                    st.error(error_mssg)
                    errors.append(error_mssg)

                st.text("Exits conditions: ")
                # Exit Fractal Inputs (conditionally displayed)
                set_fractal_exit(streamlit_inputs, saved_inputs)

                # Trail BB Band Inputs (conditionally displayed)
                check_trail_bb_band = st.checkbox(
                    "Check Trail BB Band",
                    value=saved_inputs.get("check_trail_bb_band", False),
                )
                streamlit_inputs["check_trail_bb_band"] = check_trail_bb_band
                if check_trail_bb_band:
                    trail_bb_file_number = st.text_input(
                        "Trail BB File Number",
                        value=saved_inputs.get("trail_bb_file_number", "1"),
                    )

                    options = [2.0, 2.25, 2.5, 2.75, 3.0]
                    trail_bb_band_sd = st.selectbox(
                        "Trail BB Band Standard Deviations",
                        options=options,
                        index=options.index(
                            saved_inputs.get("trail_bb_band_sd", 2.0)
                        ),
                    )

                    options = ["mean", "upper", "lower"]
                    trail_bb_band_column = st.selectbox(
                        "Trail BB Band Column",
                        options=options,
                        index=options.index(
                            saved_inputs.get("trail_bb_band_column", "mean")
                        ),
                    )

                    options = ["higher", "lower"]
                    trail_bb_band_long_direction = st.selectbox(
                        "Trail BB Band Long Direction",
                        options=options,
                        index=options.index(
                            saved_inputs.get(
                                "trail_bb_band_long_direction", "higher"
                            )
                        ),
                    )

                    trail_bb_band_short_direction = st.selectbox(
                        "Trail BB Band Short Direction",
                        options=options,
                        index=options.index(
                            saved_inputs.get(
                                "trail_bb_band_short_direction", "higher"
                            )
                        ),
                    )
                    streamlit_inputs.update(
                        {
                            "trail_bb_file_number": trail_bb_file_number,
                            "trail_bb_band_sd": trail_bb_band_sd,
                            "trail_bb_band_column": trail_bb_band_column,
                            "trail_bb_band_long_direction": trail_bb_band_long_direction,
                            "trail_bb_band_short_direction": trail_bb_band_short_direction,
                        }
                    )

    elif expander_option == "Cycle":
        with st.expander("Configuration", expanded=False):

            portfolio_ids_input = set_portfolio_ids(
                streamlit_inputs, saved_inputs
            )
            set_allowed_direction(streamlit_inputs, saved_inputs)
            set_trade_type(streamlit_inputs, saved_inputs)
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
                st.text("Entry conditions: ")
                set_fractal_entry(streamlit_inputs, saved_inputs)
                st.text("Exits conditions: ")
                set_fractal_exit(streamlit_inputs, saved_inputs)
                st.text("Cycle conditions: ")
                set_cycle_configs(streamlit_inputs, saved_inputs)

    st.header("Trade Management")

    with st.expander("Configurations", expanded=False):
        # Segment
        options = ["CASH", "FUTURE", "OPTIONS"]
        segment = st.selectbox(
            "Segment",
            options,
            index=options.index(saved_inputs.get("segment", "CASH")),
        )
        streamlit_inputs["segment"] = segment

        if segment == "OPTIONS":
            # OPTBUYING
            options = ["YES", "NO"]
            opt_buying = st.selectbox(
                "OPTBUYING",
                options,
                index=options.index(saved_inputs.get("opt_buying", "NO")),
            )
            expiry = st.number_input(
                "Expiry", min_value=1, value=saved_inputs.get("expiry", 1)
            )
            strike = st.number_input(
                "Strike", value=saved_inputs.get("strike", 1)
            )
            streamlit_inputs.update(
                {"expiry": expiry, "strike": strike, "opt_buying": opt_buying}
            )

            # Premium Feature
            premium_feature = st.checkbox(
                "Premium Feature",
                value=saved_inputs.get("premium_feature", False),
            )
            streamlit_inputs["premium_feature"] = premium_feature

        if segment == "FUTURE":
            # Hedge
            expiry = st.number_input(
                "Expiry",
                min_value=1,
                value=saved_inputs.get("expiry", 1),
                max_value=3,
            )
            hedge = st.checkbox(
                "Hedge", value=saved_inputs.get("hedge", False)
            )
            streamlit_inputs["hedge"] = hedge
            streamlit_inputs["expiry"] = expiry
            if hedge:
                hedge_expiry = st.number_input(
                    "Hedge Expiry",
                    min_value=1,
                    value=saved_inputs.get("hedge_expiry", 1),
                )
                hedge_strike = st.number_input(
                    "Hedge Strike",
                    min_value=1,
                    value=saved_inputs.get("hedge_strike", 1),
                )
                hedge_delayed_exit = st.checkbox(
                    "Hedge Delayed Exit",
                    value=saved_inputs.get("hedge_delayed_exit", False),
                )
                streamlit_inputs.update(
                    {
                        "hedge_expiry": hedge_expiry,
                        "hedge_strike": hedge_strike,
                        "hedge_delayed_exit": hedge_delayed_exit,
                    }
                )

        # Appreciation/Depreciation based entry
        ade_based_entry = st.checkbox(
            "Appreciation/Depreciation based entry",
            value=saved_inputs.get("ade_based_entry", False),
        )
        streamlit_inputs["ade_based_entry"] = ade_based_entry
        if ade_based_entry:
            options = ["APPRECIATION", "DEPRECIATION"]
            appreciation_depreciation = st.selectbox(
                "Appreciation/Depreciation",
                options,
                index=options.index(
                    saved_inputs.get(
                        "appreciation_depreciation", "APPRECIATION"
                    )
                ),
            )
            ade_percentage = st.number_input(
                "Appreciation/Depreciation %",
                min_value=0.0,
                step=0.01,
                value=saved_inputs.get("ade_percentage", 0.0),
            )
            streamlit_inputs.update(
                {
                    "appreciation_depreciation": appreciation_depreciation,
                    "ade_percentage": ade_percentage,
                }
            )

        # TARGET
        target = st.checkbox("TARGET", value=saved_inputs.get("target", False))
        streamlit_inputs["target"] = target
        if target:
            target_profit_percentage = st.number_input(
                "TARGET Profit %",
                min_value=0.0,
                step=0.01,
                value=saved_inputs.get("target_profit_percentage", 0.0),
            )
            streamlit_inputs["target_profit_percentage"] = (
                target_profit_percentage
            )

        # SL Trading
        sl_trading = st.checkbox(
            "SL Trading", value=saved_inputs.get("sl_trading", False)
        )
        streamlit_inputs["sl_trading"] = sl_trading
        if sl_trading:
            sl_percentage = st.number_input(
                "SL %",
                min_value=0.0,
                step=0.01,
                value=saved_inputs.get("sl_percentage", 0.0),
            )
            streamlit_inputs["sl_percentage"] = sl_percentage

        # Re-deployment
        re_deployment = st.checkbox(
            "Re-deployment", value=saved_inputs.get("re_deployment", False)
        )
        streamlit_inputs["re_deployment"] = re_deployment
        if re_deployment:
            re_ade_based_entry = st.checkbox(
                "RE_Appreciation/Depreciation based entry",
                saved_inputs.get("re_ade_based_entry", False),
            )
            streamlit_inputs["re_ade_based_entry"] = re_ade_based_entry
            if re_ade_based_entry:
                options = ["APPRECIATION", "DEPRECIATION"]
                re_appreciation_depreciation = st.selectbox(
                    "RE_Appreciation/Depreciation",
                    options=options,
                    index=options.index(
                        saved_inputs.get(
                            "re_appreciation_depreciation", "APPRECIATION"
                        )
                    ),
                )
                re_ade_percentage = st.number_input(
                    "RE_Appreciation/Depreciation %",
                    min_value=0.0,
                    step=0.01,
                    value=saved_inputs.get("re_ade_percentage", 0.0),
                )
                streamlit_inputs.update(
                    {
                        "re_appreciation_depreciation": re_appreciation_depreciation,
                        "re_ade_percentage": re_ade_percentage,
                    }
                )

        # DTE - Based testing
        dte_based_testing = st.checkbox(
            "DTE - Based testing",
            value=saved_inputs.get("dte_based_testing", False),
        )
        streamlit_inputs["dte_based_testing"] = dte_based_testing
        if dte_based_testing:
            dte_from = st.number_input(
                "From which DTE",
                min_value=1,
                value=saved_inputs.get("dte_from", 1),
            )
            streamlit_inputs["dte_from"] = dte_from

        # Next Expiry trading
        next_expiry_trading = st.checkbox(
            "Next Expiry trading",
            value=saved_inputs.get("next_expiry_trading", False),
        )
        streamlit_inputs["next_expiry_trading"] = next_expiry_trading
        if next_expiry_trading:
            next_dte_from = st.number_input(
                "From DTE",
                min_value=1,
                value=saved_inputs.get("next_dte_from", 1),
            )
            next_expiry = st.number_input(
                "Expiry No",
                min_value=1,
                value=saved_inputs.get("next_expiry", 1),
            )
            streamlit_inputs.update(
                {"next_dte_from": next_dte_from, "next_expiry": next_expiry}
            )

        # Volume feature
        volume_feature = st.checkbox(
            "Volume feature", value=saved_inputs.get("volume_feature", False)
        )
        streamlit_inputs["volume_feature"] = volume_feature
        if volume_feature:
            volume_minutes = st.number_input(
                "Number of minutes",
                min_value=1,
                value=saved_inputs.get("volume_minutes", 1),
            )
            streamlit_inputs["volume_minutes"] = volume_minutes

        # Capital, Risk, Leverage
        capital = st.number_input(
            "Capital",
            min_value=0,
            value=saved_inputs.get("capital", 100000000),
        )
        risk = st.number_input(
            "Risk",
            min_value=0.0,
            max_value=1.0,
            value=saved_inputs.get("risk", 0.04),
            step=0.01,
        )
        leverage = st.number_input(
            "Leverage", min_value=1, value=saved_inputs.get("leverage", 2)
        )
        streamlit_inputs.update(
            {"capital": capital, "risk": risk, "leverage": leverage}
        )

    notes = st.text_input("Notes", value=saved_inputs.get("notes", ""))
    save = st.checkbox("Save Inputs", value=saved_inputs.get("save", True))
    trigger_trade_management = st.checkbox(
        "Trigger Trade Management Module", value=False
    )
    streamlit_inputs["trigger_trade_management"] = trigger_trade_management

    required_fields = [
        streamlit_inputs["portfolio_ids_input"],
        streamlit_inputs["trade_type"],
        streamlit_inputs["instruments"],
        streamlit_inputs["portfolio_ids"],
        streamlit_inputs["possible_flags_input"],
        streamlit_inputs["possible_strategies_input"],
        streamlit_inputs["strategy_pairs"],
        streamlit_inputs["start_date"],
        streamlit_inputs["end_date"],
    ]
    if expander_option == "Signal":
        long_fields = [
            streamlit_inputs["long_entry_signals"],
            streamlit_inputs["long_exit_signals"],
        ]
        short_fields = [
            streamlit_inputs["short_entry_signals"],
            streamlit_inputs["short_exit_signals"],
        ]
        required_fields.append(streamlit_inputs["allowed_direction"])
        if (
            streamlit_inputs["allowed_direction"]
            and streamlit_inputs["allowed_direction"]
            == MarketDirection.LONG.value
        ):
            required_fields.extend(long_fields)
        elif (
            streamlit_inputs["allowed_direction"]
            and streamlit_inputs["allowed_direction"]
            == MarketDirection.SHORT.value
        ):
            required_fields.extend(short_fields)
        elif (
            streamlit_inputs["allowed_direction"]
            and streamlit_inputs["allowed_direction"]
            == MarketDirection.ALL.value
        ):

            required_fields.extend(
                [
                    *long_fields,
                    *short_fields,
                ]
            )
    elif expander_option == "Cycle":
        cycle_fields = [
            streamlit_inputs["long_entry_signals"],
            streamlit_inputs["short_entry_signals"],
            check_cycles_inputs(streamlit_inputs),
        ]
        required_fields.extend(cycle_fields)

    all_fields_filled = all(required_fields)

    if all_fields_filled:
        if st.button("Submit"):

            validate_func = validate_signal_input
            exec_func = process_strategy
            if expander_option == "Cycle":
                validate_func = validate_cycle_input
                exec_func = process_cycle

            validated_input = validate(streamlit_inputs, key=validate_func)

            if validated_input:
                if save:
                    temp = {
                        "timestamp": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "notes": notes,
                    }
                    temp.update(streamlit_inputs)
                    write_user_inputs(temp)

                # Start trade processing
                execute(validated_input, exec_func)
    else:
        st.error("Please fill in all the required fields.")


def set_entry_exit_signals(
    streamlit_inputs, saved_inputs, portfolio_ids, possible_flags_per_portfolio
):
    filtered_flag_combinations = get_flag_combinations(
        portfolio_ids, possible_flags_per_portfolio
    )
    all_flag_combinations = ["ALL"] + filtered_flag_combinations

    if streamlit_inputs["allowed_direction"] in (
        MarketDirection.LONG.value,
        MarketDirection.ALL.value,
    ):
        long_entry_signals = st.multiselect(
            "Long Entry Signals",
            all_flag_combinations,
            default=saved_inputs.get("long_entry_signals", None),
            key="long_entry_signals",
            on_change=select_all_options,
            args=(
                "long_entry_signals",
                filtered_flag_combinations,
            ),
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
            key="short_entry_signals",
            on_change=select_all_options,
            args=(
                "short_entry_signals",
                filtered_flag_combinations,
            ),
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


def set_cycle_configs(streamlit_inputs, saved_inputs):

    calculate_cycles = st.checkbox(
        "Calculate Cycles",
        value=saved_inputs.get("calculate_cycles", True),
    )
    streamlit_inputs["calculate_cycles"] = calculate_cycles
    if calculate_cycles:
        cycle_options = [
            cycle.value
            for cycle in CycleType
            if cycle != CycleType.PREVIOUS_CYCLE
        ]
        cycle_to_consider = st.selectbox(
            "Cycle to Consider",
            cycle_options,
        )
        streamlit_inputs["cycle_to_consider"] = cycle_to_consider

        st.text("BB Band 1 inputs:")

        close_time_frames_1 = st.multiselect(
            "Close Time Frame",
            TIMEFRAME_OPTIONS,
            default=saved_inputs.get("close_time_frames_1", [2]),
        )

        if close_time_frames_1:
            bb_tf_options = TIMEFRAME_OPTIONS[
                TIMEFRAME_OPTIONS.index(max(close_time_frames_1)) :
            ]
        else:
            bb_tf_options = TIMEFRAME_OPTIONS

        bb_time_frames_1 = st.multiselect(
            "BB Time Frame",
            bb_tf_options,
        )

        include_higher_and_lower = st.checkbox(
            "Include Higher and Lower BB Bands",
            value=saved_inputs.get("include_higer_and_lower", False),
        )
        # bb_band_column_1 = st.selectbox(
        #     "BB Band Column",
        #     index=2,
        #     options=["UPPER", "LOWER", "MEAN"],
        # )
        periods_1 = st.multiselect(
            "Periods",
            PERIOD_OPTIONS,
            default=saved_inputs.get("period", [20]),
        )
        sds_1 = st.multiselect(
            "Standard Deviations",
            SD_OPTIONS,
            default=saved_inputs.get("sd", [2]),
        )

        close_percent = st.number_input(
            "Close Percent",
            value=saved_inputs.get("close_percent", 0.5),
            max_value=1.0,
            min_value=0.0,
        )

        max_to_min_percent = st.number_input(
            "Max to Min Percent",
            value=saved_inputs.get("max_to_min_percent", 0.5),
            max_value=1.0,
            min_value=0.0,
        )

        streamlit_inputs.update(
            {
                "close_time_frames_1": close_time_frames_1,
                "bb_time_frames_1": bb_time_frames_1,
                "include_higher_and_lower": include_higher_and_lower,
                "periods_1": periods_1,
                "sds_1": sds_1,
                "close_percent": close_percent,
                "max_to_min_percent": max_to_min_percent,
            }
        )

        st.text("BB Band 2 inputs:")
        check_bb_2 = st.checkbox(
            "Check BB 2",
            value=saved_inputs.get("calculate_cycles", False),
        )
        streamlit_inputs["check_bb_2"] = check_bb_2
        if check_bb_2:
            if bb_time_frames_1:
                bb_2_tf_options = TIMEFRAME_OPTIONS[
                    TIMEFRAME_OPTIONS.index(max(bb_time_frames_1)) :
                ]
            else:
                bb_2_tf_options = TIMEFRAME_OPTIONS

            bb_time_frames_2 = st.multiselect(
                "BB 2 Time Frame",
                bb_2_tf_options,
            )

            if periods_1:
                bb_2_period_options = PERIOD_OPTIONS[
                    PERIOD_OPTIONS.index(max(periods_1)) + 1 :
                ]
            else:
                bb_2_period_options = PERIOD_OPTIONS

            if (
                bb_time_frames_2
                and bb_time_frames_1
                and min(bb_time_frames_2) > max(bb_time_frames_1)
            ):
                bb_2_period_options = PERIOD_OPTIONS

            periods_2 = st.multiselect(
                "BB 2 Periods",
                bb_2_period_options,
            )
            sds_2 = st.multiselect(
                "BB 2 Standard Deviations",
                SD_OPTIONS,
                default=saved_inputs.get("sd", [2]),
            )
            streamlit_inputs.update(
                {
                    "bb_time_frames_2": bb_time_frames_2,
                    "periods_2": periods_2,
                    "sds_2": sds_2,
                }
            )


def set_fractal_exit(streamlit_inputs, saved_inputs):
    check_exit_fractal = st.checkbox(
        "Check Exit Fractal",
        value=saved_inputs.get("check_exit_fractal", False),
    )
    streamlit_inputs["check_exit_fractal"] = check_exit_fractal
    if check_exit_fractal:
        exit_fractal_file_number = st.text_input(
            "Exit Fractal File Number",
            value=saved_inputs.get("exit_fractal_file_number", "2"),
        )
        fractal_exit_count = st.text_input(
            "Fractal Exit Count (e.g., 6, ALL)",
            value=saved_inputs.get("fractal_exit_count", "ALL"),
        )
        streamlit_inputs.update(
            {
                "exit_fractal_file_number": exit_fractal_file_number,
                "fractal_exit_count": fractal_exit_count,
            }
        )


def set_fractal_entry(streamlit_inputs, saved_inputs):
    # Entry Fractal Inputs (conditionally displayed)

    check_entry_fractal = st.checkbox(
        "Check Entry Fractal",
        value=saved_inputs.get("check_entry_fractal", False),
    )
    streamlit_inputs["check_entry_fractal"] = check_entry_fractal
    if check_entry_fractal:
        entry_fractal_file_number = st.text_input(
            "Entry Fractal File Number",
            value=saved_inputs.get("entry_fractal_file_number", "1"),
        )
        streamlit_inputs["entry_fractal_file_number"] = (
            entry_fractal_file_number
        )


def set_start_end_datetime(streamlit_inputs, saved_inputs):
    start_date = st.text_input(
        "Start Date (format: dd/mm/yyyy hh:mm:ss)",
        value=saved_inputs.get("start_date", "3/01/2019 09:00:00"),
    )
    end_date = st.text_input(
        "End Date (format: dd/mm/yyyy hh:mm:ss)",
        value=saved_inputs.get("end_date", "3/04/2019 16:00:00"),
    )
    streamlit_inputs.update({"start_date": start_date, "end_date": end_date})


def set_strategy_pair(
    streamlit_inputs, saved_inputs, portfolio_ids, strategy_ids_per_portfolio
):
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
    return strategy_pairs


def set_long_short_signals(
    streamlit_inputs, saved_inputs, portfolio_ids, possible_flags_per_portfolio
):
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

    return long_entry_signals, short_entry_signals


def format_set_portfolio_ids(streamlit_inputs, portfolio_ids_input):
    portfolio_ids = tuple(
        map(lambda a: a.strip(), portfolio_ids_input.split(","))
    )
    streamlit_inputs["portfolio_ids"] = portfolio_ids
    return portfolio_ids


def set_instrument(streamlit_inputs, saved_inputs):
    instruments = st.multiselect(
        "INDICES",
        options=INSTRUMENTS,
        default=saved_inputs.get("instruments", ["BANKNIFTY"]),
    )
    streamlit_inputs["instruments"] = instruments
    return instruments


def set_portfolio_ids(streamlit_inputs, saved_inputs):
    portfolio_ids_input = st.text_input(
        "Portfolio IDs (comma-separated, e.g., 1, 2, 3)",
        value=saved_inputs.get("portfolio_ids_input", "F13,F13_1"),
    )
    streamlit_inputs["portfolio_ids_input"] = portfolio_ids_input
    return portfolio_ids_input


def set_allowed_direction(streamlit_inputs, saved_inputs):
    options = [
        direction.value
        for direction in MarketDirection
        if direction != MarketDirection.PREVIOUS
    ]
    default_index = options.index(saved_inputs.get("allowed_direction", "all"))
    allowed_direction = st.selectbox(
        "Allowed Direction",
        options=options,
        index=default_index,
    )
    streamlit_inputs["allowed_direction"] = allowed_direction
    return allowed_direction


def set_trade_type(streamlit_inputs, saved_inputs):
    options = [trade_type.value for trade_type in TradeType]
    trade_type = st.selectbox(
        "Trade Type",
        options=["P", "I"],
        index=options.index(saved_inputs.get("trade_type", "P")),
    )
    streamlit_inputs["trade_type"] = trade_type

    trade_start_time, trade_end_time = None, None
    if trade_type == TradeType.INTRADAY.value:
        trade_start_time = st.text_input(
            "Trade Start Time (format: hh:mm:ss)",
            value=saved_inputs.get("trade_start_time", "09:15:00"),
        )
        trade_end_time = st.text_input(
            "Trade End Time (format: hh:mm:ss)",
            value=saved_inputs.get("trade_end_time", "15:30:00"),
        )
        streamlit_inputs.update(
            {
                "trade_start_time": trade_start_time,
                "trade_end_time": trade_end_time,
            }
        )
    return trade_type, trade_start_time, trade_end_time


def execute(validated_input, exec_func: callable):
    start = time.time()
    try:
        multiple_process(validated_input, exec_func)
        # exec_func(validated_input, (1, 1, 1), "BANKNIFTY")
    except Exception as e:
        st.error(f"Error executing trade management: {e}")
        return
    stop = time.time()

    st.success(
        f"Trade processing completed successfully! Total time taken: {stop-start} seconds"
    )


# Function to load input data from a JSON file
def load_input_from_json(filename="user_inputs.json"):
    try:
        with open(filename, "r") as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        return {}


def write_user_inputs(user_input):
    """
    Write validated user inputs to a json file.

    Args:
        user_input (dict): Validated input data.
    """
    try:
        filename = "user_inputs.json"
        if os.path.isfile(filename) and os.path.getsize(filename) > 0:
            # Load existing data
            existing_data = load_input_from_json(filename)
        else:
            # Initialize an empty dict if the file doesn't exist or is empty
            existing_data = {}

        # add the new entry to the existing data
        existing_data[user_input["notes"]] = user_input

        # Set the "save" key to False to avoid saving the data again
        user_input["save"] = False

        with open(filename, "w", newline="") as json_file:
            json.dump(existing_data, json_file, indent=4)

        st.success("User inputs written to user_inputs.json successfully!")

    except Exception as e:
        st.error(f"Error writing data to json: {e}")


def check_cycles_inputs(input) -> bool:
    if input["calculate_cycles"] and input["check_bb_2"]:
        return (
            input["periods_2"] and input["sds_2"] and input["bb_time_frames_2"]
        )
    elif input["calculate_cycles"]:
        return (
            input["close_time_frames_1"]
            and input["bb_time_frames_1"]
            and input["periods_1"]
            and input["sds_1"]
        )
    return True


if __name__ == "__main__":
    main()
