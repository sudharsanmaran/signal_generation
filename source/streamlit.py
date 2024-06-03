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
import csv
from datetime import datetime
import os
import time
import pandas as pd
from pydantic import ValidationError
import streamlit as st
from itertools import product
from dotenv import load_dotenv

# Import project-specific modules
from source.constants import POSSIBLE_STRATEGY_IDS, MarketDirection, TradeType
from source.trade import initialize
from source.trade_processor import process_trade
from source.validations import validate_input

# Load environment variables from a .env file
load_dotenv()

INSTRUMENTS = os.getenv("INSTRUMENTS").split(",")
STOCKS_FNO = os.getenv("STOCKS_FNO").split(",")
STOCKS_NON_FNO = os.getenv("STOCKS_NON_FNO").split(",")


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


def read_user_inputs():
    try:
        data = pd.read_csv(
            "user_inputs.csv",
            parse_dates=[
                "trade_start_time",
                "trade_end_time",
                "start_date",
                "end_date",
            ],
            dtype={
                "entry_fractal_file_number": str,
                "exit_fractal_file_number": str,
                "bb_file_number": str,
                "trail_bb_file_number": str,
            },
        )
        # data = data.where(pd.notnull(data), None)
        return data
    except FileNotFoundError:
        return pd.DataFrame()


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


def get_portfolio_flags(portfolio_ids):
    """
    Get possible flags for each portfolio from user input.

    Args:
        portfolio_ids (list): List of portfolio IDs.

    Returns:
        dict: Dictionary of possible flags for each portfolio.
    """
    possible_flags_per_portfolio = {}
    for portfolio_id in portfolio_ids:
        possible_flags_input = st.text_input(
            f"Possible Flags for portfolio {portfolio_id} (comma-separated, e.g., r, g, y)",
            value="RED,GREEN",
        )
        possible_flags_per_portfolio[portfolio_id] = set(
            flag.strip() for flag in possible_flags_input.split(",")
        )
    return possible_flags_per_portfolio


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
        flag for flags in possible_flags_per_portfolio.values() for flag in flags
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
        error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
        st.error("\n,".join(error_messages))
    return validated_input


def main():
    """
    Main function to run the Streamlit app.
    """
    st.header("Signal Generation")

    errors = []
    with st.expander("Configuration", expanded=False):
        portfolio_ids_input = st.text_input(
            "Portfolio IDs (comma-separated, e.g., 1, 2, 3)", value="F13,F13_1"
        )

        allowed_direction = st.selectbox(
            "Allowed Direction",
            options=[
                direction.value
                for direction in MarketDirection
                if direction != MarketDirection.PREVIOUS
            ],
            index=0,
        )

        trade_type = st.selectbox("Trade Type", options=["P", "I"], index=0)
        trade_start_time, trade_end_time = None, None
        if trade_type == TradeType.INTRADAY.value:
            trade_start_time = st.text_input(
                "Trade Start Time (format: hh:mm:ss)", value="09:15:00"
            )
            trade_end_time = st.text_input(
                "Trade End Time (format: hh:mm:ss)", value="15:20:00"
            )

        instrument = st.selectbox("INDICES", options=INSTRUMENTS, index=0)
        stocks_fno = st.selectbox("Stocks-FNO", options=STOCKS_FNO)
        stocks_non_fno = st.selectbox("Stocks-NONFNO", options=STOCKS_NON_FNO)

        if portfolio_ids_input and allowed_direction:
            portfolio_ids = tuple(
                map(lambda a: a.strip(), portfolio_ids_input.split(","))
            )
            possible_flags_per_portfolio = get_portfolio_flags(portfolio_ids)
            filtered_flag_combinations = get_flag_combinations(
                portfolio_ids, possible_flags_per_portfolio
            )
            all_flag_combinations = ["ALL"] + filtered_flag_combinations

            if allowed_direction in (
                MarketDirection.LONG.value,
                MarketDirection.ALL.value,
            ):
                long_entry_signals = st.multiselect(
                    "Long Entry Signals",
                    all_flag_combinations,
                    key="long_entry_signals",
                    on_change=select_all_options,
                    args=("long_entry_signals", filtered_flag_combinations),
                )
            else:
                long_entry_signals = []

            if allowed_direction in (
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
                    key="short_entry_signals",
                    on_change=select_all_options,
                    args=("short_entry_signals", filtered_flag_combinations),
                )
            else:
                short_entry_signals = []

            if allowed_direction in (
                MarketDirection.LONG.value,
                MarketDirection.ALL.value,
            ):
                long_exit_signals = st.multiselect(
                    "Long Exit Signals",
                    set(filtered_flag_combinations) - set(long_entry_signals),
                    default=short_entry_signals,
                )
            else:
                long_exit_signals = []

            if allowed_direction in (
                MarketDirection.SHORT.value,
                MarketDirection.ALL.value,
            ):
                short_exit_signals = st.multiselect(
                    "Short Exit Signals",
                    set(filtered_flag_combinations)
                    - set(short_entry_signals)
                    - set(long_exit_signals),
                    default=long_entry_signals,
                )
            else:
                short_exit_signals = []

            strategy_ids_per_portfolio = {
                portfolio_id: parse_strategy_ids(
                    st.text_input(
                        f"Portfolio: {portfolio_id} - Strategy IDs (e.g., ALL, 2-10, 7):"
                    )
                )
                for portfolio_id in portfolio_ids
            }

            filtered_strategy_id_combinations = get_strategy_id_combinations(
                portfolio_ids, strategy_ids_per_portfolio
            )
            all_filtered_strategy_id_combinations = [
                "ALL"
            ] + filtered_strategy_id_combinations
            strategy_pairs = st.multiselect(
                "Strategy Pairs",
                all_filtered_strategy_id_combinations,
                key="Strategy Pairs",
                on_change=select_all_options,
                args=("Strategy Pairs", filtered_strategy_id_combinations),
            )

            start_date = st.text_input(
                "Start Date (format: dd/mm/yyyy hh:mm:ss)", value="3/01/2019 09:00:00"
            )
            end_date = st.text_input(
                "End Date (format: dd/mm/yyyy hh:mm:ss)", value="3/04/2019 16:00:00"
            )
            st.text("Entry conditions: ")
            check_entry_based = st.checkbox("Check Entry Based", value=False)
            if check_entry_based:
                number_of_entries = st.number_input(
                    "Number of Entries", min_value=0, value=10, step=1
                )
                steps_to_skip = st.number_input(
                    "Steps to Skip", min_value=0, value=3, step=1
                )
            # Entry Fractal Inputs (conditionally displayed)
            check_entry_fractal = st.checkbox("Check Entry Fractal", value=False)
            check_bb_band = False
            if check_entry_fractal:
                entry_fractal_file_number = st.text_input(
                    "Entry Fractal File Number", value="1"
                )

                # Bollinger Band Inputs (conditionally displayed)
                check_bb_band = st.checkbox("Check BB Band", value=False)
                if check_bb_band:
                    bb_file_number = st.text_input("BB File Number", value="1")
                    bb_band_sd = st.selectbox(
                        "BB Band Standard Deviations",
                        options=[2.0, 2.25, 2.5, 2.75, 3.0],
                        index=0,
                    )
                    bb_band_column = st.selectbox(
                        "BB Band Column", options=["mean", "upper", "lower"], index=0
                    )
            if check_entry_based and check_entry_fractal:
                check_entry_based = False
                check_entry_fractal = False

                error_mssg = "Please select either 'Check Entry Based' or 'Check Entry Fractal', not both."
                st.error(error_mssg)
                errors.append(error_mssg)

            st.text("Exits conditions: ")
            # Exit Fractal Inputs (conditionally displayed)
            check_exit_fractal = st.checkbox("Check Exit Fractal", value=False)
            if check_exit_fractal:
                exit_fractal_file_number = st.text_input(
                    "Exit Fractal File Number", value="2"
                )
                fractal_exit_count = st.text_input(
                    "Fractal Exit Count (e.g., 6, ALL)", value="ALL"
                )

            # Trail BB Band Inputs (conditionally displayed)
            check_trail_bb_band = st.checkbox("Check Trail BB Band", value=False)
            if check_trail_bb_band:
                trail_bb_file_number = st.text_input("Trail BB File Number", value="1")
                trail_bb_band_sd = st.selectbox(
                    "Trail BB Band Standard Deviations",
                    options=[2.0, 2.25, 2.5, 2.75, 3.0],
                    index=0,
                )
                trail_bb_band_column = st.selectbox(
                    "Trail BB Band Column", options=["mean", "upper", "lower"], index=0
                )
                trail_bb_band_long_direction = st.selectbox(
                    "Trail BB Band Long Direction", options=["higher", "lower"], index=0
                )
                trail_bb_band_short_direction = st.selectbox(
                    "Trail BB Band Short Direction",
                    options=["higher", "lower"],
                    index=0,
                )

    st.header("Trade Management")

    with st.expander("Configurations", expanded=False):
        tm_input_data = {}
        # Segment
        segment = st.selectbox("Segment", ["CASH", "FUTURE", "OPTIONS"])
        tm_input_data["segment"] = segment

        if segment == "OPTIONS":
            # OPTBUYING
            opt_buying = st.selectbox("OPTBUYING", ["YES", "NO"])
            tm_input_data["opt_buying"] = opt_buying
            # if opt_buying == "YES":
            expiry = st.number_input("Expiry", min_value=1)
            strike = st.number_input("Strike")
            tm_input_data.update({"expiry": expiry, "strike": strike})

        if segment == "FUTURE":
            # Hedge
            expiry = st.number_input("Expiry", min_value=1)
            hedge = st.checkbox("Hedge")
            tm_input_data["hedge"] = hedge
            if hedge:
                hedge_expiry = st.number_input("Hedge Expiry", min_value=1)
                hedge_strike = st.number_input("Hedge Strike", min_value=1)
                hedge_delayed_exit = st.checkbox("Hedge Delayed Exit")
                tm_input_data.update(
                    {
                        "hedge_expiry": hedge_expiry,
                        "hedge_strike": hedge_strike,
                        "hedge_delayed_exit": hedge_delayed_exit,
                        "expiry": expiry,
                    }
                )

        # Appreciation/Depreciation based entry
        ade_based_entry = st.checkbox("Appreciation/Depreciation based entry")
        tm_input_data["ade_based_entry"] = ade_based_entry
        if ade_based_entry:
            appreciation_depreciation = st.selectbox(
                "Appreciation/Depreciation", ["APPRECIATION", "DEPRECIATION"]
            )
            ade_percentage = st.number_input(
                "Appreciation/Depreciation %", min_value=0.0, step=0.01
            )
            tm_input_data.update(
                {
                    "appreciation_depreciation": appreciation_depreciation,
                    "ade_percentage": ade_percentage,
                }
            )

        # TARGET
        target = st.checkbox("TARGET")
        tm_input_data["target"] = target
        if target:
            target_profit_percentage = st.number_input(
                "TARGET Profit %", min_value=0.0, step=0.01
            )
            tm_input_data["target_profit_percentage"] = target_profit_percentage

        # SL Trading
        sl_trading = st.checkbox("SL Trading")
        tm_input_data["sl_trading"] = sl_trading
        if sl_trading:
            sl_percentage = st.number_input("SL %", min_value=0.0, step=0.01)
            tm_input_data["sl_percentage"] = sl_percentage

        # Re-deployment
        re_deployment = st.checkbox("Re-deployment")
        tm_input_data["re_deployment"] = re_deployment
        if re_deployment:
            re_ade_based_entry = st.checkbox("RE_Appreciation/Depreciation based entry")
            tm_input_data["re_ade_based_entry"] = re_ade_based_entry
            if re_ade_based_entry:
                re_appreciation_depreciation = st.selectbox(
                    "RE_Appreciation/Depreciation", ["APPRECIATION", "DEPRECIATION"]
                )
                re_ade_percentage = st.number_input(
                    "RE_Appreciation/Depreciation %", min_value=0.0, step=0.01
                )
                tm_input_data.update(
                    {
                        "re_appreciation_depreciation": re_appreciation_depreciation,
                        "re_ade_percentage": re_ade_percentage,
                    }
                )

        # DTE - Based testing
        dte_based_testing = st.checkbox("DTE - Based testing")
        tm_input_data["dte_based_testing"] = dte_based_testing
        if dte_based_testing:
            dte_from = st.number_input("From which DTE", min_value=1)
            tm_input_data["dte_from"] = dte_from

        # Next Expiry trading
        next_expiry_trading = st.checkbox("Next Expiry trading")
        tm_input_data["next_expiry_trading"] = next_expiry_trading
        if next_expiry_trading:
            next_dte_from = st.number_input("From DTE", min_value=1)
            next_expiry = st.number_input("Expiry No", min_value=1)
            tm_input_data.update(
                {"next_dte_from": next_dte_from, "next_expiry": next_expiry}
            )

        # Premium Feature
        premium_feature = st.checkbox("Premium Feature")
        tm_input_data["premium_feature"] = premium_feature

        # Volume feature
        volume_feature = st.checkbox("Volume feature")
        tm_input_data["volume_feature"] = volume_feature
        if volume_feature:
            volume_minutes = st.number_input("Number of minutes", min_value=1)
            tm_input_data["volume_minutes"] = volume_minutes

        # Capital, Risk, Leverage
        capital = st.number_input("Capital", min_value=0, value=100000000)
        risk = st.number_input(
            "Risk", min_value=0.0, max_value=1.0, value=0.04, step=0.01
        )
        leverage = st.number_input("Leverage", min_value=1, value=2)
        tm_input_data.update({"capital": capital, "risk": risk, "leverage": leverage})

    if (
        allowed_direction == MarketDirection.LONG.value
        or allowed_direction == MarketDirection.ALL.value
    ):
        if not long_entry_signals or not long_exit_signals:
            error_mssg = "Please select Long Entry and Exit Signals."
            st.error(error_mssg)
            errors.append(error_mssg)
    if (
        allowed_direction == MarketDirection.SHORT.value
        or allowed_direction == MarketDirection.ALL.value
    ):
        if not short_entry_signals or not short_exit_signals:
            error_mssg = "Please select Short Entry and Exit Signals."
            st.error(error_mssg)
            errors.append(error_mssg)
    if not strategy_pairs:
        error_mssg = "Please select Strategy Pairs."
        st.error(error_mssg)
        errors.append(error_mssg)

    notes = st.text_input("Notes")
    save = st.checkbox("Save Inputs", value=True)
    trigger_trade_management_module = st.checkbox(
        "Trigger Trade Management Module", value=False
    )

    use_saved_input = st.checkbox("Use Saved Inputs", value=False)
    # Read the CSV file and display saved notes in a dropdown with search functionality
    if use_saved_input:
        user_inputs_df = read_user_inputs()
        if not user_inputs_df.empty:
            search_term = st.text_input("Search Notes")
            filtered_notes = user_inputs_df["notes"][
                user_inputs_df["notes"].str.contains(search_term, case=False, na=False)
            ].unique()

            selected_note = st.selectbox(
                "Select a note to view details", filtered_notes
            )

            if selected_note:
                # selected_data = user_inputs_df[
                #     user_inputs_df["notes"] == selected_note
                # ].iloc[0]
                selected_data = user_inputs_df.dropna(axis=1)[
                    user_inputs_df["notes"] == selected_note
                ].iloc[0]

                # Display the selected data
                st.write("Details for the selected note:")
                for key, value in selected_data.items():
                    st.write(f"{key}: {value}")

        else:
            st.info("No saved inputs found.")

    if use_saved_input:
        st.info("Using data from selected note to start module.")
        if st.button("Submit"):
            validated_data = validate(selected_data.to_dict(), key=validate_input)
            execute(validated_data)
    else:
        if not errors and st.button("Submit"):
            # Gather input data
            input_data = {
                "instrument": instrument,
                "portfolio_ids": portfolio_ids,
                "strategy_ids": strategy_pairs,
                "long_entry_signals": long_entry_signals,
                "long_exit_signals": long_exit_signals,
                "short_entry_signals": short_entry_signals,
                "short_exit_signals": short_exit_signals,
                "start_date": start_date,
                "end_date": end_date,
                "trade_start_time": trade_start_time,
                "trade_end_time": trade_end_time,
                "check_entry_fractal": check_entry_fractal,
                "check_exit_fractal": check_exit_fractal,
                "check_bb_band": check_bb_band,
                "check_trail_bb_band": check_trail_bb_band,
                "check_entry_based": check_entry_based,
                "trade_type": trade_type,
                "allowed_direction": allowed_direction,
            }
            if check_entry_fractal:
                input_data["entry_fractal_file_number"] = entry_fractal_file_number

            if check_exit_fractal:
                input_data["exit_fractal_file_number"] = exit_fractal_file_number
                input_data["fractal_exit_count"] = fractal_exit_count

            if check_bb_band:
                input_data["bb_file_number"] = bb_file_number
                input_data["bb_band_sd"] = bb_band_sd
                input_data["bb_band_column"] = bb_band_column

            if check_trail_bb_band:
                input_data["trail_bb_file_number"] = trail_bb_file_number
                input_data["trail_bb_band_sd"] = trail_bb_band_sd
                input_data["trail_bb_band_column"] = trail_bb_band_column
                input_data["trail_bb_band_long_direction"] = (
                    trail_bb_band_long_direction
                )
                input_data["trail_bb_band_short_direction"] = (
                    trail_bb_band_short_direction
                )

            if check_entry_based:
                input_data["number_of_entries"] = number_of_entries
                input_data["steps_to_skip"] = steps_to_skip

            # Validate input data
            validated_input = validate(
                {**input_data, **tm_input_data}, key=validate_input
            )

            if validated_input:
                if save:
                    temp = {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "notes": notes,
                    }
                    temp.update(validated_input)
                    write_user_inputs(temp)

                # Start trade processing
                execute(validated_input, trigger_trade_management_module)


def execute(validated_input, trigger_trade_management_module=False):
    start = time.time()

    initialize(validated_input)

    output_df = process_trade(
        validated_input.get("start_date"),
        validated_input.get("end_date"),
        validated_input.get("entry_fractal_file_number"),
        validated_input.get("exit_fractal_file_number"),
        validated_input.get("bb_file_number"),
        validated_input.get("trail_bb_file_number"),
    )
    if trigger_trade_management_module:
        # Call the trade management module
        pass
    stop = time.time()
    st.success(
        f"Trade processing completed successfully! Total time taken: {stop-start} seconds"
    )


def write_user_inputs(validated_input):
    """
    Write validated user inputs to a CSV file.

    Args:
        validated_input (dict): Validated input data.
    """
    try:
        with open("user_inputs.csv", "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=validated_input.keys())
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(validated_input)

        st.success("User inputs written to user_inputs.csv successfully!")

    except Exception as e:
        st.error(f"Error writing data to CSV: {e}")


if __name__ == "__main__":
    main()
