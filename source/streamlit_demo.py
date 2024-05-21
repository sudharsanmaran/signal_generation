import streamlit as st
from itertools import product

POSSIBLE_STRATEGY_IDS = list(range(1, 11))


def select_all_options(key, combinations):
    if "ALL" in st.session_state[key]:
        if key == "short_entry_signals":
            st.session_state["long_entry_signals"] = []
        st.session_state[key] = combinations


def parse_strategy_ids(input_str):
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
    possible_flags_per_portfolio = {}
    for portfolio_id in portfolio_ids:
        possible_flags_input = st.text_input(
            f"Possible Flags for portfolio {portfolio_id} (comma-separated, e.g., r, g, y)"
        )
        possible_flags_per_portfolio[portfolio_id] = set(
            flag.strip() for flag in possible_flags_input.split(",")
        )
    return possible_flags_per_portfolio


def get_flag_combinations(portfolio_ids, possible_flags_per_portfolio):
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


def main():
    st.title("Trading System Input")

    portfolio_ids_input = st.text_input(
        "Portfolio IDs (comma-separated, e.g., 1, 2, 3)"
    )
    if portfolio_ids_input:
        portfolio_ids = tuple(portfolio_ids_input.split(","))
        possible_flags_per_portfolio = get_portfolio_flags(portfolio_ids)
        filtered_flag_combinations = get_flag_combinations(
            portfolio_ids, possible_flags_per_portfolio
        )
        all_flag_combinations = ["ALL"] + filtered_flag_combinations

        long_entry_signals = st.multiselect(
            "Long Entry Signals",
            all_flag_combinations,
            key="long_entry_signals",
            on_change=select_all_options,
            args=("long_entry_signals", filtered_flag_combinations),
        )

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

        long_exit_signals = st.multiselect(
            "Long Exit Signals",
            set(filtered_flag_combinations) - set(long_entry_signals),
            default=short_entry_signals,
        )
        short_exit_signals = st.multiselect(
            "Short Exit Signals",
            set(filtered_flag_combinations) - set(short_entry_signals),
            default=long_entry_signals,
        )

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

        instrument = st.text_input("Instrument", value="BANKNIFTY")

        start_date = st.text_input(
            "Start Date (format: dd/mm/yyyy hh:mm:ss)", value="3/01/2019 09:15:00"
        )
        end_date = st.text_input(
            "End Date (format: dd/mm/yyyy hh:mm:ss)", value="3/04/2019 16:00:00"
        )
        entry_fractal_file_number = st.text_input(
            "Entry Fractal File Number", value="1"
        )
        exit_fractal_file_number = st.text_input("Exit Fractal File Number", value="2")
        fractal_exit_count = st.text_input(
            "Fractal Exit Count (e.g., 6, ALL)", value="ALL"
        )
        bb_file_number = st.text_input("BB File Number", value="1")
        trail_bb_file_number = st.text_input("Trail BB File Number", value="1")
        bb_band_sd = st.selectbox(
            "BB Band Standard Deviations", options=[2.0, 2.25, 2.5, 2.75, 3.0], index=0
        )
        trail_bb_band_sd = st.selectbox(
            "Trail BB Band Standard Deviations",
            options=[2.0, 2.25, 2.5, 2.75, 3.0],
            index=0,
        )
        bb_band_column = st.selectbox(
            "BB Band Column", options=["mean", "upper", "lower"], index=0
        )
        trail_bb_band_column = st.selectbox(
            "Trail BB Band Column", options=["mean", "upper", "lower"], index=0
        )
        trade_start_time = st.text_input(
            "Trade Start Time (format: hh:mm:ss)", value="13:15:00"
        )
        trade_end_time = st.text_input(
            "Trade End Time (format: hh:mm:ss)", value="15:20:00"
        )
        check_fractal = st.checkbox("Check Fractal", value=True)
        check_bb_band = st.checkbox("Check BB Band", value=False)
        check_trail_bb_band = st.checkbox("Check Trail BB Band", value=False)
        trail_bb_band_direction = st.selectbox(
            "Trail BB Band Direction", options=["higher", "lower"], index=0
        )
        trade_type = st.selectbox(
            "Trade Type", options=["positional", "intraday"], index=0
        )
        allowed_direction = st.selectbox(
            "Allowed Direction", options=["all", "long", "short"], index=0
        )

        # Submit button to process the selections
        if st.button("Submit"):
            st.write("Long Entry Signals:", long_entry_signals)
            st.write("Short Entry Signals:", short_entry_signals)
            st.write("Long Exit Signals:", long_exit_signals)
            st.write("Short Exit Signals:", short_exit_signals)
            st.write("Selected Strategy Pairs:", strategy_pairs)
            # Here you can also save the selections or perform further processing


# Run the main function when the script is executed
if __name__ == "__main__":
    main()
