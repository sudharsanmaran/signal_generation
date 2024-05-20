import streamlit as st
from datetime import datetime


def parse_datetime(dt_str, format="%d/%m/%Y %H:%M:%S"):
    try:
        return datetime.strptime(dt_str, format)
    except ValueError:
        st.error(f"Date format should be {format}")
        return None


def main():
    st.title("Trading System Input")

    # Input fields
    instrument = st.text_input("Instrument", value="BANKNIFTY")
    strategy_id = st.text_input("Strategy ID")
    start_date = st.text_input("Start Date", value="01/01/2021 09:15:00")
    end_date = st.text_input("End Date", value="01/02/2021 15:30:00")
    fractal_file_number = st.text_input("Fractal File Number")
    bb_file_number = st.text_input("BB File Number")
    bb_band_sd = st.selectbox(
        "BB Band Standard Deviation", options=[2.0, 2.25, 2.5, 2.75, 3.0]
    )
    trade_start_time = st.text_input("Trade Start Time", value="09:15:00")
    trade_end_time = st.text_input("Trade End Time", value="15:30:00")
    check_fractal = st.checkbox("Check Fractal", value=True)
    check_bb_band = st.checkbox("Check BB Band")
    check_trail_bb_band = st.checkbox("Check Trail BB Band")
    trade_type = st.selectbox("Trade Type", options=["Intraday", "Positional"])
    allowed_direction = st.selectbox(
        "Allowed Direction", options=["long", "short", "all"]
    )

    # Validate and parse date/time inputs
    start_date = parse_datetime(start_date)
    end_date = parse_datetime(end_date)
    trade_start_time = parse_datetime(trade_start_time, format="%H:%M:%S")
    trade_end_time = parse_datetime(trade_end_time, format="%H:%M:%S")

    if st.button("Run Strategy"):
        if None not in [start_date, end_date, trade_start_time, trade_end_time]:
            # Call your main trading function here with the inputs
            st.success("Strategy is running... (placeholder)")
        else:
            st.error("Please correct the input errors before running the strategy.")


if __name__ == "__main__":
    main()
