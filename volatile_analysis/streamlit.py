import time
import streamlit as st

from source.streamlit import set_start_end_datetime, validate
from volatile_analysis.processor import process_volatile
from volatile_analysis.validation import validate_inputs


def main():
    st.title("Volatility Analysis")
    streamlit_inputs = {}

    time_frame = st.number_input("Time Frame", value=2, step=1, min_value=1)
    streamlit_inputs["time_frame"] = time_frame

    instrument = st.text_input("Instrument", value="BANKNIFTY")
    streamlit_inputs["instrument"] = instrument

    periods = st.multiselect("Periods", options=[20, 40], default=[20])
    streamlit_inputs["periods"] = periods

    streamlit_inputs["parameter_id"] = {}
    for period in periods:
        parameter_id = st.number_input(
            f"Parameter ID for {period}", value=1, step=1
        )
        streamlit_inputs["parameter_id"].update({period: parameter_id})

    set_start_end_datetime(streamlit_inputs, {})

    # float for z score input
    z_score_threshold = st.number_input(
        "Z Score Threshold", value=0.5, step=0.1
    )
    streamlit_inputs["z_score_threshold"] = z_score_threshold

    # integer for window size
    sum_window_size = st.number_input(
        "Sum Window Size", value=20, step=1, min_value=1
    )
    streamlit_inputs["sum_window_size"] = sum_window_size
    avg_window_size = st.number_input(
        "Average Window Size", value=20, step=1, min_value=1
    )
    streamlit_inputs["avg_window_size"] = avg_window_size

    # integer for lv_tag and hv_tag
    lv_tag = st.number_input("LV Tag", value=5, step=1)
    streamlit_inputs["lv_tag"] = lv_tag
    hv_tag = st.number_input("HV Tag", step=1, value=15)
    streamlit_inputs["hv_tag"] = hv_tag

    required_fileds = [sum_window_size, avg_window_size, lv_tag, hv_tag]
    if all(required_fileds):

        if st.button("Submit"):
            validated_input = validate(streamlit_inputs, key=validate_inputs)
            if validated_input:
                try:
                    start = time.time()
                    process_volatile(validated_data=validated_input)
                    st.success(
                        f"Data processed successfully, time taken: {time.time()-start}"
                    )
                except Exception as e:
                    st.write(f"Error: {e}")
    else:
        st.warning("Please fill all the required fields")


if __name__ == "__main__":
    main()
