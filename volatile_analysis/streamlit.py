from pathlib import Path
import time
import streamlit as st

from source.constants import VOLATILE_OUTPUT_FOLDER
from source.streamlit import set_start_end_datetime, validate
from volatile_analysis.constants import VolatileTag
from volatile_analysis.processor import process_volatile
from volatile_analysis.summary import process_summaries
from volatile_analysis.validation import validate_inputs


def main():
    st.title("Volatility Analysis")
    streamlit_inputs = {}

    expander_option = st.selectbox(
        "Select Expander", ["Single Analysis", "Summary"]
    )
    if expander_option == "Single Analysis":
        with st.expander("Single Analysis", expanded=True):
            time_frames = st.multiselect(
                "Time Frames", options=[1, 2, 3, 4, 5, 6], default=[1]
            )
            streamlit_inputs["time_frames"] = time_frames

            instrument = st.text_input("Instrument", value="BANKNIFTY")
            streamlit_inputs["instrument"] = instrument

            periods = st.multiselect(
                "Periods", options=[5, 10, 20, 40, 80], default=[20]
            )
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

            option = [tag.value for tag in VolatileTag]
            analyze = st.selectbox("Analyze", option, index=0)
            streamlit_inputs["analyze"] = analyze

            capital_lower_threshold = st.number_input(
                "Capital Lower Threshold", value=-0.1, step=0.1
            )
            streamlit_inputs["capital_lower_threshold"] = (
                capital_lower_threshold
            )

            capital_upper_threshold = st.number_input(
                "Capital Upper Threshold", value=0.2, step=0.1
            )
            streamlit_inputs["capital_upper_threshold"] = (
                capital_upper_threshold
            )

            required_fileds = [
                sum_window_size,
                avg_window_size,
                lv_tag,
                hv_tag,
            ]
            if all(required_fileds):

                if st.button("Submit"):
                    validated_input = validate(
                        streamlit_inputs, key=validate_inputs
                    )
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

    else:
        with st.expander("Summary", expanded=True):

            # multi select for all names of output of volatile outputs
            folder = Path(VOLATILE_OUTPUT_FOLDER)
            files = [f.name for f in folder.iterdir() if f.is_file()]
            selected_files = st.multiselect("Files", files)

            if selected_files and st.button("Submit"):
                process_summaries(selected_files)


if __name__ == "__main__":
    main()
