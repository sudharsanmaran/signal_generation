from pathlib import Path
import time
import dotenv
import streamlit as st

dotenv.load_dotenv(override=True)

from source.constants import VOLUME_OUTPUT_FOLDER
from source.streamlit import set_start_end_datetime
from volume_analysis.processor import process
from volume_analysis.summary import process_summaries
from volume_analysis.validation import validate


def main():
    st.title("Volume Analysis")
    streamlit_inputs = {}
    expander_option = st.selectbox(
        "Select Expander", ["Single Analysis", "Summary"]
    )
    if expander_option == "Single Analysis":
        instrument = st.text_input("Instrument", value="HDFC")
        streamlit_inputs["instrument"] = instrument

        avg_zscore_sum_threshold = st.number_input(
            "Avg Zscore Sums Threshold", value=1, step=1
        )
        cycle_duration = st.number_input("Cycle Duration", value=120, step=1)
        cycle_skip_count = st.number_input("Cycle Skip Count", value=1, step=1)

        streamlit_inputs["avg_zscore_sum_threshold"] = avg_zscore_sum_threshold
        streamlit_inputs["cycle_duration"] = cycle_duration
        streamlit_inputs["cycle_skip_count"] = cycle_skip_count

        set_start_end_datetime(streamlit_inputs, {})

        capital_lower_threshold = st.number_input(
            "Capital Lower Threshold", value=-0.1, step=0.1
        )
        streamlit_inputs["capital_lower_threshold"] = capital_lower_threshold

        capital_upper_threshold = st.number_input(
            "Capital Upper Threshold", value=0.2, step=0.1
        )
        streamlit_inputs["capital_upper_threshold"] = capital_upper_threshold

        sub_cycle_lower_threshold = st.number_input(
            "Sub Cycle Lower Threshold", value=1.0, step=0.1
        )
        streamlit_inputs["sub_cycle_lower_threshold"] = (
            sub_cycle_lower_threshold
        )

        sub_cycle_upper_threshold = st.number_input(
            "Sub Cycle Upper Threshold", value=10.0, step=0.1
        )
        streamlit_inputs["sub_cycle_upper_threshold"] = (
            sub_cycle_upper_threshold
        )

        sub_cycle_interval = st.number_input(
            "Sub Cycle Interval", value=2, step=1
        )
        streamlit_inputs["sub_cycle_interval"] = sub_cycle_interval

        if st.button("Submit"):
            validated_input = validate(streamlit_inputs)
            if validated_input:
                # try:
                start = time.time()
                process(validated_input)
                st.success(
                    f"Data processed successfully, time taken: {time.time()-start}"
                )
            # except Exception as e:
            #     st.error(f"Error: {e}")
    else:
        with st.expander("Summary", expanded=True):

            # multi select for all names of output of volatile outputs
            folder = Path(VOLUME_OUTPUT_FOLDER)
            files = [f.name for f in folder.iterdir() if f.is_file()]
            selected_files = st.multiselect("Files", files)

            if selected_files and st.button("Submit"):
                process_summaries(selected_files)


if __name__ == "__main__":
    main()
