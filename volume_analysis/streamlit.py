import time
import dotenv
import streamlit as st

from volume_analysis.processor import process
from volume_analysis.validation import validate


dotenv.load_dotenv(override=True)


def main():
    st.title("Final Module")
    streamlit_inputs = {}

    avg_zscore_sum_threshold = st.number_input(
        "Avg Zscore Sums Threshold", value=1, step=1
    )
    cycle_duration = st.number_input("Cycle Duration", value=120, step=1)
    cycle_skip_count = st.number_input("Cycle Skip Count", value=1, step=1)

    streamlit_inputs["avg_zscore_sum_threshold"] = avg_zscore_sum_threshold
    streamlit_inputs["cycle_duration"] = cycle_duration
    streamlit_inputs["cycle_skip_count"] = cycle_skip_count

    capital_lower_threshold = st.number_input(
        "Capital Lower Threshold", value=-0.1, step=0.1
    )
    streamlit_inputs["capital_lower_threshold"] = capital_lower_threshold

    capital_upper_threshold = st.number_input(
        "Capital Upper Threshold", value=0.2, step=0.1
    )
    streamlit_inputs["capital_upper_threshold"] = capital_upper_threshold

    if st.button("Submit"):
        validated_input = validate(streamlit_inputs)
        if validated_input:
            try:
                start = time.time()
                process(validated_input)
                st.success(
                    f"Data processed successfully, time taken: {time.time()-start}"
                )
            except Exception as e:
                st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
