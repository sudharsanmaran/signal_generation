import time
import dotenv
import streamlit as st

from new_module.processor import process
from new_module.validation import validate


dotenv.load_dotenv(override=True)


def main():
    st.title("Final Module")
    streamlit_inputs = {}

    avg_zscore_sum_threshold = st.number_input(
        "Avg Zscore Sums Threshold", value=1, step=1
    )
    cycle_duration = st.number_input("Cycle Duration", value=1, step=1)
    cycle_skip_count = st.number_input("Cycle Skip Count", value=1, step=1)

    streamlit_inputs["avg_zscore_sum_threshold"] = avg_zscore_sum_threshold
    streamlit_inputs["cycle_duration"] = cycle_duration
    streamlit_inputs["cycle_skip_count"] = cycle_skip_count

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
