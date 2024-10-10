from datetime import datetime
from pathlib import Path
import time
import streamlit as st

from source.constants import VOLUME_OUTPUT_FOLDER
from source.streamlit import (
    load_input_from_json,
    set_start_end_datetime,
    write_user_inputs,
)

from volume_analysis.processor import process, process_multiple
from volume_analysis.summary import process_summaries
from volume_analysis.validation import (
    validate,
    validate_multiple_inputs,
    validate_file,
)

file_name = "volume_user_inputs.json"


def main():
    global file_name

    st.title("Volume Analysis")
    streamlit_inputs, saved_inputs = {}, {}
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
        "Select Expander", ["Single Analysis", "Summary", "Multiple Analysis"]
    )
    if expander_option == "Single Analysis":
        instrument = st.text_input(
            "Instrument", value=saved_inputs.get("instrument", "ABBOTINDIA")
        )
        streamlit_inputs["instrument"] = instrument

        time_frame = st.text_input(
            "Time Frame", value=saved_inputs.get("time_frame", "1")
        )
        streamlit_inputs["time_frame"] = time_frame

        period = st.text_input("Period", value=saved_inputs.get("period", "5"))
        streamlit_inputs["period"] = period

        parameter_id = st.text_input(
            "Parameter ID", value=saved_inputs.get("parameter_id", "1")
        )
        streamlit_inputs["parameter_id"] = parameter_id

        avg_zscore_sum_threshold = st.number_input(
            "Avg Zscore Sums Threshold",
            value=saved_inputs.get("avg_zscore_sum_threshold", 1),
            step=1,
        )
        cycle_duration = st.number_input(
            "Cycle Duration",
            value=saved_inputs.get("cycle_duration", 120),
            step=1,
        )
        cycle_skip_count = st.number_input(
            "Cycle Skip Count",
            value=saved_inputs.get("cycle_skip_count", 1),
            step=1,
        )

        streamlit_inputs["avg_zscore_sum_threshold"] = avg_zscore_sum_threshold
        streamlit_inputs["cycle_duration"] = cycle_duration
        streamlit_inputs["cycle_skip_count"] = cycle_skip_count

        set_start_end_datetime(streamlit_inputs, {})

        capital_lower_threshold = st.number_input(
            "Capital Lower Threshold",
            value=saved_inputs.get("capital_lower_threshold", -0.1),
            step=0.1,
        )
        streamlit_inputs["capital_lower_threshold"] = capital_lower_threshold

        capital_upper_threshold = st.number_input(
            "Capital Upper Threshold",
            value=saved_inputs.get("capital_upper_threshold", 0.2),
            step=0.1,
        )
        streamlit_inputs["capital_upper_threshold"] = capital_upper_threshold

        sub_cycle_lower_threshold = st.number_input(
            "Sub Cycle Lower Threshold",
            value=saved_inputs.get("sub_cycle_lower_threshold", 1.0),
            step=0.1,
        )
        streamlit_inputs["sub_cycle_lower_threshold"] = (
            sub_cycle_lower_threshold
        )

        sub_cycle_upper_threshold = st.number_input(
            "Sub Cycle Upper Threshold",
            value=saved_inputs.get("sub_cycle_upper_threshold", 10.0),
            step=0.1,
        )
        streamlit_inputs["sub_cycle_upper_threshold"] = (
            sub_cycle_upper_threshold
        )

        sub_cycle_interval = st.number_input(
            "Sub Cycle Interval",
            value=saved_inputs.get("sub_cycle_interval", 2),
            step=1,
        )
        streamlit_inputs["sub_cycle_interval"] = sub_cycle_interval
        notes = st.text_input("Notes", value=saved_inputs.get("notes", ""))
        save = st.checkbox("Save Inputs", value=saved_inputs.get("save", True))
        if st.button("Submit"):
            validated_input = validate(streamlit_inputs)
            if validated_input:
                if save:
                    temp = {
                        "timestamp": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "notes": notes,
                    }
                    temp.update(streamlit_inputs)
                    write_user_inputs(temp, file_name)
                start = time.time()
                process(validated_input)
                st.success(
                    f"Data processed successfully, time taken: {time.time()-start}"
                )

    elif expander_option == "Summary":
        with st.expander("Summary", expanded=True):

            # multi select for all names of output of volatile outputs
            folder = Path(VOLUME_OUTPUT_FOLDER)
            files = [f.name for f in folder.iterdir() if f.is_file()]
            selected_files = st.multiselect("Files", files)

            if selected_files and st.button("Submit"):
                process_summaries(selected_files)

    elif expander_option == "Multiple Analysis":
        with st.expander("Multiple Analysis Config", expanded=True):
            # get excel file
            selected_file = st.file_uploader(
                "Upload Excel File", type=["xlsx"]
            )

            st.write("Common Inputs")

            set_start_end_datetime(streamlit_inputs, saved_inputs)

            instruments = st.text_input(
                "Instruments",
                value=saved_inputs.get("instrument", "RELIANCE, ABBOTINDIA"),
            )
            streamlit_inputs["instruments"] = instruments

            avg_zscore_sum_thresholds = st.text_input(
                "Avg Zscore Sums Threshold",
                value=saved_inputs.get("avg_zscore_sum_thresholds", "1,2,3"),
            )
            streamlit_inputs["avg_zscore_sum_thresholds"] = (
                avg_zscore_sum_thresholds
            )

            required_fileds = [
                selected_file,
                instruments,
                avg_zscore_sum_thresholds,
                streamlit_inputs.get("start_date", False),
                streamlit_inputs.get("end_date", False),
            ]
            if all(required_fileds):
                if st.button("Submit"):
                    try:
                        validated_input = validate_multiple_inputs(
                            streamlit_inputs
                        )
                        validated_file = validate_file(selected_file)
                        start = time.time()
                        process_multiple(
                            validated_input=validated_input,
                            input_df=validated_file,
                        )
                        st.success(
                            f"Data processed successfully, time taken: {time.time()-start}"
                        )
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("Please fill all the required fields")


if __name__ == "__main__":
    main()
