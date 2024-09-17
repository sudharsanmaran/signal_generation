import ast
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import time
import streamlit as st

from source.constants import VOLATILE_OUTPUT_FOLDER
from source.streamlit import (
    load_input_from_json,
    set_start_end_datetime,
    validate,
    write_user_inputs,
)
from volatile_analysis.constants import VolatileTag
from volatile_analysis.processor import process_volatile
from volatile_analysis.summary import process_summaries
from volatile_analysis.validation import validate_inputs


file_name = "volatile_user_inputs.json"


def main():
    global file_name
    st.title("Volatility Analysis")
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
            if saved_inputs:
                parameter_id = saved_inputs.get("parameter_id")
                converted_parameter_id = {}
                for k, v in parameter_id.items():
                    converted_parameter_id[ast.literal_eval(k)] = v
                saved_inputs["parameter_id"] = converted_parameter_id

                stdv_parameter_id = saved_inputs.get("stdv_parameter_id")
                coverted_stdv_parameter_id = {}
                for k, v in stdv_parameter_id.items():
                    coverted_stdv_parameter_id[ast.literal_eval(k)] = v
                saved_inputs["stdv_parameter_id"] = coverted_stdv_parameter_id

    expander_option = st.selectbox(
        "Select Expander", ["Single Analysis", "Summary"]
    )
    if expander_option == "Single Analysis":
        with st.expander("Single Analysis", expanded=True):
            time_frames = st.multiselect(
                "Time Frames",
                options=[60, 120, 240, 375, 1125],
                default=saved_inputs.get("time_frames", [60]),
            )
            streamlit_inputs["time_frames"] = time_frames

            instrument = st.text_input(
                "Instrument", value=saved_inputs.get("instrument", "ATUL")
            )
            streamlit_inputs["instrument"] = instrument

            periods_map = defaultdict(list)
            std_periods_map = defaultdict(list)
            for time_frame in time_frames:
                selected_period = st.multiselect(
                    f"Period for tf:{time_frame}",
                    options=[5, 10, 20, 40, 80],
                    default=saved_inputs.get("periods_map", {}).get(
                        time_frame, [5]
                    ),
                )
                periods_map[time_frame] = selected_period
                std_period = st.multiselect(
                    f"STDV Period for tf:{time_frame}",
                    options=[1764, 1008, 504, 252, 126, 84],
                    default=saved_inputs.get("std_periods_map", {}).get(
                        time_frame, [1764]
                    ),
                )
                std_periods_map[time_frame] = std_period
            streamlit_inputs["periods"] = periods_map
            streamlit_inputs["std_periods"] = std_periods_map

            streamlit_inputs["parameter_id"] = {}
            for tf, periods in periods_map.items():
                for period in periods:

                    parameter_id = st.number_input(
                        f"Parameter ID for tf:{tf}, period:{period}",
                        value=saved_inputs.get("parameter_id", {}).get(
                            (tf, period), 1
                        ),
                        step=1,
                    )
                    streamlit_inputs["parameter_id"].update(
                        {(tf, period): parameter_id}
                    )

            streamlit_inputs["stdv_parameter_id"] = {}
            for tf, periods in std_periods_map.items():
                for period in periods:

                    parameter_id = st.number_input(
                        f"stdv Parameter ID for tf:{tf}, period:{period}",
                        value=saved_inputs.get("stdv_parameter_id", {}).get(
                            (tf, period), 1
                        ),
                        step=1,
                    )
                    streamlit_inputs["stdv_parameter_id"].update(
                        {(tf, period): parameter_id}
                    )

            set_start_end_datetime(streamlit_inputs, saved_inputs)

            # float for z score input
            z_score_threshold = st.number_input(
                "Z Score Threshold",
                value=saved_inputs.get("z_score_threshold", 0.0),
                step=0.1,
            )
            streamlit_inputs["z_score_threshold"] = z_score_threshold

            # integer for window size
            sum_window_size = st.number_input(
                "Sum Window Size",
                value=saved_inputs.get("sum_window_size", 20),
                step=1,
                min_value=1,
            )
            streamlit_inputs["sum_window_size"] = sum_window_size
            avg_window_size = st.number_input(
                "Average Window Size",
                value=saved_inputs.get("avg_window_size", 20),
                step=1,
                min_value=1,
            )
            streamlit_inputs["avg_window_size"] = avg_window_size

            # integer for lv_tag and hv_tag
            lv_tag = st.number_input(
                "LV Tag", value=saved_inputs.get("lv_tag", 5), step=1
            )
            streamlit_inputs["lv_tag"] = lv_tag
            hv_tag = st.number_input(
                "HV Tag", step=1, value=saved_inputs.get("hv_tag", 15)
            )
            streamlit_inputs["hv_tag"] = hv_tag

            option = [tag.value for tag in VolatileTag]
            analyze = st.selectbox(
                "Analyze",
                option,
                index=option.index(saved_inputs.get("analyze", option[0])),
            )
            streamlit_inputs["analyze"] = analyze

            capital_lower_threshold = st.number_input(
                "Capital Lower Threshold",
                value=saved_inputs.get("capital_lower_threshold", -0.1),
                step=0.1,
            )
            streamlit_inputs["capital_lower_threshold"] = (
                capital_lower_threshold
            )

            capital_upper_threshold = st.number_input(
                "Capital Upper Threshold",
                value=saved_inputs.get("capital_upper_threshold", 0.2),
                step=0.1,
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
                notes = st.text_input(
                    "Notes", value=saved_inputs.get("notes", "")
                )
                save = st.checkbox(
                    "Save Inputs", value=saved_inputs.get("save", True)
                )
                if st.button("Submit"):
                    validated_input = validate(
                        streamlit_inputs, key=validate_inputs
                    )
                    if validated_input:
                        if save:
                            temp = {
                                "timestamp": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "notes": notes,
                            }
                            temp.update(streamlit_inputs)
                            # Convert tuple keys to strings
                            temp["parameter_id"] = {
                                str(k): v
                                for k, v in temp["parameter_id"].items()
                            }
                            temp["stdv_parameter_id"] = {
                                str(k): v
                                for k, v in temp["stdv_parameter_id"].items()
                            }

                            # Convert defaultdict to dict
                            temp["periods"] = dict(temp["periods"])
                            temp["std_periods"] = dict(temp["std_periods"])
                            write_user_inputs(temp, file_name)
                        start = time.time()
                        process_volatile(validated_data=validated_input)
                        st.success(
                            f"Data processed successfully, time taken: {time.time()-start}"
                        )

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
