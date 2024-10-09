import logging
import numpy as np
import pandas as pd
from source.constants import VOLATILE_OUTPUT_STATUS_FOLDER
from source.utils import write_dataframe_to_csv
from volatile_analysis.processors.single import process_volatile
from volatile_analysis.validations.single import validate_inputs

logger = logging.getLogger(__name__)


def process_multiple(validated_input, input_df: pd.DataFrame):
    # Process multiple files
    # get np array string type of input_df size
    status = [None] * len(input_df)
    for index, row in input_df.iterrows():
        validated_data = {}
        for instrument in validated_input["instruments"]:
            validated_data["instrument"] = instrument
            validated_data["time_frames"] = [row["time_frame"]]
            validated_data["periods"] = {row["time_frame"]: [row["period"]]}
            validated_data["std_periods"] = {row["time_frame"]: [row["stdv"]]}
            validated_data["parameter_id"] = {
                (row["time_frame"], row["period"]): row["parameter_id"]
            }
            validated_data["stdv_parameter_id"] = {
                (row["time_frame"], row["stdv"]): row["stdv_parameter_id"]
            }
            validated_data["start_date"] = validated_input["start_date"]
            validated_data["end_date"] = validated_input["end_date"]
            validated_data["z_score_threshold"] = row["z_score_threshold"]
            validated_data["sum_window_size"] = row["sum_window_size"]
            validated_data["avg_window_size"] = row["avg_window_size"]
            validated_data["analyze"] = validated_input["analyze"]
            validated_data["capital_upper_threshold"] = row[
                "capital_upper_threshold"
            ]
            validated_data["capital_lower_threshold"] = row[
                "capital_lower_threshold"
            ]
            for lv, hv in validated_input["lv_hv_tag_combinations"]:
                validated_data["lv_tag"] = lv
                validated_data["hv_tag"] = hv
                validated_data = validate_inputs(validated_data)
                try:
                    process_volatile(validated_data)
                    status[index] = "SUCCESS"
                except Exception as e:
                    logger.error(f"Error processing volatile data: {e}")
                    status[index] = "ERROR"
                    continue
    input_df["status"] = status
    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    write_dataframe_to_csv(
        input_df,
        VOLATILE_OUTPUT_STATUS_FOLDER,
        f"{now_str}_volatile_output.csv",
    )
