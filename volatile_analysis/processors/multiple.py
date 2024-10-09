import logging
import multiprocessing
import pandas as pd
from source.constants import VOLATILE_OUTPUT_STATUS_FOLDER, cpu_percent_to_use
from source.utils import write_dataframe_to_csv
from volatile_analysis.processors.single import process_volatile
from volatile_analysis.validations.single import validate_inputs

logger = logging.getLogger(__name__)


def process_multiple(validated_input, input_df: pd.DataFrame):
    # Process multiple files
    # get np array string type of input_df size
    total_length = (
        len(input_df)
        * len(validated_input["instruments"])
        * len(validated_input["lv_hv_tag_combinations"])
    )

    status = []
    error_mssg = []
    datas = []

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
                datas.append(validated_data)

    execute_data_processing(
        total_length,
        status,
        error_mssg,
        datas,
    )

    status_df = pd.DataFrame(datas)
    status_df["status"] = status
    status_df["error_message"] = error_mssg

    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H-%M-%S")
    write_dataframe_to_csv(
        status_df,
        VOLATILE_OUTPUT_STATUS_FOLDER,
        f"{now_str}_volatile_output.csv",
    )


def execute_data_processing(total_length, status, error_mssg, datas):
    num_workers = min(
        int(multiprocessing.cpu_count() * cpu_percent_to_use),
        total_length,
    )
    with multiprocessing.Pool(num_workers) as pool:
        results = []
        for data in datas:
            result = pool.apply_async(process_volatile, args=(data,))
            results.append(result)

        # Collect results and update the status for each process
        for result in results:
            try:
                result.get()  # This will raise an exception if the process failed
                status.append("SUCCESS")
                error_mssg.append("")
            except Exception as e:
                logger.error(f"Error processing volatile data for index {e}")
                status.append("ERROR")
                error_mssg.append(str(e))
