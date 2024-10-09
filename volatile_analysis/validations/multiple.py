from ast import literal_eval
from datetime import datetime
import logging
from typing import Union
import pandas as pd
from pydantic import BaseModel, field_validator


logger = logging.getLogger(__name__)


class AnalysisInput(BaseModel):
    instruments: str
    lv_hv_tag_combinations: str
    start_date: Union[str, datetime]
    end_date: Union[str, datetime]
    analyze: str

    @field_validator("instruments", mode="after")
    def validate_instrument(cls, value):
        return tuple(value.split(","))

    @field_validator("lv_hv_tag_combinations", mode="after")
    def validate_lv_hv_tag_combinations(cls, value):
        value = literal_eval(value)
        if not isinstance(value[0], tuple):
            value = (value,)
        for lv, hv in value:
            if lv >= hv:
                raise ValueError(
                    f"lv_tag should be less than hv_tag: {lv} < {hv}"
                )
        return value


def validate_multiple_inputs(inputs: dict):
    try:
        return AnalysisInput(**inputs).model_dump()
    except Exception as e:
        logger.error(f"Error validating inputs: {e}")
        raise e


def validate_file(file):
    required_columns = {
        "time_frame",
        "period",
        "parameter_id",
        "stdv",
        "stdv_parameter_id",
        "z_score_threshold",
        "sum_window_size",
        "avg_window_size",
        "capital_lower_threshold",
        "capital_upper_threshold",
    }

    try:
        df = pd.read_excel(file)
        if missing := required_columns - set(df.columns):
            raise ValueError(
                f"Missing required columns in selected file: {missing}"
            )
        type_cast_dataframe(df)
        return df
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise e


def type_cast_dataframe(df):
    df["capital_lower_threshold"] = df["capital_lower_threshold"].astype(float)
    df["capital_upper_threshold"] = df["capital_upper_threshold"].astype(float)
    df["z_score_threshold"] = df["z_score_threshold"].astype(float)
    df["sum_window_size"] = df["sum_window_size"].astype(int)
    df["avg_window_size"] = df["avg_window_size"].astype(int)
    df["period"] = df["period"].astype(int)
    df["stdv"] = df["stdv"].astype(int)
    df["parameter_id"] = df["parameter_id"].astype(int)
    df["stdv_parameter_id"] = df["stdv_parameter_id"].astype(int)
    df["time_frame"] = df["time_frame"].astype(int)
