from datetime import datetime
from typing import Union, List
import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator


class Inputs(BaseModel):
    instrument: str
    time_frame: str
    period: str
    parameter_id: str

    avg_zscore_sum_threshold: int
    cycle_duration: int
    cycle_skip_count: int

    start_date: Union[str, datetime]
    end_date: Union[str, datetime]

    capital_upper_threshold: float
    capital_lower_threshold: float

    sub_cycle_lower_threshold: float
    sub_cycle_upper_threshold: float
    sub_cycle_interval: int

    @field_validator("start_date", "end_date")
    def convert_to_datetime(cls, v):
        """
        Convert string dates to datetime objects.
        """
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        elif isinstance(v, datetime):
            return v
        raise ValueError(
            'Invalid datetime format, should be "dd/mm/yyyy hh:mm:ss"'
        )


class MultiInputs(BaseModel):
    start_date: str
    end_date: str
    instruments: List[str]
    avg_zscore_sum_thresholds: List[int]

    @field_validator("instruments", "avg_zscore_sum_thresholds", mode="before")
    def convert_to_list(cls, v):
        """
        Convert string instruments to list.
        """
        if isinstance(v, str):
            return v.split(",")


def validate(inputs):
    try:
        validated_inputs = Inputs(**inputs)
        return validated_inputs.model_dump()
    except ValidationError as e:
        raise e


def validate_multiple_inputs(inputs):
    try:
        validated_inputs = MultiInputs(**inputs)
        return validated_inputs.model_dump()
    except ValidationError as e:
        raise e


def validate_file(file):
    required_columns = {
        "time_frame",
        "period",
        "parameter_id",
        "cycle_duration",
        "cycle_skip_count",
        "capital_upper_threshold",
        "capital_lower_threshold",
        "sub_cycle_lower_threshold",
        "sub_cycle_upper_threshold",
        "sub_cycle_interval",
    }
    try:
        df = pd.read_excel(file)
        if misssing_columns := required_columns - set(df.columns):
            raise ValidationError(
                f"Missing columns: {', '.join(misssing_columns)}"
            )
        type_cast_df(df)
        return df
    except Exception as e:
        raise e


def type_cast_df(df: pd.DataFrame) -> None:
    df["time_frame"] = df["time_frame"].astype(str)
    df["period"] = df["period"].astype(str)
    df["parameter_id"] = df["parameter_id"].astype(str)
    df["cycle_duration"] = df["cycle_duration"].astype(int)
    df["cycle_skip_count"] = df["cycle_skip_count"].astype(int)
    df["capital_upper_threshold"] = df["capital_upper_threshold"].astype(float)
    df["capital_lower_threshold"] = df["capital_lower_threshold"].astype(float)
    df["sub_cycle_lower_threshold"] = df["sub_cycle_lower_threshold"].astype(
        float
    )
    df["sub_cycle_upper_threshold"] = df["sub_cycle_upper_threshold"].astype(
        float
    )
    df["sub_cycle_interval"] = df["sub_cycle_interval"].astype(int)
