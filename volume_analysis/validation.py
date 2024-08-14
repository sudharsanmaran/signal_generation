from datetime import datetime
from typing import Dict, List, Union
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


def validate(inputs):
    try:
        validated_inputs = Inputs(**inputs)
        return validated_inputs.model_dump()
    except ValidationError as e:
        raise e
