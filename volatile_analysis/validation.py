from datetime import datetime
from typing import Dict, List, Union
from pydantic import BaseModel, field_validator


class VolitileInputs(BaseModel):
    time_frames: List[int]
    instrument: str
    periods: Dict[int, List[int]]
    parameter_id: dict[tuple[int], int]

    start_date: Union[str, datetime]
    end_date: Union[str, datetime]

    z_score_threshold: float
    sum_window_size: int
    avg_window_size: int
    lv_tag: float
    hv_tag: float

    analyze: str

    capital_upper_threshold: float
    capital_lower_threshold: float

    @field_validator("hv_tag")
    def validate_hv_tag(cls, v, values):
        if v < 0:
            raise ValueError("hv_tag should be greater than 0")
        if v <= values.data["lv_tag"]:
            raise ValueError("hv_tag should be greater than lv_tag")
        return v

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


def validate_inputs(inputs: dict) -> dict:
    try:
        validated_inputs = VolitileInputs(**inputs)
        return validated_inputs.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
