from datetime import datetime
from typing import List
from pydantic import BaseModel, field_validator


class AnalysisInput(BaseModel):
    start_date: datetime
    end_date: datetime
    portfolio_ids: tuple
    strategy_pairs: List[tuple]
    instruments: List[str]
    long_entry_signals: List[tuple]
    short_entry_signals: List[tuple]

    time_frames: List[str] = None
    periods: List[str] = None
    sds: List[str] = None
    calculate_cycles: bool = False

    @field_validator("start_date", "end_date", mode="before")
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

    @field_validator("calculate_cycles", mode="after")
    def validate_calculate_cycles(cls, v, values):
        """
        Validate the calculate_cycles field.
        """
        if not isinstance(v, bool):
            raise ValueError("calculate_cycles should be a boolean")
        if v:
            if (
                not values.data["time_frames"]
                or not values.data["periods"]
                or not values.data["sds"]
            ):
                raise ValueError(
                    "Time Frame, Period and Standard Deviation are required"
                )
        return v


def validate(analysis_input: dict) -> dict:
    """
    Validate the input data for analysis.
    """
    try:
        analysis_input = AnalysisInput(**analysis_input)
        return analysis_input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
