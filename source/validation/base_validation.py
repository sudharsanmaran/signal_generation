from datetime import datetime, time
from typing import List, Optional, Union
from pydantic import BaseModel, field_validator

from source.constants import MarketDirection


class BaseInputs(BaseModel):
    portfolio_ids: tuple
    strategy_pairs: List[tuple]
    instruments: List[str]
    allowed_direction: MarketDirection

    trade_start_time: Optional[time] = None
    trade_end_time: Optional[time] = None
    start_date: Union[str, datetime]
    end_date: Union[str, datetime]

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


class FractalInput(BaseModel):
    check_entry_fractal: bool = None
    entry_fractal_file_number: str = None

    check_exit_fractal: bool = None
    fractal_exit_count: Union[int, str] = None
    exit_fractal_file_number: str = None


class FractalCountInput(BaseModel):
    fractal_count_sd: int = None
    fractal_count_tf: int = None
    fractal_count_skip: int = None
    fractal_count: bool = None

    @field_validator("fractal_count")
    def validate_fractal_count(cls, value, values):
        if value:
            if (
                values.data["fractal_count_sd"] is None
                or values.data["fractal_count_tf"] is None
                or values.data["fractal_count_skip"] is None
            ):
                raise ValueError(
                    "Fractal Count SD, Fractal Count TF and Fractal Count Skip are required"
                )
        return value


class FractalCycleInput(BaseModel):
    fractal_sd: int = None
    fractal_tf: int = None
    fractal_cycle_start: int = None
    fractal_cycle: bool = None

    @field_validator("fractal_cycle")
    def validate_fractal_cycle(cls, value, values):
        if value:
            if (
                values.data["fractal_sd"] is None
                or values.data["fractal_tf"] is None
                or values.data["fractal_cycle_start"] is None
            ):
                raise ValueError(
                    "Fractal SD, Fractal TF and Fractal Cycle Start are required"
                )
        return value


class TargetProfitInput(BaseModel):
    tp_percentage: float = None
    tp_method: str = None
    calculate_tp: bool = False

    @field_validator("tp_percentage")
    def validate_tp_percent(cls, value):
        if value and value < 0.0 and value > 1.0:
            raise ValueError(
                "tp_percent must be between 0.0 (exclusive) and 1.1 (inclusive)"
            )
        return value

    @field_validator("tp_method")
    def validate_tp_method(cls, value):
        if value and value not in ("1", "2"):
            raise ValueError('tp_method must be one of "1" or "2"')
        return value

    @field_validator("calculate_tp", mode="after")
    def validate_calculate_tp(cls, v, values):
        if not isinstance(v, bool):
            raise ValueError("calculate_tp should be a boolean")
        if v:
            if (
                values.data["tp_percentage"] is None
                or values.data["tp_method"] is None
            ):
                raise ValueError("TP Percent and TP Method are required")
        return v
