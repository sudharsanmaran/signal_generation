from datetime import datetime
from typing import List
from pydantic import field_validator
from source.constants import CycleType, TradeType
from source.validation.base_validation import BaseInputs, FractalInput
from source.validation.validate_trade_management import TradingConfiguration


class CycleInputs(BaseInputs, FractalInput):
    long_entry_signals: List[tuple]
    short_entry_signals: List[tuple]
    long_exit_signals: List[tuple]
    short_exit_signals: List[tuple]

    close_time_frames_1: List[int]
    bb_time_frames_1: List[int]
    periods_1: List[int]
    sds_1: List[int]
    include_higher_and_lower: bool = False
    close_percent: float = None
    max_to_min_percent: float = None
    calculate_cycles: bool = False

    bb_time_frames_2: List[int] = None
    periods_2: List[int] = None
    sds_2: List[int] = None
    check_bb_2: bool = False

    cycle_to_consider: CycleType = None

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
                not values.data["close_time_frames_1"]
                or not values.data["bb_time_frames_1"]
                or not values.data["periods_1"]
                or not values.data["sds_1"]
            ):
                raise ValueError(
                    "Time Frame, Period and Standard Deviation are required"
                )
        return v

    @field_validator("check_bb_2", mode="after")
    def validate_bb_2_check(cls, v, values):
        if not isinstance(v, bool):
            raise ValueError("calculate_cycles should be a boolean")
        if v:
            if (
                not values.data["periods_2"]
                or not values.data["sds_2"]
                or not values.data["bb_time_frames_2"]
            ):
                raise ValueError("Period and Standard Deviation are required")
        return v

    @field_validator("close_percent", "max_to_min_percent", mode="after")
    def validate_percent(cls, v):
        if v is not None:
            if not isinstance(v, (int, float)):
                raise ValueError("percent should be a number")
            if v < 0.0:
                raise ValueError("percent should be a positive number")
            if v > 1.0:
                raise ValueError("percent should be less than 1")
        return v


class CycleTradeInputs(CycleInputs):
    trade_type: TradeType


class CycleInputAndTradingConfig(CycleTradeInputs, TradingConfiguration):
    pass


def validate_cycle_input(input: dict) -> dict:
    """
    Validate the input data for cycle.
    """
    try:
        input = CycleInputAndTradingConfig(**input)
        return input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
