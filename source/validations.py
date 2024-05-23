from datetime import datetime, time
from itertools import chain
from typing import List, Union
from pydantic import BaseModel, ValidationError, field_validator

from source.constants import MarketDirection, TradeType


class StrategyInput(BaseModel):
    portfolio_ids: tuple
    strategy_ids: List[tuple]
    instrument: str
    start_date: Union[str, datetime]
    end_date: Union[str, datetime]
    long_entry_signals: List[tuple]
    long_exit_signals: List[tuple]
    short_entry_signals: List[tuple]
    short_exit_signals: List[tuple]
    trade_type: TradeType
    allowed_direction: MarketDirection
    trade_start_time: time
    trade_end_time: time
    entry_fractal_file_number: str = None
    exit_fractal_file_number: str = None
    bb_file_number: str = None
    trail_bb_file_number: str = None
    bb_band_sd: float = None
    trail_bb_band_sd: float = None
    bb_band_column: str = None
    trail_bb_band_column: str = None
    check_entry_fractal: bool = None
    check_exit_fractal: bool = None
    check_bb_band: bool = None
    check_trail_bb_band: bool = None
    trail_bb_band_direction: str = None
    fractal_exit_count: Union[int, str] = None
    number_of_entries: int = None
    steps_to_skip: int = None

    @field_validator("start_date", "end_date")
    def convert_to_datetime(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        elif isinstance(v, datetime):
            return v
        raise ValueError('Invalid datetime format, should be "dd/mm/yyyy hh:mm:ss"')

    @field_validator("bb_band_sd", "trail_bb_band_sd")
    def validate_bb_band_sd(cls, v):
        allowed_values = {2.0, 2.25, 2.5, 2.75, 3.0}
        if v not in allowed_values:
            raise ValueError(
                "BB band standard deviation must be one of the following: 2.0, 2.25, 2.5, 2.75, 3.0"
            )
        return v

    @field_validator("bb_band_column", "trail_bb_band_column")
    def validate_bb_band_column(cls, v):
        allowed_values = {"mean", "upper", "lower"}
        if v not in allowed_values:
            raise ValueError(
                'BB band column must be one of the following: "mean", "upper", "lower"'
            )
        return v

    @field_validator("trail_bb_band_direction")
    def validate_trail_bb_band_direction(cls, v):
        allowed_values = {"higher", "lower"}
        if v not in allowed_values:
            raise ValueError(
                'Trail BB band direction must be one of the following: "higher", "lower"'
            )
        return v

    @field_validator("trade_type")
    def validate_trade_type(cls, v):
        if v not in {TradeType.INTRADAY, TradeType.POSITIONAL}:
            raise ValueError(
                'Trade type must be one of the following: "Intraday", "Positional"'
            )
        return v

    @field_validator("allowed_direction")
    def validate_allowed_direction(cls, v):
        if v not in {MarketDirection.LONG, MarketDirection.SHORT, MarketDirection.ALL}:
            raise ValueError(
                'Allowed direction must be one of the following: "long", "short", "all"'
            )
        return v

    @field_validator("fractal_exit_count")
    def validate_fractal_exit_count(cls, v):
        if v.isdigit():
            return int(v)
        elif isinstance(v, str) and v.lower() == MarketDirection.ALL.value:
            return v
        raise ValueError('Fractal exit count must be an integer or "ALL"')


def validate_count(validated_data: StrategyInput):
    for strategy_pair in validated_data.strategy_ids:
        assert len(strategy_pair) == len(
            validated_data.portfolio_ids
        ), "The number of strategies does not match the number of portfolio IDs"

    for signal_pair in chain(
        validated_data.long_entry_signals,
        validated_data.long_exit_signals,
        validated_data.short_entry_signals,
        validated_data.short_exit_signals,
    ):
        assert len(signal_pair) == len(
            validated_data.portfolio_ids
        ), "The number of signals does not match the number of portfolio IDs"


def check_exit_conditions(validated_data):
    short_exit_set = set(validated_data.short_exit_signals)
    long_exit_set = set(validated_data.long_exit_signals)

    if not any(
        signal_pair in short_exit_set
        for signal_pair in validated_data.long_entry_signals
    ):
        raise ValueError("long entry singals should added in short exit signals")

    if not any(
        signal_pair in long_exit_set
        for signal_pair in validated_data.short_entry_signals
    ):
        raise ValueError("short entry signals should added in long exit signals")


def validate_input(input_data):
    try:
        validated_data = StrategyInput(**input_data)
        validate_count(validated_data)
        check_exit_conditions(validated_data)
        return validated_data.model_dump()
    except ValidationError as e:
        print(f"Input validation error: {e}")
        raise e
