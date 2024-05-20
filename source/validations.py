from datetime import datetime
from itertools import chain
from typing import Union
from pydantic import BaseModel, field_validator

from source.constants import MarketDirection, TradeType


class StrategyInput(BaseModel):
    instrument: str
    strategy_ids: str
    start_date: str
    end_date: str
    entry_fractal_file_number: str
    exit_fractal_file_number: str
    bb_file_number: str
    trail_bb_file_number: str
    bb_band_sd: float
    trail_bb_band_sd: float
    bb_band_column: str
    trail_bb_band_column: str
    trade_start_time: str
    trade_end_time: str
    check_fractal: bool
    check_bb_band: bool
    check_trail_bb_band: bool
    trail_bb_band_direction: str
    trade_type: TradeType
    allowed_direction: MarketDirection
    fractal_exit_count: Union[int, str]
    long_entry_signals: str
    long_exit_signals: str
    short_entry_signals: str
    short_exit_signals: str
    portfolio_ids: str

    @field_validator("portfolio_ids")
    def split_portfolio_ids(cls, v):
        return [id.strip() for id in v.split(",")]

    @field_validator("trade_start_time", "trade_end_time")
    def convert_to_time(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%H:%M:%S").time()
        elif isinstance(v, datetime):
            return v.time()
        raise ValueError('Invalid time format, should be "hh:mm:ss"')

    @field_validator("start_date", "end_date")
    def convert_to_datetime(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        elif isinstance(v, datetime):
            return v
        raise ValueError('Invalid datetime format, should be "dd/mm/yyyy hh:mm:ss"')

    @field_validator("strategy_ids")
    def split_strategy_ids(cls, v):
        def parse_signals(signals):
            return [signal.strip() for signal in signals.split(",")]

        return [parse_signals(id) for id in v.split("|")]

    @field_validator(
        "long_entry_signals",
        "long_exit_signals",
        "short_entry_signals",
        "short_exit_signals",
    )
    def split_conditions(cls, v, values):
        def parse_signals(signals):
            return [signal.strip() for signal in signals.split(",")]

        return list(parse_signals(cond) for cond in v.split("|"))

    @field_validator("bb_band_sd", "trail_bb_band_sd")
    def validate_bb_band_sd(cls, v):
        allowed_values = [2.0, 2.25, 2.5, 2.75, 3.0]
        if v not in allowed_values:
            raise ValueError(
                "BB band standard deviation must be one of the following: 2.0, 2.25, 2.5, 2.75, 3.0"
            )
        return v

    @field_validator("bb_band_column", "trail_bb_band_column")
    def validate_bb_band_column(cls, v):
        allowed_values = ["mean", "upper", "lower"]
        if v not in allowed_values:
            raise ValueError(
                'BB band column must be one of the following: "mean", "upper", "lower"'
            )
        return v

    @field_validator("trail_bb_band_direction")
    def validate_trail_bb_band_direction(cls, v):
        allowed_values = ["higher", "lower"]
        if v not in allowed_values:
            raise ValueError(
                'Trail BB band direction must be one of the following: "higher", "lower"'
            )
        return v

    @field_validator("trade_type")
    def validate_trade_type(cls, v):
        if v not in [TradeType.INTRADAY, TradeType.POSITIONAL]:
            raise ValueError(
                'Trade type must be one of the following: "Intraday", "Positional"'
            )
        return v

    @field_validator("allowed_direction")
    def validate_allowed_direction(cls, v):
        if v not in [MarketDirection.LONG, MarketDirection.SHORT, MarketDirection.ALL]:
            raise ValueError(
                'Allowed direction must be one of the following: "long", "short", "all"'
            )
        return v

    @field_validator("fractal_exit_count")
    def validate_fractal_exit_count(cls, v):
        if v.isdigit():
            return int(v)
        elif isinstance(v, str) and v.upper() == "ALL":
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


def validate_input(**kwargs):
    try:
        input_data = {**kwargs}
        validated_data = StrategyInput(**input_data)
        validate_count(validated_data)
        # todo
        # 1. count match
        return validated_data
    except Exception as e:
        print(f"Input validation error: {e}")
        return None
