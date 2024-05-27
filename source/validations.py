"""
The `validation.py` module provided defines a Pydantic model `StrategyInput` for validating and processing trading system inputs. The module also includes functions for additional custom validation rules. Below, I will add comments and docstrings to explain the purpose and functionality of each section of the code.

### Explanation:
- **Imports**: Importing necessary libraries and project-specific constants.
- **`StrategyInput` Class**: Defines the Pydantic model for strategy input data, including attributes and validation methods.
  - **Attributes**: Defines various attributes like `portfolio_ids`, `strategy_ids`, `instrument`, `start_date`, `end_date`, `trade_type`, `allowed_direction`, and optional fields for fractal and Bollinger band checks.
  - **Field Validators**:
    - **`convert_to_datetime`**: Converts string dates to datetime objects.
    - **`validate_bb_band_sd`**: Ensures BB band standard deviation is within allowed values.
    - **`validate_bb_band_column`**: Ensures BB band column is within allowed values.
    - **`validate_trail_bb_band_direction`**: Ensures trail BB band direction is within allowed values.
    - **`validate_trade_type`**: Ensures trade type is valid.
    - **`validate_allowed_direction`**: Ensures allowed direction is valid.
    - **`validate_fractal_exit_count`**: Validates fractal exit count.
- **`validate_count` Function**: Checks if the number of strategies and signals match the number of portfolio IDs.
- **`check_exit_conditions` Function**: Ensures exit conditions include corresponding entry signals.
- **`validate_input` Function**: Validates the input data against the `StrategyInput` model and custom rules, raising validation errors if any issues are found.
"""

# Import necessary libraries
from datetime import datetime, time
from itertools import chain
from typing import List, Union
from pydantic import BaseModel, ValidationError, field_validator

# Import project-specific constants
from source.constants import MarketDirection, TradeType


class StrategyInput(BaseModel):
    """
    Pydantic model for validating strategy input data.
    """

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

    check_entry_fractal: bool = None
    entry_fractal_file_number: str = None

    check_exit_fractal: bool = None
    fractal_exit_count: Union[int, str] = None
    exit_fractal_file_number: str = None

    check_bb_band: bool = None
    bb_file_number: str = None
    bb_band_sd: float = None
    bb_band_column: str = None

    check_trail_bb_band: bool = None
    trail_bb_file_number: str = None
    trail_bb_band_sd: float = None
    trail_bb_band_column: str = None
    trail_bb_band_long_direction: str = None
    trail_bb_band_short_direction: str = None

    check_entry_based: bool = None
    number_of_entries: int = None
    steps_to_skip: int = None

    @field_validator("start_date", "end_date")
    def convert_to_datetime(cls, v):
        """
        Convert string dates to datetime objects.
        """
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        elif isinstance(v, datetime):
            return v
        raise ValueError('Invalid datetime format, should be "dd/mm/yyyy hh:mm:ss"')

    @field_validator("bb_band_sd", "trail_bb_band_sd")
    def validate_bb_band_sd(cls, v):
        """
        Validate BB band standard deviation.
        """
        allowed_values = {2.0, 2.25, 2.5, 2.75, 3.0}
        if v not in allowed_values:
            raise ValueError(
                "BB band standard deviation must be one of the following: 2.0, 2.25, 2.5, 2.75, 3.0"
            )
        return v

    @field_validator("bb_band_column", "trail_bb_band_column")
    def validate_bb_band_column(cls, v):
        """
        Validate BB band column.
        """
        allowed_values = {"mean", "upper", "lower"}
        if v not in allowed_values:
            raise ValueError(
                'BB band column must be one of the following: "mean", "upper", "lower"'
            )
        return v

    @field_validator("trail_bb_band_long_direction", "trail_bb_band_short_direction")
    def validate_trail_bb_band_direction(cls, v):
        """
        Validate trail BB band direction.
        """
        allowed_values = {"higher", "lower"}
        if v not in allowed_values:
            raise ValueError(
                'Trail BB band direction must be one of the following: "higher", "lower"'
            )
        return v

    @field_validator("trade_type")
    def validate_trade_type(cls, v):
        """
        Validate trade type.
        """
        if v not in {TradeType.INTRADAY, TradeType.POSITIONAL}:
            raise ValueError(
                'Trade type must be one of the following: "Intraday", "Positional"'
            )
        return v

    @field_validator("allowed_direction")
    def validate_allowed_direction(cls, v):
        """
        Validate allowed direction.
        """
        if v not in {MarketDirection.LONG, MarketDirection.SHORT, MarketDirection.ALL}:
            raise ValueError(
                'Allowed direction must be one of the following: "long", "short", "all"'
            )
        return v

    @field_validator("fractal_exit_count")
    def validate_fractal_exit_count(cls, v):
        """
        Validate fractal exit count.
        """
        if v.isdigit():
            return int(v)
        elif isinstance(v, str) and v.lower() == MarketDirection.ALL.value:
            return v
        raise ValueError('Fractal exit count must be an integer or "ALL"')


def validate_count(validated_data: StrategyInput):
    """
    Validate that the number of strategies and signals match the number of portfolio IDs.

    Args:
        validated_data (StrategyInput): The validated input data.

    Raises:
        AssertionError: If the counts do not match.
    """
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
    """
    Check if exit conditions include corresponding entry signals.

    Args:
        validated_data (StrategyInput): The validated input data.

    Raises:
        ValueError: If the conditions are not met.
    """
    short_exit_set = set(validated_data.short_exit_signals)
    long_exit_set = set(validated_data.long_exit_signals)

    if not any(
        signal_pair in short_exit_set
        for signal_pair in validated_data.long_entry_signals
    ):
        raise ValueError("Long entry signals should be added in short exit signals")

    if not any(
        signal_pair in long_exit_set
        for signal_pair in validated_data.short_entry_signals
    ):
        raise ValueError("Short entry signals should be added in long exit signals")


def validate_input(input_data):
    """
    Validate the input data against the StrategyInput model and custom rules.

    Args:
        input_data (dict): The input data to be validated.

    Returns:
        dict: The validated input data.

    Raises:
        ValidationError: If the validation fails.
    """
    try:
        validated_data = StrategyInput(**input_data)
        validate_count(validated_data)
        check_exit_conditions(validated_data)
        return validated_data.model_dump()
    except ValidationError as e:
        print(f"Input validation error: {e}")
        raise e
