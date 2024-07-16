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
from itertools import chain
from typing import List, Optional
from pydantic import ValidationError, field_validator

# Import project-specific constants
from source.constants import MarketDirection, TradeType
from source.validation.base_validation import BaseInputs, FractalInput
from source.validation.validate_trade_management import TradingConfiguration


class StrategyInput(BaseInputs, FractalInput):
    """
    Pydantic model for validating strategy input data.
    """

    long_entry_signals: List[tuple]
    long_exit_signals: List[tuple]
    short_entry_signals: List[tuple]
    short_exit_signals: List[tuple]

    trade_type: TradeType
    trigger_trade_management: bool = False
    pa_analysis: bool = False

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

    skip_rows: bool = False
    no_of_rows_to_skip: Optional[int] = None

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

    @field_validator(
        "trail_bb_band_long_direction", "trail_bb_band_short_direction"
    )
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


class StrategyInputAndTradingConfig(StrategyInput, TradingConfiguration):
    pass


def parse_enum(enum_class, value):
    """
    Parse a string value to an enumeration class.

    Args:
        enum_class (Enum): The enumeration class.
    """

    # Extract the class name and member name from the string
    class_name, member_name = value.split(".")

    # Use globals() to get the enum class by name
    enum_class = globals()[class_name]
    return enum_class.__members__[member_name]


def validate_count(validated_data: StrategyInput):
    """
    Validate that the number of strategies and signals match the number of portfolio IDs.

    Args:
        validated_data (StrategyInput): The validated input data.

    Raises:
        AssertionError: If the counts do not match.
    """
    for strategy_pair in validated_data.strategy_pairs:
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
    long_entry_set = set(validated_data.long_entry_signals)
    short_entry_set = set(validated_data.short_entry_signals)
    short_exit_set = set(validated_data.short_exit_signals)
    long_exit_set = set(validated_data.long_exit_signals)

    if validated_data.allowed_direction == MarketDirection.ALL:
        if not long_entry_set.issubset(short_exit_set):
            raise ValueError(
                "Long entry signals should be added in short exit signals"
            )

        if not short_entry_set.issubset(long_exit_set):
            raise ValueError(
                "Short entry signals should be added in long exit signals"
            )

    if long_entry_set & long_exit_set:
        raise ValueError(
            "Long entry signals and long exit signals cannot be the same"
        )
    if short_entry_set & short_exit_set:
        raise ValueError(
            "Short entry signals and short exit signals cannot be the same"
        )


def validate_signal_input(input_data):
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
        validated_data = StrategyInputAndTradingConfig(**input_data)
        validate_count(validated_data)
        check_exit_conditions(validated_data)
        return validated_data.model_dump()
    except ValidationError as e:
        print(f"Input validation error: {e}")
        raise e
