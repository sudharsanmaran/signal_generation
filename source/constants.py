"""
The `constants.py` module defines various enumerations and constants used throughout the trading system. These include enumerations for trade exit types, trade types, and market directions, as well as lists and dictionaries related to fractal columns. Below is the code with additional comments and explanations:

### Explanation:

1. **Enumerations (`Enum`)**:
   - **`TradeExitType`**: Specifies different types of trade exits:
     - `FRACTAL`: Exit based on a fractal pattern.
     - `SIGNAL`: Exit due to a change in the trading signal.
     - `TRAILING`: Exit due to a trailing stop.
     - `END`: Exit at the end of the trade period.
   - **`TradeType`**: Specifies whether the trade is intraday or positional.
   - **`MarketDirection`**: Specifies the direction of the market:
     - `LONG`: Long position.
     - `SHORT`: Short position.
     - `PREVIOUS`: Previous position.
     - `ALL`: All positions.

2. **Constants**:
   - **`POSSIBLE_STRATEGY_IDS`**: A list of possible strategy IDs ranging from 1 to 10.
   - **`fractal_columns`**: A list of column names related to fractal patterns.

3. **Dictionaries**:
   - **`fractal_column_dict`**: Maps entry and exit fractal columns to their respective market directions for `LONG` and `SHORT`.
   - **`confirm_fractal_column_dict`**: Similar to `fractal_column_dict`, but for confirmed fractal columns.

These enumerations and constants are used throughout the trading system to ensure consistency and readability, facilitating easier management and maintenance of the code.
"""

from enum import Enum


class TradeExitType(Enum):
    """
    Enumeration for different types of trade exits.
    """

    FRACTAL = "FE"
    SIGNAL = "SC"
    TRAILING = "TR"
    END = "EOD"


class TradeType(Enum):
    """
    Enumeration for different types of trades.
    """

    INTRADAY = "I"
    POSITIONAL = "P"


class MarketDirection(Enum):
    """
    Enumeration for market direction conditions.
    """

    LONG = "L"
    SHORT = "S"
    PREVIOUS = "previous"
    ALL = "all"


# List of possible strategy IDs
POSSIBLE_STRATEGY_IDS = list(range(1, 11))

# List of fractal column names
exit_fractal_columns = [
    "P_1_FRACTAL_CONFIRMED_LONG",
    "P_1_FRACTAL_CONFIRMED_SHORT",
]

entry_fractal_columns = [
    "P_1_FRACTAL_LONG",
    "P_1_FRACTAL_SHORT",
    *exit_fractal_columns,
]

# Dictionary mapping entry and exit fractal columns to market directions
fractal_column_dict = {
    "entry": {
        MarketDirection.LONG: "entry_P_1_FRACTAL_LONG",
        MarketDirection.SHORT: "entry_P_1_FRACTAL_SHORT",
    },
    "exit": {
        MarketDirection.LONG: "exit_P_1_FRACTAL_LONG",
        MarketDirection.SHORT: "exit_P_1_FRACTAL_SHORT",
    },
}

# Dictionary mapping confirmed entry and exit fractal columns to market directions
confirm_fractal_column_dict = {
    "entry": {
        MarketDirection.LONG: "entry_P_1_FRACTAL_CONFIRMED_LONG",
        MarketDirection.SHORT: "entry_P_1_FRACTAL_CONFIRMED_SHORT",
    },
    "exit": {
        MarketDirection.LONG: "exit_P_1_FRACTAL_CONFIRMED_LONG",
        MarketDirection.SHORT: "exit_P_1_FRACTAL_CONFIRMED_SHORT",
    },
}
