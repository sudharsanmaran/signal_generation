"""
The `trade.py` module provided defines the `Trade` class for managing trades, including their initialization, execution, and recording. The module also includes the `initialize` function for setting up class-level attributes based on validated input data. Below, I will add comments and docstrings to explain the purpose and functionality of each section of the code.

### Explanation:
- **Imports**: Importing necessary libraries and project-specific constants.
- **`Trade` Class**: Defines the structure and methods for managing trades.
  - **Attributes**: Defines class-level attributes related to trade configuration and conditions.
  - **`__init__` Method**: Initializes an instance of a trade with entry details.
  - **`calculate_pnl` Method**: Calculates the profit and loss for the trade.
  - **`add_exit` Method**: Adds an exit to the trade and updates the trade status.
  - **`is_trade_closed` Method**: Checks if the trade is closed.
  - **`formulate_output` Method**: Formulates the output details of the trade.
- **`initialize` Function**: Sets up class-level attributes for the `Trade` class based on validated input data. This function configures various trade conditions and properties that will be used when creating and managing trades.

This commented code should help clarify the purpose and functionality of each part of the module.
"""

# Import necessary libraries
from typing import Dict, Optional, List

# Import project-specific constants
from source.constants import (
    CycleType,
    MarketDirection,
    OutputColumn,
    TradeExitType,
)


class Trade:
    """
    Class for managing trades including their initialization, execution, and recording.
    """

    portfolio_ids: tuple
    strategy_pairs: Optional[tuple] = None
    entry_id_counter: int = 0
    instruments: List[str]
    trade_start_time = None
    trade_end_time = None
    type: Optional[str] = None
    market_direction_conditions: Dict = {}
    allowed_direction: Optional[str] = None
    signal_columns: Optional[tuple] = None
    trigger_trade_management: bool = False

    check_entry_fractal: bool = False
    check_exit_fractal: bool = False
    fractal_exit_count: Optional[int] = None

    check_bb_band: bool = False
    bb_band_column: Optional[str] = None

    check_trail_bb_band: bool = False
    trail_bb_band_column: Optional[str] = None
    trail_bb_band_direction: Optional[str] = None
    trail_compare_functions: Dict = {}

    check_entry_based: bool = False
    max_limit_entry_based: Optional[int] = None
    steps_entry_based: Optional[int] = None

    skip_rows: bool = False
    no_of_rows_to_skip: Optional[int] = None

    cycle_to_consider: CycleType = None
    cycle_columns: Optional[Dict] = {}
    current_cycle: Optional[str] = None

    def __init__(
        self, entry_signal, entry_datetime, entry_price, signal_count
    ):
        """
        Initialize a Trade instance with entry details.

        Args:
            entry_signal (MarketDirection): Direction of the trade entry.
            entry_datetime (datetime): Timestamp of the trade entry.
            entry_price (float): Price at which the trade was entered.
            signal_count (int): Signal count associated with the entry.
        """
        Trade.entry_id_counter += 1
        self.entry_id = Trade.entry_id_counter

        self.entry_signal = entry_signal
        self.signal_count = signal_count
        self.entry_datetime = entry_datetime
        self.entry_price = entry_price
        self.exits = []
        self.trade_closed = False
        self.exit_id_counter = 0

    def calculate_pnl(self, exit_price):
        """
        Calculate the profit and loss (PnL) for the trade.

        Args:
            exit_price (float): The price at which the trade is exited.

        Returns:
            float: The calculated PnL.
        """
        pnl = 0
        if self.entry_signal == MarketDirection.LONG:
            pnl = exit_price - self.entry_price
        else:
            pnl = self.entry_price - exit_price
        return pnl

    def add_exit(self, exit_datetime, exit_price, exit_type):
        """
        Add an exit to the trade with the specified details.

        Args:
            exit_datetime (datetime): Timestamp of the trade exit.
            exit_price (float): Price at which the trade was exited.
            exit_type (TradeExitType): Type of the trade exit.
        """
        if not self.trade_closed:
            self.exit_id_counter += 1

            if exit_type in {
                TradeExitType.SIGNAL,
                TradeExitType.TRAILING,
                TradeExitType.END,
                TradeExitType.CYCLE_CHANGE,
            }:
                self.trade_closed = True

            if Trade.fractal_exit_count:
                if (
                    exit_type == TradeExitType.FRACTAL
                    and self.exit_id_counter == Trade.fractal_exit_count
                ):
                    self.exits.append(
                        {
                            "exit_id": self.exit_id_counter,
                            "exit_datetime": exit_datetime,
                            "exit_price": exit_price,
                            "exit_type": exit_type,
                            "pnl": self.calculate_pnl(exit_price),
                        }
                    )
            else:
                self.exits.append(
                    {
                        "exit_id": self.exit_id_counter,
                        "exit_datetime": exit_datetime,
                        "exit_price": exit_price,
                        "exit_type": exit_type,
                        "pnl": self.calculate_pnl(exit_price),
                    }
                )

    def is_trade_closed(self):
        """
        Check if the trade is closed.

        Returns:
            bool: True if the trade is closed, False otherwise.
        """
        return self.trade_closed

    @classmethod
    def reset_trade_entry_id_counter(cls):
        """
        Reset the trade entry ID counter to 0.
        """
        cls.entry_id_counter = 0

    def formulate_output(self, instrument, strategy_pair, portfolio_pair=None):
        """
        Formulate the trade output details.

        Args:
            strategy_pair (str): Pair of strategy IDs.
            portfolio_pair (str, optional): Pair of portfolio IDs. Defaults to None.

        Returns:
            list: List of dictionaries with trade details.
        """
        return [
            {
                OutputColumn.INSTRUMENT.value: instrument,
                OutputColumn.PORTFOLIOS.value: portfolio_pair,
                OutputColumn.STRATEGY_IDS.value: strategy_pair,
                OutputColumn.SIGNAL.value: self.entry_signal.value,
                OutputColumn.SIGNAL_NUMBER.value: self.signal_count,
                OutputColumn.ENTRY_DATETIME.value: self.entry_datetime,
                OutputColumn.ENTRY_ID.value: self.entry_id,
                OutputColumn.EXIT_ID.value: exit["exit_id"],
                OutputColumn.EXIT_DATETIME.value: exit["exit_datetime"],
                OutputColumn.EXIT_TYPE.value: exit["exit_type"].value,
                OutputColumn.INTRADAY_POSITIONAL.value: Trade.type.value,
                OutputColumn.ENTRY_PRICE.value: self.entry_price,
                OutputColumn.EXIT_PRICE.value: exit["exit_price"],
                OutputColumn.NET_POINTS.value: exit["pnl"],
            }
            for exit in self.exits
        ]


def initialize(validated_input):

    def set_compare_functions(direction, condition):
        if condition == "higher":
            Trade.trail_compare_functions[direction]["compare_func"] = (
                lambda a, b: a > b
            )
            Trade.trail_compare_functions[direction][
                "opposite_compare_func"
            ] = (lambda a, b: a < b)
        else:
            Trade.trail_compare_functions[direction]["compare_func"] = (
                lambda a, b: a < b
            )
            Trade.trail_compare_functions[direction][
                "opposite_compare_func"
            ] = (lambda a, b: a > b)

    """
    Initialize Trade class-level attributes based on validated input data.

    Args:
        validated_input (dict): The validated input data.
    """
    Trade.entry_id_counter = 0
    Trade.portfolio_ids = validated_input.get("portfolio_ids")
    Trade.strategy_pairs = validated_input.get("strategy_pairs")
    Trade.instruments = validated_input.get("instruments")
    Trade.trade_start_time = validated_input.get("trade_start_time")
    Trade.trade_end_time = validated_input.get("trade_end_time")
    Trade.check_entry_fractal = validated_input.get("check_entry_fractal")
    Trade.check_exit_fractal = validated_input.get("check_exit_fractal")
    Trade.check_bb_band = validated_input.get("check_bb_band")
    Trade.check_trail_bb_band = validated_input.get("check_trail_bb_band")
    Trade.check_entry_based = validated_input.get("check_entry_based")
    Trade.type = validated_input.get("trade_type")
    Trade.trigger_trade_management = validated_input.get(
        "trigger_trade_management"
    )
    Trade.cycle_to_consider = validated_input.get("cycle_to_consider")
    Trade.market_direction_conditions = {
        "entry": {
            MarketDirection.LONG: validated_input.get("long_entry_signals"),
            MarketDirection.SHORT: validated_input.get("short_entry_signals"),
        },
        "exit": {
            MarketDirection.LONG: validated_input.get("long_exit_signals"),
            MarketDirection.SHORT: validated_input.get("short_exit_signals"),
        },
    }
    Trade.allowed_direction = validated_input.get("allowed_direction")
    Trade.signal_columns = [
        f"TAG_{id}" for id in validated_input.get("portfolio_ids")
    ]

    fractal_exit_count = validated_input.get("fractal_exit_count")
    Trade.fractal_exit_count = (
        fractal_exit_count if isinstance(fractal_exit_count, int) else None
    )

    if Trade.check_bb_band:
        Trade.bb_band_column = f"P_1_{validated_input.get('bb_band_column').upper()}_BAND_{validated_input.get('bb_band_sd')}"
    if Trade.check_trail_bb_band:
        Trade.trail_bb_band_column = f"P_1_{validated_input.get('trail_bb_band_column').upper()}_BAND_{validated_input.get('trail_bb_band_sd')}"

    # Initialize trail compare functions with default values
    Trade.trail_compare_functions = {
        MarketDirection.LONG: {
            "compare_func": None,
            "opposite_compare_func": None,
        },
        MarketDirection.SHORT: {
            "compare_func": None,
            "opposite_compare_func": None,
        },
    }
    set_compare_functions(
        MarketDirection.LONG,
        validated_input.get("trail_bb_band_long_direction"),
    )
    set_compare_functions(
        MarketDirection.SHORT,
        validated_input.get("trail_bb_band_short_direction"),
    )

    if Trade.check_entry_based:
        Trade.max_limit_entry_based = validated_input.get("number_of_entries")
        Trade.steps_entry_based = validated_input.get("steps_to_skip")

    if validated_input.get("skip_rows"):
        Trade.skip_rows = True
        Trade.no_of_rows_to_skip = validated_input.get("no_of_rows_to_skip")
