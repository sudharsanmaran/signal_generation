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
import logging
import os
from pathlib import Path
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


load_dotenv(override=True)


class TradeExitType(Enum):
    """
    Enumeration for different types of trade exits.
    """

    FRACTAL = "FE"
    SIGNAL = "SC"
    TRAILING = "TR"
    END = "EOD"
    CYCLE_CHANGE = "CC"
    TP = "TP"
    BB = "BB"
    EB = "EB"
    OUT_OF_LIST = "OOL"


class TradeType(Enum):
    """
    Enumeration for different types of trades.
    """

    INTRADAY = "I"
    POSITIONAL = "P"
    # ALL = "all"


class MarketDirection(Enum):
    """
    Enumeration for market direction conditions.
    """

    ALL = "all"
    LONG = "L"
    SHORT = "S"
    UNKNOWN = "NA"
    PREVIOUS = "previous"


# List of possible strategy IDs
POSSIBLE_STRATEGY_IDS = list(range(1, 11))

# List of fractal column names
exit_fractal_columns = [
    "FRACTAL_CONFIRMED_LONG",
    "FRACTAL_CONFIRMED_SHORT",
]

entry_fractal_columns = [
    "FRACTAL_LONG",
    "FRACTAL_SHORT",
    *exit_fractal_columns,
]

# Dictionary mapping entry and exit fractal columns to market directions
fractal_column_dict = {
    "entry": {
        MarketDirection.LONG: "entry_FRACTAL_LONG",
        MarketDirection.SHORT: "entry_FRACTAL_SHORT",
    },
    "exit": {
        MarketDirection.LONG: "exit_FRACTAL_LONG",
        MarketDirection.SHORT: "exit_FRACTAL_SHORT",
    },
}

# Dictionary mapping confirmed entry and exit fractal columns to market directions
confirm_fractal_column_dict = {
    "entry": {
        MarketDirection.LONG: "entry_FRACTAL_CONFIRMED_LONG",
        MarketDirection.SHORT: "entry_FRACTAL_CONFIRMED_SHORT",
    },
    "exit": {
        MarketDirection.LONG: "exit_FRACTAL_CONFIRMED_LONG",
        MarketDirection.SHORT: "exit_FRACTAL_CONFIRMED_SHORT",
    },
}

BASE_SRC_FOLDER = Path("source")
DB_FOLDER = str(BASE_SRC_FOLDER / "DB")

BASE_OUTPUT_FOLDER = Path("outputs")
SG = "sg"
PA = "pa"
VA = "va"
VOLUME = "volume"
MERGED_DF_FOLDER = str(BASE_OUTPUT_FOLDER / SG / "merged_df")
SG_OUTPUT_FOLDER = str(BASE_OUTPUT_FOLDER / SG / "signal_generation_output")
SG_CYCLE_OUTPUT_FOLDER = str(BASE_OUTPUT_FOLDER / SG / "cycle_output")
SG_FRACTAL_ANALYSIS_OUTPUT_FOLDER = str(
    BASE_OUTPUT_FOLDER / SG / "fractal_analysis_output"
)
PA_ANALYSIS_FOLDER = str(BASE_OUTPUT_FOLDER / PA / "pa_analysis_outputs")
PA_ANALYSIS_CYCLE_FOLDER = str(
    BASE_OUTPUT_FOLDER / PA / "pa_analysis_cycle_outputs"
)
PA_ANALYSIS_SUMMARY_FOLDER = str(
    BASE_OUTPUT_FOLDER / PA / "pa_analysis_summary_outputs"
)
TARGET_PROFIT_FOLDER = str(BASE_OUTPUT_FOLDER / PA / "tp_outputs")
VOLATILE_OUTPUT_FOLDER = str(BASE_OUTPUT_FOLDER / VA / "volatile_outputs")
VOLATILE_OUTPUT_STATUS_FOLDER = str(
    BASE_OUTPUT_FOLDER / VA / "volatile_status_outputs"
)
VOLATILE_OUTPUT_SUMMARY_FOLDER = str(
    BASE_OUTPUT_FOLDER / VA / "volatile_summary_outputs"
)
VOLUME_OUTPUT_FOLDER = str(BASE_OUTPUT_FOLDER / VOLUME / "volume_outputs")
VOLUME_OUTPUT_SUMMARY_FOLDER = str(
    BASE_OUTPUT_FOLDER / VOLUME / "volume_summary_outputs"
)

PORTFOLIO_OUTPUT_FOLDER = "portfolio"
PORTFOLIO_COMPANY_OUTPUT_FOLDER = str(
    BASE_OUTPUT_FOLDER / PORTFOLIO_OUTPUT_FOLDER / "company"
)
PORTFOLIO_PNL_OUTPUT_FOLDER = str(
    BASE_OUTPUT_FOLDER / PORTFOLIO_OUTPUT_FOLDER / "pnl"
)
PORTFOLIO_SUMMARY_OUTPUT_FOLDER = str(
    BASE_OUTPUT_FOLDER / PORTFOLIO_OUTPUT_FOLDER / "summary"
)

# Define the percentage of available CPU to be used
cpu_percent_to_use = 0.8  # You can adjust this percentage as needed


INSTRUMENTS = list(
    map(lambda x: x.strip(), os.getenv("INSTRUMENTS", "").split(","))
)
STOCKS_FNO = list(
    map(lambda x: x.strip(), os.getenv("STOCKS_FNO", "").split(","))
)
STOCKS_NON_FNO = list(
    map(lambda x: x.strip(), os.getenv("STOCKS_NON_FNO", "").split(","))
)
TIMEFRAME_OPTIONS = sorted(
    list(
        map(
            lambda x: int(x.strip()),
            os.getenv("TIMEFRAME_OPTIONS", "").split(","),
        )
    )
)
PERIOD_OPTIONS = sorted(
    list(
        map(
            lambda x: int(x.strip()),
            os.getenv("PERIOD_OPTIONS", "").split(","),
        )
    )
)
SD_OPTIONS = sorted(
    list(map(lambda x: int(x.strip()), os.getenv("SD_OPTIONS", "").split(",")))
)


class OutputColumn(Enum):
    """
    Enumeration for output column names.
    """

    INSTRUMENT = "SG_Instrument"
    PORTFOLIOS = "SG_Portfolios"
    STRATEGY_IDS = "SG_Strategy IDs"
    SIGNAL = "SG_Signal"
    SIGNAL_NUMBER = "SG_Signal Number"
    ENTRY_DATETIME = "SG_Entry Datetime"
    ENTRY_ID = "SG_Entry ID"
    ENTRY_TYPE = "SG_Entry Type"
    EXIT_ID = "SG_Exit ID"
    EXIT_DATETIME = "SG_Exit Datetime"
    EXIT_TYPE = "SG_Exit Type"
    INTRADAY_POSITIONAL = "SG_Intraday/ Positional"
    ENTRY_PRICE = "SG_Entry Price"
    EXIT_PRICE = "SG_Exit Price"
    NET_POINTS = "SG_Net points"


class BB_Band_Columns(Enum):
    MEAN = "MEAN"
    UPPER = "UPPER"
    LOWER = "LOWER"
    # CLOSE = "CLOSE"
    # HIGH = "HIGH"
    # LOW = "LOW"


class FirstCycleColumns(Enum):
    DURATION_SIGNAL_START_TO_CYCLE_START = (
        "Duration Signal Start to Cycle Start"
    )
    CYCLE_DURATION = "Cycle Duration"
    DURATION_BETWEEN_MAX_MIN = "Duration Between Max Min"

    MOVE = "Move"
    MOVE_PERCENT = "Move Percent"

    CYCLE_MAX = "Cycle Max"
    CYCLE_MIN = "Cycle Min"
    MAX_TO_MIN = "Max to Min"
    POINTS_FROM_MAX = "Points from Maximum"
    AVERAGE_TILL_MAX = "Average Till Max"
    AVERAGE_TILL_MIN = "Average Till Min"
    POINTS_FRM_AVG_TILL_MAX_TO_MIN = "Points from Avg till Max to Min"

    CLOSE_TO_CLOSE = "Close to Close"
    POSITIVE_NEGATIVE = "Positive / Negative"

    CUM_AVG = "Cumulative Avg"
    ROLLING_AVG_3 = "Rolling Avg 3 Months"
    ROLLING_AVG_6 = "Rolling Avg 6 Months"

    TRAILLING_30_DAYS = "Trailing 30 Days"
    TRAILLING_90_DAYS = "Trailing 90 Days"
    TRAILLING_180_DAYS = "Trailing 180 Days"
    TRAILLING_270_DAYS = "Trailing 270 Days"
    TRAILLING_365_DAYS = "Trailing 365 Days"

    Z_SCORE = "Z Score"

    MAX_TO_MIN_TO_CLOSE_PERCENT = "Max to Min to Close Percent"
    POINTS_FROM_MAX_TO_CLOSE_PERCENT = "Points from Max to Close Percent"
    CLOSE_TO_CLOSE_TO_CLOSE_PERCENT = "Close to Close to Close Percent"
    POINTS_FRM_AVG_TILL_MAX_TO_MIN_TO_CLOSE_PERCENT = (
        "Points from Avg till Max to Min to Close Percent"
    )

    # DURATION_TO_MAX = "Duration to Max"
    # DURATION_ABOVE_BB = "Duration Above BB"
    # SIGNAL_START_TO_MAX_POINTS = "Signal Start to Max Points"
    # SIGNAL_START_TO_MAX_PERCENT = "Signal Start to Max Percent"
    # CATEGORY = "Category"
    # MOVE_START_TO_MAX_CYCLE_POINTS = "Move Start to Max Cycle Points"
    # MOVE_START_TO_MAX_CYCLE_PERCENT = "Move Start to Max Cycle Percent"
    # POINTS_FRM_AVG_TILL_MIN_TO_MAX = "Points from Avg till Min to Max"
    # SIGNAL_START_TO_MINIMUM_POINTS = "Signal Start to Minimum Points"
    # SIGNAL_START_TO_MINIMUM_PERCENT = "Signal Start to Minimum Percent"
    # AVG_OF_MAX_TO_AVG_OF_MIN = "Avg of Max to Avg of Min"


class TargetProfitColumns(Enum):
    TP_CLOSE = "TP Close"
    TP_CUM_AVG_CLOSE = "TP Cum Avg Close"
    TP_CUM_AVG_CLOSE_PERCENT = "TP Cum Avg Close Percent"
    TP_END = "TP End"
    TP_CUMMIN = "TP CumMin"
    TP_CUMMAX = "TP CumMax"
    TP_CLOSE_DIFF_PERCENT = "TP Close Diff Percent"


class SecondCycleIDColumns(Enum):
    CTC_CYCLE_ID = "CTC Cycle ID"
    MTM_CYCLE_ID = "MTM Cycle ID"
    FRACTAL_CYCLE_ID = "Fractal Cycle ID"


class CycleType(Enum):
    FIRST_CYCLE = "First Cycle"
    MTM_CYCLE = "MTM Cycle"
    CTC_CYCLE = "CTC Cycle"
    PREVIOUS_CYCLE = "Previous Cycle"


class GroupAnalytics(Enum):
    MIN_MAX = "GROUP_MIN_MAX"
    CLOSE = "GROUP_CLOSE"
    CLOSE_TO_MIN_MAX_PERCENT = "GROUP_CLOSE_TO_MIN_MAX_PERCENT"
    CLOSE_TO_MIN_MAX_POINTS = "GROUP_CLOSE_TO_MIN_MAX_POINTS"
    DURATION = "GROUP_DURATION"
