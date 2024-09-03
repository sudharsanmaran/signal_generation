from enum import Enum
import logging
import os
from dotenv import load_dotenv
from portfolio.errors import ImproperEnvConfigError
from source.constants import SG_CYCLE_OUTPUT_FOLDER


# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_environment_variables():
    """Load environment variables from .env file."""
    load_dotenv(override=True)
    logger.info("Environment variables loaded")


def get_env_variable(name: str) -> str:
    """Get an environment variable or raise an error if not found."""
    value = os.getenv(name)
    if value is None or value == "":
        logger.error(f"{name} not found in .env")
        raise ImproperEnvConfigError(f"{name} not found in .env")
    return value


def initialize_paths():
    """Initialize paths from environment variables."""
    global SIGNAL_GEN_FILES_PATH, COMPANIES_INFO_PATH, TICKER_FILE_PATH

    SIGNAL_GEN_FILES_PATH = SG_CYCLE_OUTPUT_FOLDER
    COMPANIES_INFO_PATH = get_env_variable("COMPANIES_INFO_PATH")
    TICKER_FILE_PATH = get_env_variable("TICKER_FILE_PATH")


class PNLSummaryCols(Enum):
    DATE = "Date"
    COMPANY = "Company"
    OPEN_POSITION = "Open Position"
    INITIATED_POSITION = "Initiated Position"
    TRADE_ID = "Trade ID"
    NO_OF_ENTRIES_FOR_THE_DAY = "No. of Enteries for the day"
    WEIGHTED_AVERAGE_PRICE = "Weighted Average Price"
    TOTAL_VOLUME = "Total Volume"
    ALLOWED_EXPOSURE = "Allowed Exposure"
    TOTAL_AMOUNT = "Total Amount"
    EXITS = "Exits"
    OPEN_VOLUME = "Open Volume"
    OPEN_EXPOSURE_COST = "Open Exposure (Cost)"
    OPEN_EXPOSURE_PERCENT = "Open Exposure %"
    CLOSING_PRICE = "Closing Price"
    PORTFOLIO_VALUE = "Portfolio Value"
    MTM_REALIZED = "MTM Realized"
    MTM_UNREALIZED = "MTM Unrealized"
    PERCENT_OF_OPEN_POSITION = "% of Open Position"
    MTM_UNREALIZED_BY_CAPITAL = "MTM Unrealized by Capital"
    MTM_UNREALIZED_BY_EXPOSURE = "MTM Unrealized by Exposure"
    MTM_REALIZED_BY_CAPITAL = "MTM Realized by Capital"
    MTM_REALIZED_BY_EXPOSURE = "MTM Realized by Exposure"


try:
    load_environment_variables()
    initialize_paths()
    logger.info("Paths initialized successfully")
except ImproperEnvConfigError as e:
    logger.error(f"Configuration error: {e}")
    raise e
