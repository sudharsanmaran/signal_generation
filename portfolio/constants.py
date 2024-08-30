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


try:
    load_environment_variables()
    initialize_paths()
    logger.info("Paths initialized successfully")
except ImproperEnvConfigError as e:
    logger.error(f"Configuration error: {e}")
