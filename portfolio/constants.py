import os
from dotenv import load_dotenv

from source.constants import SG_CYCLE_OUTPUT_FOLDER


load_dotenv(override=True)


SIGNAL_GEN_FILES_PATH = SG_CYCLE_OUTPUT_FOLDER
COMPANIES_INFO_PATH = os.getenv("COMPANIES_INFO_PATH")
TICKER_FILE_PATH = os.getenv("TICKER_FILE_PATH")
