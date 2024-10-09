import os
import logging
from pathlib import Path
import pandas as pd
from portfolio.constants import (
    COMPANIES_INFO_PATH,
    SIGNAL_GEN_FILES_PATH,
    TICKER_FILE_PATH,
)
from portfolio.utils import fetch_ticker
from portfolio.validation import CompaniesInput
from source.constants import OutputColumn
from tradesheet.constants import OUTPUT_PATH


logger = logging.getLogger(__name__)


def read_csv_file(file_path: str, index_col: str = None) -> pd.DataFrame:
    """Generic function to read a CSV file with error handling."""
    try:
        df = pd.read_csv(file_path, index_col=index_col)
        logger.info(f"Successfully read file: {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        logger.error(f"File is empty: {file_path}")
    except Exception as e:
        logger.exception(f"An error occurred while reading {file_path}: {e}")
    return pd.DataFrame()


def read_company_data(company_data: CompaniesInput) -> pd.DataFrame:
    file_path = os.path.join(
        COMPANIES_INFO_PATH,
        company_data.segment.lower(),
        f"{company_data.parameter_id}_result.csv",
    )
    df = read_csv_file(file_path)
    df["Date"] = pd.to_datetime(df["Date"])
    if df.empty:
        raise ValueError(f"Company Data not found for {company_data}")
    return df


def unique_company_names(df: pd.DataFrame) -> list:
    try:
        company_names = df["Name of Company"].unique().tolist()
        logger.info("Successfully retrieved unique company names")
        return company_names
    except KeyError:
        logger.error("Column 'Name of Company' not found in DataFrame")
    except Exception as e:
        logger.exception(
            f"An error occurred while retrieving company names: {e}"
        )
    return []


def read_company_tickers() -> pd.DataFrame:
    return read_csv_file(TICKER_FILE_PATH, index_col="Company Name")


def get_signal_gen_files() -> list:
    try:
        folder = Path(OUTPUT_PATH)
        sg_files = [f.name for f in folder.iterdir() if f.is_file()]
        logger.info("Successfully retrieved signal generation files")
        return sg_files
    except FileNotFoundError:
        logger.error(f"Directory not found: {SIGNAL_GEN_FILES_PATH}")
    except Exception as e:
        logger.exception(
            f"An error occurred while retrieving signal generation files: {e}"
        )
    return []


def read_signal_gen_file(file_name: str) -> pd.DataFrame:
    file_path = os.path.join(OUTPUT_PATH, file_name)
    df = read_csv_file(file_path)
    for col in [
        OutputColumn.ENTRY_DATETIME.value,
        OutputColumn.EXIT_DATETIME.value,
    ]:
        df[col] = pd.to_datetime(df[col])
    return df


def get_company_close_price_df(
    company_data: CompaniesInput, years_list, company_list, company_tickers
) -> pd.DataFrame:
    base_path = os.getenv("COMPANY_CLOSE_PRICE_PATH")
    company_close_df_map = {}
    if company_data.segment == "Cash":
        for company in company_list:
            ticker = fetch_ticker(company_tickers, company)
            for year in years_list:
                file_path = os.path.join(
                    base_path, 'CM', ticker, f"{year}.csv"
                )
                df = read_csv_file(file_path)

                if df.empty:
                    continue

                df["Date"] = pd.to_datetime(df["Date"])
                df.set_index("Date", inplace=True)
                company_close_df_map[(company, year)] = df
    else:
        raise NotImplementedError("Only CASH segment is supported")
    return company_close_df_map
