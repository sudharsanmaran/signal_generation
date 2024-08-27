import logging
import streamlit as st

from portfolio.data_reader import (
    get_signal_gen_files,
    read_company_data,
    read_company_tickers,
    unique_company_names,
)
from portfolio.processor import process_portfolio
from portfolio.validation import validate_companies_input, validate_input_data

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    st.header("Portfolio")

    # Manage company information input
    companies_data = manage_company_info()

    if companies_data:
        st.session_state["companies_data"] = companies_data
        df = read_company_data(companies_data)
        st.session_state["companies_df"] = df
        company_lists = unique_company_names(df)
        st.session_state["company_lists"] = company_lists
        st.session_state["company_sg_map"] = manage_signal_gen_files(
            company_lists
        )
        st.session_state["configs"] = manage_configs()

    required_fields = [
        "company_lists",
        "companies_data",
        "company_sg_map",
        "configs",
    ]

    if all(field in st.session_state for field in required_fields):
        submit = st.button("Submit")
        if submit:
            try:
                validated_data = validate_input_data(
                    {
                        "company_lists": st.session_state["company_lists"],
                        "companies_data": st.session_state["companies_data"],
                        "company_sg_map": st.session_state["company_sg_map"],
                        "configs": st.session_state["configs"],
                        "companies_df": st.session_state["companies_df"],
                    }
                )
                process_portfolio(validated_data)
            except Exception as e:
                st.error(e)
    else:
        st.warning("Please fill out all required fields.")


def manage_company_info():
    """Handles input for company segment and parameter ID."""

    if "companies_data" not in st.session_state:
        with st.expander("Companies Info", expanded=True):
            segment = st.selectbox("Segment", ["Cash", "Future", "Options"])
            parameter_id = st.number_input("Parameter ID", value=1)

            if segment and parameter_id:
                if st.button("Get companies data"):
                    try:
                        companies_data = validate_companies_input(
                            {"segment": segment, "parameter_id": parameter_id}
                        )
                        return companies_data
                    except Exception as e:
                        st.error(e)
            else:
                st.warning("Please fill out all required fields.")
    else:
        st.write("Companies data already retrieved.")
    return None


def manage_signal_gen_files(company_lists):
    """Handles signal generation files selection for each company."""
    if "company_sg_map" not in st.session_state:
        ticker_df = read_company_tickers()
        signal_gen_files = get_signal_gen_files()
        company_signal_gen_files = {}

        with st.expander("Signal Gen Files", expanded=True):
            for company in company_lists:
                st.write(company)
                ticker = (
                    ticker_df.loc[company, "Ticker Symbol"]
                    if company in ticker_df.index
                    else None
                )
                if ticker:
                    options = [
                        file for file in signal_gen_files if ticker in file
                    ]
                    if options:
                        signal_gen_file = st.selectbox(
                            f"{company} Signal Gen File", options
                        )
                        company_signal_gen_files[company] = signal_gen_file
                    else:
                        st.error(f"No signal gen file found for {company}")
                else:
                    st.error(f"{company} not found in ticker file.")

        return company_signal_gen_files
    else:
        st.write("Signal Gen Files already selected.")
    return st.session_state["company_sg_map"]


def manage_configs():
    """Handles configuration input for portfolio management."""
    if "configs" not in st.session_state:
        with st.expander("Configs", expanded=True):
            capital = st.number_input("Capital", value=100000000)
            cash_percent = st.number_input("Cash Percent", value=10)
            risk_per_entry_fractal = st.number_input(
                "Risk Per Entry Fractal", value=0.10
            )
            open_volume_percent = st.number_input(
                "Open Volume Percent", value=50
            )

            configs = {
                "capital": capital,
                "cash_percent": cash_percent,
                "risk_per_entry_fractal": risk_per_entry_fractal,
                "open_volume_percent": open_volume_percent,
            }

            return configs
    else:
        st.write("Configs already set.")
    return st.session_state["configs"]


if __name__ == "__main__":
    main()
