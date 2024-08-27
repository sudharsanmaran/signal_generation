import logging
from portfolio.data_reader import read_signal_gen_file
from portfolio.validation import InputData


logger = logging.getLogger(__name__)


def process_portfolio(validated_data: InputData):
    for group_id, group_data in validated_data.companies_df.groupby("Date"):
        group_data.index = group_data["Name of Company"]
        for company in group_data["Name of Company"].unique().tolist():
            if company in validated_data.company_sg_map:
                logger.info(f"Processing {company} for {group_id}")
                sg_df = read_signal_gen_file(
                    validated_data.company_sg_map[company]
                )
                if sg_df.empty:
                    logger.error(f"Signal gen file is empty for {company}")
                    continue
            else:
                logger.warning(f"{company} not found in signal gen files")
