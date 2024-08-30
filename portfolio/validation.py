from typing import List, Literal
import pandas as pd
from pydantic import BaseModel, ConfigDict


class CompaniesInput(BaseModel):
    segment: Literal["Cash", "Options", "Future"]
    parameter_id: int


class SignalGenFiles(BaseModel):
    company_sg_map: dict


class Configs(BaseModel):
    capital: int
    cash_percent: int
    risk_per_entry_fractal: float
    open_volume_percent: int


class InputData(SignalGenFiles):
    companies_data: CompaniesInput
    company_lists: List[str]
    companies_df: pd.DataFrame
    company_tickers: pd.DataFrame
    configs: Configs

    model_config = ConfigDict(arbitrary_types_allowed=True)


def validate_companies_input(data: dict) -> CompaniesInput:
    return CompaniesInput(**data)


def validate_input_data(data: dict) -> InputData:
    return InputData(**data)
