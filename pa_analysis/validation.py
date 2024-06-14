from datetime import datetime
from typing import List
from pydantic import BaseModel, field_validator


class AnalysisInput(BaseModel):
    start_date: datetime
    end_date: datetime
    portfolio_ids: tuple
    strategy_pairs: List[tuple]
    instruments: List[str]
    long_entry_signals: List[tuple]
    short_entry_signals: List[tuple]

    @field_validator("start_date", "end_date", mode="before")
    def convert_to_datetime(cls, v):
        """
        Convert string dates to datetime objects.
        """
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        elif isinstance(v, datetime):
            return v
        raise ValueError(
            'Invalid datetime format, should be "dd/mm/yyyy hh:mm:ss"'
        )


def validate(analysis_input: dict) -> dict:
    """
    Validate the input data for analysis.
    """
    try:
        analysis_input = AnalysisInput(**analysis_input)
        return analysis_input.model_dump()
    except Exception as e:
        return {"error": str(e)}
