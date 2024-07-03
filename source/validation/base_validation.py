from datetime import datetime, time
from typing import List, Optional, Union
from pydantic import BaseModel, field_validator

from source.constants import MarketDirection


class BaseInputs(BaseModel):
    portfolio_ids: tuple
    strategy_pairs: List[tuple]
    instruments: List[str]
    allowed_direction: MarketDirection

    trade_start_time: Optional[time] = None
    trade_end_time: Optional[time] = None
    start_date: Union[str, datetime]
    end_date: Union[str, datetime]

    @field_validator("start_date", "end_date")
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


class FractalInput(BaseModel):
    check_entry_fractal: bool = None
    entry_fractal_file_number: str = None

    check_exit_fractal: bool = None
    fractal_exit_count: Union[int, str] = None
    exit_fractal_file_number: str = None
