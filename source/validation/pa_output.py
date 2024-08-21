from datetime import time
from typing import Optional
from source.constants import CycleType, MarketDirection, TradeType
from source.validation.base_validation import BollingerBandInput, FractalInput


class PAOutput(FractalInput, BollingerBandInput):
    pa_file: str
    cycle_to_consider: CycleType
    tp_percentage: float
    tp_method: str
    trade_type: TradeType
    allowed_direction: MarketDirection
    trade_start_time: Optional[time] = None
    trade_end_time: Optional[time] = None
    calculate_fractal_analysis: bool = False


def validate_pa_input(input):
    try:
        input = PAOutput(**input)
        return input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
