from datetime import time
from typing import List, Optional
from source.constants import CycleType, MarketDirection, TradeType
from source.validation.base_validation import BollingerBandInput, FractalInput
from source.validation.validate_trade_management import TradingConfiguration


class PAOutput(FractalInput, BollingerBandInput, TradingConfiguration):
    pa_file: str
    cycle_to_consider: List[CycleType] = []
    calculate_tp: bool = False
    tp_percentage: float = 0.0
    tp_method: str = 1
    trade_type: TradeType
    allowed_direction: MarketDirection
    trade_start_time: Optional[time] = None
    trade_end_time: Optional[time] = None
    calculate_fractal_analysis: bool = False

    trigger_trade_management: bool = False


def validate_pa_input(input):
    try:
        input = PAOutput(**input)
        return input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
