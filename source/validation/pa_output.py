from source.constants import CycleType
from source.validation.base_validation import BollingerBandInput, FractalInput


class PAOutput(FractalInput, BollingerBandInput):
    pa_file: str
    cycle_to_consider: CycleType
    tp_percentage: float
    tp_method: str


def validate_pa_input(input):
    try:
        input = PAOutput(**input)
        return input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
