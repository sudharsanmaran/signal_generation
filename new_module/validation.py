from pydantic import BaseModel, ValidationError


class Inputs(BaseModel):
    avg_zscore_sum_threshold: int
    cycle_duration: int
    cycle_skip_count: int


def validate(inputs):
    try:
        validated_inputs = Inputs(**inputs)
        return validated_inputs.model_dump()
    except ValidationError as e:
        raise e
