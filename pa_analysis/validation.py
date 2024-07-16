from source.validation.cycle_validation import CycleInputs


class CycleAnalysis(CycleInputs):
    pass


def validate(analysis_input: dict) -> dict:
    """
    Validate the input data for analysis.
    """
    try:
        analysis_input = CycleInputs(**analysis_input)
        return analysis_input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
