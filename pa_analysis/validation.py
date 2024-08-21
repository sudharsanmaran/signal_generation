from pydantic import field_validator
from source.validation.cycle_validation import CycleInputs


class CycleAnalysis(CycleInputs):
    volatile_file: str = None
    volatile_tag_to_process: str = None
    volume_file: str = None
    volume_tag_to_process: str = None

    include_volume: bool = False
    include_volatile: bool = False

    category: int = None

    # @field_validator("include_volume", mode="before")
    # def validate_include_volume(cls, v, values):
    #     """
    #     Validate the include_volume field.
    #     """
    #     if v:
    #         if not values.data.get("volume_file"):
    #             raise ValueError(
    #                 "volume_file is required when include_volume is True"
    #             )
    #         if not values.data.get("volume_tag_to_process"):
    #             raise ValueError(
    #                 "volume_tag_to_process is required when include_volume is True"
    #             )

    # @field_validator("include_volatile", mode="before")
    # def validate_include_volatile(cls, v, values):
    #     """
    #     Validate the include_volatile field.
    #     """
    #     if v:
    #         if not values.data.get("volatile_file"):
    #             raise ValueError(
    #                 "volatile_file is required when include_volatile is True"
    #             )
    #         if not values.data.get("volatile_tag_to_process"):
    #             raise ValueError(
    #                 "volatile_tag_to_process is required when include_volatile is True"
    #             )


def validate(analysis_input: dict) -> dict:
    """
    Validate the input data for analysis.
    """
    try:
        analysis_input = CycleAnalysis(**analysis_input)
        return analysis_input.model_dump()
    except Exception as e:
        print("error", str(e))
        raise e
