from enum import Enum


class AnalysisConstant(Enum):
    CUM_STD = "cumulative_stddev"
    CUM_AVG_VOLATILITY = "cumulutaive_avg_volatility"
    Z_SCORE = "z_score"
    NORM_Z_SCORE = "normalized_z_score"
    TRAIL_WINDOW_SUM = "trailing_window_sum"
    TRAIL_WINDOW_AVG = "trailing_window_avg"
    VOLATILE_TAG = "volatile_tag"
    CYCLE_ID = "cycle_id"


class VolatileTag(Enum):
    LV = "LV"
    HV = "HV"
