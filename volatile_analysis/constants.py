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
    GROUP_ID = "group_id"


class VolatileTag(Enum):
    ALL = "All"
    LV = "LV"
    HV = "HV"


class AnalysisColumn(Enum):
    CYCLE_DURATION = "cycle_duration"
    CYCLE_MAX_1 = "cycle_max"
    CYCLE_MIN_1 = "cycle_min"
    MAX_TO_MIN = "max_to_min"
    MAX_TO_MIN_DURATION = "max_to_min_duration"
    MAX_TO_MIN_TO_CLOSE = "max_to_min_to_close"
    CYCLE_MAX_2 = "cycle_max_2"
    CYCLE_MIN_2 = "cycle_min_2"
    MIN_TO_MAX = "min_to_max"
    MIN_TO_MAX_DURATION = "min_to_max_duration"
    MIN_TO_MAX_TO_CLOSE = "min_to_max_to_close"
    CTC = "ctc"
    CAPITAL = "capital"
    CAPITAL_O_S = "capital_o/s"
    CYCLE_CAPITAL_MIN = "cycle_capital_min"
    CYCLE_CAPITAL_MAX = "cycle_capital_max"
    CYCLE_CAPITAL_CLOSE = "cycle_capital_close"
    CYCLE_CAPITAL_TO_CLOSE = "Cycle Capital To Close"
    POSITIVE_NEGATIVE = "positive_negative"
    CYCLE_CAPITAL_POS_NEG_MAX = "Cycle Capital Positive Negative_Max"
    PROBABILITY = "Probability"
    CYCLE_CAPITAL_DD = "Cycle Capital DD"
    RISK_REWARD_MAX = "Risk Reward_Max"
    RISK_REWARD_CTC = "Risk Reward_CTC"
    MIN_MAX_TO_CLOSE = "Min / Max to close ratio"


class PosNegConstant(Enum):
    POSITIVE = "Positive"
    NEGATIVE = "Negative"
    POSITIVE_MINUS = "Positive (< Upper Threshold)"
    NEGATIVE_PLUS = "Negative (> Lower Threshold)"
