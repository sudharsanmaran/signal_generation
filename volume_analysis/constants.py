from enum import Enum


class Operations(Enum):
    AVERAGE = "average"
    MEDIAN = "median"
    SUM = "sum"
    MAX = "max"
    WEIGHTED_AVERAGE_PRICE = "weighted_average"


class SummaryColumn(Enum):
    STRATEGY_ID = "strategy_id"
    TIME_FRAME = "time_frame"
    INSTRUMENT = "instrument"
    CATEGORY = "category"
    DURATION = "test_duration"
    DURATION_IN_YEARS = "test_duration_in_years"
    NO_OF_CYCLES = "no_of_cycles"
    NO_OF_COUNTS = "no_of_counts"
    AVG_VOLUME_TRADED = "avg_volume_traded"
    AVG_CYCLE_DURATION = "Avg cycle_duration"
    AVG_COUNT_DURATION = "Avg count_duration"
    AVG_ZSCORE = "Avg zscore"
    AVG_ZSCORE_RANK = "Avg zscore rank"
    CTC = "CTC"
    CYCLE_CAPITAL_POS_NEG_MAX = "Cycle Capital Positive Negative_Max"
    MIN_MAX_TO_CLOSE = "Min / Max to close ratio"
    RISK_REWARD_MAX = "Risk Reward_Max"
    RISK_REWARD_CTC = "Risk Reward_CTC"
