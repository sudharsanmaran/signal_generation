from enum import Enum
import os


class OutputHeader(Enum):
    SIGNAL = "No. of signals"
    POINTS = "Net Points"
    POINTS_PERCENT = "Net Points Percentage"
    PROBABILITY = "Probability"
    POINTS_PER_SIGNAL = "Net Points Per Signal"
    POINTS_PER_SIGNAL_PERCENT = "Net Points Per Signal Percent"
    RISK_REWARD = "Risk Reward Ratio"
    SIGNAL_DURATION = "Signal Duration (in days)"
    WEIGHTED_AVERAGE_SIGNAL_DURATION = (
        "Weighted Average Signal Duration (in days)"
    )
    RANKING = "Ranking"


class SignalColumns(Enum):
    LONG_PLUS = "Long +"
    LONG_MINUS = "Long -"
    LONG_NET = "Long (Net)"
    SHORT_PLUS = "Short +"
    SHORT_MINUS = "Short -"
    SHORT_NET = "Short (Net)"
    LONG = "Long"
    SHORT = "Short"


class ProbabilityColumns(Enum):
    LONG = "Long"
    SHORT = "Short"
    TOTAL = "Total"


class RankingColumns(Enum):
    PROBABILITY_LONG = "Probability Long"
    PROBABILITY_SHORT = "Probability Short"
    PROBABILITY_TOTAL = "Probability Total"
    NET_POINTS_PER_SIGNAL_LONG_PLUS = "Net Points Long +"
    NET_POINTS_PER_SIGNAL_LONG_MINUS = "Net Points Long -"
    NET_POINTS_PER_SIGNAL_SHORT_PLUS = "Net Points Short +"
    NET_POINTS_PER_SIGNAL_SHORT_MINUS = "Net Points Short -"
    RISK_REWARD_LONG = "R_R_L"
    RISK_REWARD_SHORT = "R_R_S"
    WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_PLUS = (
        "Weighted Average Signal Duration Long +"
    )
    WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_MINUS = (
        "Weighted Average Signal Duration Long -"
    )
    WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_PLUS = (
        "Weighted Average Signal Duration Short +"
    )
    WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_MINUS = (
        "Weighted Average Signal Duration Short -"
    )
    TOTAL = "Total"


TIMEFRAME_OPTIONS = sorted(
    list(
        map(
            lambda x: int(x.strip()),
            os.getenv("TIMEFRAME_OPTIONS", "").split(","),
        )
    )
)
PERIOD_OPTIONS = sorted(
    list(
        map(
            lambda x: int(x.strip()),
            os.getenv("PERIOD_OPTIONS", "").split(","),
        )
    )
)
SD_OPTIONS = sorted(
    list(map(lambda x: int(x.strip()), os.getenv("SD_OPTIONS", "").split(",")))
)


class MTMCrossedCycleColumns(Enum):
    IS_MTM_CROSS_PNT_3 = "IS_MTM Crossed .3"
    IS_MTM_CROSS_PNT_5 = "IS_MTM Crossed .5"
    IS_MTM_CROSS_PNT_75 = "IS_MTM Crossed .75"
    IS_MTM_CROSS_1 = "IS_MTM Crossed 1"
