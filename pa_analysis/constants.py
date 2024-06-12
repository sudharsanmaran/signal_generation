from enum import Enum


class OutputHeader(Enum):
    SIGNAL = "No. of signals"
    POINTS = "Net Points"
    PROBABILITY = "Probability"
    POINTS_PER_SIGNAL = "Net Points per Signal"
    RISK_REWARD = "Risk Reward Ratio"
    SIGNAL_DURATION = "Signal Duration (in days)"
    WEIGHTED_AVERAGE_SIGNAL_DURATION = (
        "Weighted Average Signal Duration (in days)"
    )


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
