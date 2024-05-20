from enum import Enum


class TradeExitType(Enum):
    FRACTAL = "Fractal Exit"
    SIGNAL = "Signal Change"
    TRAILING = "Trailling"
    END = "Trade End Exit"


class TradeType(Enum):
    INTRADAY = "Intraday"
    POSITIONAL = "Positional"


class MarketDirection(Enum):
    LONG = "long"
    SHORT = "short"
    PREVIOUS = "previous"
    ALL = "all"
