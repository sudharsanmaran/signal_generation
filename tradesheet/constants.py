from datetime import time

from source.constants import OutputColumn, MarketDirection

INPUT_FILE = "input_file.csv"
# INPUT_FILES_PATH = "H:\\Sudharsan\\signal_generation\\calathea_capital\\tradesheet\\"
INPUT_FILES_PATH = "D:\\code\\SUDHARSAN\\signal_generation\\tradesheet\\"
DB_FILE_PATH = f"{INPUT_FILES_PATH}DB\\"
OUTPUT_PATH = f"{INPUT_FILES_PATH}Output\\"
DATABASE_PATH = "D:\\MBM\\MBM_DATA_BACKTEST\\"

ENTRY_EXIT_FILE = f"{DB_FILE_PATH}module_1_output.csv"
EXPIRY_FILE = f"{DB_FILE_PATH}EXPIRY.csv"
STRIKE_FILE = f"{DB_FILE_PATH}STRIKE_DIFF.csv"
LOT_FILE = f"{DB_FILE_PATH}LOTSIZE.csv"

STOCKS_EXPIRY_FILE = f"{DB_FILE_PATH}STOCKS_EXPIRY.csv"
STOCKS_STRIKE_FILE = f"{DB_FILE_PATH}STOCKS_STRIKE_DIFF.csv"
STOCKS_LOT_FILE = f"{DB_FILE_PATH}STOCKS_LOTSIZE.csv"

CASH_FILE_PATH = f"{DATABASE_PATH}CSV"
CASH_FILE_PREFIX = "GFDLNFO_MINUTE_{}_SPOT_"

FUTURE_FILE_PATH = f"{DATABASE_PATH}CSV_FUTURE"
FUTURE_FILE_PREFIX = "GFDLNFO_MINUTE_{}-{}.NFO_{}.csv"

OPTION_FILE_PATH = f"{DATABASE_PATH}CSV_OPT"
OPTION_FILE_NAME = "{}_{}_OPT.csv"
OPTION_DATE_FORMAT = "%d/%m/%Y %H:%M:%S"

CASH = "CASH"
ENTRY = "Entry"
PRE_EXIT = "PRE_EXIT"
EXIT = "Exit"
CHECK_AD = "Check A/D"
DATE = "DateTime"
ONLY_DATE = "only_date"
EXIT_DATE = "exit_date"
EXIT_EXPIRY = "exit_expiry"

STRIKE_DIFF = "StrikeDiff"
LOT_SIZE = "LotSize"
DTE_COL = "DTE_CNT"


class ExitTypes:
    SIGNAL_EXIT = "Signal Change"
    TARGET_EXIT = "Target profit"
    SL_EXIT = "SL"
    EXPIRY_EXIT = "Expiry Exit"
    DELAYED_EXIT = "Delayed Exit"
    DTE_BASED_EXIT = "DTE Based Exit"


class InputValues:
    APPRECIATION = "Appreciation"
    DEPRECIATION = "Depreciation"


class TradeType:
    REDEPLOYMENT = "redeployment"
    ROLLOVER = "rollover"


class InputFileCols:
    START_DATE = "start_date"
    END_DATE = "end_date"
    INSTRUMENT = "instrument"
    SEGMENT = "segment"

    DTE_BASED_TESTING = "dte_based_testing"
    DTE_FROM = "dte_from"

    AD_BASED_ENTRY = "ade_based_entry"
    AD = "appreciation_depreciation"
    AD_PERCENTAGE = "ade_percentage"
    EXPIRY = "expiry"
    STRIKE = "strike"
    DELAYED_EXIT = "Delayed Exit"

    TP_TRADING = "target"
    TP_PERCENTAGE = "target_profit_percentage"
    SL_TRADING = "sl_trading"
    SL_PERCENTAGE = "sl_percentage"

    IS_REDEPLOY = "re_deployment"
    RE_AD_BASED_ENTRY = "re_ade_based_entry"
    RE_AD = "re_appreciation_depreciation"
    RE_AD_PERCENTAGE = "re_ade_percentage"

    IS_NEXT_EXPIRY = "next_expiry_trading"
    NEXT_EXPIRY = "next_expiry"
    NEXT_DTE_FROM = "next_dte_from"

    PREMIUM = "premium_feature"
    VOLUME = "volume_feature"
    VOLUME_MIN = "volume_minutes"
    CAPITAL = "capital"
    RISK = "risk"
    LEVERAGE = "leverage"

    DTE_BASED_EXIT = "dte_based_exit"
    EXIT_DTE_NUMBER = "exit_dte_number"
    EXIT_DTE_TIME = "exit_dte_time"
    ROLLOVER_CANDLE = "rollover_candle"

    HEDGE = "hedge"
    HEDGE_DELAYED_EXIT = "hedge_delayed_exit"
    HEDGE_EXPIRY = "hedge_expiry"
    HEDGE_STRIKE = "hedge_strike"


class InputCols:
    TAG = OutputColumn.SIGNAL.value
    EXIT_CLOSE = OutputColumn.EXIT_PRICE.value
    ENTRY_CLOSE = OutputColumn.ENTRY_PRICE.value
    EXIT_TYPE = OutputColumn.EXIT_TYPE.value
    ENTRY_DT = OutputColumn.ENTRY_DATETIME.value
    EXIT_DT = OutputColumn.EXIT_DATETIME.value
    GREEN = MarketDirection.LONG.value
    RED = MarketDirection.SHORT.value


class CashCols:
    HIGH = "High"
    LOW = "Low"
    OPEN = "Open"
    CLOSE = "Close"
    TICKER = "Ticker"
    VOLUME = "Volume"


class ExpiryCols:
    DATE = "Date"
    SYMBOL = "Symbol"
    EXPIRY_PREFIX = "EXPIRY_"
    DATE_FORMAT = "%m/%d/%Y"


class StrikeDiffCols:
    DATE = "TIMESTAMP"
    SYMBOL = "SYMBOL"
    DATE_FORMAT = "%m/%d/%Y"


class OutputCols:
    TRADE_ID = "Trade Id"
    ROLLOVER_ID = "RollOver Id"
    EXPIRY_DATE = "Expiry date"
    DTE = "DTE"
    TICKER = "tradingSymbol"
    TRACKING_PRICE = "Tracking Price"
    TRACKING_PRICE_TIME = "Tracking Price Time"
    TRACKING_PRICE_REVISED_TIME = "Tracking Price Revised Time"
    AD = "Appreciation / depreciation"
    AD_PERCENT = "Appreciation %"
    AD_PRICE = "Revised Appreciated Price"
    AD_TIME = "Revised Appreciated Time"
    AD_PRICE_LEVEL = "A/D price Level"
    TARGET_PROFIT = "TARGET Profit %"
    SL_PERCENT = "SL %"
    ENTRY_TIME = "Entry Time"
    EXIT_TIME = "Exit Time"
    EXIT_PRICE = "Exit Price"
    NET_POINTS = "Net points"
    EXIT_TYPE = "Exit Type"
    RE_AD = "RE_Appreciation / Depreciation"
    RE_AD_PERCENT = "RE_Appreciation / Depreciation %"
    RE_AD_PRICE = "RE_Appreciation / Depreciation Price"
    RE_AD_PRICE_LEVEL = "RE A/D price Level"
    RE_AD_TIME = "RE_Appreciation / Depreciation Time"
    RE_AD_ENTRY_TIME = "RE_Entry TIme"
    RE_AD_EXIT_PRICE = "RE_Appreciation / Depreciation Exit Price"
    RE_EXIT_TYPE = "RE_Appreciation / Depreciation Exit Type"
    RE_EXIT_TIME = "RE_Appreciation / Depreciation Exit Time"
    MAX_P = "max_price - Between Signal start to signal end time"
    MIN_P = "min_price - Between Signal start to signal end time"
    MAX_AD_P = "max_price - Between P_Price and appreciation/ depreciation price"
    MIN_AD_P = "Min_price - Between P_Price and appreciation/ depreciation price"
    MAX_EXIT_P = "After entry of appreciation/depreciation price till exit - max_price"
    MIN_EXIT_P = "After entry of appreciation/depreciation price till exit - min_price"
    MAX_RE_EXIT_P = "After entry of RE_appreciation/depreciation price till exit - max_price"
    MIN_RE_EXIT_P = "After entry of RE_appreciation/depreciation price till exit - min_price"
    CAPITAL = "Capital"
    QTY = "Qty"
    REVISED_QTY = "Revised Qty"
    RE_QTY = "Qty_Reappreciation"
    REVISED_RE_QTY = "Revised Qty_Reappreciation"
    ROI = "ROI_Original"
    RE_ROI = "ROI_Reappreciation"
    PROBABILITY = "Probability_Original"
    RE_PROBABILITY = "Probability_Reappreciation"
    ENTRY_VOLUME = "Volume at entry"
    EXIT_VOLUME = "Volume at exit"
    VOLUME_MIN = "Volume for x mins"

    H_TICKER = "Hedge Symbol"
    H_ENTRY_PRICE = "Hedge Entry Price"
    H_EXIT_PRICE = "Hedge Exit Price"
    H_P_L = "Hedge Profit/loss"
    H_PRICE_NA = "Hedge price NA"
    H_RE_ENTRY_PRICE = "RE Hedge Entry Price"
    H_RE_EXIT_PRICE = "RE Hedge Exit Price"
    H_RE_P_L = "RE Hedge Profit/loss"
    H_RE_PRICE_NA = "RE Hedge price NA"


exclude = [OutputCols.EXPIRY_DATE, OutputCols.DTE]

RESULT_DICT = {value: None for name, value in vars(OutputCols).items(
) if not callable(value) and not name.startswith('_') and value not in exclude}
EXPIRY_COL = OutputCols.EXPIRY_DATE
EXPIRY_NUMBER_COL = "Expiry Number"

EXPIRY_EXIT_TIME = time(14, 00)
