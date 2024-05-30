from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, Literal, Union


class TradingConfiguration(BaseModel):
    segment: Literal["CASH", "FUTURE", "OPTIONS"]

    opt_buying: Optional[Literal["YES", "NO"]] = None
    expiry: Optional[int] = None
    strike: Optional[int] = None

    hedge: Optional[bool] = None
    hedge_expiry: Optional[int] = None
    hedge_strike: Optional[int] = None
    hedge_delayed_exit: Optional[bool] = None

    ade_based_entry: Optional[bool] = False
    appreciation_depreciation: Optional[Literal["APPRECIATION", "DEPRECIATION"]] = None
    ade_percentage: Optional[float] = None

    target: Optional[bool] = False
    target_profit_percentage: Optional[float] = None

    sl_trading: Optional[bool] = False
    sl_percentage: Optional[float] = None

    re_deployment: Optional[bool] = False
    re_ade_based_entry: Optional[bool] = None
    re_appreciation_depreciation: Optional[Literal["APPRECIATION", "DEPRECIATION"]] = (
        None
    )
    re_ade_percentage: Optional[float] = None

    dte_based_testing: Optional[bool] = False
    dte_from: Optional[int] = None

    next_expiry_trading: Optional[bool] = False
    next_dte_from: Optional[int] = None
    next_expiry: Optional[int] = None

    premium_feature: Optional[bool] = False
    volume_feature: Optional[bool] = False
    volume_minutes: Optional[int] = None

    capital: float = 100000000
    risk: float = Field(..., gt=0, lt=1)
    leverage: int = 2

    @field_validator("opt_buying", "expiry", "strike", pre=True, always=True)
    def validate_options(cls, v, values):
        if values.get("segment") == "OPTIONS":
            if v is None:
                raise ValueError("This field is required when segment is OPTIONS")
        return v

    @field_validator(
        "hedge",
        "hedge_expiry",
        "hedge_strike",
        "hedge_delayed_exit",
        pre=True,
        always=True,
    )
    def validate_future(cls, v, values):
        if values.get("segment") == "FUTURE":
            if values.get("hedge") and (v is None):
                raise ValueError(
                    "This field is required when segment is FUTURE and hedge is True"
                )
        return v

    @field_validator(
        "appreciation_depreciation", "ade_percentage", pre=True, always=True
    )
    def validate_ade(cls, v, values):
        if values.get("ade_based_entry"):
            if v is None:
                raise ValueError(
                    "This field is required when Appreciation/Depreciation based entry is True"
                )
        return v

    @field_validator("target_profit_percentage", pre=True, always=True)
    def validate_target(cls, v, values):
        if values.get("target") and v is None:
            raise ValueError("This field is required when TARGET is True")
        return v

    @field_validator("sl_percentage", pre=True, always=True)
    def validate_sl_trading(cls, v, values):
        if values.get("sl_trading") and v is None:
            raise ValueError("This field is required when SL Trading is True")
        return v

    @field_validator(
        "re_appreciation_depreciation", "re_ade_percentage", pre=True, always=True
    )
    def validate_re_deployment(cls, v, values):
        if values.get("re_deployment"):
            if values.get("re_ade_based_entry") and v is None:
                raise ValueError(
                    "This field is required when Re-deployment and RE_Appreciation/Depreciation based entry are True"
                )
        return v

    @field_validator("dte_from", pre=True, always=True)
    def validate_dte_based_testing(cls, v, values):
        if values.get("dte_based_testing") and v is None:
            raise ValueError("This field is required when DTE - Based testing is True")
        return v

    @field_validator("next_dte_from", "next_expiry", pre=True, always=True)
    def validate_next_expiry_trading(cls, v, values):
        if values.get("next_expiry_trading") and v is None:
            raise ValueError("This field is required when Next Expiry trading is True")
        return v

    @field_validator("volume_minutes", pre=True, always=True)
    def validate_volume_feature(cls, v, values):
        if values.get("volume_feature") and v is None:
            raise ValueError("This field is required when Volume feature is True")
        return v


try:
    config = TradingConfiguration(
        segment="OPTIONS",
        opt_buying="YES",
        expiry=1,
        strike=1,
        hedge=False,
        hedge_expiry=2,
        hedge_strike=1,
        hedge_delayed_exit=False,
        ade_based_entry=True,
        appreciation_depreciation="APPRECIATION",
        ade_percentage=0.1,
        target=True,
        target_profit_percentage=0.03,
        sl_trading=True,
        sl_percentage=0.03,
        re_deployment=True,
        re_ade_based_entry=True,
        re_appreciation_depreciation="DEPRECIATION",
        re_ade_percentage=0.02,
        dte_based_testing=True,
        dte_from=7,
        next_expiry_trading=True,
        next_dte_from=1,
        next_expiry=3,
        premium_feature=True,
        volume_feature=True,
        volume_minutes=5,
        capital=100000000,
        risk=0.04,
        leverage=2,
    )
    print(config)
except ValidationError as e:
    print(e)
