from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, Literal


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
    appreciation_depreciation: Optional[
        Literal["APPRECIATION", "DEPRECIATION"]
    ] = None
    ade_percentage: Optional[float] = None

    target: Optional[bool] = False
    target_profit_percentage: Optional[float] = None

    sl_trading: Optional[bool] = False
    sl_percentage: Optional[float] = None

    re_deployment: Optional[bool] = False
    re_ade_based_entry: Optional[bool] = None
    re_appreciation_depreciation: Optional[
        Literal["APPRECIATION", "DEPRECIATION"]
    ] = None
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

    @field_validator("opt_buying", "expiry", "strike")
    def validate_options(cls, v, values):
        if values.data.get("segment") == "OPTIONS":
            if v is None:
                raise ValueError(
                    "This field is required when segment is OPTIONS"
                )
        return v

    @field_validator(
        "hedge",
        "hedge_expiry",
        "hedge_strike",
        "hedge_delayed_exit",
    )
    def validate_future(cls, v, values):
        if values.data.get("segment") == "FUTURE":
            if values.data.get("hedge") and (v is None):
                raise ValueError(
                    "This field is required when segment is FUTURE and hedge is True"
                )
        return v

    @field_validator("appreciation_depreciation", "ade_percentage")
    def validate_ade(cls, v, values):
        if values.data.get("ade_based_entry"):
            if v is None:
                raise ValueError(
                    "This field is required when Appreciation/Depreciation based entry is True"
                )
        return v

    @field_validator("target_profit_percentage")
    def validate_target(cls, v, values):
        if values.data.get("target") and v is None:
            raise ValueError("This field is required when TARGET is True")
        return v

    @field_validator("sl_percentage")
    def validate_sl_trading(cls, v, values):
        if values.data.get("sl_trading") and v is None:
            raise ValueError("This field is required when SL Trading is True")
        return v

    @field_validator("re_appreciation_depreciation", "re_ade_percentage")
    def validate_re_deployment(cls, v, values):
        if values.data.get("re_deployment"):
            if values.data.get("re_ade_based_entry") and v is None:
                raise ValueError(
                    "This field is required when Re-deployment and RE_Appreciation/Depreciation based entry are True"
                )
        return v

    @field_validator("dte_from")
    def validate_dte_based_testing(cls, v, values):
        if values.data.get("dte_based_testing") and v is None:
            raise ValueError(
                "This field is required when DTE - Based testing is True"
            )
        return v

    @field_validator("next_dte_from", "next_expiry")
    def validate_next_expiry_trading(cls, v, values):
        if values.data.get("next_expiry_trading") and v is None:
            raise ValueError(
                "This field is required when Next Expiry trading is True"
            )
        return v

    @field_validator("volume_minutes")
    def validate_volume_feature(cls, v, values):
        if values.data.get("volume_feature") and v is None:
            raise ValueError(
                "This field is required when Volume feature is True"
            )
        return v


def validate_trade_management(user_input):
    try:
        config = TradingConfiguration(**user_input)
        return config.model_dump()
    except ValidationError as e:
        print(e)
        raise e
