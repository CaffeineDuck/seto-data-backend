# pyright: reportArgumentType=false, reportIncompatibleVariableOverride=false, reportAssignmentType=false

import typing as ty

import pandas as pd
import pandera as pa
import pandera.typing as pty
from dagster import Output, asset, graph_asset, op

from .download_report import download_customs_report

type Month = ty.Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


class RawTradeBalanceSchema(pa.DataFrameModel):
    export: pty.Series[float] = pa.Field(alias="Exports_Value")
    import_: pty.Series[float] = pa.Field(alias="Imports_Value")
    trade_balance: pty.Series[float] = pa.Field(alias="Trade_Balance")
    import_revenue: pty.Series[float] = pa.Field(alias="Imports_Revenue")

    class Config:
        coerce = True


class TradeBalanceSchema(RawTradeBalanceSchema):
    year: pty.Series[int]
    month: pty.Series[Month]

    class Config:
        coerce = True


@op
@pa.check_types
def get_trade_balance(
    year: int,
    month: Month,
    report_path: str,
) -> Output[pty.DataFrame[TradeBalanceSchema]]:
    raw_trade_balance_df = pd.read_excel(
        report_path, sheet_name="Trade_Balance_Chapter"
    )

    trade_balance_df = raw_trade_balance_df.dropna(subset=["Export", "Import"])
    trade_balance_df.assign(year=year, month=month)

    validated_df = TradeBalanceSchema.validate(raw_trade_balance_df)
    return Output(ty.cast(pty.DataFrame[TradeBalanceSchema], validated_df))


@asset(deps=(download_customs_report,))
def parse_customs_report(): ...
