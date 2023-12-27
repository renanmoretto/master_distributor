from dataclasses import dataclass
from typing import TypedDict

import pandas as pd
import polars as pl


class TradeID(TypedDict):
    BROKER: str
    TICKER: str
    SIDE: str


class Distribution(TypedDict):
    BROKER: str
    TICKER: str
    SIDE: str
    QUANTITY: int
    PRICE: float
    PORTFOLIO: str


@dataclass(frozen=True, slots=True)
class DistributionData:
    master_lazy: pl.LazyFrame
    allocations_lazy: pl.LazyFrame
    trade_ids: list[TradeID]

    def master_by_trade_id(self, trade_id: TradeID) -> pl.LazyFrame:
        return _filter_lazy_by_trade_id(self.master_lazy, trade_id)

    def allocations_by_trade_id(self, trade_id: TradeID) -> pl.LazyFrame:
        return _filter_lazy_by_trade_id(self.allocations_lazy, trade_id)


def _filter_lazy_by_trade_id(
    lazyframe: pl.LazyFrame,
    trade_id: TradeID,
) -> pl.LazyFrame:
    return lazyframe.filter(
        (pl.col('BROKER') == trade_id['BROKER'])
        & (pl.col('TICKER') == trade_id['TICKER'])
        & (pl.col('SIDE') == trade_id['SIDE'])
    )


def _ensure_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    int_columns: list[str],
    float_columns: list[str],
) -> pd.DataFrame:
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f'missing columns on dataframe {missing_cols}')

    df = df[required_columns].copy()

    for int_col in int_columns:
        df[int_col] = df[int_col].astype(int)  # type: ignore

    for float_col in float_columns:
        df[float_col] = df[float_col].astype(float)  # type: ignore

    return df


def _parse_dataframe_to_lazy(
    df: pd.DataFrame,
    required_columns: list[str],
    int_columns: list[str],
    float_columns: list[str],
    consolidate_by: list[str] | None = None,
) -> pl.LazyFrame:
    df = df.copy()
    df = _ensure_columns(
        df=df,
        required_columns=required_columns,
        int_columns=int_columns,
        float_columns=float_columns,
    )
    df_lazy = pl.from_pandas(df).lazy()
    if consolidate_by:
        df_lazy = df_lazy.group_by(consolidate_by).sum()
    df_lazy = df_lazy.select(required_columns)
    return df_lazy


def _compare_quantitites(master: pl.LazyFrame, allocations: pl.LazyFrame):
    master_gpd = master.group_by(['BROKER', 'TICKER', 'SIDE']).sum().drop(['PRICE'])
    allocations_gpd = (
        allocations.group_by(['BROKER', 'TICKER', 'SIDE']).sum().drop(['PORTFOLIO'])
    )
    if not master_gpd.collect().equals(allocations_gpd.collect()):
        raise ValueError('Quantities for master and allocations are not equal')


def parse_data(
    trades_master: pd.DataFrame,
    allocations: pd.DataFrame,
) -> DistributionData:
    master_lazy: pl.LazyFrame = _parse_dataframe_to_lazy(
        df=trades_master,
        required_columns=['BROKER', 'TICKER', 'SIDE', 'QUANTITY', 'PRICE'],
        int_columns=['QUANTITY'],
        float_columns=['PRICE'],
        consolidate_by=['BROKER', 'TICKER', 'SIDE', 'PRICE'],
    )

    allocations_lazy: pl.LazyFrame = _parse_dataframe_to_lazy(
        df=allocations,
        required_columns=['BROKER', 'TICKER', 'SIDE', 'QUANTITY', 'PORTFOLIO'],
        int_columns=['QUANTITY'],
        float_columns=[],
        consolidate_by=['BROKER', 'TICKER', 'SIDE', 'PORTFOLIO'],
    )

    _compare_quantitites(master_lazy, allocations_lazy)

    trade_ids: list[TradeID] = (
        master_lazy.select(['BROKER', 'TICKER', 'SIDE']).collect().unique().to_dicts()
    )  # type: ignore

    return DistributionData(
        master_lazy=master_lazy,
        allocations_lazy=allocations_lazy,
        trade_ids=trade_ids,
    )
