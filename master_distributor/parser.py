from dataclasses import dataclass
from typing import TypedDict

import pandas as pd
import polars as pl

from ._types import (
    TupleTradesAlias,
    TupleAllocationAlias,
    TradesRowsAlias,
    AllocationsRowsAlias,
)


class Slice(TypedDict):
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


@dataclass
class DistributionData:
    master_lazy: pl.LazyFrame
    allocations_lazy: pl.LazyFrame
    slices: list[Slice]

    def __post_init__(self):
        master_slices: list[pl.LazyFrame] = []
        allocations_slices: list[pl.LazyFrame] = []
        for slice in self.slices:
            master_slice = _filter_lazy_by_slice(self.master_lazy, slice)
            allocation_slice = _filter_lazy_by_slice(self.allocations_lazy, slice)
            master_slices.append(master_slice)
            allocations_slices.append(allocation_slice)

        self.master_slices: list[pl.LazyFrame] = master_slices
        self.allocations_slices: list[pl.LazyFrame] = allocations_slices

    def items(
        self, raw: bool = False
    ) -> (
        list[tuple[pl.LazyFrame, pl.LazyFrame, Slice]]
        | list[tuple[TradesRowsAlias, AllocationsRowsAlias, Slice]]
    ):
        if not raw:
            return [
                (m, a, s)
                for (m, a, s) in zip(
                    self.master_slices, self.allocations_slices, self.slices
                )
            ]

        raw_data: list[tuple[TradesRowsAlias, AllocationsRowsAlias, Slice]] = []
        for master_lazy_slice, allocations_lazy_slice, slice in zip(
            self.master_slices, self.allocations_slices, self.slices
        ):
            master_slice_rows: list[TupleTradesAlias] = master_lazy_slice.collect()[
                ['QUANTITY', 'PRICE']
            ].rows()  # type: ignore
            allocations_slice_rows: list[
                TupleAllocationAlias
            ] = allocations_lazy_slice.collect()[['PORTFOLIO', 'QUANTITY']].rows()  # type: ignore
            raw_data.append((master_slice_rows, allocations_slice_rows, slice))

        return raw_data


def _filter_lazy_by_slice(
    lazyframe: pl.LazyFrame,
    slice: Slice,
) -> pl.LazyFrame:
    return lazyframe.filter(
        (pl.col('BROKER') == slice['BROKER'])
        & (pl.col('TICKER') == slice['TICKER'])
        & (pl.col('SIDE') == slice['SIDE'])
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
        df_lazy = df_lazy.group_by(consolidate_by, maintain_order=True).sum()
    df_lazy = df_lazy.select(required_columns)
    return df_lazy


def _compare_quantitites(master: pl.LazyFrame, allocations: pl.LazyFrame):
    master_gpd = (
        master.group_by(['BROKER', 'TICKER', 'SIDE'], maintain_order=True)
        .sum()
        .drop(['PRICE'])
        .sort(['BROKER', 'TICKER', 'SIDE'])
    )
    allocations_gpd = (
        allocations.group_by(['BROKER', 'TICKER', 'SIDE'], maintain_order=True)
        .sum()
        .drop(['PORTFOLIO'])
        .sort(['BROKER', 'TICKER', 'SIDE'])
    )
    if not master_gpd.collect().equals(allocations_gpd.collect()):
        raise ValueError('Quantities for master and allocations are not equal')


def parse_data(
    master: pd.DataFrame,
    allocations: pd.DataFrame,
) -> DistributionData:
    master_lazy: pl.LazyFrame = _parse_dataframe_to_lazy(
        df=master,
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

    slices: list[Slice] = (
        master_lazy.select(['BROKER', 'TICKER', 'SIDE']).collect().unique().to_dicts()
    )  # type: ignore

    return DistributionData(
        master_lazy=master_lazy,
        allocations_lazy=allocations_lazy,
        slices=slices,
    )
