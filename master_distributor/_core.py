import random
from typing import TypedDict, Callable, Any

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


def _filter_lazy_by_trade_id(
    lazyframe: pl.LazyFrame,
    trade_id: TradeID,
) -> pl.LazyFrame:
    return lazyframe.filter(
        (pl.col('BROKER') == trade_id['BROKER'])
        & (pl.col('TICKER') == trade_id['TICKER'])
        & (pl.col('SIDE') == trade_id['SIDE'])
    )


def _distribute_trade_id(
    master_trade_id: list[tuple[str, str, str, int, float]],
    allocations_trade_id: list[tuple[str, str, str, int, str]],
    qty_calculator: Callable[[dict[str, Any]], int],
    shuffle_orders: bool = True,
) -> list[Distribution]:
    orders = list(master_trade_id)
    if shuffle_orders:
        random.shuffle(orders)

    _qts = (tup[3] for tup in allocations_trade_id)
    _portfolios = (tup[4] for tup in allocations_trade_id)
    vertical_qty_per_portfolio = {
        portfolio: qty for portfolio, qty in zip(_portfolios, _qts) if qty != 0
    }
    remaining_vertical_qty_per_portfolio = vertical_qty_per_portfolio

    portfolios = tuple(remaining_vertical_qty_per_portfolio.keys())

    distribution_trade_id: list[Distribution] = []

    for order in orders:
        broker = order[0]
        ticker = order[1]
        side = order[2]
        quantity = order[3]
        price = order[4]

        remaining_order_qty = quantity

        while remaining_order_qty > 0:
            for portfolio in portfolios:
                if remaining_order_qty == 0:
                    break
                max_qty_portfolio = remaining_vertical_qty_per_portfolio[portfolio]
                if max_qty_portfolio == 0:
                    continue

                qty = qty_calculator(locals())

                remaining_order_qty -= qty
                remaining_vertical_qty_per_portfolio[portfolio] -= qty

                distribution_trade_id.append(
                    Distribution(
                        BROKER=broker,
                        TICKER=ticker,
                        SIDE=side,
                        QUANTITY=qty,
                        PRICE=price,
                        PORTFOLIO=portfolio,
                    )
                )
    return distribution_trade_id


def _distribute_trade_ids(
    master_lazy: pl.LazyFrame,
    allocations_lazy: pl.LazyFrame,
    trade_ids: list[TradeID],
    qty_calculator: Callable[[dict[str, Any]], int],
    shuffle_orders: bool = True,
) -> list[Distribution]:
    distributions: list[Distribution] = []
    for trade_id in trade_ids:
        master_trade_id = _filter_lazy_by_trade_id(master_lazy, trade_id)
        allocations_trade_id = _filter_lazy_by_trade_id(allocations_lazy, trade_id)

        trade_id_distribution = _distribute_trade_id(
            master_trade_id=master_trade_id.collect().rows(),  # type: ignore
            allocations_trade_id=allocations_trade_id.collect().rows(),  # type: ignore
            qty_calculator=qty_calculator,
            shuffle_orders=shuffle_orders,
        )  # type: ignore

        distributions += trade_id_distribution
    return distributions


def default_distribute(
    trades_master: pd.DataFrame,
    allocations: pd.DataFrame,
    qty_calculator: Callable[[dict[str, Any]], int],
    shuffle_orders: bool = True,
) -> pd.DataFrame:
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

    distribution = _distribute_trade_ids(
        master_lazy=master_lazy,
        allocations_lazy=allocations_lazy,
        trade_ids=trade_ids,
        qty_calculator=qty_calculator,
        shuffle_orders=shuffle_orders,
    )

    return pd.DataFrame(distribution)
