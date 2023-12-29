from collections import defaultdict
from typing import Union

import pandas as pd

from master_distributor.parser import Slice
from master_distributor._types import (
    TupleDistributionAlias,
    TupleFullDistributionAlias,
)


def _distribution_average_price(
    distribution: list[TupleDistributionAlias],
) -> dict[str, float]:
    """Returns a dict containing the average price for each portfolio"""
    portfolio_totals: dict[str, dict[str, Union[float, int]]] = defaultdict(
        lambda: {'QUANTITY': 0, 'VOLUME': 0}
    )
    for qty, price, portfolio in distribution:
        portfolio_totals[portfolio]['QUANTITY'] += qty
        portfolio_totals[portfolio]['VOLUME'] += qty * price

    average_price_per_portfolio: dict[str, float] = defaultdict(float)
    for _, _, portfolio in distribution:
        total_volume = portfolio_totals[portfolio]['VOLUME']
        total_qty = portfolio_totals[portfolio]['QUANTITY']
        average_price_per_portfolio[portfolio] = total_volume / total_qty
    return average_price_per_portfolio


def distribution_max_deviation(distribution: list[TupleDistributionAlias]) -> float:
    _dist_average_price = _distribution_average_price(distribution)
    values = list(_dist_average_price.values())
    max_value = max(values)
    min_value = min(values)
    return abs(max_value / min_value - 1)


def add_slice_data_to_distribution(
    slice: Slice,
    slice_distribution: list[TupleDistributionAlias],
) -> list[TupleFullDistributionAlias]:
    broker = slice['BROKER']
    ticker = slice['TICKER']
    side = slice['SIDE']

    slice_dist_list: list[TupleFullDistributionAlias] = []
    for qty, price, portfolio in slice_distribution:
        _slice_tup: TupleFullDistributionAlias = (
            broker,
            ticker,
            side,
            qty,
            price,
            portfolio,
        )
        slice_dist_list.append(_slice_tup)
    return slice_dist_list


def distribution_as_dataframe(
    distribution: list[TupleFullDistributionAlias],
    consolidate: bool = True,
) -> pd.DataFrame:
    cols = ['BROKER', 'TICKER', 'SIDE', 'QUANTITY', 'PRICE', 'PORTFOLIO']
    dist_df = pd.DataFrame(distribution)
    dist_df.columns = cols

    if consolidate:
        dist_df = (
            dist_df.groupby(['BROKER', 'TICKER', 'SIDE', 'PRICE', 'PORTFOLIO'])[  # type: ignore
                'QUANTITY'
            ]
            .sum()
            .reset_index()
        )  # type: ignore
        dist_df = dist_df[cols]
    return dist_df.copy()
