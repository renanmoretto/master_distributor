from collections import defaultdict
from typing import Union

import pandas as pd
import polars as pl

from ._slice_distributors import TupleDistributionAlias


def _distribution_average_price(
    distribution: list[TupleDistributionAlias],
) -> dict[str, float]:
    """Returns a dict containing the average price for each portfolio"""
    portfolio_totals: dict[str, dict[str, Union[float, int]]] = defaultdict(
        dict[str, Union[float, int]]
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

    # Remove after impl
    return 1.0


def verify_distribution(dist: pd.DataFrame, master: pd.DataFrame) -> bool:
    dist_lazy = pl.DataFrame(dist).lazy()
    master_lazy = pl.from_pandas(master).lazy()

    cols_to_consolidate = ['BROKER', 'TICKER', 'SIDE', 'PRICE']

    master_grpd = (
        master_lazy.group_by(cols_to_consolidate)
        .sum()
        .rename({'QUANTITY': 'QTY_MASTER'})
    )

    dist_grpd = (
        dist_lazy.group_by(cols_to_consolidate)
        .sum()
        .drop(['PORTFOLIO'])
        .rename({'QUANTITY': 'QTY_DIST'})
    )

    df = master_grpd.join(dist_grpd, on=cols_to_consolidate).with_columns(
        (pl.col('QTY_MASTER') - pl.col('QTY_DIST') == 0).alias('OK')
    )
    return df.collect()['OK'].all()


def distribution_as_dataframe(
    distribution: list[TupleDistributionAlias],
) -> pd.DataFrame:
    ...
