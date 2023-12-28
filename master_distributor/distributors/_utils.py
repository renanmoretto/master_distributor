# TODO
import pandas as pd
import polars as pl

from ._slice_distributors import TupleDistributionAlias


def distribution_average_price(
    distribution: list[TupleDistributionAlias],
) -> dict[str, float]:
    """Returns a dict containing the average price for each portfolio"""
    ...


def distribution_max_deviation(distribution: list[TupleDistributionAlias]) -> float:
    _dist_average_price = distribution_average_price(distribution)

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
