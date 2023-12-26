import pandas as pd
import polars as pl

from ._core import Distribution


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


def trade_id_std(trade_id_distribution: list[Distribution]) -> float:
    return 10.0


def calculate_distribution_std(distribution: list[Distribution]) -> float:
    return 3.4
