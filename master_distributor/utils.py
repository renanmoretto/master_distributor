import pandas as pd
import polars as pl


def verify_distribution(dist: pd.DataFrame, master: pd.DataFrame) -> bool:
    dist_lazy = pl.DataFrame(dist).lazy()
    master_lazy = pl.from_pandas(master).lazy()

    master_grpd = (
        master_lazy.group_by(['BROKER', 'TICKER', 'SIDE'])
        .sum()
        .drop(columns='PRICE')
        .rename({'QUANTITY': 'QTY_MASTER'})
    )

    dist_grpd = (
        dist_lazy.group_by(['BROKER', 'TICKER', 'SIDE'])
        .sum()
        .drop(columns=['PRICE', 'FUNDO'])
        .rename({'QUANTITY': 'QTY_DIST'})
    )

    df = master_grpd.join(dist_grpd, on=['BROKER', 'TICKER', 'SIDE']).with_columns(
        (pl.col('QTY_MASTER') - pl.col('QTY_DIST') == 0).alias('OK')
    )
    return df.collect()['OK'].all()
