import pandas as pd
import polars as pl


def verify_distribution(dist: pd.DataFrame, master: pd.DataFrame) -> bool:
    dist_lazy = pl.from_pandas(dist).lazy()
    master_lazy = pl.from_pandas(master).lazy()

    # Ensure types
    dist_lazy = dist_lazy.with_columns(
        (pl.col('QUANTITY').cast(pl.Int32)), (pl.col('PRICE').cast(pl.Float64))
    )
    master_lazy = master_lazy.with_columns(
        (pl.col('QUANTITY').cast(pl.Int32)), (pl.col('PRICE').cast(pl.Float64))
    )

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


def compare_average_price(
    master: pd.DataFrame, distribution: pd.DataFrame
) -> pd.DataFrame:
    master_avg = (
        pl.from_pandas(master)
        .with_columns(VOLUME=(pl.col('QUANTITY') * pl.col('PRICE')))
        .group_by(['BROKER', 'TICKER', 'SIDE'], maintain_order=True)
        .agg((pl.col('VOLUME').sum() / pl.col('QUANTITY').sum()).alias('AVG_PRICE'))
    )

    comp = (
        pl.from_pandas(distribution)
        .with_columns(VOLUME=(pl.col('QUANTITY') * pl.col('PRICE')))
        .group_by(['BROKER', 'TICKER', 'SIDE', 'PORTFOLIO'], maintain_order=True)
        .agg((pl.col('VOLUME').sum() / pl.col('QUANTITY').sum()).alias('AVG_PRICE'))
    )

    comp = comp.join(master_avg, on=['BROKER', 'TICKER', 'SIDE'], suffix='_MASTER')
    comp = comp.with_columns(
        (pl.col('AVG_PRICE') - pl.col('AVG_PRICE_MASTER'))
        .round(2)
        .alias('NOMINAL_DIFF')
    )
    comp = comp.with_columns(
        (pl.col('NOMINAL_DIFF') / pl.col('AVG_PRICE'))
        .mul(100)
        .round(2)
        .alias('PP_DIFF')
    )

    comp = comp.sort(['BROKER', 'TICKER', 'SIDE', 'PP_DIFF'])

    return pd.DataFrame(comp, columns=comp.columns)  # type: ignore
