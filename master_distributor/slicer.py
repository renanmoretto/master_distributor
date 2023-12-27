import polars as pl


def slice_by(by: str | list[str]):
    if isinstance(by, str):
        bys = [by]
    elif isinstance(by, list):
        bys = by.copy()
    else:
        raise TypeError(f'by must be str or list')
