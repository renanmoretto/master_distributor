import time

from typing import Protocol, Callable, Any

import pandas as pd

from master_distributor.parser import parse_data
from ._utils import distribution_max_deviation, distribution_as_dataframe
from ._slice_distributors import (
    TupleTradesAlias,
    TupleAllocationAlias,
    TupleDistributionAlias,
    distribute_slice_random,
)


class Distributor(Protocol):
    _func_distribute_slice: Callable  # type: ignore

    def distribute(self) -> pd.DataFrame:
        ...


class RandomLoopDistributor(Distributor):
    def __init__(
        self,
        shuffle_orders: bool = False,
        std_break: float | None = None,
        else_return_best: bool = True,
        max_its: int = 1_000,
        verbose: bool = False,
    ):
        self._shuffle_orders = shuffle_orders
        self._std_break = std_break
        self._else_return_best = else_return_best
        self._max_its = max_its
        self._verbose = verbose

        self._func_distribute_slice = distribute_slice_random

    def distribute(  # type: ignore
        self,
        trades: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        ...


def _loop_distributor(
    trades: pd.DataFrame,
    allocations: pd.DataFrame,
    distribute_slice: Callable[
        [list[TupleTradesAlias], list[TupleAllocationAlias]],
        list[TupleDistributionAlias],
    ],
    shuffle_orders: bool,
    std_break: float | None,
    else_return_best: bool,
    max_its: int,
    verbose: bool,
) -> pd.DataFrame:
    std_break = std_break if std_break else 0
    data = parse_data(master=trades, allocations=allocations)

    distribution = []
    for master_slice, allocations_slice in zip(
        data.master_slices, data.allocations_slices
    ):
        master_slice_rows: list[TupleTradesAlias] = master_slice.collect().rows()  # type: ignore
        allocations_slice_rows: list[
            TupleAllocationAlias
        ] = allocations_slice.collect().rows()  # type: ignore

        if verbose:
            start = time.time()

        best_distribution = []
        best_std = 0
        it = 0
        dist_std = 1_000
        while it < max_its and dist_std <= std_break:
            slice_distribution = distribute_slice(
                master_slice_rows, allocations_slice_rows
            )
            dist_std = distribution_max_deviation(slice_distribution)
            if dist_std < best_std:
                best_std = dist_std
                best_distribution = dist_std
            it += 1

        if verbose:
            end = time.time()
            total_time = end - start  # type: ignore
            print(
                f'{it=}',
                round(total_time, 4),
                f"velocidade it/s: {round(it/total_time,4)}, f'melhor_desvio={best_std:,.2%}",
            )
        distribution.append(best_distribution)
    return distribution_as_dataframe(distribution)
