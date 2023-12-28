import time

from typing import Protocol, Callable, Any

import pandas as pd

from master_distributor.parser import parse_data
from ._utils import (
    distribution_max_deviation,
    distribution_as_dataframe,
    add_slice_data_to_distribution,
)
from ._slice_distributors import distribute_slice_random
from ._types import (
    TupleTradesAlias,
    TupleAllocationAlias,
    TupleDistributionAlias,
    FuncDistributeAlias,
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

        self._func_distribute_slice: FuncDistributeAlias = distribute_slice_random

    def distribute(  # type: ignore
        self,
        trades: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        return _loop_distributor(
            trades=trades,
            allocations=allocations,
            distribute_slice=self._func_distribute_slice,
            shuffle_orders=self._shuffle_orders,
            std_break=self._std_break,
            max_its=self._max_its,
            verbose=self._verbose,
        )


def _loop_distributor(
    trades: pd.DataFrame,
    allocations: pd.DataFrame,
    distribute_slice: FuncDistributeAlias,
    shuffle_orders: bool,
    std_break: float | None,
    max_its: int,
    verbose: bool,
) -> pd.DataFrame:
    std_break = std_break if std_break else 0
    data = parse_data(master=trades, allocations=allocations)

    distribution: list[tuple[str, str, str, int, float, str]] = []
    for master_slice, allocations_slice, slice_data in zip(
        data.master_slices, data.allocations_slices, data.slices
    ):
        master_slice_rows: list[TupleTradesAlias] = master_slice.collect()[
            ['QUANTITY', 'PRICE']
        ].rows()  # type: ignore
        allocations_slice_rows: list[
            TupleAllocationAlias
        ] = allocations_slice.collect()[['PORTFOLIO', 'QUANTITY']].rows()  # type: ignore

        start = time.time()

        best_distribution: list[TupleDistributionAlias] = []
        best_std = 1_000
        dist_std = 1_000
        it = 0
        while it < max_its and dist_std >= std_break:
            _slice_distribution = distribute_slice(
                master_slice_rows, allocations_slice_rows
            )
            dist_std = distribution_max_deviation(_slice_distribution)
            if dist_std < best_std:
                best_std = dist_std
                best_distribution = _slice_distribution
            it += 1

        end = time.time()
        if start != end:
            total_time = end - start
            vel = round(it / total_time, 4)
        else:
            total_time = 0
            vel = 0

        if verbose:
            print(
                f'{it=}',
                round(total_time, 4),
                f"velocidade it/s: {vel}, f'melhor_desvio={best_std:,.2%}",
            )

        distribution += add_slice_data_to_distribution(slice_data, best_distribution)
    return distribution_as_dataframe(distribution)
