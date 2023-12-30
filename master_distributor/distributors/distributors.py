import time

from typing import Protocol, Callable

import pandas as pd

from master_distributor.parser import parse_data
from master_distributor._types import (
    TupleDistributionAlias,
    FuncDistributeAlias,
    TupleFullDistributionAlias,
    TradesRowsAlias,
    AllocationsRowsAlias,
)
from ._utils import (
    distribution_max_deviation,
    distribution_as_dataframe,
    add_slice_data_to_distribution,
)
from ._slice_distributors import (
    distribute_slice_weighted,
    distribute_slice_random,
)


class Distributor(Protocol):
    _func_distribute_slice: Callable  # type: ignore

    def distribute(
        self,
        trades: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        ...


class WeightedDistributor(Distributor):
    def __init__(self, verbose: bool = False):
        self._verbose = verbose
        self._func_distribute_slice: FuncDistributeAlias = distribute_slice_weighted

    def distribute(
        self,
        trades: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        return _single_distributor(
            trades=trades,
            allocations=allocations,
            func_distribute_slice=self._func_distribute_slice,
            verbose=self._verbose,
        )


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

    def distribute(
        self,
        trades: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        return _loop_distributor(
            trades=trades,
            allocations=allocations,
            func_distribute_slice=self._func_distribute_slice,
            shuffle_orders=self._shuffle_orders,
            std_break=self._std_break,
            max_its=self._max_its,
            verbose=self._verbose,
        )


def _single_distributor(
    trades: pd.DataFrame,
    allocations: pd.DataFrame,
    func_distribute_slice: FuncDistributeAlias,
    verbose: bool,
) -> pd.DataFrame:
    data = parse_data(trades, allocations)

    distribution: list[TupleFullDistributionAlias] = []
    for master_slice_rows, allocations_slice_rows, slice in data.items_raw():
        slice_distribution = func_distribute_slice(
            master_slice_rows,  # type: ignore
            allocations_slice_rows,  # type: ignore
        )
        distribution += add_slice_data_to_distribution(slice, slice_distribution)
    return distribution_as_dataframe(distribution)


# ------------------------ Funcs for loop distributors ----------------------- #


def _loop_get_best_distribution(
    trades_slice_rows: TradesRowsAlias,
    allocations_slice_rows: AllocationsRowsAlias,
    func_distribute_slice: FuncDistributeAlias,
    max_its: int,
    std_break: float,
    verbose: bool = False,
) -> list[TupleDistributionAlias]:
    best_distribution: list[TupleDistributionAlias] = []
    best_std = float('inf')
    dist_std = float('inf')
    it = 0

    start = time.time()
    for it in range(1, max_its + 1):
        slice_distribution = func_distribute_slice(
            trades_slice_rows,  # type: ignore
            allocations_slice_rows,  # type: ignore
        )
        dist_std = distribution_max_deviation(slice_distribution)
        if dist_std < std_break:
            best_std = dist_std
            best_distribution = slice_distribution
            break
        if dist_std < best_std:
            best_std = dist_std
            best_distribution = slice_distribution

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
            f'it/s: {vel}, best_std={best_std:,.2%}',
        )
    return best_distribution


def _loop_distributor(
    trades: pd.DataFrame,
    allocations: pd.DataFrame,
    func_distribute_slice: FuncDistributeAlias,
    shuffle_orders: bool,
    std_break: float | None,
    max_its: int,
    verbose: bool,
) -> pd.DataFrame:
    std_break = std_break if std_break else 0
    data = parse_data(master=trades, allocations=allocations)

    distribution: list[TupleFullDistributionAlias] = []

    for master_slice_rows, allocations_slice_rows, slice in data.items_raw():
        # best_distribution: list[TupleDistributionAlias] = []
        # best_std = float('inf')
        # dist_std = float('inf')
        # it = 0
        # while it < max_its and dist_std >= std_break:
        #     _slice_distribution = func_distribute_slice(
        #         master_slice_rows,  # type: ignore
        #         allocations_slice_rows,  # type: ignore
        #     )
        #     dist_std = distribution_max_deviation(_slice_distribution)
        #     if dist_std < best_std:
        #         best_std = dist_std
        #         best_distribution = _slice_distribution
        #     it += 1
        best_distribution = _loop_get_best_distribution(
            trades_slice_rows=master_slice_rows,
            allocations_slice_rows=allocations_slice_rows,
            func_distribute_slice=func_distribute_slice,
            max_its=max_its,
            std_break=std_break,
            verbose=verbose,
        )

        distribution += add_slice_data_to_distribution(slice, best_distribution)
    return distribution_as_dataframe(distribution)


def _parallel_loop_distributor(
    trades: pd.DataFrame,
    allocations: pd.DataFrame,
    func_distribute_slice: FuncDistributeAlias,
    n_jobs: int,
    shuffle_orders: bool,
    std_break: float | None,
    max_its: int,
    verbose: bool,
) -> pd.DataFrame:
    std_break = std_break if std_break else 0
    data = parse_data(master=trades, allocations=allocations)

    distribution: list[TupleFullDistributionAlias] = []

    for master_slice_rows, allocations_slice_rows, slice in data.items(raw=True):
        start = time.time()

        best_distribution: list[TupleDistributionAlias] = []
        best_std = float('inf')
        dist_std = float('inf')
        it = 0
        while it < max_its and dist_std >= std_break:
            _slice_distribution = func_distribute_slice(
                master_slice_rows,  # type: ignore
                allocations_slice_rows,  # type: ignore
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
                f'it/s: {vel}, best_std={best_std:,.2%}',
            )

        distribution += add_slice_data_to_distribution(slice, best_distribution)
    return distribution_as_dataframe(distribution)
