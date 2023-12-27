import time

from typing import Protocol, Callable

import pandas as pd

from ._core import distribute_slice_random


class Distributor(Protocol):
    _func_distribute_slice: Callable

    def distribute(self) -> pd.DataFrame:
        ...


class RandomDistributor(Distributor):
    def __init__(
        self,
        shuffle_orders: bool = False,
        loop: bool = False,
        std_break: float | None = None,
        else_return_best: bool = True,
        max_its: int = 1_000,
        verbose: bool = False,
    ):
        self._shuffle_orders = shuffle_orders
        self._loop = loop
        self._std_break = std_break
        self._else_return_best = else_return_best
        self._max_its = max_its
        self._verbose = verbose

        self._func_distribute_slice = distribute_slice_random

    def distribute(self, master: int) -> pd.DataFrame:
        # TODO
        ...
