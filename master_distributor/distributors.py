import random

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd

from ._core import default_distribute


Kwargs = dict[str, Any]


class Distributor(ABC):
    @staticmethod
    @abstractmethod
    def _qty_calculator(kw_locals: dict[str, Any]) -> int:
        pass

    @abstractmethod
    def distribute(
        self, trades_master: pd.DataFrame, allocations: pd.DataFrame
    ) -> pd.DataFrame:
        pass


class RandomDistributor(Distributor):
    def __init__(
        self,
        shuffle_orders: bool = False,
        loop: bool = False,
        std_break: Optional[float] = None,
        else_return_best: bool = True,
        max_its: int = 50_000,
        verbose: bool = False,
    ):
        self._shuffle_orders = shuffle_orders
        self._loop = loop

    @staticmethod
    def _qty_calculator(kw_locals: dict[str, Any]) -> int:
        remaining_order_qty = int(kw_locals.get('remaining_order_qty'))  # type: ignore
        max_qty_portfolio = int(kw_locals.get('max_qty_portfolio'))  # type: ignore
        max_qty_random = min(max_qty_portfolio, remaining_order_qty)
        return random.randint(1, max_qty_random)

    def distribute(
        self,
        trades_master: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        return default_distribute(
            trades_master=trades_master,
            allocations=allocations,
            qty_calculator=self._qty_calculator,
            shuffle_orders=self._shuffle_orders,
        )


class UnitDistributor(Distributor):
    pass


class BestDistributor(Distributor):
    pass
