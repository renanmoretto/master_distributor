import random
import time

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from ._core import (
    MasterRowsAlias,
    AllocationRowsAlias,
    Distribution,
    parse_data,
    distribute_trade_id,
)


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

    @staticmethod
    def _qty_calculator(kw_locals: dict[str, Any]) -> int:
        remaining_order_qty = int(kw_locals.get('remaining_order_qty'))  # type: ignore
        max_qty_portfolio = int(kw_locals.get('max_qty_portfolio'))  # type: ignore
        max_qty_random = min(max_qty_portfolio, remaining_order_qty)
        return random.randint(1, max_qty_random)

    def _distribute_trade_id(
        self,
        master_rows: list[MasterRowsAlias],
        allocations_rows: list[AllocationRowsAlias],
    ) -> list[Distribution]:
        if not self._loop:
            return distribute_trade_id(
                master_trade_id=master_rows,  # type: ignore
                allocations_trade_id=allocations_rows,  # type: ignore
                qty_calculator=self._qty_calculator,
                shuffle_orders=self._shuffle_orders,
            )

        if not self._std_break:
            std_break = 0
        else:
            std_break = self._std_break

        best_std = 10.0
        best_distribution = []
        start = time.time()
        it = 0
        while it < self._max_its and best_std > std_break:
            trade_id_distribution = distribute_trade_id(
                master_trade_id=master_rows,  # type: ignore
                allocations_trade_id=allocations_rows,  # type: ignore
                qty_calculator=self._qty_calculator,
                shuffle_orders=self._shuffle_orders,
            )
            distribution_std = _calculate_trade_id_str(trade_id_distribution)

            if distribution_std < best_std:
                best_std = distribution_std
                best_distribution = trade_id_distribution

            it += 1
        end = time.time()
        total_time = end - start

        if self._verbose:
            print(
                f'{it=}',
                round(total_time, 2),
                f'it/s: {round(it/total_time,2)}',
                f'{best_std=:,.2%}',
            )
            print(_calculate_trade_id_average_prices(best_distribution))

        return best_distribution

    def distribute(
        self,
        trades_master: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        distribution_data = parse_data(trades_master, allocations)

        distributions: list[Distribution] = []
        for trade_id in distribution_data.trade_ids:
            master_trade_id = distribution_data.master_by_trade_id(trade_id)
            allocations_trade_id = distribution_data.allocations_by_trade_id(trade_id)

            master_rows = master_trade_id.collect().rows()
            allocations_rows = allocations_trade_id.collect().rows()

            trade_id_distribution = self._distribute_trade_id(
                master_rows,  # type: ignore
                allocations_rows,  # type: ignore
            )

            distributions += trade_id_distribution

        return pd.DataFrame(distributions)


class UnitDistributor(Distributor):
    def __init__(
        self,
        shuffle_orders: bool = True,
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

        self._random_distributor = RandomDistributor(
            shuffle_orders=shuffle_orders,
            loop=loop,
            std_break=std_break,
            else_return_best=else_return_best,
            max_its=max_its,
            verbose=verbose,
        )

    @staticmethod
    def _qty_calculator(kw_locals: dict[str, Any]) -> int:
        return 1

    def distribute(
        self,
        trades_master: pd.DataFrame,
        allocations: pd.DataFrame,
    ) -> pd.DataFrame:
        random_distributor = self._random_distributor
        random_distributor._qty_calculator = self._qty_calculator  # type: ignore
        return random_distributor.distribute(trades_master, allocations)


class BestDistributor(Distributor):
    pass


def _calculate_trade_id_average_prices(
    trade_id_distribution: list[Distribution],
) -> dict[str, float]:
    aux_ports: dict[str, Any] = {}
    for dist in trade_id_distribution:
        port = dist['PORTFOLIO']
        volume = dist['QUANTITY'] * dist['PRICE']
        qty = dist['QUANTITY']
        dict_port = {
            'volume': volume,
            'qty': qty,
        }
        if port not in aux_ports.keys():
            aux_ports.update({port: dict_port})
        else:
            aux_ports[port]['volume'] += volume
            aux_ports[port]['qty'] += qty

    average_price_ports: dict[str, float] = {}
    for port in aux_ports.keys():
        average_price = aux_ports[port]['volume'] / aux_ports[port]['qty']
        average_price_ports.update({port: average_price})
    return average_price_ports


def _calculate_trade_id_str(distribution_trade_id: list[Distribution]) -> float:
    average_prices = _calculate_trade_id_average_prices(distribution_trade_id)
    max_price = max(average_prices.values())
    min_price = min(average_prices.values())
    return abs(max_price / min_price - 1)
