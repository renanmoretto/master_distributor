"""
Module containing the 'raw' functions that distributes a slice.
"""

import random
from collections import defaultdict

from master_distributor._types import (
    TupleTradesAlias,
    TupleAllocationAlias,
    TupleDistributionAlias,
)


def _get_vertical_qty_per_portfolio(
    allocations: list[tuple[str, int]],
) -> dict[str, int]:
    vertical_qty_per_portfolio: dict[str, int] = defaultdict(int)
    for portfolio, qty in allocations:
        if qty != 0:
            vertical_qty_per_portfolio[portfolio] += qty
    return vertical_qty_per_portfolio


def _get_trades_average_price(trades: list[TupleTradesAlias]) -> float:
    total_volume = 0.0
    total_qty = 0.0
    for qty, price in trades:
        total_volume += qty * price
        total_qty += qty
    return total_volume / total_qty


def _sort_by_average_price(orders: list[TupleTradesAlias]) -> list[TupleTradesAlias]:
    avg_price = _get_trades_average_price(orders)
    return sorted(orders, key=lambda x: abs(x[1] - avg_price))


# ---------------------------- single distributors --------------------------- #

# For performance reasons, there is a single function for every
# implemented distributor.


def distribute_slice_weighted(
    trades: list[TupleTradesAlias],
    allocations: list[TupleAllocationAlias],
    shuffle_orders: bool = True,
) -> list[TupleDistributionAlias]:
    orders = trades

    # Start by the closest price to the average price of all trades
    orders = _sort_by_average_price(orders)

    remaining_vertical_qty_per_portfolio = _get_vertical_qty_per_portfolio(allocations)
    total_qty = sum(remaining_vertical_qty_per_portfolio.values())
    weights: dict[str, float] = {
        k: v / total_qty for k, v in remaining_vertical_qty_per_portfolio.items()
    }

    portfolios = tuple(remaining_vertical_qty_per_portfolio.keys())

    slice_distribution: list[TupleDistributionAlias] = []

    for order in orders:
        quantity = order[0]
        price = order[1]

        remaining_order_qty = quantity
        second_loop = False
        while remaining_order_qty > 0:
            for portfolio in portfolios:
                if remaining_order_qty == 0:
                    break
                max_qty_portfolio = remaining_vertical_qty_per_portfolio[portfolio]
                if max_qty_portfolio == 0:
                    continue

                if second_loop:
                    qty = 1
                else:
                    qty = int(weights[portfolio] / quantity)

                remaining_order_qty -= qty
                remaining_vertical_qty_per_portfolio[portfolio] -= qty

                slice_distribution.append((qty, price, portfolio))

    return slice_distribution


def distribute_slice_random(
    trades: list[TupleTradesAlias],
    allocations: list[TupleAllocationAlias],
    shuffle_orders: bool = True,
) -> list[TupleDistributionAlias]:
    orders = trades
    if shuffle_orders:
        random.shuffle(orders)

    remaining_vertical_qty_per_portfolio = _get_vertical_qty_per_portfolio(allocations)

    portfolios = tuple(remaining_vertical_qty_per_portfolio.keys())

    slice_distribution: list[TupleDistributionAlias] = []

    for order in orders:
        quantity = order[0]
        price = order[1]

        remaining_order_qty = quantity

        while remaining_order_qty > 0:
            for portfolio in portfolios:
                if remaining_order_qty == 0:
                    break
                max_qty_portfolio = remaining_vertical_qty_per_portfolio[portfolio]
                if max_qty_portfolio == 0:
                    continue

                max_qty_random = min(max_qty_portfolio, remaining_order_qty)
                qty = random.randint(1, max_qty_random)

                remaining_order_qty -= qty
                remaining_vertical_qty_per_portfolio[portfolio] -= qty

                slice_distribution.append((qty, price, portfolio))

    return slice_distribution
