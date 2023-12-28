"""
Module containing the 'raw' functions that distributes a slice.
"""

import random
from collections import defaultdict

from ._types import (
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


# ---------------------------- single distributors --------------------------- #

# For performance reasons, there is a single function for every
# implemented distributor.


def distribute_slice_random(
    trades: list[TupleTradesAlias],
    allocations: list[TupleAllocationAlias],
    shuffle_orders: bool = True,
) -> list[TupleDistributionAlias]:
    orders = list(trades)
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
