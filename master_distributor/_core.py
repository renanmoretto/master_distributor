# FIXME
# Refactor to single trade id distribution only
# Slicing trades should be another module

import random
from collections import defaultdict


# Alias

Portfolio = str
Price = float
Qty = int
RawAlloctionAlias = tuple[Portfolio, Qty]
RawMasterAlias = tuple[Qty, Price]
RawDistributionAlias = tuple[Qty, Price, Portfolio]


def _get_vertical_qty_per_portfolio(
    allocations: list[RawAlloctionAlias],
) -> dict[str, int]:
    vertical_qty_per_portfolio: dict[str, int] = defaultdict(int)
    for portfolio, qty in allocations:
        if qty != 0:
            vertical_qty_per_portfolio[portfolio] += qty
    return vertical_qty_per_portfolio


# ---------------------------- Single Distributors --------------------------- #

# For performance reasons, there is a single function for every
# implemented distributor.


def distribute_single_slice_random(
    trades: list[RawMasterAlias],
    allocations: list[RawAlloctionAlias],
    shuffle_orders: bool = True,
) -> list[RawDistributionAlias]:
    orders = list(trades)
    if shuffle_orders:
        random.shuffle(orders)

    remaining_vertical_qty_per_portfolio = _get_vertical_qty_per_portfolio(allocations)

    portfolios = tuple(remaining_vertical_qty_per_portfolio.keys())

    slice_distribution: list[RawDistributionAlias] = []

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


# def distribute_trade_ids(
#     distribution_data: DistributionData,
#     qty_calculator: Callable[[dict[str, Any]], int],
#     shuffle_orders: bool = True,
# ) -> list[Distribution]:
#     distributions: list[Distribution] = []
#     for trade_id in distribution_data.trade_ids:
#         master_trade_id = _filter_lazy_by_trade_id(
#             distribution_data.master_lazy, trade_id
#         )
#         allocations_trade_id = _filter_lazy_by_trade_id(
#             distribution_data.allocations_lazy, trade_id
#         )

#         trade_id_distribution = distribute_trade_id(
#             master_trade_id=master_trade_id.collect().rows(),  # type: ignore
#             allocations_trade_id=allocations_trade_id.collect().rows(),  # type: ignore
#             qty_calculator=qty_calculator,
#             shuffle_orders=shuffle_orders,
#         )  # type: ignore

#         distributions += trade_id_distribution
#     return distributions
