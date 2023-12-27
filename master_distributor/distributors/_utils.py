# TODO
from ._slice_distributors import TupleDistributionAlias


def distribution_average_price(
    distribution: list[TupleDistributionAlias],
) -> dict[str, float]:
    """Returns a dict containing the average price for each portfolio"""
    ...


def distribution_max_deviation(distribution: list[TupleDistributionAlias]) -> float:
    dist_average_price = distribution_average_price(distribution)

    # Remove after impl
    return 1.0
