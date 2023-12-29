from typing import Callable

TupleTradesAlias = tuple[int, float]
TradesRowsAlias = list[TupleTradesAlias]

TupleAllocationAlias = tuple[str, int]
AllocationsRowsAlias = list[TupleAllocationAlias]

TupleDistributionAlias = tuple[int, float, str]

TupleFullDistributionAlias = tuple[str, str, str, int, float, str]

FuncDistributeAlias = Callable[
    [list[TupleTradesAlias], list[TupleAllocationAlias]], list[TupleDistributionAlias]
]
