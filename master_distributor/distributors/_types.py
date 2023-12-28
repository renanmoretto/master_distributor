from typing import Callable

TupleTradesAlias = tuple[int, float]
TupleAllocationAlias = tuple[str, int]
TupleDistributionAlias = tuple[int, float, str]
FuncDistributeAlias = Callable[
    [list[TupleTradesAlias], list[TupleAllocationAlias]], list[TupleDistributionAlias]
]
TupleFullDistributionAlias = tuple[str, str, str, int, float, str]
