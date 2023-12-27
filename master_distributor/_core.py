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
