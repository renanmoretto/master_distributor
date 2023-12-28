from unittest import TestCase

import pandas as pd

from master_distributor.distributors import (
    RandomLoopDistributor,
)
from master_distributor.utils import verify_distribution

# Read samples
master_sample = pd.read_csv('samples/master_sample.csv', sep=';', decimal=',')  # type: ignore
master_sample['QUANTITY'] = master_sample['QUANTITY'].astype(int)  # type: ignore
master_sample['PRICE'] = master_sample['PRICE'].astype(float)  # type: ignore
allocations_sample = pd.read_csv('samples/allocations_sample.csv', sep=';', decimal=',')  # type: ignore
allocations_sample['QUANTITY'] = allocations_sample['QUANTITY'].astype(int)  # type: ignore


class TestRandomDistribution(TestCase):
    def test_distribution_no_args(self):
        distributor = RandomLoopDistributor()
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)

    def test_distribution_all_args(self):
        distributor = RandomLoopDistributor(
            shuffle_orders=True,
            std_break=0.05 / 100,
            else_return_best=True,
            max_its=10_000,
            verbose=True,
        )
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)
