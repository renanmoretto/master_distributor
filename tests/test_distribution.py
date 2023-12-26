from unittest import TestCase

import pandas as pd

from master_distributor.distributors import (
    RandomDistributor,
    UnitDistributor,
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
        distributor = RandomDistributor()
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)

    def test_distribution_all_args(self):
        distributor = RandomDistributor(
            shuffle_orders=True,
            loop=True,
            std_break=0.05,
            else_return_best=True,
            max_its=10_000,
            verbose=False,
        )
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)


class TestUnitDistribution(TestCase):
    def test_distribution_no_args(self):
        distributor = UnitDistributor()
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)

    def test_distribution_all_args(self):
        distributor = UnitDistributor(
            shuffle_orders=True,
            loop=True,
            std_break=0.05,
            else_return_best=True,
            max_its=10_000,
            verbose=False,
        )
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)
