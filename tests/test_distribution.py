from unittest import TestCase

import pandas as pd

from master_distributor.distributors import RandomDistributor
from master_distributor.utils import verify_distribution

# Read samples
master_sample = pd.read_csv('samples/master_sample.csv', sep=';', decimal=',')  # type: ignore
allocations_sample = pd.read_csv('samples/allocations_sample.csv', sep=';', decimal=',')  # type: ignore


class TestRandomDistribution(TestCase):
    def test_distribution_not_empty(self):
        distributor = RandomDistributor()
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert not distribution.empty

    def test_distribution_final_qty(self):
        distributor = RandomDistributor()
        distribution = distributor.distribute(master_sample, allocations_sample)
        assert verify_distribution(distribution, master_sample)
