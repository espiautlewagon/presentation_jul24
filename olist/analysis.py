import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order

class WhatIfAnalysis:
    def __init__(self, seller_data, alpha=3157.27, beta=978.23, initial_it_costs=500_000):
        self.seller_data = seller_data
        self.alpha = alpha
        self.beta = beta
        self.initial_it_costs = initial_it_costs

    def update_it_costs(self, n_sellers, n_items):
        return self.alpha * np.sqrt(n_sellers) + self.beta * np.sqrt(n_items)

    def perform_analysis(self):
        # Sort sellers by increasing profits
        sorted_sellers = self.seller_data.sort_values(by='profits')

        results = []

        # Remove sellers one-by-one
        for i in range(len(sorted_sellers)):
            print(sorted_sellers.iloc[i])
            n_sellers_remaining = len(sorted_sellers) - i
            n_items_remaining = sorted_sellers.iloc[i:]['number_of_items'].sum()

            it_costs = self.update_it_costs(n_sellers_remaining, n_items_remaining)
            total_profit = sorted_sellers.iloc[i:]['profits'].sum() - it_costs
            results.append((n_sellers_remaining, total_profit))

        return results
