
import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order


class Product:
    def __init__(self):
        # Import data only once
        olist = Olist()
        self.data = olist.get_data()
        self.order = Order()

    def get_product_features(self):
        """
        Returns a DataFrame with:
       'product_id', 'product_category_name', 'product_name_length',
       'product_description_length', 'product_photos_qty', 'product_weight_g',
       'product_length_cm', 'product_height_cm', 'product_width_cm'
        """
        products = self.data['products']

        # (optional) convert name to English
        en_category = self.data['product_category_name_translation']
        df = products.merge(en_category, on='product_category_name')
        df.drop(['product_category_name'], axis=1, inplace=True)
        df.rename(columns={
            'product_category_name_english': 'category',
            'product_name_lenght': 'product_name_length',
            'product_description_lenght': 'product_description_length'
        },
                  inplace=True)

        return df

    def get_price(self):
        """
        Return a DataFrame with:
        'product_id', 'price'
        """
        order_items = self.data['order_items']
        # There are many different order_items per product_id, each with different prices. Take the mean of the various prices
        return order_items[['product_id',
                            'price']].groupby('product_id').mean()

    def get_wait_time(self):
        """
        Returns a DataFrame with:
        'product_id', 'wait_time'
        """
        orders_wait_time = self.order.get_wait_time()
        orders_products = self.data['order_items'][['order_id', 'product_id']].drop_duplicates()
        orders_products_with_time = orders_products.merge(orders_wait_time, on='order_id')

        return orders_products_with_time.groupby('product_id',
                          as_index=False).agg({'wait_time': 'mean'})

    def get_review_score(self):
        """
        Returns a DataFrame with:
        'product_id', 'share_of_five_stars', 'share_of_one_stars',
        'review_score'
        """
        orders_reviews = self.order.get_review_score()
        orders_products = self.data['order_items'][['order_id',
                                         'product_id']].drop_duplicates()
        df = orders_products.merge(orders_reviews, on='order_id')
        result = df.groupby('product_id', as_index=False).agg({
            'dim_is_one_star':
            'mean',
            'dim_is_five_star':
            'mean',
            'review_score':
            'mean',
        })
        result.columns = [
            'product_id', 'share_of_one_stars', 'share_of_five_stars',
            'review_score'
        ]

        return result

    def get_quantity(self):
        """
        Returns a DataFrame with:
        'product_id', 'n_orders', 'quantity'
        """
        order_items = self.data['order_items']

        n_orders =\
            order_items.groupby('product_id')['order_id'].nunique().reset_index()
        n_orders.columns = ['product_id', 'n_orders']

        quantity = \
            order_items.groupby('product_id',
                                   as_index=False).agg({'order_id': 'count'})
        quantity.columns = ['product_id', 'quantity']

        return n_orders.merge(quantity, on='product_id')

    def get_sales(self):
        """
        Returns a DataFrame with:
        'product_id', 'sales'
        """
        return self.data['order_items'][['product_id', 'price']]\
            .groupby('product_id')\
            .sum()\
            .rename(columns={'price': 'sales'})
    
    def get_revenues(self):
        """
        Calculate revenues from sales fees for each product
        """
        order_items = self.data['order_items']
        revenues = order_items.groupby('product_id')['price'].sum() * 0.1
        return revenues.reset_index().rename(columns={'price': 'revenues'})
        
    
    def get_review_costs(self):
        """
        Calculate the cost associated with bad reviews for each product
        """
        order_reviews = self.data['order_reviews'].copy()
        order_items = self.data['order_items'][['order_id', 'product_id']].drop_duplicates()
        orders = self.data['orders'].copy()

        order_reviews['cost'] = order_reviews['review_score'].map({1: 100, 2: 50, 3: 40, 4: 0, 5: 0})
        orders_costs = orders.merge(order_reviews[['cost', 'order_id']], on='order_id')
        orders_costs_product = orders_costs.merge(order_items[['order_id', 'product_id']], on='order_id')
        orders_costs_product.to_csv('orders_costs_product.csv')
        review_costs = orders_costs_product.groupby('product_id')['cost'].sum().reset_index()
        return review_costs
    

    def get_training_data(self):
        """
        Returns a DataFrame with:
        ['product_id', 'product_name_length', 'product_description_length',
       'product_photos_qty', 'product_weight_g', 'product_length_cm',
       'product_height_cm', 'product_width_cm', 'category', 'wait_time',
       'price', 'share_of_one_stars', 'share_of_five_stars', 'review_score',
       'n_orders', 'quantity', 'sales'],
        """
        training_set =\
            self.get_product_features()\
                .merge(
                self.get_wait_time(), on='product_id'
               ).merge(
                self.get_price(), on='product_id'
               ).merge(
                self.get_review_score(), on='product_id'
               ).merge(
                self.get_quantity(), on='product_id'
               ).merge(
                self.get_sales(), on='product_id'
               ).merge(
                self.get_revenues(), on='product_id'
               ).merge(
                self.get_review_costs(), on='product_id'
               )
        training_set['profits'] = training_set['revenues'] - training_set['cost']


        return training_set

    def get_product_cat(self, agg="mean"):
        '''
        Returns a DataFrame with `category` as index, and aggregating various properties for each category in columns such as:
        - `quantity`: total number of products sold for this category.
        - `product_weight_g`: mean or median weight per category
        - ...
        '''
        pass  # YOUR CODE HERE

