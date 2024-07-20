
import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order


class Seller:
    def __init__(self):
        # Import data only once
        olist = Olist()
        self.data = olist.get_data()
        self.order = Order()

    def get_seller_features(self):
        """
        Returns a DataFrame with:
        'seller_id', 'seller_city', 'seller_state'
        """
        sellers = self.data['sellers'].copy(
        )  # Make a copy before using inplace=True so as to avoid modifying self.data
        sellers.drop('seller_zip_code_prefix', axis=1, inplace=True)
        sellers.drop_duplicates(
            inplace=True)  # There can be multiple rows per seller
        return sellers

    def get_seller_delay_wait_time(self):
        """
        Returns a DataFrame with:
        'seller_id', 'delay_to_carrier', 'wait_time'
        """
        # Get data
        order_items = self.data['order_items'].copy()
        orders = self.data['orders'].query("order_status=='delivered'").copy()

        ship = order_items.merge(orders, on='order_id')

        # Handle datetime
        ship.loc[:, 'shipping_limit_date'] = pd.to_datetime(
            ship['shipping_limit_date'])
        ship.loc[:, 'order_delivered_carrier_date'] = pd.to_datetime(
            ship['order_delivered_carrier_date'])
        ship.loc[:, 'order_delivered_customer_date'] = pd.to_datetime(
            ship['order_delivered_customer_date'])
        ship.loc[:, 'order_purchase_timestamp'] = pd.to_datetime(
            ship['order_purchase_timestamp'])

        # Compute delay and wait_time
        def delay_to_logistic_partner(d):
            days = np.mean(
                (d.order_delivered_carrier_date - d.shipping_limit_date) /
                np.timedelta64(24, 'h'))
            if days > 0:
                return days
            else:
                return 0

        def order_wait_time(d):
            days = np.mean(
                (d.order_delivered_customer_date - d.order_purchase_timestamp)
                / np.timedelta64(24, 'h'))
            return days

        delay = ship.groupby('seller_id')\
                    .apply(delay_to_logistic_partner)\
                    .reset_index()
        delay.columns = ['seller_id', 'delay_to_carrier']

        wait = ship.groupby('seller_id')\
                   .apply(order_wait_time)\
                   .reset_index()
        wait.columns = ['seller_id', 'wait_time']

        df = delay.merge(wait, on='seller_id')

        return df

    def get_active_dates(self):
        """
        Returns a DataFrame with:
        'seller_id', 'date_first_sale', 'date_last_sale', 'months_on_olist'
        """
        # First, get only orders that are approved
        orders_approved = self.data['orders'][[
            'order_id', 'order_approved_at'
        ]].dropna()

        # Then, create a (orders <> sellers) join table because a seller can appear multiple times in the same order
        orders_sellers = orders_approved.merge(self.data['order_items'],
                                               on='order_id')[[
                                                   'order_id', 'seller_id',
                                                   'order_approved_at'
                                               ]].drop_duplicates()
        orders_sellers["order_approved_at"] = pd.to_datetime(
            orders_sellers["order_approved_at"])

        # Compute dates
        orders_sellers["date_first_sale"] = orders_sellers["order_approved_at"]
        orders_sellers["date_last_sale"] = orders_sellers["order_approved_at"]
        df = orders_sellers.groupby('seller_id').agg({
            "date_first_sale": min,
            "date_last_sale": max
        })
        df['months_on_olist'] = round(
            (df['date_last_sale'] - df['date_first_sale']) /
            np.timedelta64(1, 'M'))
        return df

    def get_quantity(self):
        """
        Returns a DataFrame with:
        'seller_id', 'n_orders', 'quantity', 'quantity_per_order'
        """
        order_items = self.data['order_items']

        n_orders = order_items.groupby('seller_id')['order_id']\
            .nunique()\
            .reset_index()
        n_orders.columns = ['seller_id', 'n_orders']

        quantity = order_items.groupby('seller_id', as_index=False).agg(
            {'order_id': 'count'})
        quantity.columns = ['seller_id', 'quantity']

        result = n_orders.merge(quantity, on='seller_id')
        result['quantity_per_order'] = result['quantity'] / result['n_orders']
        return result
    
    def get_number_of_items(self):
        """
        Calculate the number of items sold by each seller
        """
        order_items = self.data['order_items'].copy()
        number_of_items = order_items.groupby('seller_id')['order_item_id'].count().reset_index()
        number_of_items.columns = ['seller_id', 'number_of_items']
        return number_of_items
    

    def get_sales(self):
        """
        Returns a DataFrame with:
        'seller_id', 'sales'
        """
        return self.data['order_items'][['seller_id', 'price']]\
            .groupby('seller_id')\
            .sum()\
            .rename(columns={'price': 'sales'})

    def get_review_score(self):
        """
        Returns a DataFrame with:
        'seller_id', 'share_of_five_stars', 'share_of_one_stars', 'review_score'
        """

        orders_orderreviews_df = pd.merge(self.data['orders'].copy(), self.data['order_reviews'].copy(), on='order_id')
        orders_orderreviews_orderitems_df = pd.merge(orders_orderreviews_df, self.data['order_items'].copy(), on='order_id')
        orders_orderreviews_orderitems_sellers_df = pd.merge(orders_orderreviews_orderitems_df, self.data['sellers'], on='seller_id')
        orders_orderreviews_orderitems_sellers_df['dim_is_one_star'] = orders_orderreviews_orderitems_sellers_df.groupby('seller_id')['review_score'].apply(lambda x: x == 1)
        orders_orderreviews_orderitems_sellers_df['dim_is_five_star'] = orders_orderreviews_orderitems_sellers_df.groupby('seller_id')['review_score'].apply(lambda x: x == 5)

        seller_review_df = orders_orderreviews_orderitems_sellers_df[['order_id', 'seller_id', 'review_score', 'dim_is_one_star', 'dim_is_five_star']]

        seller_reviews_mean_df = seller_review_df.groupby('seller_id').agg({
                    'dim_is_five_star': 'mean',
                    'dim_is_one_star': 'mean',
                    'review_score': 'mean'
                }).reset_index()
        return seller_reviews_mean_df.rename(columns={
            'dim_is_five_star': 'share_of_five_stars',
            'dim_is_one_star': 'share_of_one_stars'
        })

    def get_revenues(self):
        """
        Calculate revenues from sales fees and subscription fees
        """
        order_items = self.data['order_items'].copy()
        orders = self.data['orders'].copy()

        orders_order_items = pd.DataFrame.merge(orders[['order_id', 'order_status']], order_items[['order_id', 'seller_id', 'price']], on='order_id')
        sales_fees = orders_order_items.groupby('seller_id')['price'].sum() * 0.1
        sales_fees = sales_fees.reset_index()
        subscription_fees = self.get_active_dates()
        revenues = sales_fees.merge(subscription_fees['months_on_olist'], on='seller_id')
        revenues['sales_fees'] = revenues['price']
        revenues['subscription_fees'] = revenues['months_on_olist'] *80
        revenues['revenues'] = revenues['price'] + revenues['months_on_olist'] *80

        return revenues[['seller_id', 'sales_fees', 'subscription_fees', 'revenues']]
    

    def get_cost_of_reviews(self):
        """
        Calculate the cost associated with bad reviews
        """
        #order_items = self.data['order_items'].copy()
        order_items = self.data['order_items'][['order_id', 'seller_id']].drop_duplicates()
        order_reviews = self.data['order_reviews'].copy()
        orders = self.data['orders'].copy()

        order_reviews['cost'] = order_reviews['review_score'].map({1: 100, 2: 50, 3: 40, 4: 0, 5: 0})
        orders_costs = orders.merge(order_reviews[['cost', 'order_id']], on='order_id')
        orders_costs_sellerID = orders_costs.merge(order_items[['order_id', 'seller_id']], on='order_id')
        review_costs = orders_costs_sellerID.groupby('seller_id')['cost'].sum().reset_index()
       
        return review_costs



    def get_training_data(self):
        '''
        Returns a DataFrame with:
        ['seller_id', 'seller_city', 'seller_state', 'delay_to_carrier',
        'wait_time', 'date_first_sale', 'date_last_sale', 'months_on_olist', 'share_of_one_stars',
        'share_of_five_stars', 'review_score', 'n_orders', 'quantity',
        'quantity_per_order', 'sales']
        '''

        training_set =\
            self.get_seller_features()\
                .merge(
                self.get_seller_delay_wait_time(), on='seller_id'
               ).merge(
                self.get_active_dates(), on='seller_id'
               ).merge(
                self.get_quantity(), on='seller_id'
               ).merge(
                self.get_sales(), on='seller_id'
               ).merge(
                self.get_revenues(), on='seller_id'        
               ).merge(
                self.get_cost_of_reviews(), on='seller_id'
               ).merge(
                   self.get_number_of_items(), on='seller_id'
               )
        
        training_set['profits'] = training_set['revenues'] - training_set['cost']

        if self.get_review_score() is not None:
            training_set = training_set.merge(self.get_review_score(),
                                              on='seller_id')

        return training_set