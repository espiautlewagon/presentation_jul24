import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        orders = self.data['orders'].copy()
        if is_delivered:
            delivered_orders = orders[orders['order_status'] == 'delivered']
        #delivered_orders.groupby('order_status').count()
        delivered_orders['order_purchase_timestamp'] = pd.to_datetime(delivered_orders['order_purchase_timestamp'])
        delivered_orders['order_delivered_carrier_date'] = pd.to_datetime(delivered_orders['order_delivered_carrier_date'])
        delivered_orders['order_delivered_customer_date'] = pd.to_datetime(delivered_orders['order_delivered_customer_date'])
        delivered_orders['order_estimated_delivery_date'] = pd.to_datetime(delivered_orders['order_estimated_delivery_date'])

        delivered_orders['wait_time'] = (delivered_orders['order_delivered_customer_date'] - delivered_orders['order_purchase_timestamp']).dt.days
        delivered_orders['expected_wait_time'] = (delivered_orders['order_estimated_delivery_date'] - delivered_orders['order_purchase_timestamp']).dt.days
        delivered_orders['delay_vs_expected'] = delivered_orders['wait_time'] - delivered_orders['expected_wait_time']
        delivered_orders['delay_vs_expected'] = [x if x > 0 else 0 for x in delivered_orders['delay_vs_expected']]

        output_df = delivered_orders[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected', 'order_status']]

        return output_df

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews'].copy()
        reviews['dim_is_five_star'] = reviews['review_score'].apply(lambda x: x == 5)
        reviews['dim_is_one_star'] = reviews['review_score'].apply(lambda x: x == 1)
        output_df = reviews[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]
        return output_df

    def get_number_items(self):
        """
        Returns a DataFrame with:
        order_id, number_of_items
        """
        order_items = self.data['order_items'].copy()
        number_of_items = order_items.groupby('order_id')['order_item_id'].count().reset_index()
        number_of_items.columns = ['order_id', 'number_of_items']
        return number_of_items


    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_items_df = self.data['order_items'].copy()
        number_of_sellers = order_items_df.groupby('order_id')['seller_id'].nunique()
        output_df = number_of_sellers.reset_index(name='number_of_sellers')
        return output_df

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items_df = self.data['order_items'].copy()
        order_items_df = order_items_df.groupby('order_id').agg(sum)
        order_items_df = order_items_df.reset_index()
        output_df = order_items_df[['order_id', 'price', 'freight_value']]
        return output_df

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        geoloc_df = self.data['geolocation'].copy()
        #geoloc_unique_df = geoloc_df.drop_duplicates(subset='geolocation_zip_code_prefix')
        geoloc_unique_df = geoloc_df.groupby('geolocation_zip_code_prefix').agg({'geolocation_lat': 'mean', 'geolocation_lng': 'mean' }).reset_index()

        orders_customers_df = pd.merge(self.data['orders'].copy(), self.data['customers'].copy(), on='customer_id')
        orders_customers_geo_df = pd.merge(orders_customers_df, geoloc_unique_df, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

        orders_order_items_df = pd.merge(self.data['orders'].copy(), self.data['order_items'].copy(), on='order_id')
        orders_order_items_sellers_df = pd.merge(orders_order_items_df, self.data['sellers'].copy(), on='seller_id')
        orders_order_items_sellers_geo_df = pd.merge(orders_order_items_sellers_df, geoloc_unique_df, left_on='seller_zip_code_prefix', right_on='geolocation_zip_code_prefix')

        orders_customers_geo_df = orders_customers_geo_df[['order_id',  'geolocation_lng', 'geolocation_lat']]
        orders_order_items_sellers_geo_df = orders_order_items_sellers_geo_df[['order_id', 'seller_id', 'geolocation_lng', 'geolocation_lat']]

        orders_customers_geo_df = orders_customers_geo_df.rename(columns={'geolocation_lat': 'customer_lat', 'geolocation_lng': 'customer_lng'})
        orders_order_items_sellers_geo_df = orders_order_items_sellers_geo_df.rename(columns={'geolocation_lat': 'seller_lat', 'geolocation_lng': 'seller_lng'})

        merged_df = pd.merge(orders_customers_geo_df, orders_order_items_sellers_geo_df, on='order_id')
        merged_df['distance_seller_customer'] = merged_df.apply(
                    lambda row: haversine_distance(
                        row['customer_lng'], row['customer_lat'],
                        row['seller_lng'], row['seller_lat']
                    ), axis=1)
        
        return merged_df.groupby('order_id').agg({'distance_seller_customer': 'mean'}).reset_index()


    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_items', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above

        wait_time_df = self.get_wait_time()
        review_df = self.get_review_score()
        number_of_items_df = self.get_number_items()
        number_of_sellers_df = self.get_number_sellers()
        price_freight_df = self.get_price_and_freight()
        
        
        # Merge all features into a single DataFrame
        df = wait_time_df.merge(review_df, on='order_id')
        df = df.merge(number_of_items_df, on='order_id')
        df = df.merge(number_of_sellers_df, on='order_id')
        df = df.merge(price_freight_df, on='order_id')

        if with_distance_seller_customer:
            distance_seller_customer_df = self.get_distance_seller_customer()
            df = df.merge(distance_seller_customer_df, on='order_id')
        
        if is_delivered:
            df = df[df['order_status'] == 'delivered']
        
        df = df.dropna()
        
        return df

