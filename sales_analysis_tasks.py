import pandas as pd
import numpy as np
import util
#Part1  1.	Load all 5 CSV files into separate DataFrames
customers_df=pd.read_csv("customers.csv")
order_items_df=pd.read_csv("order_items.csv")
orders_df=pd.read_csv("orders.csv")
products_df=pd.read_csv("products.csv")
reviews_df=pd.read_csv("reviews.csv")

#Part1  2.	Display first 5 rows of each dataset
print('-----------Display first 5 rows of each dataset------------')
print('Customer')
print(customers_df.head())
print('Order Items')
print(order_items_df.head())
print('Order')
print(orders_df.head())
print('Products')
print(products_df.head())
print('Reviews')
print(reviews_df.head())

#Part1  3.	Check data types, shape, and basic info
print('-----------Display basic info------------')
print('Customer')
customers_df.info()
print('Order Items')
order_items_df.info()
print('Order')
orders_df.info()
print('Products')
products_df.info()
print('Reviews')
reviews_df.info()
print('-----------Display data types------------')
print('Customer')
print(customers_df.dtypes)
print('Order Items')
print(order_items_df.dtypes)
print('Order')
print(orders_df.dtypes)
print('Products')
print(products_df.dtypes)
print('Reviews')
print(reviews_df.dtypes)
print('-----------Display shape------------')
print('Customer')
print(customers_df.shape)
print('Order Items')
print(order_items_df.shape)
print('Order')
print(orders_df.shape)
print('Products')
print(products_df.shape)
print('Reviews')
print(reviews_df.shape)

#Part1  4.	Identify missing values in each dataset
print('Identify missing values in each dataset')
print('Customer')
print(customers_df.isnull().sum())
print('Order Items')
print(order_items_df.isnull().sum())
print('Order')
print(orders_df.isnull().sum())
print('Products')
print(products_df.isnull().sum())
print('Reviews')
print(reviews_df.isnull().sum())

#Part1  5.	Find and remove duplicate records
print('Find and remove duplicate records')
print('Customer')
util.removeDuplicate(customers_df)
print('Order Items')
util.removeDuplicate(order_items_df)
print('Order')
util.removeDuplicate(orders_df)
print('Products')
util.removeDuplicate(products_df)
print('Reviews')
util.removeDuplicate(reviews_df)
##Part1  7.	Handle null values appropriately
customers_df.fillna({'city': 'Unknown'}, inplace=True)
orders_df.fillna({'payment_method': 'Unknown'}, inplace=True)
products_df.fillna({'stock_quantity': 0}, inplace=True)
reviews_df.fillna({'review_text': 'Unknown'}, inplace=True)
##Part1  6.	Fix data type issues (dates, numbers, etc.)
print("Fix data type issues (dates, numbers, etc.)")
customers_df['registration_date']=pd.to_datetime(customers_df['registration_date'],errors="coerce")
orders_df['order_date']=pd.to_datetime(orders_df['order_date'],errors="coerce")
reviews_df['review_date']=pd.to_datetime(reviews_df['review_date'],errors="coerce")
order_items_df['discount_percent']=order_items_df['discount_percent'].astype('float64')
products_df['stock_quantity']=products_df['stock_quantity'].astype('int64')

#Part1  Date range of the data
print('Date range of the data')
print('Customer')
print(f"Date Range : {customers_df['registration_date'].min()} to {customers_df['registration_date'].max()}")
print('Order')
print(f"Date Range : {orders_df['order_date'].min()} to {orders_df['order_date'].max()}")
print('Reviews')
print(f"Date Range : {reviews_df['review_date'].min()} to {reviews_df['review_date'].max()}")

#Part 2 - 1.	Merge order_items with products to get prices
order_items_products_merge_df=order_items_df.merge(products_df,on='product_id',how='left')
#Part 2 - 2.	Calculate total amount for each order item (price × quantity - discount)
order_items_products_merge_df['item_total_price']=(order_items_products_merge_df['price']*order_items_products_merge_df['quantity'])-order_items_products_merge_df['discount_percent']
order_items_products_merge_df['item_profit'] = order_items_products_merge_df['item_total_price'] - (order_items_products_merge_df['cost'] * order_items_products_merge_df['quantity'])
#Part 2 - 3.	Aggregate order items to get total order value
order_cost_df=order_items_products_merge_df.groupby('order_id').agg(
        total_order_value=('item_total_price','sum'),
        total_order_profit=('item_profit','sum')
     ).reset_index()
#Part 2 - 4.	Merge orders with customers
order_customer_df=orders_df.merge(customers_df,on='customer_id',how='left')
#Part 2 - 5.	Create a complete sales DataFrame with all information
sales_df=order_customer_df.merge(order_cost_df,on='order_id',how='left')
#Part 2 - 6.	Add calculated columns: 
#Part 2   Profit margin
sales_df['profit_margin'] = (sales_df['total_order_profit'] / sales_df['total_order_value']) * 100
#Part 2   Customer lifetime value (CLV)
clv_map = sales_df.groupby('customer_id')['total_order_value'].sum()
sales_df['CLV'] = sales_df['customer_id'].map(clv_map)
#Part 2	Days since registration
current_date = sales_df['order_date'].max()
sales_df['days_since_registration'] = (current_date - sales_df['registration_date']).dt.days
#Part 2	What's the average order value?
average_order_value = sales_df['total_order_value'].mean()
print(f"The Average Order Value is: {average_order_value:.2f}")
#Part 2   Customer has the highest lifetime value
top_customer = sales_df.sort_values('CLV', ascending=False).iloc[0]
print(f"Top Customer ID: {top_customer['customer_id']} - {top_customer['first_name']} {top_customer['last_name']}")
print(f"Highest Lifetime Value: ${top_customer['CLV']:.2f}")
#Part 2   Profit margin by category
category_analysis = order_items_products_merge_df.groupby('category').agg({
    'item_total_price': 'sum',
    'item_profit': 'sum'
})
category_analysis['margin_percentage'] = (category_analysis['item_profit'] / category_analysis['item_total_price']) * 100
category_analysis = category_analysis.sort_values('margin_percentage', ascending=False)
print(category_analysis)