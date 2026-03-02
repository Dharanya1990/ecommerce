import pandas as pd
import numpy as np
import util
from itertools import combinations
from collections import Counter
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
print(f"Highest Lifetime Value: {top_customer['CLV']:.2f}")
#Part 2   Profit margin by category
category_analysis = order_items_products_merge_df.groupby('category').agg({
    'item_total_price': 'sum',
    'item_profit': 'sum'
})
category_analysis['margin_percentage'] = (category_analysis['item_profit'] / category_analysis['item_total_price']) * 100
category_analysis = category_analysis.sort_values('margin_percentage', ascending=False)
print(category_analysis)
#Part 3: Sales Analysis
#Part 3 Revenue Analysis: Total revenue by month/year
sales_df['year'] = sales_df['order_date'].dt.year
sales_df['month_year'] = sales_df['order_date'].dt.to_period('M')
yearly_rev = sales_df.groupby('year')['total_order_value'].sum()
print("Total revenue by year")
print(yearly_rev)

#Part 3 Revenue Analysis: High Sales Month
high_sales_month = sales_df.groupby('month_year')['total_order_value'].sum().nlargest(1)
print("High Sales Month")
print(high_sales_month)

#Part 3 Revenue Analysis: Revenue Growth Rate 
yoy_growth = yearly_rev.pct_change() * 100
print("Revenue growth rate year-over-year")
print(yoy_growth)

#Part 3 Revenue Analysis: Revenue by Category
category_rev = order_items_products_merge_df.groupby('category')['item_total_price'].sum()
print("Revenue by Category")
print(category_rev)

#Part 3 Revenue Analysis: Revenue by Segment
segment_rev = sales_df.groupby('customer_segment')['total_order_value'].sum()
print("Revenue by Segment")
print(segment_rev)

#Part 3 Revenue Analysis: Top 10 Customers
top_10_cust = sales_df.groupby('customer_id')['total_order_value'].sum().nlargest(10)
print("Top 10 Customers")
print(top_10_cust)

#Part 3 Product Analysis: Best-selling products (by quantity and revenue)
product_performance = order_items_products_merge_df.groupby('product_name').agg({
    'quantity': 'sum',
    'item_total_price': 'sum'
}).reset_index()

#Part 3 Product Analysis: Best-selling products (by quantity)
top_5_quantity = product_performance.nlargest(5, 'quantity')
print('Best-selling products (by quantity)')
print(top_5_quantity)

#Part 3 Product Analysis: Best-selling products (revenue)
top_5_revenue = product_performance.nlargest(5, 'item_total_price')
print('Best-selling products (revenue)')
print(top_5_revenue)

#Part 3 Product Analysis: Worst-performing products
worst_5_revenue = product_performance.nsmallest(5, 'item_total_price')
print('Worst-performing products')
print(worst_5_revenue)

#Part 3 Product Analysis: Average price by category
category_performance = order_items_products_merge_df.groupby('category').agg({
    'item_total_price': 'sum'
}).reset_index()

#Part 3 Product Analysis: Product category generates the most revenue
topRevenueCategory = category_performance.nlargest(1,'item_total_price')
print("Product category generates the most revenue")
print(topRevenueCategory)

#Part 3 Product Analysis: Average price by category
print("Average price by category")
average_price_category=order_items_products_merge_df.groupby('category')['item_total_price'].mean()
print(average_price_category)

#Part 3 Product Analysis: Products with low stock
print('Products with low stock')
stock_threshold=10
lowStockProduct=products_df[products_df['stock_quantity']<stock_threshold][['product_id','product_name','category','stock_quantity']]
print(lowStockProduct)

#Part 3 Customer Analysis: Customer distribution by state
print('Customer distribution by state')
state_dist = customers_df['state'].value_counts()
top_state = state_dist.idxmax()
print(state_dist)
print(f'State has the most customers : {top_state}')

#Part 3 Customer Analysis: New customers per month
print('New customers per month')
customers_df['reg_month']=customers_df['registration_date'].dt.to_period('M')
new_customers_monthly=customers_df['reg_month'].value_counts().reset_index().sort_values(['reg_month'],ascending=True)
print(new_customers_monthly)

#Part 3 Customer Analysis: Customer retention rate
purchase_counts = sales_df.groupby('customer_id')['order_id'].nunique()
repeat_customers = purchase_counts[purchase_counts > 1].count()
total_customers = customers_df['customer_id'].nunique()
retention_rate = (repeat_customers / total_customers) * 100
print(f'Customer retention rate : {retention_rate:.2f}')

#Part 3 Customer Analysis: Average order value by customer segment
segment_metrics = sales_df.groupby('customer_segment').agg({
    'total_order_value': 'mean',
    'CLV': 'mean'
}).rename(columns={'total_order_value': 'Avg_Order_Value', 'CLV': 'Avg_CLV'})
print('Average order value by customer segment')
print(segment_metrics)

#Part 3 Customer Analysis: Percentage of customers are Premium segment
print('Percentage of customers are Premium segment')
segment_counts = customers_df['customer_segment'].value_counts(normalize=True) * 100
premium_pct = segment_counts.get('Premium', 0)
print(premium_pct)

#Part 3 Customer Analysis: Average customer lifetime value
total_revenue_per_customer = sales_df.groupby('customer_id')['total_order_value'].sum()
avg_clv = total_revenue_per_customer.mean()
print(f"The Average Customer Lifetime Value : {avg_clv:.2f}")

#Part 3 Customer Analysis: Customers made repeat purchases
print(f"Customers made repeat purchases : {repeat_customers}")

#Part 3 Time Series Analysis: Daily/weekly/monthly sales trends
monthly_sales = sales_df.set_index('order_date').resample('ME')['total_order_value'].sum()
print('Monthly sales trends')
print(monthly_sales)

#Part 3 Time Series Analysis: Seasonal patterns
sales_df['month_name'] = sales_df['order_date'].dt.month_name()
seasonal_monthly = sales_df.groupby('month_name')['total_order_value'].sum().reindex([
    'January', 'February', 'March', 'April', 'May', 'June', 
    'July', 'August', 'September', 'October', 'November', 'December'
])
print('Seasonal patterns')
print(seasonal_monthly)

#Part 3 Time Series Analysis: Year-over-year growth
yearly_revenue = sales_df.set_index('order_date').resample('YE')['total_order_value'].sum()
yoy_growth = yearly_revenue.pct_change() * 100

print("Year-over-year growth")
for year, rev in yearly_revenue.items():
    growth = yoy_growth[year]
    print(f"Year {year.year}: {rev:,.2f} (Growth: {growth:.2f}%)")

#Part 3 Time Series Analysis: Average time between first and second purchase
ordered_purchases = sales_df.sort_values(['customer_id', 'order_date'])
first_second_orders = ordered_purchases.groupby('customer_id').head(2)
first_second_orders['order_rank'] = first_second_orders.groupby('customer_id').cumcount() + 1
purchase_diff = first_second_orders.pivot(index='customer_id', columns='order_rank', values='order_date')
if 2 in purchase_diff.columns:
    days_to_second_order = (purchase_diff[2] - purchase_diff[1]).dt.days.mean()
    print(f"Average time between first and second purchase: {days_to_second_order:.1f} days")

#Part 3 Time Series Analysis: Payment method is most popular
popular_payment = sales_df['payment_method'].value_counts().idxmax()
print(f"Most Popular Payment method: {popular_payment}")

#Part 3 Time Series Analysis: Order cancellation rate
cancel_rate = (sales_df['status'] == 'Cancelled').mean() * 100
print(f"Order cancellation rate: {cancel_rate:.2f}%")

#Part 3 Time Series Analysis: Average discount given
avg_discount = order_items_products_merge_df['discount_percent'].mean()
print(f"Average Discount: {avg_discount:.2f}%")

#Part 3 Review Analysis: Average rating by product
review_data = reviews_df.merge(products_df[['product_id', 'product_name', 'category', 'price']], 
                           on='product_id', 
                           how='left')

#Part 3 Review Analysis: Average rating by product
avg_rating_product = review_data.groupby('product_name')['rating'].mean().sort_values(ascending=False)
print('Average rating by product')
print(avg_rating_product)

#Part 3 Review Analysis: Average rating by category
avg_rating_category = review_data.groupby('category')['rating'].mean().sort_values(ascending=False)
print('Average rating by category')
print(avg_rating_category)

#Part 3 Review Analysis: Products with most reviews
most_reviewed = review_data['product_name'].value_counts().head(10)
print('Products with most reviews')
print(most_reviewed)

#Part 3 Review Analysis: Correlation between price and rating
correlation = review_data['price'].corr(review_data['rating'])
print(f"Correlation between Price and Rating: {correlation:.4f}")
if correlation > 0.1:
    print("Insight: Higher priced products tend to have slightly higher ratings.")
elif correlation < -0.1:
    print("Insight: Higher priced products tend to have lower ratings (higher expectations).")
else:
    print("Insight: There is no significant link between price and rating.")

#Part 4 Cohort Analysis: Group customers by registration month
customers_df['cohort_month'] = customers_df['registration_date'].dt.to_period('M')
sales_df = sales_df.merge(customers_df[['customer_id', 'cohort_month']], on='customer_id', how='left')
sales_df['order_month'] = sales_df['order_date'].dt.to_period('M')
print('Group customers by registration month')
print(sales_df[['customer_id','first_name','last_name','order_month']])

#Part 4 Cohort Analysis: Track purchase behavior over time
sales_df['cohort_index'] = util.get_month_diff(sales_df)
cohort_pivot = sales_df.pivot_table(index='cohort_month', 
                                    columns='cohort_index', 
                                    values='customer_id', 
                                    aggfunc='nunique')
print('Track purchase behavior over time')
print(cohort_pivot)

#Part 4 RFM Analysis: Recency: Days since last purchase
#Part 4 RFM Analysis: Frequency: Number of orders
#Part 4 RFM Analysis: Monetary: Total amount spent

snapshot_date = sales_df['order_date'].max() + pd.Timedelta(days=1)
rfm = sales_df.groupby('customer_id').agg({
    'order_date': lambda x: (snapshot_date - x.max()).days, # Recency
    'order_id': 'nunique',                                 # Frequency
    'total_order_value': 'sum'                                   # Monetary
}).reset_index()
rfm.columns = ['customer_id', 'Recency', 'Frequency', 'Monetary']
rfm['R_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
rfm['M_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])
rfm[['R_Score', 'F_Score', 'M_Score']] = rfm[['R_Score', 'F_Score', 'M_Score']].astype(int)

#Part 4 RFM Analysis: Create customer segments based on RFM
rfm['Segment'] = rfm.apply(util.assign_segment, axis=1)
segment_counts = rfm['Segment'].value_counts().reset_index()
print('Customer segments based on RFM (Recency, Frequency, Monetary)')
print(segment_counts)

#Part 4 Product Affinity: Products are often bought together
order_groups = order_items_products_merge_df.groupby('order_id')['product_name'].apply(list)
item_pairs = []
for items in order_groups:
    if len(items) > 1:
        items.sort()
        item_pairs.extend(list(combinations(items, 2)))
pair_counts = Counter(item_pairs)
mba_df = pd.DataFrame(pair_counts.most_common(10), columns=['Product_Pair', 'Frequency'])

print('Top 10 Products Frequently Bought Together')
print(mba_df)

#Part 4 Product Affinity: Simple market basket analysis
total_orders = orders_df['order_id'].nunique()
mba_df['Support_%'] = (mba_df['Frequency'] / total_orders) * 100
print('Simple market basket analysis')
print(mba_df[['Product_Pair', 'Frequency', 'Support_%']])

#Part 4 Churn Analysis: Customers who haven't purchased in 6+ months
churn_threshold = 180
churned_customers_df = rfm[rfm['Recency'] >= churn_threshold].copy()
print(f"Churned Customers (No purchase in {churn_threshold}+ days)")
print(churned_customers_df[['customer_id', 'Recency', 'Monetary']].head())

#Part 4 Churn Analysis: Calculate churn rate
total_customers = rfm['customer_id'].nunique()
num_churned = len(churned_customers_df)
churn_rate = (num_churned / total_customers) * 100
print(f"Total Customers: {total_customers}")
print(f"Number of Churned Customers: {num_churned}")
print(f"Overall Churn Rate: {churn_rate:.2f}%")