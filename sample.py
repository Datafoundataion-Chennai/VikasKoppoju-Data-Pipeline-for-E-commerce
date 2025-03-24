# Imports
import streamlit as st # For Visuliazation
from google.cloud import bigquery # To handle data
import os
import pandas as pd # For File Handling
import plotly.express as px # Plotting of data
import matplotlib.pyplot as plt
import logging
# Set up logging
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#  Get key from Google Bigquery to get access
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"
# Connect with remote client
client = bigquery.Client()
# Project Name in Bigquery in GCP
PROJECT_ID = "valued-ceiling-454014-a9"
# Dataset ID
DATASET_ID = "sam1"
# Reuses the cache to load the contents
@st.cache_data
def fetch_data(query):
    try:
        logging.info(f"Executing query: {query}")
        return client.query(query).to_dataframe()
    except Exception as e:
        logging.error(f"Query execution failed: {e}")
        return pd.DataFrame()

# Steamlit code starts
st.title(":shopping_trolley: Event-driven Data Pipeline for E-commerce")
logging.info("Streamlit app started")
# Queries for Statstics

# total orders

# Finds total orders from olist_orders table
total_orders = fetch_data(f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`")


total_revenue = fetch_data(f"SELECT SUM(payment_value) AS total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
avg_order_value = fetch_data(f"SELECT SUM(payment_value) / COUNT(DISTINCT order_id) AS avg_order_value FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
total_customers = fetch_data(f"SELECT COUNT(DISTINCT customer_unique_id) AS total_customers FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`")


# Create column's to represent stats
col1, col2 = st.columns(2)
with col1:
    st.metric(f":package: Total Orders",total_orders['total_orders'][0])
    st.metric(":moneybag: Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
with col2:
    st.metric(f":busts_in_silhouette: Total Customers",total_customers['total_customers'][0])
    st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")
logging.info(f"Displayed Metrics - Total Orders: {total_orders}, Total Revenue: {total_revenue}, "
                 f"Total Customers: {total_customers}, Avg Order Value: {avg_order_value}")


# Plotting for Orders Over the Time Uses olist orders dataset
logging.info("Fetching Orders Over Time Data")
orders_over_time = fetch_data(f"""
     SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
     GROUP BY order_date ORDER BY order_date
""")
logging.info("Fetched Orders Over Time Data")
fig_orders = px.line(orders_over_time, x="order_date", y="order_count",title="📅 Orders Over Time",labels={"order_date": "Order Date", "order_count": "Number of Orders"} )
st.plotly_chart(fig_orders)

# Plotting for Revenue Over the Time from orders and payments dataset
logging.info("Fetching Revenue Over Time Data")
revenue_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, SUM(payment_value) AS revenue 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
    GROUP BY order_date ORDER BY order_date
""")
logging.info("Fetched Orders Over Time Data")
fig_revenue = px.line(revenue_over_time, x="order_date", y="revenue", title="💰 Revenue Over Time", labels={"order_date":"Order Date","revenue":"Revenue"})
st.plotly_chart(fig_revenue)


# Plotting for Payment Type Distribution in pie
payment_types = fetch_data(f"""
    SELECT payment_type, COUNT(payment_type) AS count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
    GROUP BY payment_type ORDER BY count DESC
""")
fig_payments = px.pie(
    payment_types, 
    names="payment_type", 
    values="count", 
    title="💳 Payment Type Distribution",
    hover_data={"count": True},  
)
fig_payments.update_traces(
    hovertemplate="<b>%{label}</b>: %{value} orders<br>Percentage: %{percent}"
)
st.plotly_chart(fig_payments)


# Plotting for Top selling categories
top_categories = fetch_data(f"""
    SELECT product_category_name, COUNT(product_id) AS product_count
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
    GROUP BY product_category_name ORDER BY product_count DESC LIMIT 5
""")

fig_categories = px.bar(top_categories, x="product_category_name", y="product_count", title="📦 Top Selling Product Categories",labels={
        "product_category_name": "Product Category", 
        "product_count": "Number of Products Sold"  
    })
st.plotly_chart(fig_categories)


# Fliters
st.sidebar.header("📊 Filters")
date_range = st.sidebar.date_input("Select Date Range", [])
city_filter = st.sidebar.text_input("Enter City Name")
category_filter = st.sidebar.text_input("Enter Product Category")

if st.sidebar.button("Apply Filters"):
    query = f"""
        SELECT order_purchase_timestamp, customer_city, product_category_name, payment_value 
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` USING(customer_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` USING(order_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` USING(product_id) 
        
        WHERE 1=1
    """
    if city_filter:
        query += f" AND customer_city = '{city_filter}'"
    if category_filter:
        query += f" AND product_category_name = '{category_filter}'"
    if len(date_range) == 2:
        query += f" AND DATE(order_purchase_timestamp) BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
    query+=f" ORDER BY order_purchase_timestamp"

    filtered_data = fetch_data(query)
    st.dataframe(filtered_data)


# Reviews
query = fetch_data(f"""
SELECT 
    p.product_id,
    p.product_category_name,
    COUNT(r.review_id) AS review_count
FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset` r
JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` oi ON r.order_id = oi.order_id
JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_category_name
ORDER BY review_count DESC
LIMIT 10
""")
fig = px.bar(
        query, 
        x="product_category_name", 
        y="review_count", 
        title="Top 10 Products with the Most Reviews",
        labels={"product_category_name": "Product Category", "review_count": "Number of Reviews"},
        color="review_count",
        color_continuous_scale="blues"
    )
st.plotly_chart(fig)
st.subheader("Have a glance on the Data")
option = st.selectbox(
    "Which table data you want",
    ("olist_orders_dataset", "olist_products_dataset", "olist_order_items_dataset","olist_order_payments_dataset","olist_customers_dataset"),
    index=None,
    placeholder="Select contact method...",
)
if option:
    column_query = f"SELECT column_name FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='{option}'"
    columns_df = fetch_data(column_query)

    if not columns_df.empty:
        columns = columns_df["column_name"].tolist()

        sort_column = st.selectbox(
            "Select column to sort by",
            columns,
            placeholder="Select column..."
        )

        sort_order = st.radio("Sort order", ("Ascending", "Descending"), index=0)

        
        if sort_column:
            query = f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{option}`
            ORDER BY `{sort_column}` {'ASC' if sort_order == 'Ascending' else 'DESC'}
            LIMIT 10
            """
        else:
            
            query = f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{option}`
            LIMIT 10
            """

        data = fetch_data(query)
        st.write(data)
    else:
        st.warning("No columns found for the selected table.")


# st.write(query)
st.subheader("ER Diagram of Dataset")
st.image("./data/image/ER.jpeg", caption="Relations of the Dataset")