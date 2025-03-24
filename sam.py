import streamlit as st
from google.cloud import bigquery 
import os
import pandas as pd 
import plotly.express as px 
import logging

logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"
client = bigquery.Client()

PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sam1"

@st.cache_data
def fetch_data(query):
    try:
        logging.info(f"Executing query: {query}")
        return client.query(query).to_dataframe()
    except Exception as e:
        logging.error(f"Query execution failed: {e}")
        return pd.DataFrame()

st.title(":shopping_trolley: Event-driven Data Pipeline for E-commerce")
logging.info("Streamlit app started")

# Metrics Queries
total_orders = fetch_data(f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`")
total_revenue = fetch_data(f"SELECT SUM(payment_value) AS total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
avg_order_value = fetch_data(f"SELECT SUM(payment_value) / COUNT(DISTINCT order_id) AS avg_order_value FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
total_customers = fetch_data(f"SELECT COUNT(DISTINCT customer_unique_id) AS total_customers FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`")

# Display Metrics
col1, col2 = st.columns(2)
with col1:
    st.metric(f":package: Total Orders", total_orders['total_orders'][0])
    st.metric(":moneybag: Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
with col2:
    st.metric(f":busts_in_silhouette: Total Customers", total_customers['total_customers'][0])
    st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")

# Sidebar Filters
st.sidebar.header("📊 Filters")
date_range = st.sidebar.date_input("Select Date Range", [])
city_filter = st.sidebar.text_input("Enter City Name")
category_filter = st.sidebar.text_input("Enter Product Category")
payment_type_filter = st.sidebar.selectbox("Select Payment Type", ["", "credit_card", "boleto", "voucher", "debit_card", "paypal"])
order_status_filter = st.sidebar.selectbox("Select Order Status", ["", "delivered", "shipped", "processing", "canceled", "unavailable"])

if st.sidebar.button("Apply Filters"):
    query = f"""
        SELECT order_purchase_timestamp, customer_city, product_category_name, payment_type, order_status, payment_value 
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` USING(customer_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` USING(order_id)
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` USING(product_id) 
        WHERE 1=1
    """
    if city_filter:
        query += f" AND customer_city LIKE '%{city_filter}%'"
    if category_filter:
        query += f" AND product_category_name LIKE '%{category_filter}%'"
    if payment_type_filter:
        query += f" AND payment_type = '{payment_type_filter}'"
    if order_status_filter:
        query += f" AND order_status = '{order_status_filter}'"
    if len(date_range) == 2:
        query += f" AND DATE(order_purchase_timestamp) BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
    query += " ORDER BY order_purchase_timestamp"

    filtered_data = fetch_data(query)
    st.dataframe(filtered_data)

# Orders Over Time Visualization
orders_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    GROUP BY order_date ORDER BY order_date
""")
fig_orders = px.line(orders_over_time, x="order_date", y="order_count", title="📅 Orders Over Time")
st.plotly_chart(fig_orders)

# Revenue Over Time Visualization
revenue_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, SUM(payment_value) AS revenue 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
    GROUP BY order_date ORDER BY order_date
""")
fig_revenue = px.line(revenue_over_time, x="order_date", y="revenue", title="💰 Revenue Over Time")
st.plotly_chart(fig_revenue)

# Payment Type Distribution
payment_types = fetch_data(f"""
    SELECT payment_type, COUNT(payment_type) AS count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
    GROUP BY payment_type ORDER BY count DESC
""")
fig_payments = px.pie(payment_types, names="payment_type", values="count", title="💳 Payment Type Distribution")
st.plotly_chart(fig_payments)

# Top Selling Product Categories
top_categories = fetch_data(f"""
    SELECT product_category_name, COUNT(product_id) AS product_count
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
    GROUP BY product_category_name ORDER BY product_count DESC LIMIT 5
""")
fig_categories = px.bar(top_categories, x="product_category_name", y="product_count", title="📦 Top Selling Product Categories")
st.plotly_chart(fig_categories)

st.subheader("ER Diagram of Dataset")
st.image("./data/image/ER.jpeg", caption="Relations of the Dataset")
