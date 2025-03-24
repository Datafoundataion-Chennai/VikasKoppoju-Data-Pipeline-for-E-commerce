import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import plotly.express as px
import logging

# Logging setup
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set up Google BigQuery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"

# Initialize BigQuery Client
client = bigquery.Client()
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sam1"

# Streamlit Page Config
st.set_page_config(page_title="E-commerce Data Dashboard", layout="wide")

# Title
st.title(":shopping_trolley: E-commerce Data Dashboard")

# Function to fetch data from BigQuery with caching
@st.cache_data
def fetch_data(query):
    try:
        logging.info(f"Executing query: {query}")
        return client.query(query).to_dataframe()
    except Exception as e:
        logging.error(f"Query execution failed: {e}")
        return pd.DataFrame()

# Sidebar for user selection
st.sidebar.header("Select Visualization")
option = st.sidebar.selectbox("Choose a dataset to analyze", 
                              ["Customer Distribution", "Orders Over Time", "Seller Locations", "Product Popularity", 
                               "Payment Methods", "Review Scores", "Delivery Time Analysis"])

# 1️⃣ Customer Distribution
if option == "Customer Distribution":
    query = f"""
        SELECT customer_state, COUNT(*) as customer_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_customer_dataset`
        GROUP BY customer_state
    """
    df = fetch_data(query)
    st.subheader("📌 Customer Distribution by State")
    fig = px.bar(df, x="customer_state", y="customer_count", color="customer_count", title="Customer Count by State")
    st.plotly_chart(fig)

# 2️⃣ Orders Over Time
elif option == "Orders Over Time":
    query = f"""
        SELECT DATE_TRUNC(order_purchase_timestamp, MONTH) as order_month, COUNT(*) as order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        GROUP BY order_month
        ORDER BY order_month
    """
    df = fetch_data(query)
    st.subheader("📌 Orders Trend Over Time")
    fig = px.line(df, x="order_month", y="order_count", title="Monthly Order Count")
    st.plotly_chart(fig)

# 3️⃣ Seller Locations
elif option == "Seller Locations":
    query = f"""
        SELECT seller_state, COUNT(*) as seller_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_sellers_dataset`
        GROUP BY seller_state
    """
    df = fetch_data(query)
    st.subheader("📌 Seller Distribution by State")
    fig = px.bar(df, x="seller_state", y="seller_count", color="seller_count", title="Sellers by State")
    st.plotly_chart(fig)

# 4️⃣ Most Popular Products
elif option == "Product Popularity":
    query = f"""
        SELECT p.product_category_name_english, COUNT(o.product_id) as product_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` o
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` p
        ON o.product_id = p.product_id
        GROUP BY p.product_category_name_english
        ORDER BY product_count DESC
        LIMIT 10
    """
    df = fetch_data(query)
    st.subheader("📌 Top 10 Selling Product Categories")
    fig = px.bar(df, x="product_category_name_english", y="product_count", color="product_count", title="Popular Products")
    st.plotly_chart(fig)

# 5️⃣ Payment Methods
elif option == "Payment Methods":
    query = f"""
        SELECT payment_type, COUNT(*) as payment_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
        GROUP BY payment_type
    """
    df = fetch_data(query)
    st.subheader("📌 Payment Method Usage")
    fig = px.pie(df, names="payment_type", values="payment_count", title="Payment Method Distribution")
    st.plotly_chart(fig)

# 6️⃣ Review Scores
elif option == "Review Scores":
    query = f"""
        SELECT review_score, COUNT(*) as review_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset`
        GROUP BY review_score
    """
    df = fetch_data(query)
    st.subheader("📌 Customer Review Scores")
    fig = px.bar(df, x="review_score", y="review_count", color="review_count", title="Review Score Distribution")
    st.plotly_chart(fig)

# 7️⃣ Delivery Time Analysis
elif option == "Delivery Time Analysis":
    query = f"""
        SELECT customer_state, 
               AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)) as avg_delivery_days
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customer_dataset`
        ON olist_orders.customer_id = olist_customer_dataset.customer_id
        GROUP BY customer_state
    """
    df = fetch_data(query)
    st.subheader("📌 Average Delivery Time by State")
    fig = px.bar(df, x="customer_state", y="avg_delivery_days", color="avg_delivery_days", title="Delivery Time Analysis")
    st.plotly_chart(fig)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("🚀 **Developed by Vikas Kumar Koppoju**")
