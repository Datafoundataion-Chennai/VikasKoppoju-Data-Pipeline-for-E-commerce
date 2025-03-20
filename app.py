import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import plotly.express as px

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"

# Initialize BigQuery client
client = bigquery.Client()
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sam1"

# Streamlit App Title
st.title("📊 Olist E-commerce Dashboard")

# --- 📌 Fetch Data from BigQuery ---
@st.cache_data
def fetch_data(query):
    return client.query(query).to_dataframe()

# --- ✅ Key Metrics Queries ---
total_orders = fetch_data(f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`")
total_customers = fetch_data(f"SELECT COUNT(DISTINCT customer_unique_id) AS total_customers FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`")
total_revenue = fetch_data(f"SELECT SUM(payment_value) AS total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
avg_order_value = fetch_data(f"SELECT SUM(payment_value) / COUNT(DISTINCT order_id) AS avg_order_value FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")

# Display Key Metrics
st.metric("📦 Total Orders", total_orders['total_orders'][0])
st.metric("👥 Total Customers", total_customers['total_customers'][0])
st.metric("💰 Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")

# --- 📌 Orders Over Time ---
orders_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    GROUP BY order_date ORDER BY order_date
""")

fig_orders = px.line(orders_over_time, x="order_date", y="order_count", title="📅 Orders Over Time")
st.plotly_chart(fig_orders)

# --- 📌 Revenue Over Time ---
revenue_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, SUM(payment_value) AS revenue 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
    GROUP BY order_date ORDER BY order_date
""")

fig_revenue = px.line(revenue_over_time, x="order_date", y="revenue", title="💰 Revenue Over Time")
st.plotly_chart(fig_revenue)

# --- 📌 Top 5 Cities by Orders ---
top_cities = fetch_data(f"""
    SELECT customer_city, COUNT(order_id) AS total_orders 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` USING(customer_id)
    GROUP BY customer_city ORDER BY total_orders DESC LIMIT 5
""")

fig_cities = px.bar(top_cities, x="customer_city", y="total_orders", title="🏙️ Top 5 Cities by Orders")
st.plotly_chart(fig_cities)

# --- 📌 Payment Type Distribution ---
payment_types = fetch_data(f"""
    SELECT payment_type, COUNT(payment_type) AS count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
    GROUP BY payment_type ORDER BY count DESC
""")

fig_payments = px.pie(payment_types, names="payment_type", values="count", title="💳 Payment Type Distribution")
st.plotly_chart(fig_payments)

# --- 📌 Average Delivery Time by State ---
delivery_times = fetch_data(f"""
    SELECT customer_state, AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)) AS avg_delivery_days
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` USING(customer_id)
    WHERE order_delivered_customer_date IS NOT NULL
    GROUP BY customer_state ORDER BY avg_delivery_days DESC
""")

fig_delivery = px.bar(delivery_times, x="customer_state", y="avg_delivery_days", title="🚚 Avg Delivery Time by State")
st.plotly_chart(fig_delivery)

# --- 📌 Top Selling Product Categories ---
top_categories = fetch_data(f"""
    SELECT product_category_name, COUNT(product_id) AS product_count
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
    GROUP BY product_category_name ORDER BY product_count DESC LIMIT 5
""")

fig_categories = px.bar(top_categories, x="product_category_name", y="product_count", title="📦 Top Selling Product Categories")
st.plotly_chart(fig_categories)

# --- 📌 Product Reviews Analysis ---
reviews = fetch_data(f"""
    SELECT review_score, COUNT(review_id) AS review_count
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset`
    GROUP BY review_score ORDER BY review_score
""")

fig_reviews = px.bar(reviews, x="review_score", y="review_count", title="🌟 Review Score Distribution")
st.plotly_chart(fig_reviews)

# --- 📌 Filters for Custom Analysis ---
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

    filtered_data = fetch_data(query)
    st.dataframe(filtered_data)

# --- 📌 Additional Insights ---
st.subheader("📝 Insights")
st.write("""
- **Most Orders Come from Large Cities** 📍
- **Credit Card is the Most Popular Payment Method** 💳
- **Most Orders are Delivered Within 5-10 Days** 🚚
- **Electronics & Home Products are the Best-Selling Categories** 🏠📱
""")
