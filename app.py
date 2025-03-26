# Imports
import streamlit as st # For Visuliazation
from google.cloud import bigquery # To handle data
import os
import pandas as pd # For File Handling
import plotly.express as px # Plotting of data
import matplotlib.pyplot as plt
import logging
import random

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

st.title(" Event-driven Data Pipeline for E-commerce ✨")
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
    st.metric(f"<h3> 📦 Total Orders </h3>",total_orders['total_orders'][0])
    st.metric(":moneybag: Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
with col2:
    st.metric(f":busts_in_silhouette: Total Customers",total_customers['total_customers'][0])
    st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")
logging.info(f"Displayed Metrics - Total Orders: {total_orders}, Total Revenue: {total_revenue}, "
                 f"Total Customers: {total_customers}, Avg Order Value: {avg_order_value}")


# ----------------------Plottings-------------------------
st.header("📅 Orders Over Time")


# Fetch data
logging.info("Fetching Orders Over Time Data")
orders_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    GROUP BY order_date ORDER BY order_date
""")
logging.info("Fetched Orders Over Time Data")

# Convert order_date to datetime
orders_over_time['order_date'] = pd.to_datetime(orders_over_time['order_date'])

# Filter options
order_status_options = ["All", "delivered", "shipped", "canceled"]
selected_status = st.selectbox("Filter by Order Status", order_status_options)

if selected_status != "All":
    orders_filtered = fetch_data(f"""
        SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        WHERE order_status = '{selected_status}'
        GROUP BY order_date ORDER BY order_date
    """)
else:
    orders_filtered = orders_over_time

# Plot using Matplotlib
fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(orders_filtered['order_date'], orders_filtered['order_count'], linestyle='-', color='tab:purple', label='Number of Orders')
ax.set_title("Orders Over Time", fontsize=14, fontweight='bold')
ax.set_xlabel("Order Date", fontsize=12)
ax.set_ylabel("Number of Orders", fontsize=12)
ax.legend()
ax.grid(True, linestyle='--', alpha=0.6)
plt.xticks(rotation=45)
plt.tight_layout()

# Display the plot
st.pyplot(fig)



st.header("💰 Revenue Over Time")

# Fetch data
logging.info("Fetching Revenue Over Time Data")
revenue_over_time = fetch_data(f"""
    SELECT DATE(order_purchase_timestamp) AS order_date, SUM(payment_value) AS revenue 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
    GROUP BY order_date ORDER BY order_date
""")
logging.info("Fetched Revenue Over Time Data")

# Convert order_date to datetime
revenue_over_time['order_date'] = pd.to_datetime(revenue_over_time['order_date'])

# Plot using Matplotlib
fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(revenue_over_time['order_date'], revenue_over_time['revenue'], linestyle='-', color='tab:green', label='Revenue')
ax.set_title("💰 Revenue Over Time", fontsize=14, fontweight='bold')
ax.set_xlabel("Order Date", fontsize=12)
ax.set_ylabel("Revenue", fontsize=12)
ax.legend()
ax.grid(True, linestyle='--', alpha=0.6)
plt.xticks(rotation=45)
plt.tight_layout()
# Display the plot
st.pyplot(fig)


st.title("💳 Payment Type Distribution")
payment_types = fetch_data(f"""
    SELECT payment_type, COUNT(payment_type) AS count 
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
    GROUP BY payment_type ORDER BY count DESC
""")

# Pie chart


# Generate a random color list
colors = ["#FF9999", "#66B3FF", "#99FF99", "#FFCC99", "#FFD700", "#FF6347", "#8A2BE2", "#20B2AA"]
random.shuffle(colors)
colors = colors[:len(payment_types)]  # Adjust color list to data size

# Reduce figure size
fig, ax = plt.subplots()

# Pie chart with adjusted labels and colors
wedges, texts, autotexts = ax.pie(
    payment_types["count"], 
    labels=payment_types["payment_type"], 
    autopct="%1.1f%%", 
    startangle=140, 
    colors=colors,  # Apply color list
    wedgeprops={"edgecolor": "k"}
)
ax.set_title("Payment Type Distribution")

# Add legend
ax.legend(wedges, payment_types["payment_type"], title="Payment Types", loc="best", bbox_to_anchor=(1, 0.5))

# Display in Streamlit
st.pyplot(fig)
