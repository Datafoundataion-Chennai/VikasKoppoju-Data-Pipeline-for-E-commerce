import streamlit as st
from google.cloud import bigquery 
import os
import pandas as pd 
import plotly.express as px 
import matplotlib.pyplot as plt
import logging

logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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


total_orders = fetch_data(f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`")


total_revenue = fetch_data(f"SELECT SUM(payment_value) AS total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
avg_order_value = fetch_data(f"SELECT SUM(payment_value) / COUNT(DISTINCT order_id) AS avg_order_value FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
total_customers = fetch_data(f"SELECT COUNT(DISTINCT customer_unique_id) AS total_customers FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`")



col1, col2 = st.columns(2)
with col1:
    st.metric(f":package: Total Orders",total_orders['total_orders'][0])
    st.metric(":moneybag: Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
with col2:
    st.metric(f":busts_in_silhouette: Total Customers",total_customers['total_customers'][0])
    st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")
logging.info(f"Displayed Metrics - Total Orders: {total_orders}, Total Revenue: {total_revenue}, "
                 f"Total Customers: {total_customers}, Avg Order Value: {avg_order_value}")


logging.info("Fetching Orders Over Time Data")
orders_over_time = fetch_data(f"""
     SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
     GROUP BY order_date ORDER BY order_date
""")


logging.info("Fetching Orders Over Time Data")
orders_over_time = fetch_data(f"""
     SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count 
     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
     GROUP BY order_date ORDER BY order_date
""")
logging.info("Fetched Orders Over Time Data")
fig_orders = px.line(orders_over_time, x="order_date", y="order_count",title="📅 Orders Over Time",labels={"order_date": "Order Date", "order_count": "Number of Orders"} )
st.plotly_chart(fig_orders)

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

    filtered_df = fetch_data(query)

    # Pagination Setup
    rows_per_page = 10
    if "page_number" not in st.session_state:
        st.session_state.page_number = 1

    def next_page(total_pages):
        if st.session_state.page_number < total_pages:
            st.session_state.page_number += 1

    def prev_page():
        if st.session_state.page_number > 1:
            st.session_state.page_number -= 1

    # Display Filtered Data with Pagination
    st.subheader("🎯 Filtered Data")
    if filtered_df.empty:
        st.write("No data found for the selected filters.")
    else:
        total_pages = max((len(filtered_df) // rows_per_page) + (len(filtered_df) % rows_per_page > 0), 1)
        start_idx = (st.session_state.page_number - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, len(filtered_df))
        df_display = filtered_df.iloc[start_idx:end_idx].copy()
        st.dataframe(df_display, use_container_width=True)

        # Pagination Controls
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.session_state.page_number > 1:
                st.button("⬅ Previous", on_click=prev_page)
        with col2:
            st.write(f"Page {st.session_state.page_number} of {total_pages}")
        with col3:
            if st.session_state.page_number < total_pages:
                st.button("Next ➡", on_click=lambda: next_page(total_pages))

# st.sidebar.header("📊 Filters")
# date_range = st.sidebar.date_input("Select Date Range", [])
# city_filter = st.sidebar.text_input("Enter City Name")
# category_filter = st.sidebar.text_input("Enter Product Category")

# if st.sidebar.button("Apply Filters"):
#     query = f"""
#         SELECT order_purchase_timestamp, customer_city, product_category_name, payment_value 
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
#         JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` USING(customer_id)
#         JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
#         JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` USING(order_id)
#         JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` USING(product_id) 
        
#         WHERE 1=1
#     """
#     if city_filter:
#         query += f" AND customer_city LIKE '%{city_filter}%'"
#     if category_filter:
#         query += f" AND product_category_name LIKE '%{category_filter}%'"
#     if len(date_range) == 2:
#         query += f" AND DATE(order_purchase_timestamp) BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
#     query += " ORDER BY order_purchase_timestamp"

#     filtered_data = fetch_data(query)
#     st.dataframe(filtered_data)






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
        title="Top 10 Product Categories with the Most Reviews",
        labels={"product_category_name": "Product Category", "review_count": "Number of Reviews"},
        color="review_count",
        color_continuous_scale="blues"
    )


histo=fetch_data(f"""
    SELECT 
    order_id,
    TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS delivery_days
FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
WHERE order_delivered_customer_date IS NOT NULL AND order_purchase_timestamp IS NOT NULL
""")   
fig = px.histogram(histo, x="delivery_days", nbins=20, title="Delivery Time Distribution",
                   labels={"delivery_days": "Delivery Time (Days)","count":"Deliveries Count"},
                   color_discrete_sequence=["#636EFA"])


# Display chart
# st.plotly_chart(fig, use_container_width=True,key="unique_key_1")
st.plotly_chart(fig)



mapl=fetch_data(
    f"""
SELECT 
    g.geolocation_lat AS latitude,
    g.geolocation_lng AS longitude,
    c.customer_city AS city,
    COUNT(o.order_id) AS order_count
FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` c
JOIN `{PROJECT_ID}.{DATASET_ID}.olist_geolocation_dataset` g 
ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o 
ON c.customer_id = o.customer_id
GROUP BY city, latitude, longitude
HAVING COUNT(o.order_id) > 5  -- Show cities with >5 orders
ORDER BY order_count DESC
"""
)
st.title("📍 Customers Per City")

if not mapl.empty:
    # Create Map
    fig = px.scatter_mapbox(mapl, 
                            lat="latitude", 
                            lon="longitude", 
                            size="order_count",  
                            color="order_count",  
                            color_continuous_scale="Cividis",  # Lighter Blue Color Palette
                            hover_name="city", 
                            hover_data={
                                "city": True,
                                "order_count": True, 
                                "latitude": False, 
                                "longitude": False
                            },
                            zoom=5,
                            opacity=0.75,  # Increase opacity for better visibility
                            mapbox_style="open-street-map",  # Brighter Background
                            title="Orders Per City")
    # Show Map
    st.plotly_chart(fig, use_container_width=True)
else:
    st.write("⚠️ No data available!")


order_status=fetch_data(f"""
SELECT 
    order_status,
    COUNT(order_id) AS order_count
FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
GROUP BY order_status
ORDER BY order_count DESC
""")
st.title("📊 Order Status Breakdown")

if not order_status.empty:
    # Pie Chart with Bright Colors
    fig = px.pie(order_status, 
                 names="order_status", 
                 values="order_count", 
                 color="order_status",
                 color_discrete_map={
                     "delivered": "#2E91E5",
                     "shipped": "#E15F99",
                     "canceled": "#1CA71C",
                     "unavailable": "#FB0D0D",
                     "processing": "#FFD700",
                     "invoiced": "#FFA07A",
                     "created": "#8A2BE2",
                     "approved": "#FF69B4"
                 },
                 hole=0.4,  # Donut-style pie chart
                 title="Order Status Distribution")

    # Show Chart
    st.plotly_chart(fig, use_container_width=True)
    
    # Show Data Table
    st.subheader("📋 Order Status Data")
    st.dataframe(order_status, use_container_width=True)
    
else:
    st.write("⚠️ No data available!")




# cus_sell=fetch_data(f"""
# SSELECT 
#     g.geolocation_lat AS latitude,
#     g.geolocation_lng AS longitude,
#     c.customer_city AS city,
#     COUNT(o.order_id) AS order_count,
#     'Customer' AS type
# FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers` c
# JOIN `{PROJECT_ID}.{DATASET_ID}.olist_geolocation` g 
# ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
# LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o 
# ON c.customer_id = o.customer_id
# GROUP BY city, latitude, longitude

# UNION ALL

# SELECT 
#     g.geolocation_lat AS latitude,
#     g.geolocation_lng AS longitude,
#     s.seller_city AS city,
#     COUNT(oi.order_id) AS order_count,
#     'Seller' AS type
# FROM `{PROJECT_ID}.{DATASET_ID}.olist_sellers` s
# JOIN `{PROJECT_ID}.{DATASET_ID}.olist_geolocation` g 
# ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
# JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items` oi 
# ON s.seller_id = oi.seller_id
# JOIN `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o 
# ON oi.order_id = o.order_id
# GROUP BY city, latitude, longitude
# """)
# st.title("📍 Geographical Distribution of Customers & Sellers")

# if not cus_sell.empty:
#     # Create Brighter Interactive Map
#     fig = px.scatter_mapbox(cus_sell, 
#                             lat="latitude", 
#                             lon="longitude", 
#                             size="order_count",  
#                             color="type",  # Different colors for Customers & Sellers
#                             color_discrete_map={"Customer": "#636EFA", "Seller": "#EF553B"},  # Blue & Red
#                             hover_name="city", 
#                             hover_data={"city": True, "order_count": True, "latitude": False, "longitude": False},
#                             zoom=4,
#                             opacity=0.8,  
#                             mapbox_style="carto-positron",  # Light-colored map for better contrast
#                             title="Customers & Sellers Distribution")

#     # Show Map
#     st.plotly_chart(fig, use_container_width=True)
    
#     # Show Data Table for Detailed View
#     st.subheader("📊 City-Level Data")
#     st.dataframe(cus_sell, use_container_width=True)
    
# else:
#     st.write("⚠️ No data available!")























st.subheader("Have a glance on the Data")
option = st.selectbox(
    "Which table data you want",
    ("olist_orders_dataset", "olist_products_dataset", "olist_order_items_dataset","olist_order_payments_dataset","olist_customers_dataset","olist_geolocation_dataset"),
    index=None,
    placeholder="Select contact method...",
)
if option:
    column_query = f"""
        SELECT column_name 
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` 
        WHERE table_name='{option}'
    """
    columns_df = fetch_data(column_query)
    

    if not columns_df.empty:
        columns = columns_df["column_name"].tolist()

        sort_column = st.selectbox(
            "Select column to sort by",
            columns,
            placeholder="Select column..."
        )

        sort_order = st.radio("Sort order", ("Ascending", "Descending"), index=1)

        query = f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{option}`
            ORDER BY `{sort_column}` {'ASC' if sort_order == 'Ascending' else 'DESC'}
            LIMIT 100
        """ if sort_column else f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{option}`
            LIMIT 100
        """

        data = fetch_data(query)
        data.reset_index(drop=True, inplace=True)
        data.index += 1  # Start index from 1

        # Dynamically rename columns
        data.columns = [col.replace("_", " ").title() for col in data.columns]

        st.write(data)
    else:
        st.warning("No columns found for the selected table.")




# st.write(query)
st.subheader("ER Diagram of Dataset")
st.image("./data/image/ER.jpeg", caption="Relations of the Dataset")