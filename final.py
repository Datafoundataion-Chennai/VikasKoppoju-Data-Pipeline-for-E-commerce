import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import matplotlib.pyplot as plt
import logging
import random
import folium
from streamlit_folium import folium_static
from streamlit_folium import st_folium
import matplotlib.cm as cm
import numpy as np
import math
# Set up logging
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Google BigQuery authentication
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
    
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select Page", ["Overall Tables", "Individual Tables"])

st.title("Event-driven Data Pipeline for E-commerce ✨")
logging.info("Streamlit app started")

if not page == "Overall Tables":
    page = st.sidebar.radio("Go to", ["Customers","Geolocation","Order Items","Payments", "Reviews", "Orders","Products"])
    
    def Customers():
        st.write("tables")

        cus = fetch_data(f"""
        SELECT customer_id, 
            customer_unique_id, 
            customer_zip_code_prefix, 
            customer_city, 
            customer_state
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`
        ORDER BY customer_city;
        """)

        st.sidebar.title("Filters")
        # Pagination Setup
        records_per_page = 10
        total_pages = math.ceil(len(cus) / records_per_page) if len(cus) > 0 else 1

        # Filters
        city_filter = st.sidebar.selectbox("Filter by City", ["All"] + sorted(cus["customer_city"].dropna().unique()))
        if city_filter != "All":
            cus = cus[cus["customer_city"] == city_filter]

        state_filter = st.sidebar.selectbox("Filter by State", ["All"] + sorted(cus["customer_state"].dropna().unique()))
        if state_filter != "All":
            cus = cus[cus["customer_state"] == state_filter]

        # Get current page index
        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        # Paginate Data
        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = cus.iloc[start_idx:end_idx].reset_index(drop=True)
        
        # Fix index to start from 1 instead of 0
        paginated_df.index = range(start_idx + 1, end_idx + 1)
        if not paginated_df.empty:
            paginated_df.index = range(start_idx + 1, start_idx + 1 + len(paginated_df))

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)

        # Bar Chart - Customers per State
        st.subheader("Customer Distribution by State")
        if not cus.empty:
            state_counts = cus["customer_state"].value_counts().reset_index()
            state_counts.columns = ["State", "Count"]
            st.bar_chart(state_counts.set_index("State"))
        else:
            st.warning("No data available for the selected filters.")
    def Geolocation():
        st.write("Geolocation Table")

        geo = fetch_data(f"""
        SELECT geolocation_zip_code_prefix, 
            geolocation_lat, 
            geolocation_lng, 
            geolocation_city, 
            geolocation_state
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_geolocation_dataset`
        ORDER BY geolocation_city;
        """)

        # Rename columns for Streamlit compatibility
        geo = geo.rename(columns={"geolocation_lat": "latitude", "geolocation_lng": "longitude"})

        st.sidebar.title("Filters")
        records_per_page = 10
        total_pages = math.ceil(len(geo) / records_per_page) if len(geo) > 0 else 1

        city_filter = st.sidebar.selectbox("Filter by City", ["All"] + sorted(geo["geolocation_city"].dropna().unique()))
        if city_filter != "All":
            geo = geo[geo["geolocation_city"] == city_filter]

        state_filter = st.sidebar.selectbox("Filter by State", ["All"] + sorted(geo["geolocation_state"].dropna().unique()))
        if state_filter != "All":
            geo = geo[geo["geolocation_state"] == state_filter]

        # Pagination
        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)
        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = geo.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = range(start_idx + 1, end_idx + 1)

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)

        # Display map
        st.map(geo[["latitude", "longitude"]].dropna())

    def OrderItems():
        st.write("Order Items Table")

        orders = fetch_data(f"""
        SELECT order_id, 
               order_item_id, 
               product_id, 
               seller_id, 
               shipping_limit_date, 
               price, 
               freight_value
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset`
        ORDER BY order_id;
        """)

        st.sidebar.title("Filters")
        records_per_page = 10
        total_pages = math.ceil(len(orders) / records_per_page) if len(orders) > 0 else 1

        product_filter = st.sidebar.text_input("Filter by Product ID")
        if product_filter:
            orders = orders[orders["product_id"].str.contains(product_filter, na=False)]

        seller_filter = st.sidebar.text_input("Filter by Seller ID")
        if seller_filter:
            orders = orders[orders["seller_id"].str.contains(seller_filter, na=False)]

        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = orders.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = range(start_idx + 1, end_idx + 1)

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)
    def Payments():
        st.write("Payments Table")

        payments = fetch_data(f"""
        SELECT order_id, 
            COALESCE(payment_type, 'Unknown') AS payment_type, 
            payment_installments, 
            payment_value
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
        ORDER BY payment_value DESC;
        """)

        st.write("Columns in Payments Dataset:", payments.columns.tolist())  # Debugging step

        st.sidebar.title("Filters")
        records_per_page = 10
        total_pages = math.ceil(len(payments) / records_per_page) if len(payments) > 0 else 1

        payment_type_filter = st.sidebar.selectbox("Filter by Payment Type", ["All"] + sorted(payments["payment_type"].dropna().unique()))
        if payment_type_filter != "All":
            payments = payments[payments["payment_type"] == payment_type_filter]

        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = payments.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = range(start_idx + 1, end_idx + 1)

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)

    def Reviews():
        st.write("Reviews Table")

        reviews = fetch_data(f"""
        SELECT review_id, order_id, review_score, review_comment_title, 
               review_comment_message, review_creation_date, review_answer_timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset`
        ORDER BY review_creation_date DESC;
        """)

        st.sidebar.title("Filters")
        records_per_page = 10
        total_pages = math.ceil(len(reviews) / records_per_page) if len(reviews) > 0 else 1

        review_score_filter = st.sidebar.selectbox("Filter by Review Score", ["All"] + sorted(reviews["review_score"].dropna().unique()))
        if review_score_filter != "All":
            reviews = reviews[reviews["review_score"] == int(review_score_filter)]

        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = reviews.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = range(start_idx + 1, end_idx + 1)

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)
    def Orders():
        st.write("Orders Table")

        orders = fetch_data(f"""
        SELECT order_id, customer_id, order_status, order_purchase_timestamp, 
               order_approved_at, order_delivered_carrier_date, 
               order_delivered_customer_date, order_estimated_delivery_date
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        ORDER BY order_purchase_timestamp DESC;
        """)

        st.sidebar.title("Filters")
        records_per_page = 10
        total_pages = math.ceil(len(orders) / records_per_page) if len(orders) > 0 else 1

        order_status_filter = st.sidebar.selectbox("Filter by Order Status", ["All"] + sorted(orders["order_status"].dropna().unique()))
        if order_status_filter != "All":
            orders = orders[orders["order_status"] == order_status_filter]

        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = orders.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = range(start_idx + 1, end_idx + 1)

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)
    def Products():
        st.title("Products Table")

        
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
        ORDER BY product_category_name;
        """

        products = fetch_data(query)

        #  Check if the DataFrame is empty
        if products.empty:
            st.warning("No product data available.")
            return  # Exit function early

        st.sidebar.title("Filters")

        #  Check if the column exists before filtering
        if "product_category_name" in products.columns:
            category_filter = st.sidebar.selectbox(
                "Filter by Category", 
                ["All"] + sorted(products["product_category_name"].dropna().unique())
            )
            if category_filter != "All":
                products = products[products["product_category_name"] == category_filter]
        else:
            st.sidebar.warning("Category data not available.")
            category_filter = "All"

        #  Pagination Setup
        records_per_page = 10
        total_pages = math.ceil(len(products) / records_per_page) if len(products) > 0 else 1

        # Get current page index
        page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

        # Paginate Data
        start_idx = (page_num - 1) * records_per_page
        end_idx = start_idx + records_per_page
        paginated_df = products.iloc[start_idx:end_idx].reset_index(drop=True)

        #  Fix index to start from 1 instead of 0
        paginated_df.index = range(start_idx + 1, min(end_idx + 1, len(products) + 1))

        st.write(f"Showing page {page_num} of {total_pages}")
        st.dataframe(paginated_df)

        #  Bar Chart - Products per Category
        st.subheader("Product Distribution by Category")
        if "product_category_name" in products.columns and not products.empty:
            category_counts = products["product_category_name"].value_counts().reset_index()
            category_counts.columns = ["Category", "Count"]
            st.bar_chart(category_counts.set_index("Category"))
        else:
            st.warning("No category data available for visualization.")

        #  Additional Plotting with Matplotlib
        st.subheader("Product Weight Distribution")
        if "product_weight_g" in products.columns and not products["product_weight_g"].dropna().empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.hist(products["product_weight_g"].dropna(), bins=30, edgecolor="black")
            ax.set_xlabel("Weight (g)")
            ax.set_ylabel("Count")
            ax.set_title("Distribution of Product Weights")
            st.pyplot(fig)
        else:
            st.warning("No weight data available for plotting.")

    if page == "Customers":
        Customers()
    elif page == "Geolocation":
        Geolocation()
    elif page == "Order Items":
        OrderItems()
    elif page == "Payments":
        Payments()
    elif page == "Reviews":
        Reviews()
    elif page == "Orders":
        Orders()
    elif page == "Products":
        Products()

else:
    
    total_orders = fetch_data(f"SELECT COUNT(DISTINCT order_id) AS total_orders FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`")
    total_revenue = fetch_data(f"SELECT SUM(payment_value) AS total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
    avg_order_value = fetch_data(f"SELECT SUM(payment_value) / COUNT(DISTINCT order_id) AS avg_order_value FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`")
    total_customers = fetch_data(f"SELECT COUNT(DISTINCT customer_unique_id) AS total_customers FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`")


    # Create column's to represent stats
    col1, col2 = st.columns(2)
    with col1:
        st.metric(f"📦 Total Orders",total_orders['total_orders'][0])
        st.metric(":moneybag: Total Revenue", f"${total_revenue['total_revenue'][0]:,.2f}")
    with col2:
        st.metric(f":busts_in_silhouette: Total Customers",total_customers['total_customers'][0])
        st.metric("🛒 Avg Order Value", f"${avg_order_value['avg_order_value'][0]:,.2f}")
    logging.info(f"Displayed Metrics - Total Orders: {total_orders}, Total Revenue: {total_revenue}, "
                    f"Total Customers: {total_customers}, Avg Order Value: {avg_order_value}")

    # Sidebar Filters
    st.sidebar.header("Filters")
    order_status_options = ["All", "delivered", "shipped", "canceled"]
    selected_status = st.sidebar.selectbox("Filter by Order Status", order_status_options)

    date_range = st.sidebar.date_input("Select Date Range", [])

    # Orders Over Time
    st.header("📅 Orders Over Time")
    query = f"""
        SELECT DATE(order_purchase_timestamp) AS order_date, COUNT(order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`"""
    if selected_status != "All":
        query += f" WHERE order_status = '{selected_status}'"
    query += " GROUP BY order_date ORDER BY order_date"
    orders_over_time = fetch_data(query)
    orders_over_time['order_date'] = pd.to_datetime(orders_over_time['order_date'])

    # Apply date filter
    if date_range:
        orders_over_time = orders_over_time[(orders_over_time['order_date'] >= pd.to_datetime(date_range[0])) &
                                            (orders_over_time['order_date'] <= pd.to_datetime(date_range[-1]))]

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.plot(orders_over_time['order_date'], orders_over_time['order_count'], linestyle='-', color='tab:purple')
    ax.set_title("Orders Over Time")
    ax.set_xlabel("Order Date")
    ax.set_ylabel("Number of Orders")
    ax.grid(True, linestyle='--', alpha=0.6)
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # Revenue Over Time
    st.header("💰 Revenue Over Time")
    query = f"""
        SELECT DATE(order_purchase_timestamp) AS order_date, SUM(payment_value) AS revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` USING(order_id)
        GROUP BY order_date ORDER BY order_date
    """
    revenue_over_time = fetch_data(query)
    revenue_over_time['order_date'] = pd.to_datetime(revenue_over_time['order_date'])

    if date_range:
        revenue_over_time = revenue_over_time[(revenue_over_time['order_date'] >= pd.to_datetime(date_range[0])) &
                                            (revenue_over_time['order_date'] <= pd.to_datetime(date_range[-1]))]

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.plot(revenue_over_time['order_date'], revenue_over_time['revenue'], linestyle='-', color='tab:green')
    ax.set_title("Revenue Over Time")
    ax.set_xlabel("Order Date")
    ax.set_ylabel("Revenue")
    ax.grid(True, linestyle='--', alpha=0.6)
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # Payment Type Distribution
    st.header("💳 Payment Type Distribution")
    payment_types = fetch_data(f"""
        SELECT payment_type, COUNT(payment_type) AS count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
        GROUP BY payment_type ORDER BY count DESC
    """)
    colors = ["#FF9999", "#66B3FF", "#99FF99", "#FFCC99", "#FFD700"]
    random.shuffle(colors)
    fig, ax = plt.subplots()
    wedges, texts, autotexts = ax.pie(payment_types["count"], labels=payment_types["payment_type"], autopct="%1.1f%%", colors=colors)
    ax.set_title("Payment Type Distribution")
    st.pyplot(fig)




    # Streamlit Sidebar Filters
    st.header("📦 Top Selling Product Categories")
    num_categories = st.sidebar.slider("Select number of top categories", 1, 10, 5)

    # Fetch data with the filter
    top_categories = fetch_data(f"""
        SELECT product_category_name, COUNT(product_id) AS product_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
        GROUP BY product_category_name ORDER BY product_count DESC LIMIT {num_categories}
    """)

    # Plot using Matplotlib
    fig, ax = plt.subplots(figsize=(10, 7))  # Increased height
    ax.bar(top_categories["product_category_name"], top_categories["product_count"], color="tab:purple")  # Changed color
    ax.set_title("Top Selling Product Categories", fontsize=14, fontweight='bold')
    ax.set_xlabel("Product Category", fontsize=12)
    ax.set_ylabel("Number of Products Sold", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Display in Streamlit
    st.pyplot(fig)

    # num_categories = st.sidebar.slider("Select number of top categories", 1, 10, 5)
    st.header("📦 Delivery Time Distribution")
    # Fetch histogram data
    histo = fetch_data(f"""
        SELECT order_id, TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS delivery_days
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
        WHERE order_delivered_customer_date 
        IS NOT NULL AND order_purchase_timestamp IS NOT NULL
    """)   
    num_bins = st.sidebar.slider("Select number of bins for histogram", 5, 50, 20)

    # Plot histogram using Matplotlib
    fig_hist, ax_hist = plt.subplots(figsize=(10, 6))  # Increased height
    ax_hist.hist(histo["delivery_days"], bins=num_bins, color="tab:red", edgecolor="black", alpha=0.7)  # Changed color to red
    ax_hist.set_title("Delivery Time Distribution", fontsize=14, fontweight='bold')
    ax_hist.set_xlabel("Delivery Time (Days)", fontsize=12)
    ax_hist.set_ylabel("Deliveries Count", fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # Display in Streamlit
    st.pyplot(fig_hist)


    st.header("🌟 Top 10 Product Categories with the Most Reviews")
    num_reviewed_categories = st.sidebar.slider("Select number of top reviewed categories", 1, 10, 10)

    top_reviewed_categories = fetch_data(f"""
        SELECT p.product_id,p.product_category_name, COUNT(r.review_id) AS review_count
        FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset` r
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` oi ON r.order_id = oi.order_id
        JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` p ON oi.product_id = p.product_id
        GROUP BY p.product_id, p.product_category_name
        ORDER BY review_count DESC
        LIMIT {num_reviewed_categories}
    """)

    # Plot using Matplotlib
    fig_reviews, ax_reviews = plt.subplots(figsize=(10, 7))  # Increased height
    ax_reviews.bar(top_reviewed_categories["product_category_name"], top_reviewed_categories["review_count"], color="tab:green")
    ax_reviews.set_title("🌟 Top 10 Product Categories with the Most Reviews", fontsize=14, fontweight='bold')
    ax_reviews.set_xlabel("Product Category", fontsize=12)
    ax_reviews.set_ylabel("Number of Reviews", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    st.pyplot(fig_reviews)





    st.header("Top 10 Orders by Payment Value")
    # Fetch Top 10 Orders by Payment Value
    top_ten_orders = fetch_data(f"""
        SELECT o.order_id, p.product_category_name, SUM(op.payment_value) AS total_payment_value
    FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset` AS op
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` AS o ON op.order_id = o.order_id
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` AS oi ON o.order_id = oi.order_id
    JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` AS p ON oi.product_id = p.product_id
    GROUP BY o.order_id, p.product_category_name
    ORDER BY total_payment_value DESC
    LIMIT 10;
    """)

    # Check if data is available
    if not top_ten_orders.empty:
        # Generate a vibrant color palette
        num_categories = len(top_ten_orders)
        colors = cm.get_cmap('viridis', num_categories)(np.linspace(0, 1, num_categories))

        # Create Pie Chart
        fig_top10, ax_top10 = plt.subplots()
        ax_top10.pie(
            top_ten_orders['total_payment_value'], 
            labels=top_ten_orders['product_category_name'], 
            autopct='%1.1f%%', 
            startangle=90, 
            colors=colors,  
            wedgeprops={'edgecolor': 'black'}
        )
        ax_top10.set_title("Top 10 Orders by Payment Value")

        st.pyplot(fig_top10)
    else:
        st.warning("No data available for the top 10 orders by payment value.")


