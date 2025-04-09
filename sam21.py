# import streamlit as st
# from google.cloud import bigquery
# import os
# import pandas as pd
# import matplotlib.pyplot as plt
# import logging
# import random
# import folium
# from streamlit_folium import folium_static
# from streamlit_folium import st_folium
# import matplotlib.cm as cm
# import numpy as np
# import math
# @st.cache_data
# def fetch_data(query):
#     try:
#         logging.info(f"Executing query: {query}")
#         return client.query(query).to_dataframe()
#     except Exception as e:
#         logging.error(f"Query execution failed: {e}")
#         return pd.DataFrame()

# def Individual():
#     page = st.sidebar.radio("Go to", ["Customers","Geolocation","Order Items","Payments", "Reviews", "Orders","Products"])
    
#     def Customers():
#         st.write("tables")

#         cus = fetch_data(f"""
#         SELECT customer_id, 
#             customer_unique_id, 
#             customer_zip_code_prefix, 
#             customer_city, 
#             customer_state
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset`
#         ORDER BY customer_city;
#         """)

#         st.sidebar.title("Filters")
#         # Pagination Setup
#         records_per_page = 10
#         total_pages = math.ceil(len(cus) / records_per_page) if len(cus) > 0 else 1

#         # Filters
#         city_filter = st.sidebar.selectbox("Filter by City", ["All"] + sorted(cus["customer_city"].dropna().unique()))
#         if city_filter != "All":
#             cus = cus[cus["customer_city"] == city_filter]

#         state_filter = st.sidebar.selectbox("Filter by State", ["All"] + sorted(cus["customer_state"].dropna().unique()))
#         if state_filter != "All":
#             cus = cus[cus["customer_state"] == state_filter]

#         # Get current page index
#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         # Paginate Data
#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = cus.iloc[start_idx:end_idx].reset_index(drop=True)

#         # Fix index to start from 1 instead of 0
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)

#         # Bar Chart - Customers per State
#         st.subheader("Customer Distribution by State")
#         if not cus.empty:
#             state_counts = cus["customer_state"].value_counts().reset_index()
#             state_counts.columns = ["State", "Count"]
#             st.bar_chart(state_counts.set_index("State"))
#         else:
#             st.warning("No data available for the selected filters.")
#     def Geolocation():
#         st.write("Geolocation Table")

#         geo = fetch_data(f"""
#         SELECT geolocation_zip_code_prefix, 
#             geolocation_lat, 
#             geolocation_lng, 
#             geolocation_city, 
#             geolocation_state
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_geolocation_dataset`
#         ORDER BY geolocation_city;
#         """)

#         # Rename columns for Streamlit compatibility
#         geo = geo.rename(columns={"geolocation_lat": "latitude", "geolocation_lng": "longitude"})

#         st.sidebar.title("Filters")
#         records_per_page = 10
#         total_pages = math.ceil(len(geo) / records_per_page) if len(geo) > 0 else 1

#         city_filter = st.sidebar.selectbox("Filter by City", ["All"] + sorted(geo["geolocation_city"].dropna().unique()))
#         if city_filter != "All":
#             geo = geo[geo["geolocation_city"] == city_filter]

#         state_filter = st.sidebar.selectbox("Filter by State", ["All"] + sorted(geo["geolocation_state"].dropna().unique()))
#         if state_filter != "All":
#             geo = geo[geo["geolocation_state"] == state_filter]

#         # Pagination
#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)
#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = geo.iloc[start_idx:end_idx].reset_index(drop=True)
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)

#         # Display map
#         st.map(geo[["latitude", "longitude"]].dropna())

#     def OrderItems():
#         st.write("Order Items Table")

#         orders = fetch_data(f"""
#         SELECT order_id, 
#                order_item_id, 
#                product_id, 
#                seller_id, 
#                shipping_limit_date, 
#                price, 
#                freight_value
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset`
#         ORDER BY order_id;
#         """)

#         st.sidebar.title("Filters")
#         records_per_page = 10
#         total_pages = math.ceil(len(orders) / records_per_page) if len(orders) > 0 else 1

#         product_filter = st.sidebar.text_input("Filter by Product ID")
#         if product_filter:
#             orders = orders[orders["product_id"].str.contains(product_filter, na=False)]

#         seller_filter = st.sidebar.text_input("Filter by Seller ID")
#         if seller_filter:
#             orders = orders[orders["seller_id"].str.contains(seller_filter, na=False)]

#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = orders.iloc[start_idx:end_idx].reset_index(drop=True)
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)
#     def Payments():
#         st.write("Payments Table")

#         payments = fetch_data(f"""
#         SELECT order_id, 
#             COALESCE(payment_type, 'Unknown') AS payment_type, 
#             payment_installments, 
#             payment_value
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_payments_dataset`
#         ORDER BY payment_value DESC;
#         """)

#         st.write("Columns in Payments Dataset:", payments.columns.tolist())  # Debugging step

#         st.sidebar.title("Filters")
#         records_per_page = 10
#         total_pages = math.ceil(len(payments) / records_per_page) if len(payments) > 0 else 1

#         payment_type_filter = st.sidebar.selectbox("Filter by Payment Type", ["All"] + sorted(payments["payment_type"].dropna().unique()))
#         if payment_type_filter != "All":
#             payments = payments[payments["payment_type"] == payment_type_filter]

#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = payments.iloc[start_idx:end_idx].reset_index(drop=True)
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)

#     def Reviews():
#         st.write("Reviews Table")

#         reviews = fetch_data(f"""
#         SELECT review_id, order_id, review_score, review_comment_title, 
#                review_comment_message, review_creation_date, review_answer_timestamp
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_order_reviews_dataset`
#         ORDER BY review_creation_date DESC;
#         """)

#         st.sidebar.title("Filters")
#         records_per_page = 10
#         total_pages = math.ceil(len(reviews) / records_per_page) if len(reviews) > 0 else 1

#         review_score_filter = st.sidebar.selectbox("Filter by Review Score", ["All"] + sorted(reviews["review_score"].dropna().unique()))
#         if review_score_filter != "All":
#             reviews = reviews[reviews["review_score"] == int(review_score_filter)]

#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = reviews.iloc[start_idx:end_idx].reset_index(drop=True)
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)
#     def Orders():
#         st.write("Orders Table")

#         orders = fetch_data(f"""
#         SELECT order_id, customer_id, order_status, order_purchase_timestamp, 
#                order_approved_at, order_delivered_carrier_date, 
#                order_delivered_customer_date, order_estimated_delivery_date
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`
#         ORDER BY order_purchase_timestamp DESC;
#         """)

#         st.sidebar.title("Filters")
#         records_per_page = 10
#         total_pages = math.ceil(len(orders) / records_per_page) if len(orders) > 0 else 1

#         order_status_filter = st.sidebar.selectbox("Filter by Order Status", ["All"] + sorted(orders["order_status"].dropna().unique()))
#         if order_status_filter != "All":
#             orders = orders[orders["order_status"] == order_status_filter]

#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = orders.iloc[start_idx:end_idx].reset_index(drop=True)
#         paginated_df.index = range(start_idx + 1, end_idx + 1)

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)
#     def Products():
#         st.title("Products Table")

        
#         query = f"""
#         SELECT 
#             *
#         FROM `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset`
#         ORDER BY product_category_name;
#         """

#         products = fetch_data(query)

#         #  Check if the DataFrame is empty
#         if products.empty:
#             st.warning("No product data available.")
#             return  # Exit function early

#         st.sidebar.title("Filters")

#         #  Check if the column exists before filtering
#         if "product_category_name" in products.columns:
#             category_filter = st.sidebar.selectbox(
#                 "Filter by Category", 
#                 ["All"] + sorted(products["product_category_name"].dropna().unique())
#             )
#             if category_filter != "All":
#                 products = products[products["product_category_name"] == category_filter]
#         else:
#             st.sidebar.warning("Category data not available.")
#             category_filter = "All"

#         #  Pagination Setup
#         records_per_page = 10
#         total_pages = math.ceil(len(products) / records_per_page) if len(products) > 0 else 1

#         # Get current page index
#         page_num = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)

#         # Paginate Data
#         start_idx = (page_num - 1) * records_per_page
#         end_idx = start_idx + records_per_page
#         paginated_df = products.iloc[start_idx:end_idx].reset_index(drop=True)

#         #  Fix index to start from 1 instead of 0
#         paginated_df.index = range(start_idx + 1, min(end_idx + 1, len(products) + 1))

#         st.write(f"Showing page {page_num} of {total_pages}")
#         st.dataframe(paginated_df)

#         #  Bar Chart - Products per Category
#         st.subheader("Product Distribution by Category")
#         if "product_category_name" in products.columns and not products.empty:
#             category_counts = products["product_category_name"].value_counts().reset_index()
#             category_counts.columns = ["Category", "Count"]
#             st.bar_chart(category_counts.set_index("Category"))
#         else:
#             st.warning("No category data available for visualization.")

#         #  Additional Plotting with Matplotlib
#         st.subheader("Product Weight Distribution")
#         if "product_weight_g" in products.columns and not products["product_weight_g"].dropna().empty:
#             fig, ax = plt.subplots(figsize=(10, 5))
#             ax.hist(products["product_weight_g"].dropna(), bins=30, edgecolor="black")
#             ax.set_xlabel("Weight (g)")
#             ax.set_ylabel("Count")
#             ax.set_title("Distribution of Product Weights")
#             st.pyplot(fig)
#         else:
#             st.warning("No weight data available for plotting.")

#     if page == "Customers":
#         Customers()
#     elif page == "Geolocation":
#         Geolocation()
#     elif page == "Order Items":
#         OrderItems()
#     elif page == "Payments":
#         Payments()
#     elif page == "Reviews":
#         Reviews()
#     elif page == "Orders":
#         Orders()
#     elif page == "Products":
#         Products()