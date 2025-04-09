

    # Fetch order location data
    # Fetch Data
    # order_locations = fetch_data(f"""
    #     SELECT c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng, COUNT(o.order_id) AS order_count
    #     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o
    #     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` c ON o.customer_id = c.customer_id
    #     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_geolocation_dataset` g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    #     GROUP BY c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng
    #     ORDER BY order_count DESC
    # """)

    # st.write(order_locations)  # Display DataFrame

    # # Create Folium Map (Centered on Brazil)
    # m = folium.Map(location=[-14.2350, -51.9253], zoom_start=4)

    # # Check if DataFrame is not empty before plotting
    # if not order_locations.empty:
    #     max_orders = order_locations["order_count"].max()
        
    #     for _, row in order_locations.iterrows():
    #         folium.CircleMarker(
    #             location=[row["geolocation_lat"], row["geolocation_lng"]],
    #             radius=max(row["order_count"] / max_orders * 10, 3),  # Normalize radius
    #             color="blue",
    #             fill=True,
    #             fill_color="blue",
    #             fill_opacity=0.6,
    #             popup=f"{row['customer_city']}, {row['customer_state']}\nOrders: {row['order_count']}"
    #         ).add_to(m)
    # else:
    #     st.warning("No data available to display on the map.")

    # # Display map in Streamlit
    # st_folium(m)
    # if st.checkbox("Show Map", False):
    #     folium_static(m)
    # st.subheader("📍 Orders Distribution Across Locations")
    # order_map = folium.Map(location=[-23.5505, -46.6333], zoom_start=5)

    # # Check if DataFrame is not empty
    # if not order_locations.empty:
    #     for _, row in order_locations.iterrows():
    #         folium.CircleMarker(
    #             location=[row["geolocation_lat"], row["geolocation_lng"]],
    #             radius=max(row["order_count"] / 10, 3),  # Ensure radius is visible
    #             popup=f"{row['customer_city']}, {row['customer_state']}: {row['order_count']} Orders",
    #             color="blue",
    #             fill=True,
    #             fill_color="blue"
    #         ).add_to(order_map)
    # else:
    #     st.warning("No data available to display on the map.")

    # # Display Folium Map in Streamlit
    # # folium_static(order_map)

    # from streamlit_folium import st_folium
    # st_folium(order_map)


    # Create Folium Map
    # st.subheader("📍 Top 100 Orders Distribution Across Locations")
    # top_order_map = folium.Map(location=[-23.5505, -46.6333], zoom_start=5)
    # for _, row in top_order_locations.iterrows():
    #     folium.CircleMarker(
    #         location=[row["geolocation_lat"], row["geolocation_lng"]],
    #         radius=row["order_count"] / 10,  # Scale marker size
    #         popup=f"{row['customer_city']}, {row['customer_state']}: {row['order_count']} Orders",
    #         color="blue",
    #         fill=True,
    #         fill_color="blue"
    #     ).add_to(top_order_map)

    # # Display Folium Map in Streamlit
    # folium_static(top_order_map)





    # import folium
    # from folium.plugins import MarkerCluster
    # import streamlit as st

    # # Fetch Data
    # customer_orders = fetch_data(f"""
    #     SELECT c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng, COUNT(o.order_id) AS order_count
    #     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o
    #     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_customers_dataset` c ON o.customer_id = c.customer_id
    #     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_geolocation_dataset` g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    #     GROUP BY c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng
    #     ORDER BY order_count DESC
    # """)

    # st.write(customer_orders)

    # # Check if data is available
    # if not customer_orders.empty:
    #     # Calculate map center
    #     map_center = [customer_orders['geolocation_lat'].mean(), customer_orders['geolocation_lng'].mean()]
    #     m = folium.Map(location=map_center, zoom_start=5, tiles="OpenStreetMap")

    #     # Create a marker cluster
    #     marker_cluster = MarkerCluster().add_to(m)

    #     # Add markers for each city
    #     for _, row in customer_orders.iterrows():
    #         folium.CircleMarker(
    #             location=[row['geolocation_lat'], row['geolocation_lng']],
    #             radius=row['order_count'] / 10,  # Adjust marker size based on order count
    #             color="blue",
    #             fill=True,
    #             fill_color="blue",
    #             fill_opacity=0.6,
    #             popup=folium.Popup(
    #                 f"<b>City:</b> {row['customer_city']} ({row['customer_state']})<br>"
    #                 f"<b>Orders:</b> {row['order_count']}",
    #                 max_width=300
    #             )
    #         ).add_to(marker_cluster)

    #     # Display map in Streamlit
    #     st.components.v1.html(m._repr_html_(), height=600)
    # else:
    #     st.warning("No data available for customer orders.")


#     import streamlit as st
#     import pandas as pd
#     import matplotlib.pyplot as plt
#     import folium
#     from folium.plugins import MarkerCluster

#     # Function to fetch data
#     def fetch_data(query):
#         # Replace this with actual database query execution
#         return pd.DataFrame()  # Dummy return, replace with actual query result

#     # SQL Query for fetching top 5 product categories
#     TOP_CATEGORIES_QUERY = f"""
#     WITH TopCategories AS (
#     SELECT 
#         pcnt.product_category_name_english, 
#         COUNT(o.order_id) AS total_orders
#     FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset` o
#     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_order_items_dataset` oi 
#         ON o.order_id = oi.order_id
#     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_products_dataset` p 
#         ON oi.product_id = p.product_id
#     JOIN `{PROJECT_ID}.{DATASET_ID}.olist_product_category_name_translation` pcnt 
#         ON p.product_category_name = pcnt.product_category_name
#     GROUP BY pcnt.product_category_name_english
#     ORDER BY total_orders DESC
#     LIMIT 5
# )
# SELECT * FROM TopCategories;

#     """

#     # SQL Query for purchases by hour
#     HOURLY_PURCHASES_QUERY = """
#     HourlyPurchases AS (
#         SELECT 
#             p.product_category_name, 
#             EXTRACT(HOUR FROM o.order_purchase_timestamp) AS purchase_hour,
#             COUNT(o.order_id) AS purchase_count
#         FROM `olist_orders_dataset` o
#         JOIN `olist_order_items_dataset` oi ON o.order_id = oi.order_id
#         JOIN `olist_products_datasets` p ON oi.product_id = p.product_id
#         WHERE p.product_category_name IN (SELECT product_category_name FROM TopCategories)
#         GROUP BY p.product_category_name, purchase_hour
#         ORDER BY purchase_hour
#     )
#     SELECT * FROM HourlyPurchases;
#     """

#     # SQL Query for customer locations
#     CUSTOMER_ORDERS_QUERY = """
#     SELECT c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng, COUNT(o.order_id) AS order_count
#     FROM `olist_orders_dataset` o
#     JOIN `olist_customer_dataset` c ON o.customer_id = c.customer_id
#     JOIN `olist_geolocation_dataset` g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
#     GROUP BY c.customer_city, c.customer_state, g.geolocation_lat, g.geolocation_lng
#     ORDER BY order_count DESC;
#     """

#     # Sidebar Filters
#     st.sidebar.header("Filters")
#     category_filter = st.sidebar.selectbox("Select Category:", ["All"] + ["Category 1", "Category 2", "Category 3", "Category 4", "Category 5"])  # Placeholder categories
#     map_toggle = st.sidebar.checkbox("Show Customer Order Map", value=True)

#     # Fetch data
#     top_categories_df = fetch_data(TOP_CATEGORIES_QUERY)
#     st.write(top_categories)
#     hourly_purchases_df = fetch_data(HOURLY_PURCHASES_QUERY)
#     st.write(hourly_purchases_df)
#     customer_orders_df = fetch_data(CUSTOMER_ORDERS_QUERY)
#     st.write(customer_orders_df)

#     # Display top categories
#     st.write("### Top 5 Product Categories by Purchases")
#     st.dataframe(top_categories_df)

#     # Line Chart for Hourly Purchases
#     if not hourly_purchases_df.empty:
#         st.write("### Number of Purchases by Hour for Top 5 Categories")
        
#         fig, ax = plt.subplots(figsize=(10, 6))

#         # Apply category filter
#         filtered_df = hourly_purchases_df if category_filter == "All" else hourly_purchases_df[hourly_purchases_df["product_category_name_english"] == category_filter]

#         # Plot data for each category
#         for category in filtered_df["product_category_name"].unique():
#             category_data = filtered_df[filtered_df["product_category_name"] == category]
#             ax.plot(
#                 category_data["purchase_hour"], 
#                 category_data["purchase_count"], 
#                 marker="o", 
#                 label=category
#             )

#         ax.set_xlabel("Hour of the Day")
#         ax.set_ylabel("Number of Purchases")
#         ax.set_title("Purchases by Hour")
#         ax.legend()
#         ax.grid(True)
        
#         st.pyplot(fig)
#     else:
#         st.warning("No data available for hourly purchases.")

#     # Customer Order Map
#     if map_toggle and not customer_orders_df.empty:
#         st.write("### Customer Orders Map")

#         # Map Center
#         map_center = [customer_orders_df["geolocation_lat"].mean(), customer_orders_df["geolocation_lng"].mean()]
#         m = folium.Map(location=map_center, zoom_start=5)

#         # Add Markers
#         marker_cluster = MarkerCluster().add_to(m)
#         for _, row in customer_orders_df.iterrows():
#             folium.Marker(
#                 location=[row["geolocation_lat"], row["geolocation_lng"]],
#                 popup=f"{row['customer_city']}, {row['customer_state']} - Orders: {row['order_count']}",
#                 icon=folium.Icon(color="blue", icon="info-sign")
#             ).add_to(marker_cluster)

#         # Save and Display Map
#         map_file = "customer_orders_map.html"
#         m.save(map_file)
#         st.components.v1.html(open(map_file, "r").read(), height=600)

#     else:
#         st.warning("No data available for customer orders.")
