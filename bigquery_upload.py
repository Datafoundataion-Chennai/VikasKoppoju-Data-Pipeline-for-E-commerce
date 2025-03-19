import numpy as np
from google.cloud import bigquery
import os
from data_processing import download_and_clean_data

# ist of CSV files to process
csv_files = [
    "./data/olist_customers_dataset.csv",
    "./data/olist_geolocation_dataset.csv",
    "./data/olist_order_items_dataset.csv",
    "./data/olist_order_payments_dataset.csv",
    "./data/olist_order_reviews_dataset.csv",
    "./data/olist_orders_dataset.csv",
    "./data/olist_products_dataset.csv",
    "./data/olist_sellers_dataset.csv",
    "./data/product_category_name_translation.csv"
]

# Step 1: Set Google Cloud Authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"

# Step 2: Initialize BigQuery Client
client = bigquery.Client()

# Step 3: Define BigQuery Dataset
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sample"
dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

# Ensure Dataset Exists
try:
    client.get_dataset(dataset_ref)
    print(f"✅ Dataset {DATASET_ID} already exists.")
except:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-south1"
    client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Created dataset {DATASET_ID}.")

for csv_file in csv_files:
    try:
        print(f"\n🚀 Processing file: {csv_file}")

        # Step 4.1: Load Processed Data
        df, schema_dict = download_and_clean_data(csv_file)

        # Step 4.2: Define Table Name Dynamically
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        # Step 4.3: Prepare Schema for BigQuery
        schema = [bigquery.SchemaField(col, dtype) for col, dtype in schema_dict.items()]

        # Step 4.4: Load Data into BigQuery Table
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()  # Wait for completion

        print(f"✅ Data successfully uploaded to BigQuery table: {TABLE_ID}")

    except Exception as e:
        print(f"❌ Error processing {csv_file}: {e}")

print("\n🎉 All datasets successfully uploaded to BigQuery!")