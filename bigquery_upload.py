import numpy as np
from google.cloud import bigquery
import os
from data_processing import download_and_clean_data
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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"


client = bigquery.Client()
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sample"
dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {DATASET_ID} already exists.")
except:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-south1"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {DATASET_ID}.")
count=0
for csv_file in csv_files:
    try:
        print(f"\n🚀 Processing file: {csv_file}")
        df, schema_dict = download_and_clean_data(csv_file)
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        schema = [bigquery.SchemaField(col, dtype) for col, dtype in schema_dict.items()]
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()
        count+=1

        print(f"Data successfully uploaded to BigQuery table: {TABLE_ID}")

    except Exception as e:
        print(f"Error processing {csv_file}: {e}")

print(f"\n All datasets successfully uploaded to BigQuery! {count}")
