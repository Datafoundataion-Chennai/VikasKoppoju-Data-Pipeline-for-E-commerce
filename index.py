import kagglehub
from google.cloud import bigquery
import pandas as pd
import numpy as np
import os

# ✅ Step 1: Download dataset from Kaggle
# path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
# print("Dataset downloaded at:", path)

# ✅ Step 2: Set up Google Cloud Authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"

# ✅ Step 3: Initialize BigQuery Client
client = bigquery.Client()

# ✅ Step 4: Read CSV File
csv_file = "D:/data pipeline/data/olist_customers_dataset.csv"
df = pd.read_csv(csv_file)

# ✅ Step 5: Data Cleaning
df.dropna(how="all", inplace=True)  # Remove empty rows
df.drop_duplicates(inplace=True)  # Remove duplicate records
df.rename(columns={"customer_id": "CustomerID"}, inplace=True)  # Rename for consistency

# ✅ Step 6: Infer Schema Dynamically
type_mapping = {
    str: "STRING",
    "int64": "INTEGER",
    np.int64: "INTEGER",
    float: "FLOAT",
    bool: "BOOLEAN"
}

data_types_dict = {
    col: type_mapping.get(type(df[col].dropna().iloc[0]), "STRING") for col in df.columns if not df[col].isna().all()
}

schema = [bigquery.SchemaField(col, dtype) for col, dtype in data_types_dict.items()]

# ✅ Step 7: Set BigQuery Project, Dataset, and Table
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "student"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.olist_customers"

# ✅ Step 8: Ensure Dataset Exists
dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

try:
    client.get_dataset(dataset_ref)
    print(f"✅ Dataset {DATASET_ID} already exists.")
except:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-south1"  # ✅ Correct region
    client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Dataset {DATASET_ID} created.")

# ✅ Step 9: Load Data into BigQuery Table
job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
job.result()  # Wait for completion

print("🎉 Data successfully loaded into BigQuery!")
