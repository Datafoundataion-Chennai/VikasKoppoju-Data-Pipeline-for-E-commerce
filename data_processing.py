import kagglehub
import pandas as pd
import numpy as np

def download_and_clean_data(csv_path):
    """Downloads dataset, cleans it, and returns a DataFrame with inferred schema."""
    
    # # Read CSV file
    df = pd.read_csv(csv_path, dtype=str)  # Read all as string initially to avoid conversion issues
    
    # Data Cleaning
    df.dropna(how="all", inplace=True)  # Remove empty rows
    df.drop_duplicates(inplace=True)  # Remove duplicates
    
    # Detect and convert appropriate column types
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='ignore')  # Convert numbers while keeping strings intact
        except Exception as e:
            print(f"⚠ Warning: Could not convert column {col}. Error: {e}")

    # Infer Schema Types
    type_mapping = {
        "int64": "INTEGER",
        "float64": "FLOAT64",
        "bool": "BOOLEAN",
        "object": "STRING"
    }

    schema_dict = {col: type_mapping.get(str(df[col].dtype), "STRING") for col in df.columns}

    # Ensure float columns are explicitly float64 for BigQuery
    float_columns = [col for col, dtype in schema_dict.items() if dtype == "FLOAT64"]
    df[float_columns] = df[float_columns].astype(np.float64)

    # Replace NaN values with None (BigQuery does not accept NaN)
    df = df.where(pd.notnull(df), None)

    print(f"📌 Data cleaned and schema inferred: {schema_dict}")
    return df, schema_dict