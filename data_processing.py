import pandas as pd
import numpy as np
import re
def infer_object_type(series):
    """Infer if an object-type column is a date, time, or timestamp."""
    try:
        parsed_series = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d %H:%M:%S")
        if parsed_series.notna().all():
            return "TIMESTAMP"
    except Exception:
        pass  

    date_pattern = r"^\d{4}-\d{2}-\d{2}$"  
    time_pattern = r"^\d{2}:\d{2}(:\d{2})?$"  
    timestamp_pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$"

    for value in series.dropna().astype(str):
        if re.match(timestamp_pattern, value):
            return "TIMESTAMP"
        elif re.match(date_pattern, value):
            return "DATE"
        elif re.match(time_pattern, value):
            return "TIME"

    return "STRING" 

def download_and_clean_data(csv_path):
    """Downloads dataset, cleans it, and returns a DataFrame with inferred schema."""
    df = pd.read_csv(csv_path, dtype=str)

    # Drop empty and duplicate rows
    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)

    # Infer schema
    schema_dict = {}
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])  # Convert numerical columns
        except ValueError:
            pass  

        dtype = str(df[col].dtype)
        if dtype == "object":
            inferred_type = infer_object_type(df[col])
            schema_dict[col] = inferred_type
            print(f"if object {schema_dict[col]}")
            
            # Ensure TIMESTAMP columns are converted
            if inferred_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S")

        else:
            schema_dict[col] = {
                "int64": "INTEGER",
                "float64": "FLOAT64",
                "bool": "BOOLEAN"
            }.get(dtype, "STRING")
            print(f"if not {schema_dict[col]}")

    # Convert FLOAT64 columns explicitly
    float_columns = [col for col, dtype in schema_dict.items() if dtype == "FLOAT64"]
    df[float_columns] = df[float_columns].astype(np.float64)

    # Ensure NaN values are handled correctly
    df = df.where(pd.notnull(df), None)

    print(f"Data cleaned and schema inferred: {schema_dict}")
    return df, schema_dict
