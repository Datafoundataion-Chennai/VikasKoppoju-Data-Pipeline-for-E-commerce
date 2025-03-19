import pandas as pd
import numpy as np

def infer_object_type(series):
    """Infer if an object-type column is a date, time, or timestamp."""
    try:
        parsed_col = pd.to_datetime(series, errors="coerce")  
        if parsed_col.notna().all():
            if parsed_col.dt.time.nunique() > 1 and parsed_col.dt.date.nunique() > 1:
                return "TIMESTAMP"
            elif parsed_col.dt.time.nunique() > 1:
                return "TIME"
            else:
                return "DATE"
    except Exception:
        pass
    return "STRING"

def download_and_clean_data(csv_path):
    """Downloads dataset, cleans it, and returns a DataFrame with inferred schema."""
    df = pd.read_csv(csv_path, dtype=str)

    # Drop empty and duplicate rows
    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col]) 
        except ValueError:  
            pass  

    # Type mapping
    type_mapping = {
        "int64": "INTEGER",
        "float64": "FLOAT64",
        "bool": "BOOLEAN",
        "object": "STRING"
    }

    schema_dict = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype == "object":
            schema_dict[col] = infer_object_type(df[col])
        else:
            schema_dict[col] = type_mapping.get(dtype, "STRING")

    float_columns = [col for col, dtype in schema_dict.items() if dtype == "FLOAT64"]
    df[float_columns] = df[float_columns].astype(np.float64)

    df = df.where(pd.notnull(df), None)

    print(f"✅ Data cleaned and schema inferred: {schema_dict}")
    return df, schema_dict
