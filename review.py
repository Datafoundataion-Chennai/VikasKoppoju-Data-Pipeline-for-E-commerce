import pandas as pd
from google.cloud import bigquery
from index import fetch_data  

def test_bigquery_connection():
    try:
        client = bigquery.Client()
        assert client is not None
        print("BigQuery connection established successfully.")
    except Exception as e:
        print(f"BigQuery connection failed: {e}")

def test_fetch_data():
    query = "SELECT 1 AS col1"
    df = fetch_data(query)

    if isinstance(df, pd.DataFrame) and not df.empty:
        print("fetch_data() returned a valid DataFrame.")
    else:
        print("fetch_data() did not return a valid DataFrame.")

def test_fetch_data_error_handling():
    query = "INVALID QUERY"
    df = fetch_data(query)

    if isinstance(df, pd.DataFrame) and df.empty:
        print("fetch_data() handled the error correctly and returned an empty DataFrame.")
    else:
        print("fetch_data() did not handle the error correctly.")

if __name__ == "__main__":
    test_bigquery_connection()
    test_fetch_data()
    test_fetch_data_error_handling()
