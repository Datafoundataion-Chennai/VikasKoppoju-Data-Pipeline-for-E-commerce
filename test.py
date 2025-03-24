import pytest
import pandas as pd
from unittest.mock import patch
from index import fetch_data,PROJECT_ID,DATASET_ID

@pytest.fixture
def mock_bigquery_client():
    with patch("index.client.query") as mock_query:
        yield mock_query


def test_fetch_data_success(mock_bigquery_client):
    mock_df = pd.DataFrame({"test_column": [1, 2, 3]})
    mock_bigquery_client.return_value.to_dataframe.return_value = mock_df
    
    query = F"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.olist_orders_dataset`"
    result = fetch_data(query)
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "test_column" in result.columns


def test_fetch_data_failure(mock_bigquery_client):
    mock_bigquery_client.side_effect = Exception("Query execution failed")
    
    query = "SELECT * FROM some_table"
    result = fetch_data(query)
    
    assert isinstance(result, pd.DataFrame)
    assert result.empty

if __name__ == "__main__":
    pytest.main()
