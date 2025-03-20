import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import plotly.express as px

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/koppo/Downloads/valued-ceiling-454014-a9-f6887630e113.json"
client = bigquery.Client()
PROJECT_ID = "valued-ceiling-454014-a9"
DATASET_ID = "sam1"

st.title("Event-driven Data Pipeline for E-commerce")