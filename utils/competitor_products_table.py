import os
from google.cloud import bigquery
import pandas as pd

bq_client = bigquery.Client()

OUTPUT_PATH = "Catalog/Current_master_table.csv"

def load_query(filename):
    query_path = os.path.join("Queries", filename)
    with open(query_path, 'r') as f:
        return f.read()

def load_current_mapping_table():
    query = load_query("competitor_products_table.sql")
    result = bq_client.query(query)
    df = result.to_dataframe()
    return df

def save_dataframe_to_csv(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    try:
        df = load_current_mapping_table()
        save_dataframe_to_csv(df, OUTPUT_PATH)
    except Exception as e:
        import traceback
        traceback.print_exc() 

