import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv
import os


load_dotenv()

# Authenticate
credentials = service_account.Credentials.from_service_account_file("service_key.json")
project_id = "ibuyflower-dwh"
dataset_id = "competitor_product_mapping"
table_name = "competitor_product_mapping"

def upload_mapping_table_to_bq():
    csv_file_path = "/Users/diego/Scrapers_ibf_v2/Catalog/Current_master_table_v2.csv"
    

    df = pd.read_csv(csv_file_path, dtype={"competitor_product_id": str, "variety_key": str})
    table_id = f"{project_id}.{dataset_id}.{table_name}"


    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="replace",  # use 'replace' if you want to overwrite the table
        credentials=credentials,
        location="EU"
    )

if __name__ == "__main__":
    upload_mapping_table_to_bq()