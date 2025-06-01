import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import pandas_gbq
import asyncio
from google.oauth2 import service_account
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.slack_notifier import _slack

load_dotenv()
credentials = service_account.Credentials.from_service_account_file("service_key.json")
project_id = os.getenv("PROJECT_ID")
dataset_id = "competitor_price"
logger = get_logger(__name__, log_files="bigquery_export.log")

BIGQUERY_FIELDS = {
    "mayesh": [
        "created_at", "eta_date", "state", "competitor", "grower_name", "grower_country",
        "competitor_product_id", "stem_length", "color_name", "available_units",
        "stems_per_unit", "stem_price", "margin", "unit_price", "unit", "competitor_product_name"
    ],
    "petaljet": [
        "competitor", "competitor_product_id", "competitor_product_name",
        "competitor_product_group_name", "color", "stem_length", "stems_per_unit",
        "unit_price", "stem_price", "created_at", "eta_date", "flowermarketplace"
    ],
    "flowermarketplace": [
        "created_at", "eta_date", "competitor", "competitor_product_id",
        "competitor_product_name", "competitor_product_group_name",
        "stem_length", "stems_per_unit", "unit_price", "stem_price"
    ],
    "dvflora": [
        "created_at", "eta_date", "competitor_product_id", "competitor",
        "competitor_product_name", "stem_length", "stems_per_unit",
        "stem_price", "unit_price"
    ],
}

async def _send_slack_notifications(table_name: str, count: int):
    try:
        message = f"*{table_name.title()}*: {count:,} products uploaded to BigQuery"
        await _slack.send_message(message, "good")
    except Exception as e:
        pass

def export_products_to_bigquery(products, table_name):
    if not products:
        logger.warning("⚠️ No products to export.")
        return
    
    df = pd.DataFrame(products)
    allowed_fields = BIGQUERY_FIELDS.get(table_name.lower())
    if not allowed_fields:
        logger.error(f"❌ No allowed fields defined for table '{table_name}'.")
        return
    
    df_filtered = df[[col for col in allowed_fields if col in df.columns]].copy()
    if 'stem_length' in df_filtered.columns:
        df_filtered['stem_length'] = pd.to_numeric(df_filtered['stem_length'], errors='coerce').astype('Int64')

    table_id = f"{dataset_id}.{table_name}"
    pandas_gbq.to_gbq(
        df_filtered,
        table_id,
        project_id=project_id,
        if_exists="replace", 
        credentials=credentials,
        progress_bar=True
    )
    
    logger.info(f"✅ Uploaded {len(df_filtered)} products to {table_id}.")

    try:
        loop = asyncio.get_event_loop()
        loop.create_task(_send_slack_notifications(table_name, len(df_filtered)))
    except RuntimeError:
        pass