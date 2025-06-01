import asyncio
import httpx
import os
from datetime import date, datetime
from dotenv import load_dotenv
import traceback

from Extractors.mayesh_extractor import extract_mayesh_product
from utils.save_csv import save_products_to_csv
from utils.eta_fetcher import get_earliest_eta_date
from schemas.product_schema import ProductSchema
from utils.logger import get_logger
from utils.bigquery_export import export_products_to_bigquery

from tqdm import tqdm 

logger = get_logger(__name__, log_files="mayesh_scraper.log")

load_dotenv()

LOGIN_URL = "https://www.mayesh.com/api/auth/login"
INVENTORY_URL = "https://www.mayesh.com/api/auth/inventory"
HEADERS = {"Content-Type": "application/json"}

async def fetch_mayesh_products(eta_date):
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        login_resp = await client.post(LOGIN_URL, json={"email": email, "password": password}, headers=HEADERS)
        login_resp.raise_for_status()
        token = login_resp.json()["data"]["token"]
        auth_headers = HEADERS | {"Authorization": f"Bearer {token}"}

        payload = {
            "filters": {
                "perPage": 2000,
                "sortBy": "Name-ASC",
                "pageNumb": 1,
                "date": eta_date,
                "is_sales_rep": 0,
                "is_e_sales": 0,
                "criteria": {"filter_program": ["5"]},
                "criteriaInt": {"filter_program": {"5": "Farm Direct Boxlots"}},
                "search": ""
            }
        }

        inv_resp = await client.post(INVENTORY_URL, json=payload, headers=auth_headers)
        inv_resp.raise_for_status()
        raw_products = inv_resp.json().get("products", [])

    all_products = []
    for raw in tqdm(raw_products, desc="Processing Mayesh products"):
        product = extract_mayesh_product(raw)
        if product:
            product = product.model_copy(update={"eta_date": eta_date}) 
            all_products.append(product.dict()) 
    os.makedirs("output/mayesh", exist_ok=True)
    save_products_to_csv(all_products, f"output/mayesh/mayesh_inventory_{eta_date}.csv")        
    return all_products 

if __name__ == "__main__":
    eta_date = get_earliest_eta_date()
    products = asyncio.run(fetch_mayesh_products(eta_date))
    export_products_to_bigquery(products, table_name="mayesh")
    logger.info(f"\n✅ Scraped {len(products)} products.")
    
