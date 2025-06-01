import os
import asyncio
import httpx
from datetime import date
from dotenv import load_dotenv

from Extractors.petaljet_extractor import extract_petaljet_product
from utils.save_csv import save_products_to_csv
from utils.eta_fetcher import get_earliest_eta_date
from utils.logger import get_logger
from utils.bigquery_export import export_products_to_bigquery

logger = get_logger(__name__, log_files="petaljet_scraper.log")

load_dotenv()

CONCURRENT_REQUESTS = 5
TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

async def fetch_page(client, page_number):
    url = f"https://petaljet.com/collections/all-products/products.json?page={page_number}"
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("products", [])
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch page {page_number}: {e}")
        return []

async def fetch_petaljet_products(eta_date):
    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT) as client:
        tasks = [fetch_page(client, page) for page in range(1, 22)]
        pages = await asyncio.gather(*tasks)

    all_products = []
    for product_list in pages:
        for raw_product in product_list:
            extracted_products = extract_petaljet_product(raw_product)
            for product in extracted_products:
                product["eta_date"] = eta_date
                all_products.append(product)
    os.makedirs("output/petaljet", exist_ok=True)
    save_products_to_csv(all_products, f"output/petaljet/petaljet_inventory_{eta_date}.csv")
    
    return all_products

if __name__ == "__main__":
    os.makedirs("output/petaljet", exist_ok=True)
    eta_date = get_earliest_eta_date()
    products = asyncio.run(fetch_petaljet_products(eta_date))
    export_products_to_bigquery(products, table_name="petaljet")

    logger.info(f"✅ Scraped {len(products)} products.")
    print(f"✅ Scraped {len(products)} products.")

    save_products_to_csv(products, f"output/petaljet/petaljet_inventory_{eta_date}.csv")