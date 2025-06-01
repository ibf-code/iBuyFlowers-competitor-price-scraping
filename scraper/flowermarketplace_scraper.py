import asyncio
import httpx
import os
import sys
import traceback
from datetime import date
from dotenv import load_dotenv
from tqdm import tqdm

from Extractors.flowermarketplace_extractor import extract_flowermarketplace_product
from utils.save_csv import save_products_to_csv
from utils.eta_fetcher import get_earliest_eta_date
from utils.logger import get_logger
from utils.bigquery_export import export_products_to_bigquery

logger = get_logger(__name__, log_files="flowermarketplace_scraper.log")
load_dotenv()
CONCURRENT_REQUESTS = 10  # Safer concurrency level
TIMEOUT = 60

async def fetch_page(client, eta_date, page, retries=3, backoff=1):
    url = f"https://flowermarketplace.com/wp-admin/admin-ajax.php?action=wpf_product_listings&model=landed&date_text={eta_date}&page_no={page}"
    for attempt in range(1, retries + 1):
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"⚠️ Page {page} attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                await asyncio.sleep(backoff * attempt)
    return None

def get_total_pages(page_data):
    try:
        pages = page_data.get("pages", [])
        return int(pages[1]) if len(pages) > 1 else 1
    except Exception:
        return 1

async def fetch_flowermarketplace_products(eta_date):
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    limits = httpx.Limits(max_connections=CONCURRENT_REQUESTS)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    all_products = []
    async with httpx.AsyncClient(headers=headers, timeout=TIMEOUT, limits=limits) as client:
        page1_data = await fetch_page(client, eta_date, page=1)
        if not page1_data:
            logger.error("❌ Failed to fetch initial page. Exiting.")
            return []

        total_pages = get_total_pages(page1_data)
        logger.info(f"Detected total pages: {total_pages}")
        pages_data = [page1_data]

        async def fetch_task(page):
            async with semaphore:
                return await fetch_page(client, eta_date, page)

        results = await asyncio.gather(*(fetch_task(p) for p in range(2, total_pages + 1)))
        pages_data.extend(filter(None, results))

    total_items = sum(len(page.get("items", [])) for page in pages_data)
    with tqdm(total=total_items, desc="Processing products", unit="item", ncols=100) as pbar:
        for page in pages_data:
            for raw in page.get("items", []):
                product = extract_flowermarketplace_product(raw)
                if product:
                    product["eta_date"] = eta_date
                    all_products.append(product)
                pbar.update(1)
    os.makedirs("output/flowermarketplace", exist_ok=True)
    save_products_to_csv(all_products, f"output/flowermarketplace/flowermarketplace_inventory_{eta_date}.csv")

    return all_products

if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)  # Ensure console flush
    eta_date = get_earliest_eta_date()
    os.makedirs("output/flowermarketplace", exist_ok=True)

    products = asyncio.run(fetch_flowermarketplace_products(eta_date))
    if products:
        export_products_to_bigquery(products, table_name="flowermarketplace")
        save_products_to_csv(products, f"output/flowermarketplace/flowermarketplace_inventory_{eta_date}.csv")
        logger.info(f"✅ Scraped {len(products)} products.")
    else:
        logger.warning("⚠️ No products scraped.")