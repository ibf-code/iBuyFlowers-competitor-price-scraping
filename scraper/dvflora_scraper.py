import asyncio
import csv
import os
import re
from datetime import date
from pathlib import Path
from tqdm import tqdm
import aiohttp
from selectolax.parser import HTMLParser
from Extractors.dvflora_extractor import extract_dvflora_product
from utils.save_csv import save_products_to_csv
from utils.logger import get_logger
from utils.bigquery_export import export_products_to_bigquery

logger = get_logger(__name__, log_files="DVFlora_scraper.log")
URLS_CSV = Path("utils/dvflora_urls.csv")
CONCURRENCY = 50
sem = asyncio.Semaphore(CONCURRENCY)

async def fetch_page(session, row):
    async with sem:
        try:
            async with session.get(row["url"], timeout=30) as response:
                response.raise_for_status()
                html = await response.text()
                return extract_from_html(html, row)
        except Exception as e:
            logger.error(f"Error fetching {row['url']}: {e}")
            return []

def extract_from_html(html, row):
    tree = HTMLParser(html)
    tbody = tree.css_first("#PageText > form:nth-of-type(2) > table > tbody")
    if not tbody:
        return []

    products = []
    seen = set()
    today = date.today().isoformat()

    for tr in tbody.css("tr"):
        cells = tr.css("td")
        if len(cells) < 8:
            continue

        item_number = cells[1].text(strip=True)
        if not item_number or item_number == "Item ##" or "Item #" in item_number:
            continue

        if item_number in seen:
            continue
        seen.add(item_number)

        name_node = cells[2].css_first("a")
        raw_name = name_node.text(strip=True) if name_node else ""
        if not raw_name:
            raw_name = re.sub(r'\d+/\d+cm', '', raw_name)
            raw_name = re.sub(r'\d+c/\d+', '', raw_name)

        origin_text = cells[2].text() or ""
        origin = origin_text.split("Origin:")[-1].strip() if "Origin:" in origin_text else ""

        raw = {
            "item_number": item_number,
            "product_name": raw_name,
            "origin": origin,
            "sold_as": cells[3].text(strip=True) if cells[3].text() else "",
            "unit_price": None,
            "best_value_price": None,
            "best_value_quantity": None,
            "product_group": row["product_group"],
            "color": row["color"],
            "url": row["url"],
            "unit_price_fallback": None  # NEW COLUMN
        }

        # NEW: Extract unit price from the specific <td> (e.g., 8th column)
        unit_price_td = cells[8]  # Assuming 9th <td> holds the unit price
        price_node = unit_price_td.css_first("a")
        if price_node:
            price_text = price_node.text(strip=True)
            price_match = re.search(r'\$?(\d+(?:\.\d+)?)', price_text)
            if price_match:
                price_value = float(price_match.group(1))
                raw["unit_price_fallback"] = price_value

        prices = []
        for td in cells:
            cell_text = td.text()
            if cell_text:
                matches = re.findall(r"(\d+)\s*@\s*\$?(\d+\.\d+)", cell_text)
                for qty, price in matches:
                    prices.append((int(qty), float(price)))

        if prices:
            prices.sort(key=lambda x: x[1]/x[0])
            raw["unit_price"] = prices[0][1]
            raw["best_value_quantity"], raw["best_value_price"] = prices[0]

        extracted = extract_dvflora_product(raw)
        if extracted:
            extracted["eta_date"] = today
            products.append(extracted)

    return products

async def fetch_dvflora_products(eta_date):
    all_products = []
    with open(URLS_CSV, newline="") as f:
        urls = list(csv.DictReader(f))

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [fetch_page(session, row) for row in urls]
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="📦 Scraping DVFlora"):
            result = await coro
            for product in result:
                product["eta_date"] = eta_date       
            all_products.extend(result)

    filtered_products = [
        p for p in all_products 
        if p.get("competitor_product_id") and 
           not (isinstance(p.get("competitor_product_id"), str) and 
                ("Item #" in p.get("competitor_product_id") or p.get("competitor_product_id") == "Item ##"))
    ]

    os.makedirs("output/dvflora", exist_ok=True)
    path = f"output/dvflora/dvflora_inventory_{eta_date}.csv"
    removed = len(all_products) - len(filtered_products)
    save_products_to_csv(filtered_products, path)
    logger.info(f"✅ Saved {len(filtered_products)} products to {path} (filtered out {removed} items)")
    
    return filtered_products

if __name__ == "__main__":
    from utils.eta_fetcher import get_earliest_eta_date

    eta_date = get_earliest_eta_date()
    products = asyncio.run(fetch_dvflora_products(eta_date))
    export_products_to_bigquery(products, table_name="dvflora")
   
