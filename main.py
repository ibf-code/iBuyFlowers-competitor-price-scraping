import asyncio
import os
from scraper.petaljet_scraper import fetch_petaljet_products
from scraper.mayesh_scraper import fetch_mayesh_products
from scraper.flowermarketplace_scraper import fetch_flowermarketplace_products
from scraper.dvflora_scraper import fetch_dvflora_products
from utils.logger import get_logger
from utils.eta_fetcher import get_earliest_eta_date
from utils.bigquery_export import export_products_to_bigquery
# from utils.slack_notifier import notify_scraping_start, notify_all_complete

logger = get_logger(__name__, log_files="main_scraper.log")

async def run_scraper(scraper_func, eta_date, table_name):
    logger.info(f"Starting {table_name} scraping...")
    products = await scraper_func(eta_date)
    await asyncio.to_thread(export_products_to_bigquery, products, table_name)

async def main():
    eta_date = get_earliest_eta_date()
    os.makedirs("output", exist_ok=True)

    await asyncio.gather(
        run_scraper(fetch_mayesh_products, eta_date, "mayesh"),
        run_scraper(fetch_petaljet_products, eta_date, "petaljet"),
        run_scraper(fetch_flowermarketplace_products, eta_date, "flowermarketplace"),
        run_scraper(fetch_dvflora_products, eta_date, "dvflora")
    )

if __name__ == "__main__":
    asyncio.run(main())

# Slack Integration was tested and seemed to be working fine, yet it cluttered Main.py
# and i'm not sure wether it is a feature we want to keep or not.
# Yet in case we do want to keep it, here is the commented code for Slack notifications:


# logger = get_logger(__name__, log_files="main_scraper.log")

# async def run_scraper(scraper_func, eta_date, table_name):
#     logger.info(f"Starting {table_name} scraping...")
#     products = await scraper_func(eta_date)
#     await asyncio.to_thread(export_products_to_bigquery, products, table_name)
#     return len(products)

# async def main():
#     start_time = time.time()
#     eta_date = get_earliest_eta_date()
#     os.makedirs("output", exist_ok=True)
    
#     scrapers = ["mayesh", "petaljet", "flowermarketplace", "dvflora"]
    
#     # Send start notification
#     await notify_scraping_start(eta_date, scrapers)

#     # Run all scrapers and get product counts
#     results = await asyncio.gather(
#         run_scraper(fetch_mayesh_products, eta_date, "mayesh"),
#         run_scraper(fetch_petaljet_products, eta_date, "petaljet"),
#         run_scraper(fetch_flowermarketplace_products, eta_date, "flowermarketplace"),
#         run_scraper(fetch_dvflora_products, eta_date, "dvflora")
#     )
    
#     # Create results dictionary for breakdown
#     scraper_results = {
#         "mayesh": results[0],
#         "petaljet": results[1], 
#         "flowermarketplace": results[2],
#         "dvflora": results[3]
#     }
    
#     # Send completion notification with breakdown
#     duration = time.time() - start_time
#     await notify_all_complete(scraper_results, duration)

# if __name__ == "__main__":
#     asyncio.run(main())