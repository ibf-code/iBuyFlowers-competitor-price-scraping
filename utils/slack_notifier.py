import os
import httpx
import asyncio
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)
# SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SLACK_WEBHOOK_URL = "https://httpbin.org/post"

class SlackNotifier:
    async def send_message(self, message: str, color: str = "good") -> bool:
        if not SLACK_WEBHOOK_URL:
            logger.info("No Slack webhook URL configured")
            return False
        
        payload = {
            "attachments": [{
                "color": color,
                "text": message,
                "ts": int(datetime.now().timestamp())
            }]
        }
        
        try:
            logger.info(f"Sending Slack message to: {SLACK_WEBHOOK_URL}")
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(SLACK_WEBHOOK_URL, json=payload)
                response_data = response.json()
                logger.info(f"Slack response: {response_data}")
                response.raise_for_status()
                return True
        except Exception as e:
            logger.error(f"Slack error: {e}")
            return False

_slack = SlackNotifier()

async def notify_scraping_start(eta_date: str, scrapers: list):
    scraper_list = ", ".join(scrapers)
    message = f"*Scraping Started*\n📅 ETA Date: `{eta_date}`\n🔧 Scrapers: {scraper_list}"
    logger.info(f"Sending start notification: {message}")
    await _slack.send_message(message, "warning")

async def notify_all_complete(scraper_results: dict, duration: float):
    total = sum(scraper_results.values())
    breakdown = "\n".join([f"• {name.title()}: {count:,}" for name, count in scraper_results.items()])
    message = f"✅*Scraping Complete!*\n📦 Total Products: {total:,}\n\n{breakdown}\n\n⏱️ Duration: {duration:.1f}s"
    logger.info(f"Sending completion notification: {message}")
    await _slack.send_message(message, "good")