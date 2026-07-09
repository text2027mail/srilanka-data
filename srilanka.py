#!/usr/bin/env python3
"""
Sri Lanka Boxoffice Tracker – Fetches seat data from lk.bookmyshow.com
using Playwright to handle Cloudflare and call the API directly.
Saves daily JSON files with a "last_updated" timestamp (IST).
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Optional, Dict, List, Any

import pytz
from playwright.async_api import async_playwright

# ========== CONFIGURATION ==========
IST = pytz.timezone("Asia/Kolkata")

BASE_URL = "https://lk.bookmyshow.com"
API_ENDPOINT = "/pwa/api/uapi/movies/"   # Adjust if needed

OUTPUT_DIR = "srilanka/boxoffice"

# Headers that mimic a real browser (used in the request)
API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL + "/",
    "Origin": BASE_URL,
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
}

# ========== HELPERS ==========
def ist_now() -> datetime:
    return datetime.now(IST)

def get_ist_timestamp() -> str:
    return ist_now().strftime("%Y-%m-%d %H:%M IST")

def get_date_str() -> str:
    return ist_now().strftime("%Y%m%d")

def get_output_filepath() -> str:
    now = ist_now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    dir_path = os.path.join(OUTPUT_DIR, year)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{month}-{day}.json")

# ========== FETCH API VIA PLAYWRIGHT ==========
async def fetch_api_data() -> Optional[Dict]:
    """
    Use Playwright to:
    1. Load the main page (to solve Cloudflare).
    2. Then call the API endpoint from the same context.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=API_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 720}
        )
        page = await context.new_page()

        try:
            # Step 1: Navigate to main page to get cookies / solve challenge
            print("🌐 Navigating to main page:", BASE_URL)
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            # Allow extra time for any async scripts
            await page.wait_for_timeout(3000)

            # Optional: check that we are not on a challenge page
            # If the page title contains "Just a moment", wait longer or reload.
            title = await page.title()
            if "Just a moment" in title:
                print("⏳ Challenge page detected, waiting...")
                await page.wait_for_timeout(10000)
                # Reload and wait again
                await page.reload(wait_until="networkidle")
                await page.wait_for_timeout(3000)

            # Step 2: Now call the API using the same page (or a new request in the same context)
            # We'll use page.request.get() which inherits cookies and headers.
            api_url = BASE_URL + API_ENDPOINT
            print("📡 Calling API:", api_url)

            # Set extra headers for the request
            response = await page.request.get(
                api_url,
                headers=API_HEADERS
            )
            status = response.status
            print(f"   Status: {status}")

            if status == 200:
                data = await response.json()
                print("✅ API data fetched successfully.")
                return data
            else:
                # If it fails, try to read as text (might be HTML challenge)
                text = await response.text()
                print(f"❌ API returned {status}. First 200 chars: {text[:200]}")
                return None

        except Exception as e:
            print("❌ Playwright error:", e)
            return None
        finally:
            await browser.close()

# ========== DATA PROCESSING (PLACEHOLDER) ==========
def process_api_data(raw_data: Dict) -> Dict[str, List[List[Any]]]:
    """
    Parse the API response and extract required data.
    Replace with your own logic.
    """
    # Example: if raw_data contains a "movies" list.
    # For now, just store the raw data under a dummy key.
    processed = {"raw_data": raw_data}
    return processed

# ========== SAVE ==========
def save_daily_data(data: Dict[str, List[List[Any]]]) -> None:
    filepath = get_output_filepath()
    output = {
        "data": data,
        "last_updated": get_ist_timestamp()
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(',', ':'), ensure_ascii=False)
    print(f"💾 Saved {filepath} (last_updated: {output['last_updated']})")

# ========== MAIN ==========
async def main():
    print("🚀 Sri Lanka Boxoffice Tracker Started...")
    print("📅 Processing date:", get_date_str())

    raw_data = await fetch_api_data()
    if not raw_data:
        print("❌ Failed to fetch data. Exiting.")
        return

    processed = process_api_data(raw_data)
    save_daily_data(processed)
    print("✅ Done.")

if __name__ == "__main__":
    asyncio.run(main())
