#!/usr/bin/env python3
"""
Sri Lanka Boxoffice Tracker – Fetches seat data from lk.bookmyshow.com
handling Cloudflare protection via Playwright.
Saves daily JSON files with a "last_updated" timestamp (IST).
"""

import asyncio
import json
import os
import random
import string
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import pytz
import requests
from playwright.async_api import async_playwright, Browser

# ========== CONFIGURATION ==========
# --- Timezone (IST for last_updated) ---
IST = pytz.timezone("Asia/Kolkata")

# --- BookMyShow Sri Lanka ---
BASE_URL = "https://lk.bookmyshow.com"
API_ENDPOINT = "/pwa/api/uapi/movies/"   # <-- Replace with actual endpoint
# If you need a different endpoint, change it.

# --- Output directory ---
OUTPUT_DIR = "srilanka/boxoffice"   # You can change this

# --- Headers to mimic a real browser ---
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
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
    """Return current IST time as 'YYYY-MM-DD HH:MM IST'."""
    return ist_now().strftime("%Y-%m-%d %H:%M IST")

def get_date_str() -> str:
    """Return today's date in YYYYMMDD (IST) for file naming."""
    return ist_now().strftime("%Y%m%d")

def get_output_filepath() -> str:
    """Create a daily file under OUTPUT_DIR/YYYY/MM-DD.json."""
    now = ist_now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    dir_path = os.path.join(OUTPUT_DIR, year)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{month}-{day}.json")

# ========== COOKIE FETCHING VIA PLAYWRIGHT ==========
async def fetch_cf_cookies() -> Optional[Dict[str, str]]:
    """
    Use Playwright to navigate to the main BookMyShow page,
    let Cloudflare challenge complete, and extract cookies.
    """
    async with async_playwright() as p:
        # Launch a headless browser (you can set headless=False for debugging)
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 720}
        )
        page = await context.new_page()

        try:
            print("🌐 Navigating to main page:", BASE_URL)
            # Go to the main page, wait for network to be idle (challenge may take time)
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            # Sometimes a secondary challenge appears; wait a bit more.
            await page.wait_for_timeout(5000)

            # Check if we are on the actual site (not the challenge page)
            # You might want to wait for a known element, e.g., the title or a specific selector
            # For example: await page.wait_for_selector("body", state="attached")

            # Get all cookies
            cookies = await context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            print("✅ Cookies obtained:", list(cookie_dict.keys()))

            # Also try to get localStorage if needed (sometimes used)
            # localStorage = await page.evaluate("() => window.localStorage")
            # print("LocalStorage keys:", list(localStorage.keys()))

            await browser.close()
            return cookie_dict

        except Exception as e:
            print("❌ Playwright failed:", e)
            await browser.close()
            return None

# ========== API CALL WITH COOKIES ==========
def call_api(cookies: Dict[str, str], endpoint: str) -> Optional[Dict]:
    """
    Use requests.Session with the provided cookies to call the API.
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    # Add cookies
    for name, value in cookies.items():
        session.cookies.set(name, value)

    url = BASE_URL + endpoint
    print(f"📡 Calling API: {url}")
    try:
        resp = session.get(url, timeout=30)
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"   Response text (first 200): {resp.text[:200]}")
            return None
    except Exception as e:
        print("❌ API request error:", e)
        return None

# ========== DATA PROCESSING (PLACEHOLDER) ==========
def process_api_data(raw_data: Dict) -> Dict[str, List[List[Any]]]:
    """
    Parse the API response and extract the required data.
    This is a placeholder – replace with your own parsing logic.
    Returns a dict mapping movie titles to lists of show entries:
        { "Movie Name": [ [programId, location, showTime, totalSeats, soldSeats], ... ] }
    """
    # Example: if the API returns a "movies" list with shows
    # You should adapt this to the actual Sri Lanka API structure.
    processed = {}
    # Placeholder logic – you'll need to inspect the actual API response.
    # For now, we just echo the raw data.
    processed["raw"] = raw_data
    return processed

# ========== SAVE WITH LAST_UPDATED ==========
def save_daily_data(data: Dict[str, List[List[Any]]]) -> None:
    """Save the data with a top-level 'data' and 'last_updated' field."""
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

    # Step 1: Get CF cookies via Playwright
    cookies = await fetch_cf_cookies()
    if not cookies:
        print("❌ Could not obtain CF cookies. Exiting.")
        return

    # Step 2: Call the API with those cookies
    raw_data = call_api(cookies, API_ENDPOINT)
    if not raw_data:
        print("❌ API call failed. Exiting.")
        return

    # Step 3: Process the data (replace with your own parsing)
    processed_data = process_api_data(raw_data)

    # Step 4: Save with last_updated
    save_daily_data(processed_data)

    print("✅ Done.")

if __name__ == "__main__":
    asyncio.run(main())
