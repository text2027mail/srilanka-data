#!/usr/bin/env python3
"""
Sri Lanka Boxoffice Tracker – Fetches seat data from lk.bookmyshow.com
using Playwright. The API is called via page.evaluate() to avoid header/cookie mismatches.
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

# Headers used only for the main page navigation
PAGE_HEADERS = {
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

# ========== FETCH API VIA PAGE.EVALUATE ==========
async def fetch_api_data() -> Optional[Dict]:
    """
    Use Playwright to:
    1. Load the main page and solve Cloudflare.
    2. Use page.evaluate() to call the API via fetch, which carries all session cookies.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=PAGE_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 720}
        )
        page = await context.new_page()

        try:
            # Step 1: Navigate to main page and wait for the challenge to be solved.
            print("🌐 Navigating to main page:", BASE_URL)
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)

            # Wait for a known element that appears only after the page loads fully.
            # Adjust the selector to something that exists on the actual site.
            # For example, the logo or the navigation bar.
            try:
                await page.wait_for_selector("header", timeout=30000)  # or 'nav', '.logo', etc.
                print("✅ Main page loaded (challenge likely solved).")
            except Exception:
                print("⏳ Timeout waiting for header – page might still be under challenge.")
                # Try reloading once
                await page.reload(wait_until="networkidle")
                await page.wait_for_timeout(5000)
                # Check again
                await page.wait_for_selector("header", timeout=15000)

            # Step 2: Now call the API using fetch inside the page context.
            api_url = BASE_URL + API_ENDPOINT
            print("📡 Calling API via page.evaluate():", api_url)

            # We'll pass the API URL to evaluate and return the JSON response.
            result = await page.evaluate("""
                async (url) => {
                    const response = await fetch(url, {
                        method: 'GET',
                        headers: {
                            'Accept': 'application/json, text/plain, */*',
                            'X-Requested-With': 'XMLHttpRequest',
                            'Referer': document.location.origin + '/',
                        },
                        credentials: 'include'
                    });
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}`);
                    }
                    return await response.json();
                }
            """, api_url)

            print("✅ API data fetched successfully.")
            return result

        except Exception as e:
            print("❌ Playwright error:", e)
            # Optionally print the page content for debugging
            # content = await page.content()
            # print(content[:500])
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
