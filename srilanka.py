#!/usr/bin/env python3
"""
Sri Lanka Box Office Script – Fetches seat data for the current IST date only.
Auto‑refreshes Cloudflare cookies via Playwright each run, targeting the protected API.
"""

import json
import os
import random
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import asyncio

# ========== Try to import scraping libraries ==========
try:
    from curl_cffi import requests as curl_req
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

# ========== Playwright for CF cookie refresh ==========
try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ========== CONFIG ==========
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 5
MAX_RETRIES_PER_EVENT = 3
TIMEOUT_SEC = 30
CUT_OFF_MINUTES = 200
REGION_CODE = "SNLK"   # Original working region code

IST = ZoneInfo("Asia/Kolkata")
TARGET_DATE = datetime.now(IST).strftime("%Y%m%d")
YEAR = datetime.now(IST).strftime("%Y")

BASE_DIR = "srilanka"
BOXOFFICE_DIR = os.path.join(BASE_DIR, "boxoffice", YEAR)
MOVIE_DATA_DIR = os.path.join(BASE_DIR, "movie", "data")
os.makedirs(BOXOFFICE_DIR, exist_ok=True)
os.makedirs(MOVIE_DATA_DIR, exist_ok=True)

DAILY_FILE = os.path.join(BOXOFFICE_DIR, f"{datetime.now(IST).strftime('%m-%d')}.json")

CF_CLEARANCE = os.environ.get("CF_CLEARANCE", "")
CF_BM = os.environ.get("CF_BM", "")
print(f"🧩 CF_CLEARANCE present: {bool(CF_CLEARANCE)}")
print(f"🧩 CF_BM present: {bool(CF_BM)}")

AVG_PRICE = 500

# ========== HELPERS ==========
def slugify(title: str) -> str:
    slug = re.sub(r'[^a-zA-Z0-9\s]', '', title).strip().lower()
    slug = re.sub(r'\s+', '-', slug)
    return slug

def atomic_dump(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, path)

def random_user_agent():
    ios = f"Mozilla/5.0 (iPhone; CPU iPhone OS {random.randint(15,18)}_{random.randint(0,7)} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{random.randint(16,18)}.0 Mobile/15E148 Safari/604.1"
    android = f"Mozilla/5.0 (Linux; Android {random.choice(['10','11','12','13','14','15'])}; Pixel {random.randint(3,9)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110,129)}.0.{random.randint(1000,7000)}.{random.randint(50,250)} Mobile Safari/537.36"
    windows = f"Mozilla/5.0 (Windows NT {random.choice(['10.0','11.0'])}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110,129)}.0.{random.randint(1000,7000)}.{random.randint(50,250)} Safari/537.36"
    mac = f"Mozilla/5.0 (Macintosh; Intel Mac OS X {random.choice(['10_15_7','11_6','12_6','13_4','14_0','15_0'])}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{random.randint(14,18)}.0 Safari/605.1.15"
    return random.choice([ios, android, windows, mac])

def build_headers(extra=None, use_mobile=False):
    ua = random_user_agent()
    is_mobile = "Mobile" in ua or "iPhone" in ua or "Android" in ua
    platform = "iOS" if "iPhone" in ua else "Android" if "Android" in ua else "macOS" if "Mac" in ua else "Windows"
    chrome_ver = random.randint(110, 129)

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": random.choice(["en-GB,en;q=0.9", "en-US,en;q=0.8", "en-IN,en;q=0.9"]),
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "User-Agent": ua,
        "Referer": "https://m.bookmyshow.com/" if use_mobile else random.choice([
            "https://lk.bookmyshow.com/",
            "https://www.google.com/",
            "https://m.bookmyshow.com/"
        ]),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Origin": "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com",
        "Sec-CH-UA": f'"Google Chrome";v="{chrome_ver}", "Chromium";v="{chrome_ver}", "Not)A;Brand";v="{random.randint(24,99)}"',
        "Sec-CH-UA-Mobile": "?1" if is_mobile else "?0",
        "Sec-CH-UA-Platform": f'"{platform}"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1, i",
        "Connection": "keep-alive",
    }
    global CF_CLEARANCE, CF_BM
    cookie_parts = []
    if CF_CLEARANCE:
        cookie_parts.append(f"cf_clearance={CF_CLEARANCE}")
    if CF_BM:
        cookie_parts.append(f"__cf_bm={CF_BM}")
    if cookie_parts:
        headers["Cookie"] = "; ".join(cookie_parts)

    if extra:
        headers.update(extra)
    return {k: v for k, v in headers.items() if v is not None}

# ========== PLAYWRIGHT COOKIE FETCHER (targeting protected API) ==========
async def get_cf_cookies_playwright(protected_url: str, timeout: int = 60) -> Tuple[Optional[str], Optional[str]]:
    """
    Navigate to the protected API endpoint to trigger the Cloudflare challenge,
    then wait for the cf_clearance and __cf_bm cookies to be set.
    """
    if not HAS_PLAYWRIGHT:
        return None, None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 720},
            locale="en-US"
        )
        page = await context.new_page()
        print(f"🌐 Navigating to protected URL: {protected_url}")

        try:
            # Navigate to the API endpoint – it will return a 403 challenge page
            response = await page.goto(protected_url, wait_until="networkidle", timeout=timeout * 1000)
            print(f"📄 Initial response status: {response.status if response else 'N/A'}")

            # Wait for the challenge to be solved and cookies to appear
            start = time.time()
            cf_clearance = None
            cf_bm = None

            while time.time() - start < timeout:
                cookies = await context.cookies()
                cf_clearance = next((c['value'] for c in cookies if c['name'] == 'cf_clearance'), None)
                cf_bm = next((c['value'] for c in cookies if c['name'] == '__cf_bm'), None)
                if cf_clearance and cf_bm:
                    print("✅ Cookies found after challenge.")
                    break
                await asyncio.sleep(2)

            # If not found, maybe the challenge wasn't triggered; try reloading or fallback
            if not cf_clearance:
                print("⏳ No cookies yet. Reloading the page to trigger challenge...")
                await page.reload(wait_until="networkidle")
                await page.wait_for_timeout(5000)
                cookies = await context.cookies()
                cf_clearance = next((c['value'] for c in cookies if c['name'] == 'cf_clearance'), None)
                cf_bm = next((c['value'] for c in cookies if c['name'] == '__cf_bm'), None)

            if not cf_clearance:
                print("⚠️ Still no cf_clearance. Trying homepage as fallback...")
                await page.goto("https://lk.bookmyshow.com/", wait_until="networkidle")
                await page.wait_for_timeout(10000)
                cookies = await context.cookies()
                cf_clearance = next((c['value'] for c in cookies if c['name'] == 'cf_clearance'), None)
                cf_bm = next((c['value'] for c in cookies if c['name'] == '__cf_bm'), None)

        except Exception as e:
            print(f"⚠️ Playwright error: {e}")

        await browser.close()
        return cf_clearance, cf_bm

def ensure_cf_cookies(protected_url: str = "https://lk.bookmyshow.com/pwa/api/uapi/movies/", force: bool = False) -> bool:
    """
    Ensure CF_CLEARANCE and CF_BM are set. If force is True or cookies are missing,
    fetch fresh ones using Playwright targeting the protected API.
    """
    global CF_CLEARANCE, CF_BM

    if not force and CF_CLEARANCE and CF_BM:
        print("✅ Using existing CF cookies.")
        # Still test them quickly (optional)
        return True

    if not HAS_PLAYWRIGHT:
        print("⚠️ Playwright not installed. Cannot refresh cookies.")
        return False

    print("⏳ Fetching fresh CF cookies via Playwright (targeting protected API)...")
    try:
        cf_clearance, cf_bm = asyncio.run(get_cf_cookies_playwright(protected_url))
        if cf_clearance and cf_bm:
            CF_CLEARANCE = cf_clearance
            CF_BM = cf_bm
            os.environ["CF_CLEARANCE"] = cf_clearance
            os.environ["CF_BM"] = cf_bm
            print("✅ Fresh CF cookies obtained.")
            return True
        else:
            print("❌ Playwright did not return valid cookies.")
            return False
    except Exception as e:
        print(f"❌ Playwright failed: {e}")
        return False

# ========== SESSION CREATION ==========
def create_session():
    candidates = []

    if HAS_CURL:
        candidates.append(("curl_chrome120", lambda: curl_req.Session(impersonate="chrome120", timeout=TIMEOUT_SEC)))
        candidates.append(("curl_safari", lambda: curl_req.Session(impersonate="safari17_0", timeout=TIMEOUT_SEC)))

    if HAS_CLOUDSCRAPER:
        candidates.append(("cloudscraper", lambda: cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})))

    try:
        import requests
        candidates.append(("requests", lambda: requests.Session()))
    except:
        pass

    for name, creator in candidates:
        print(f"🧪 Trying {name}...")
        try:
            session = creator()
            test_url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
            test_payload = {"regionCode": REGION_CODE, "page": 1, "limit": 1}
            headers = build_headers()
            resp = session.post(test_url, json=test_payload, headers=headers, timeout=TIMEOUT_SEC)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"✅ {name} works! (status 200, got JSON)")
                    return session, False
                except:
                    print(f"❌ {name} returned 200 but not JSON – likely challenge page")
            else:
                print(f"❌ {name} – API status {resp.status_code}")
                # If 403, maybe cookies expired – trigger a refresh and retry once
                if resp.status_code == 403:
                    print("🔄 Attempting to refresh cookies and retry...")
                    if ensure_cf_cookies(force=True):
                        headers = build_headers()
                        resp = session.post(test_url, json=test_payload, headers=headers, timeout=TIMEOUT_SEC)
                        if resp.status_code == 200:
                            try:
                                data = resp.json()
                                print(f"✅ {name} works after refresh!")
                                return session, False
                            except:
                                pass
        except Exception as e:
            print(f"❌ {name} error: {e}")

    # Mobile subdomain fallback (only if curl is available)
    if HAS_CURL:
        print("🧪 Trying mobile subdomain...")
        try:
            session = curl_req.Session(impersonate="chrome120", timeout=TIMEOUT_SEC)
            test_url = "https://m.bookmyshow.com/pwa/api/uapi/movies/"
            test_payload = {"regionCode": REGION_CODE, "page": 1, "limit": 1}
            headers = build_headers(use_mobile=True)
            resp = session.post(test_url, json=test_payload, headers=headers, timeout=TIMEOUT_SEC)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print("✅ Mobile subdomain works!")
                    return session, True
                except:
                    pass
        except Exception as e:
            print(f"❌ Mobile subdomain error: {e}")

    return None, False

# ========== SAFE REQUEST (with auto‑refresh on 403) ==========
def safe_request(url, method="GET", payload=None, session=None, retries=RETRY_PER_REQUEST, use_mobile=False):
    if session is None:
        return None, "NO_SESSION"
    last_err = "UNKNOWN"
    for attempt in range(retries):
        try:
            headers = build_headers(use_mobile=use_mobile)
            if method == "POST":
                resp = session.post(url, json=payload, headers=headers)
            else:
                resp = session.get(url, headers=headers)

            if resp.status_code == 403:
                print("🔄 Received 403 – refreshing CF cookies...")
                if ensure_cf_cookies(force=True):
                    headers = build_headers(use_mobile=use_mobile)
                    if method == "POST":
                        resp = session.post(url, json=payload, headers=headers)
                    else:
                        resp = session.get(url, headers=headers)
                    if resp.status_code == 200:
                        try:
                            return resp.json(), None
                        except:
                            return None, "INVALID_JSON"
                else:
                    print("❌ Could not refresh CF cookies.")
                    return None, "HTTP_403"

            if resp.status_code != 200 and resp.status_code != 404:
                snippet = resp.text[:200] if resp.text else "empty"
                print(f"  ⚠️ Response snippet: {snippet}")

            if resp.status_code == 200:
                if resp.text.strip().startswith("<!DOCTYPE"):
                    print("  ❌ Received HTML")
                    return None, "HTML_RESPONSE"
                try:
                    return resp.json(), None
                except json.JSONDecodeError:
                    print(f"  ❌ Invalid JSON: {resp.text[:200]}")
                    return None, "INVALID_JSON"
            elif resp.status_code == 404:
                return None, "HTTP_404"
            else:
                last_err = f"HTTP_{resp.status_code}"
            time.sleep(random.uniform(1.0, 3.0))
        except Exception as e:
            print(f"  ❌ Request exception: {e}")
            last_err = str(e)
            time.sleep(random.uniform(1.0, 3.0))
    return None, last_err

# ========== API CALLS ==========
def get_movies(session, use_mobile=False):
    base = "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com"
    url = f"{base}/pwa/api/uapi/movies/"
    body = {
        "regionCode": REGION_CODE,
        "subCode": "",
        "filters": {},
        "genres": [],
        "languages": [],
        "formats": [],
        "page": 1,
        "limit": 200
    }
    return safe_request(url, "POST", payload=body, session=session, use_mobile=use_mobile)

def get_showtimes(event_code, date, session, use_mobile=False):
    base = "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com"
    url = f"{base}/pwa/api/de/showtimes/byevent?regionCode={REGION_CODE}&subCode=&eventCode={event_code}&dateCode={date}"
    return safe_request(url, "GET", session=session, use_mobile=use_mobile)

# ========== PARSERS ==========
def extract_movies(raw):
    if not isinstance(raw, dict):
        return []
    if "nowShowing" in raw and "arrEvents" in raw["nowShowing"]:
        return raw["nowShowing"]["arrEvents"]
    if "arrEvents" in raw:
        return raw["arrEvents"]
    if "movies" in raw:
        return raw["movies"]
    return []

def extract_venues(raw, date):
    details = raw.get("BookMyShow", {}).get("ShowDetails", [])
    for d in details:
        if str(d.get("Date")) == str(date):
            return d.get("Venues", [])
    return []

def flatten(movie_obj, venue, sh, date):
    session_id = sh.get("SessionId") or sh.get("Id") or ""
    total = sum(int(c.get("MaxSeats", 0)) for c in sh.get("Categories", []))
    avail = sum(int(c.get("SeatsAvail", 0)) for c in sh.get("Categories", []))
    price = float(sh.get("MinPrice", 0))
    sold = total - avail
    gross = sold * price
    occupancy = round((sold / total * 100), 2) if total else 0
    bad = False
    if sold < 0 or gross < 0 or avail > total or total == 0:
        sold, gross, occupancy = 0, 0, 0
        bad = True
    return {
        "movie": movie_obj["title"],
        "format": movie_obj["format"],
        "language": movie_obj["language"],
        "eventCode": movie_obj["eventCode"],
        "venue": venue.get("VenueName"),
        "sessionId": str(session_id),
        "time": sh.get("ShowTime"),
        "totalSeats": total,
        "available": avail,
        "sold": sold,
        "gross": gross,
        "occupancy": occupancy,
        "date": date,
        "badData": bad
    }

def scrape_event(movie, date, attempt, session_pool, use_mobile=False):
    session = session_pool.get()
    title = f"{movie['title']} ({movie['format'] or 'Standard'})"
    code = movie["eventCode"]
    res, err = get_showtimes(code, date, session=session, use_mobile=use_mobile)
    session_pool.put(session)
    if err in ["HTTP_404", "HTTP_500"]:
        return title, [], False
    if not res:
        return title, [], False
    venues = extract_venues(res, date)
    if not venues:
        return title, [], False
    rows = []
    for v in venues:
        for sh in v.get("ShowTimes", []):
            rows.append(flatten(movie, v, sh, date))
    return title, rows, True

# ========== MERGE & SAVE ==========
def load_existing_data(filepath: str) -> Dict[str, List[List[Any]]]:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def merge_and_save_daily(filepath: str, new_shows: List[Dict]):
    existing = load_existing_data(filepath)
    existing_map = {}
    for movie, entries in existing.items():
        for entry in entries:
            if len(entry) >= 6:
                eventCode, venue, showTime, total, sold, sessionId = entry
            else:
                eventCode, venue, showTime, total, sold = entry[:5]
                sessionId = f"{eventCode}-{showTime}"
            key = (eventCode, venue, sessionId)
            existing_map[key] = (movie, [eventCode, venue, showTime, total, sold, sessionId])

    for show in new_shows:
        key = (show["eventCode"], show["venue"], show["sessionId"])
        entry = [show["eventCode"], show["venue"], show["time"], show["totalSeats"], show["sold"], show["sessionId"]]
        existing_map[key] = (show["movie"], entry)

    result = {}
    for (_, _, _), (movie, entry) in existing_map.items():
        result.setdefault(movie, []).append(entry)

    atomic_dump(filepath, result)
    print(f"💾 Updated {filepath}")

# ========== MOVIE DATABASE BUILDER ==========
def update_movie_database():
    print("\n📊 Building movie database...")
    base_dir = os.path.join(BASE_DIR, "boxoffice")
    if not os.path.exists(base_dir):
        print("⚠️ No boxoffice data found.")
        return

    daily_files = []
    for year_dir in os.listdir(base_dir):
        year_path = os.path.join(base_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
        for file in os.listdir(year_path):
            if file.endswith(".json") and "-" in file:
                month_day = file.replace(".json", "")
                month, day = month_day.split("-")
                date_str = f"{year_dir}{month}{day}"
                daily_files.append((date_str, os.path.join(year_path, file)))

    if not daily_files:
        print("⚠️ No daily files found.")
        return

    movie_agg = defaultdict(lambda: defaultdict(lambda: {
        "shows": 0,
        "seats": 0,
        "sold": 0,
        "venues": set()
    }))

    for date_str, filepath in daily_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except:
            continue
        for movie, entries in data.items():
            shows = len(entries)
            seats = sum(e[3] for e in entries if len(e) >= 4)
            sold = sum(e[4] for e in entries if len(e) >= 5)
            venues = {e[1] for e in entries if len(e) >= 2}

            agg = movie_agg[movie][date_str]
            agg["shows"] += shows
            agg["seats"] += seats
            agg["sold"] += sold
            agg["venues"].update(venues)

    os.makedirs(MOVIE_DATA_DIR, exist_ok=True)
    index = []

    for movie, dates in movie_agg.items():
        slug = slugify(movie)
        day_rows = []
        total_gross = 0
        total_tickets = 0

        for date_str, stats in sorted(dates.items()):
            gross = stats["sold"] * AVG_PRICE
            total_gross += gross
            total_tickets += stats["sold"]
            venues_count = len(stats["venues"])
            day_rows.append([
                int(date_str),
                gross,
                stats["shows"],
                stats["seats"],
                venues_count
            ])

        movie_file = os.path.join(MOVIE_DATA_DIR, f"{slug}.json")
        atomic_dump(movie_file, day_rows)
        print(f"   📄 {movie_file}")

        index.append({
            "name": movie,
            "slug": slug,
            "totalGross": total_gross,
            "totalTickets": total_tickets
        })

    index_file = os.path.join(BASE_DIR, "movie", "index.json")
    atomic_dump(index_file, index)
    print(f"💾 {index_file}")
    print("✅ Movie database updated.\n")

# ========== MAIN ==========
def main():
    print("\n🚀 Sri Lanka Boxoffice Tracker Started...\n")
    target_date = TARGET_DATE
    print(f"📅 Processing date: {target_date} (IST)")

    # Always try to refresh cookies if Playwright is available
    protected_api_url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
    if HAS_PLAYWRIGHT:
        print("🔄 Playwright available – fetching fresh CF cookies from protected API.")
        if not ensure_cf_cookies(protected_url=protected_api_url, force=True):
            print("⚠️ Fresh cookie fetch failed. Will try without (may fail).")
    else:
        if not ensure_cf_cookies(force=False):
            print("⚠️ No valid CF cookies. Will try without (likely to fail).")

    # Create session
    session, use_mobile = create_session()
    if session is None:
        print("❌ All session creation strategies failed. Exiting.")
        sys.exit(1)

    # Fetch movies
    movies_raw, err = get_movies(session=session, use_mobile=use_mobile)
    if not movies_raw:
        print(f"❌ Failed to fetch movies. Error: {err}")
        if use_mobile:
            print("🔄 Retrying with desktop...")
            movies_raw, err = get_movies(session=session, use_mobile=False)
            if movies_raw:
                use_mobile = False
        if not movies_raw:
            print("❌ Cannot continue.")
            sys.exit(1)

    parent_movies = extract_movies(movies_raw)
    print(f"📽️ Found {len(parent_movies)} parent movies")

    expanded_movies = []
    for movie in parent_movies:
        for c in movie.get("ChildEvents", []):
            expanded_movies.append({
                "title": movie.get("EventTitle", "Unknown"),
                "eventCode": c.get("EventCode", ""),
                "format": c.get("EventDimension", ""),
                "language": c.get("EventLanguage", ""),
                "release": c.get("EventDate", "9999-99-99")
            })
    print(f"🎬 Expanded to {len(expanded_movies)} event variants")

    if not expanded_movies:
        print("⚠️ No event variants found – check API response")
        sys.exit(0)

    # Session pool
    session_pool = Queue()
    for _ in range(MAX_THREADS + 2):
        session_pool.put(session)

    retry_count = {m["eventCode"]: 0 for m in expanded_movies}
    all_rows = []
    pending = expanded_movies.copy()

    for attempt in range(1, SCRAPE_PASSES + 1):
        if not pending:
            break
        print(f"\n🔄 Scrape pass {attempt}/{SCRAPE_PASSES} – {len(pending)} events pending")
        next_round = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            futures = {pool.submit(scrape_event, m, target_date, attempt, session_pool, use_mobile): m for m in pending}
            for job in as_completed(futures):
                movie = futures[job]
                _, rows, ok = job.result()
                if ok:
                    all_rows.extend(rows)
                else:
                    code = movie["eventCode"]
                    retry_count[code] = retry_count.get(code, 0) + 1
                    if retry_count[code] < MAX_RETRIES_PER_EVENT:
                        next_round.append(movie)
                    else:
                        print(f"⏭️ Skipping {code} after {MAX_RETRIES_PER_EVENT} failed attempts")
        pending = next_round

    # Filter out shows that are too far in the past
    def parse_time(date_str, t):
        for fmt in ["%I:%M %p", "%H:%M"]:
            try:
                return datetime.strptime(f"{date_str} {t}", f"%Y%m%d {fmt}").replace(tzinfo=IST)
            except:
                pass
        return None

    def is_within_cutoff(show):
        st = parse_time(target_date, show["time"])
        if not st:
            return True
        mins_left = int((st - datetime.now(IST)).total_seconds() / 60)
        return mins_left < CUT_OFF_MINUTES

    eligible_new = [s for s in all_rows if is_within_cutoff(s)]
    print(f"✅ New shows scraped: {len(eligible_new)}")

    if eligible_new:
        merge_and_save_daily(DAILY_FILE, eligible_new)
    else:
        print("⚠️ No new shows to add.")

    update_movie_database()

    print("\n================================================")
    print(f"🎬 Event Variants Fetched: {len(expanded_movies)}")
    print(f"🎟 New Shows Added Today: {len(eligible_new)}")
    print(f"📁 Daily file → {DAILY_FILE}")
    print("================================================")
    print("🎉 DONE — Sri Lanka Boxoffice updated.\n")

if __name__ == "__main__":
    main()
