#!/usr/bin/env python3
"""
Sri Lanka Advance Bookings – Fetches seat data for the next 6 days (excluding today).
Saves each date to: srilanka/advance/YYYY/MM-DD.json (minified, value-only arrays).
No cutoff – all shows are included.
"""

import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, List, Any, Optional, Tuple

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

# ========== CONFIG ==========
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 5
MAX_RETRIES_PER_EVENT = 3
TIMEOUT_SEC = 30
REGION_CODE = "SNLK"
DAYS_AHEAD = 1   # number of future days to fetch

IST = ZoneInfo("Asia/Kolkata")
TODAY = datetime.now(IST).date()
YEAR = datetime.now(IST).strftime("%Y")

# Output directories – mirror advance structure
BASE_DIR = "srilanka"
ADVANCE_BASE = os.path.join(BASE_DIR, "advance")
os.makedirs(ADVANCE_BASE, exist_ok=True)

# Cloudflare cookies
CF_CLEARANCE = os.environ.get("CF_CLEARANCE", "")
CF_BM = os.environ.get("CF_BM", "")
print(f"🧩 CF_CLEARANCE present: {bool(CF_CLEARANCE)}")
print(f"🧩 CF_BM present: {bool(CF_BM)}")

# ========== HELPERS ==========
def atomic_dump(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, path)

def get_future_dates(days_ahead: int = DAYS_AHEAD) -> List[str]:
    """Return list of date strings (YYYYMMDD) for the next `days_ahead` days, excluding today."""
    return [(TODAY + timedelta(days=i)).strftime("%Y%m%d") for i in range(1, days_ahead + 1)]

def get_advance_filepath(date_str: str) -> str:
    """Return file path for a given date string (YYYYMMDD)."""
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:8]
    dir_path = os.path.join(ADVANCE_BASE, year)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{month}-{day}.json")

# ========== RANDOM HEADERS (same as box office) ==========
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

# ========== SESSION CREATION ==========
def create_session():
    """Try different libraries/impersonations, test by calling movie API."""
    candidates = []

    if HAS_CURL:
        candidates.append(("curl_safari", lambda: curl_req.Session(impersonate="safari17_0", timeout=TIMEOUT_SEC)))
        candidates.append(("curl_chrome", lambda: curl_req.Session(impersonate="chrome124", timeout=TIMEOUT_SEC)))
        candidates.append(("curl_edge", lambda: curl_req.Session(impersonate="edge124", timeout=TIMEOUT_SEC)))

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
        except Exception as e:
            print(f"❌ {name} error: {e}")

    # Mobile subdomain fallback
    if HAS_CURL:
        print("🧪 Trying mobile subdomain...")
        try:
            session = curl_req.Session(impersonate="safari17_0", timeout=TIMEOUT_SEC)
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

# ========== SAFE REQUEST ==========
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
            elif resp.status_code == 403:
                print("  🔄 403 detected, retrying...")
                last_err = "HTTP_403"
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

# ========== PROCESS A SINGLE DATE ==========
def process_date(date_str: str, session, use_mobile: bool, session_pool: Queue) -> Dict[str, List[List[Any]]]:
    """
    Fetch all shows for a given date (YYYYMMDD) and return a dict:
    {movie_title: [[eventCode, venue, showTime, totalSeats, sold, sessionId], ...]}
    """
    print(f"\n📅 Processing {date_str}...")
    
    # Fetch movie list (we do this once per date; could cache but fine)
    movies_raw, err = get_movies(session=session, use_mobile=use_mobile)
    if not movies_raw:
        print(f"❌ Failed to fetch movies for {date_str}: {err}")
        return {}

    parent_movies = extract_movies(movies_raw)
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

    if not expanded_movies:
        print(f"⚠️ No events for {date_str}")
        return {}

    retry_count = {m["eventCode"]: 0 for m in expanded_movies}
    all_rows = []
    pending = expanded_movies.copy()

    for attempt in range(1, SCRAPE_PASSES + 1):
        if not pending:
            break
        print(f"  🔄 Pass {attempt}/{SCRAPE_PASSES} – {len(pending)} events pending")
        next_round = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            futures = {pool.submit(scrape_event, m, date_str, attempt, session_pool, use_mobile): m for m in pending}
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
                        print(f"    ⏭️ Skipping {code} after {MAX_RETRIES_PER_EVENT} failed attempts")
        pending = next_round

    # No cutoff – include all shows
    print(f"  ✅ Collected {len(all_rows)} shows for {date_str}")

    # Convert to movie->list of compact arrays
    movie_data: Dict[str, List[List[Any]]] = {}
    for show in all_rows:
        movie_title = show["movie"]
        compact = [
            show["eventCode"],
            show["venue"],
            show["time"],
            show["totalSeats"],
            show["sold"],
            show["sessionId"]
        ]
        movie_data.setdefault(movie_title, []).append(compact)

    return movie_data

# ========== MAIN ==========
def main():
    print("\n🚀 Sri Lanka Advance Bookings Tracker Started...\n")
    future_dates = get_future_dates(DAYS_AHEAD)
    print(f"📅 Target dates: {future_dates}")

    # Create session
    session, use_mobile = create_session()
    if session is None:
        print("❌ All session creation strategies failed. Exiting.")
        sys.exit(1)

    # Session pool
    session_pool = Queue()
    for _ in range(MAX_THREADS + 2):
        session_pool.put(session)

    # Process each date
    for date_str in future_dates:
        data = process_date(date_str, session, use_mobile, session_pool)
        if not data:
            print(f"⚠️ No data for {date_str}, skipping file.")
            continue

        filepath = get_advance_filepath(date_str)
        atomic_dump(filepath, data)
        print(f"💾 Saved {filepath}")

    print("\n✅ All advance bookings updated.\n")

if __name__ == "__main__":
    main()
