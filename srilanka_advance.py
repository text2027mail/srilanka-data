#!/usr/bin/env python3
"""
Sri Lanka Advance Bookings Scraper
Fetches showtimes for TOMORROW, overwrites daily file.
No cutoff, no merge, no movie DB.
"""

import json
import os
import random
import time
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from collections import defaultdict

# Only use curl_cffi and cloudscraper as fallback
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

#########################################
# CONFIG
#########################################
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 5
MAX_RETRIES_PER_EVENT = 3
TIMEOUT_SEC = 30
REGION_CODE = "SNLK"
IST = ZoneInfo("Asia/Kolkata")

# Paths – advance data stored separately
BASE_DIR = "srilanka"
ADVANCE_DIR = os.path.join(BASE_DIR, "advance")
os.makedirs(ADVANCE_DIR, exist_ok=True)

# Cloudflare cookies from env
CF_CLEARANCE = os.environ.get("CF_CLEARANCE", "")
CF_BM = os.environ.get("CF_BM", "")
print(f"🧩 CF_CLEARANCE present: {bool(CF_CLEARANCE)}")
print(f"🧩 CF_BM present: {bool(CF_BM)}")

def atomic_dump(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, separators=(",", ":"))  # minified
    os.replace(tmp, path)

def now_ist_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

def get_tomorrow():
    return (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

def get_daily_file_path(date_str):
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:8]
    path = os.path.join(ADVANCE_DIR, year, f"{month}-{day}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

#########################################
# RANDOM HEADERS (identical to boxoffice)
#########################################
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
    # Add both cookies if present
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

#########################################
# SESSION CREATION – identical
#########################################
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
            # Test it by calling the movie API
            test_url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
            test_payload = {"regionCode": REGION_CODE, "page": 1, "limit": 1}
            headers = build_headers()
            resp = session.post(test_url, json=test_payload, headers=headers, timeout=TIMEOUT_SEC)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"✅ {name} works! (status 200, got JSON)")
                    return session, False  # session, use_mobile=False
                except:
                    print(f"❌ {name} returned 200 but not JSON – likely challenge page")
            else:
                print(f"❌ {name} – API status {resp.status_code}")
        except Exception as e:
            print(f"❌ {name} error: {e}")

    # If everything fails, try mobile subdomain with curl
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

#########################################
# SAFE REQUEST – identical
#########################################
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

#########################################
# API CALLS – identical
#########################################
def get_movies(session=None, use_mobile=False):
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

def get_showtimes(event_code, date, session=None, use_mobile=False):
    base = "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com"
    url = f"{base}/pwa/api/de/showtimes/byevent?regionCode={REGION_CODE}&subCode=&eventCode={event_code}&dateCode={date}"
    return safe_request(url, "GET", session=session, use_mobile=use_mobile)

#########################################
# PARSERS – identical
#########################################
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

#########################################
# SAVE ADVANCE FILE (no merge, just overwrite)
#########################################
def save_advance_file(date_str, shows_list):
    """Save shows_list (list of show dicts) to daily advance file, overwriting."""
    # Group by movie title
    movies = defaultdict(list)
    for show in shows_list:
        movies[show["movie"]].append([
            show["eventCode"],
            show["venue"],
            show["time"],
            show["sessionId"],
            show["totalSeats"],
            show["sold"],
            show["gross"]
        ])
    data = {
        "last_updated": now_ist_str(),
        "movies": dict(movies)
    }
    daily_path = get_daily_file_path(date_str)
    atomic_dump(daily_path, data)
    print(f"💾 Saved advance file: {daily_path}")

#########################################
# MAIN
#########################################
def main():
    print("\n🚀 Sri Lanka Advance Bookings Tracker Started...\n")
    target_date = get_tomorrow()  # tomorrow's date
    print(f"📅 Target date: {target_date}")

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
            sys.exit(1)

    parent_movies = extract_movies(movies_raw)
    print(f"📽️ Found {len(parent_movies)} parent movies")

    expanded_movies = []
    for movie in parent_movies:
        for c in movie["ChildEvents"]:
            expanded_movies.append({
                "title": movie["EventTitle"],
                "eventCode": c["EventCode"],
                "format": c.get("EventDimension", ""),
                "language": c.get("EventLanguage", ""),
                "release": c.get("EventDate", "9999-99-99")
            })
    print(f"🎬 Expanded to {len(expanded_movies)} event variants")

    if not expanded_movies:
        print("⚠️ No event variants found – check API response")
        sys.exit(0)

    # Create session pool
    session_pool = Queue()
    for _ in range(MAX_THREADS + 2):
        session_pool.put(session)

    retry_count = {m["eventCode"]: 0 for m in expanded_movies}
    all_shows = []
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
                    all_shows.extend(rows)
                else:
                    code = movie["eventCode"]
                    retry_count[code] = retry_count.get(code, 0) + 1
                    if retry_count[code] < MAX_RETRIES_PER_EVENT:
                        next_round.append(movie)
                    else:
                        print(f"⏭️ Skipping {code} after {MAX_RETRIES_PER_EVENT} failed attempts")
        pending = next_round

    print(f"✅ Total shows scraped (no cutoff): {len(all_shows)}")

    # Save directly (overwrite, no merge)
    save_advance_file(target_date, all_shows)

    print("\n🎉 DONE — ADVANCE BOOKINGS SAVED\n")

if __name__ == "__main__":
    main()
