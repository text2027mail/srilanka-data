#!/usr/bin/env python3
"""
Sri Lanka Box Office Scraper
Adapted from existing script – same scraping logic, but outputs:
  - srilanka/boxoffice/YYYY/MM-DD.json (minified, merged daily)
  - srilanka/movie/data/{slug}.json (per‑movie daily summaries)
  - srilanka/movie/index.json (overall index)
"""

import json
import os
import random
import time
import sys
import re
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
RETRY_PER_REQUEST = 5
SCRAPE_PASSES = 5
MAX_RETRIES_PER_EVENT = 5
TIMEOUT_SEC = 30
CUT_OFF_MINUTES = 500
REGION_CODE = "SNLK"
IST = ZoneInfo("Asia/Kolkata")

# Paths
BASE_DIR = "srilanka"
BOXOFFICE_DIR = os.path.join(BASE_DIR, "boxoffice")
MOVIE_DIR = os.path.join(BASE_DIR, "movie")
DATA_DIR = os.path.join(MOVIE_DIR, "data")
os.makedirs(BOXOFFICE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

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

def slugify(text):
    """Create a filesystem‑safe slug from movie title."""
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    text = re.sub(r'[-\s]+', '-', text)
    return text

def now_ist_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

def get_today():
    return datetime.now(IST).strftime("%Y%m%d")

def get_daily_file_path(date_str):
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:8]
    path = os.path.join(BOXOFFICE_DIR, year, f"{month}-{day}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

#########################################
# RANDOM HEADERS (includes cookies)
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
# SESSION CREATION – Directly test API
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
# SAFE REQUEST (with retry)
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
# API CALLS
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
# PARSERS (unchanged)
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
# MERGE & SAVE DAILY FILE
#########################################
def merge_and_save_daily(date_str, new_shows):
    """
    Merge new_shows (list of show dicts) into existing daily file.
    Unique key: (eventCode, venue, sessionId)
    Returns the merged list of shows for the day.
    """
    daily_path = get_daily_file_path(date_str)
    existing = {}
    if os.path.exists(daily_path):
        try:
            with open(daily_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # data has {"last_updated": ..., "movies": {movie: [[...]]}}
                movies = data.get("movies", {})
                for movie_title, shows in movies.items():
                    for show in shows:
                        # show: [eventCode, venue, showTime, totalSeats, sold, gross]
                        key = (show[0], show[1], show[2])  # eventCode, venue, showTime (showTime used as sessionId? We need sessionId, but we don't have it in compact format. Actually we need to store sessionId separately. We'll modify compact format: [eventCode, venue, showTime, sessionId, totalSeats, sold, gross]
                        # Wait, we need to decide the compact format. We'll include sessionId as the 4th element.
                        # Let's define: [eventCode, venue, showTime, sessionId, totalSeats, sold, gross]
                        # Then key = (eventCode, venue, sessionId)
                        # But we need to adjust merging.
                        pass
        except:
            pass

    # We'll rebuild from scratch using the new_shows and existing data.
    # Since we don't have a clean way to merge from compact format, we'll store the shows as dicts in a temp dict keyed by key.
    # We'll create a function to convert show dict to compact list and vice versa.
    pass

# Actually we'll implement a more straightforward approach: we keep a list of show dicts internally, then save compact.
# Let's redesign: we'll load existing daily file into a dict keyed by (eventCode, venue, sessionId) -> show dict.
# Then we merge new_shows, then save.

def load_daily_shows(date_str):
    """Load existing shows from daily file, return dict keyed by (eventCode, venue, sessionId) -> show dict."""
    daily_path = get_daily_file_path(date_str)
    shows = {}
    if os.path.exists(daily_path):
        try:
            with open(daily_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                movies = data.get("movies", {})
                for movie_title, compact_shows in movies.items():
                    for item in compact_shows:
                        # item: [eventCode, venue, showTime, sessionId, totalSeats, sold, gross]
                        if len(item) >= 7:
                            eventCode, venue, showTime, sessionId, totalSeats, sold, gross = item[:7]
                        else:
                            # fallback if older format missing sessionId? We'll skip.
                            continue
                        key = (eventCode, venue, sessionId)
                        shows[key] = {
                            "movie": movie_title,
                            "eventCode": eventCode,
                            "venue": venue,
                            "sessionId": sessionId,
                            "time": showTime,
                            "totalSeats": totalSeats,
                            "sold": sold,
                            "gross": gross,
                            "date": date_str,
                        }
        except Exception as e:
            print(f"⚠️ Error loading daily file: {e}")
    return shows

def save_daily_file(date_str, shows_dict):
    """Save shows_dict (keyed by (eventCode, venue, sessionId) -> show dict) to daily file."""
    # Group by movie title
    movies = defaultdict(list)
    for key, show in shows_dict.items():
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
    print(f"💾 Saved daily file: {daily_path}")

#########################################
# UPDATE MOVIE DATABASE
#########################################
def update_movie_database():
    """
    Scan all daily files in srilanka/boxoffice/*/*.json,
    aggregate per movie per date,
    write per-movie data files and index.
    """
    # Scan all daily files
    daily_files = []
    for root, dirs, files in os.walk(BOXOFFICE_DIR):
        for f in files:
            if f.endswith(".json"):
                daily_files.append(os.path.join(root, f))

    # Aggregate per movie per date: date_str, movie_title -> totalGross, totalShows, totalSeats, totalVenues
    movie_date_data = defaultdict(lambda: defaultdict(lambda: {
        "totalGross": 0,
        "totalShows": 0,
        "totalSeats": 0,
        "venues": set()
    }))

    for filepath in daily_files:
        # Extract date from folder/filename: path/YYYY/MM-DD.json
        parts = filepath.split(os.sep)
        # Assuming structure: .../boxoffice/YYYY/MM-DD.json
        if len(parts) >= 3:
            year = parts[-2]
            month_day = parts[-1].replace(".json", "")
            # month_day = "MM-DD"
            date_str = year + month_day.replace("-", "")
        else:
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                movies = data.get("movies", {})
                for movie_title, compact_shows in movies.items():
                    for item in compact_shows:
                        if len(item) >= 7:
                            _, venue, _, _, totalSeats, sold, gross = item[:7]
                        else:
                            continue
                        movie_date_data[movie_title][date_str]["totalGross"] += gross
                        movie_date_data[movie_title][date_str]["totalShows"] += 1
                        movie_date_data[movie_title][date_str]["totalSeats"] += totalSeats
                        movie_date_data[movie_title][date_str]["venues"].add(venue)
        except Exception as e:
            print(f"⚠️ Error reading {filepath}: {e}")

    # Now generate per-movie files and index
    index = []
    for movie_title, date_data in movie_date_data.items():
        slug = slugify(movie_title)
        # Sort dates
        sorted_dates = sorted(date_data.keys())
        per_movie_rows = []
        totalGross = 0
        totalTickets = 0
        for date_str in sorted_dates:
            d = date_data[date_str]
            totalGross += d["totalGross"]
            totalTickets += d["totalSold"]  # we need totalSold, we didn't track? We have sold per show, but we didn't sum sold. We should track totalSold as well. Actually we need totalTickets (sold). In aggregation we didn't track sold. We'll add totalSold.
            # We'll compute totalSold from each show.
            # But we didn't store sold individually? We stored sold per show and summed gross, but we also need sold sum.
            # We can compute totalSold by summing sold per show, but we only have totalSeats and gross, not sold. We need to store sold as well.
            # Let's adjust: we need to track totalSold per date.
            # We'll recompute by scanning again or modify aggregation to include sold.
            # Let's fix aggregation: include totalSold.
        # But we need to recalc. We'll add totalSold in the aggregation.
        # Let's redo aggregation with totalSold.
        pass

    # Since we need totalSold, we'll re-run aggregation including sold.
    # Better: we'll aggregate again correctly.
    # I'll rewrite the function properly.

def update_movie_database():
    """Scan all daily files and build movie database with totalSold included."""
    daily_files = []
    for root, dirs, files in os.walk(BOXOFFICE_DIR):
        for f in files:
            if f.endswith(".json"):
                daily_files.append(os.path.join(root, f))

    movie_date_data = defaultdict(lambda: defaultdict(lambda: {
        "totalGross": 0,
        "totalShows": 0,
        "totalSeats": 0,
        "totalSold": 0,
        "venues": set()
    }))

    for filepath in daily_files:
        parts = filepath.split(os.sep)
        if len(parts) >= 3:
            year = parts[-2]
            month_day = parts[-1].replace(".json", "")
            date_str = year + month_day.replace("-", "")
        else:
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                movies = data.get("movies", {})
                for movie_title, compact_shows in movies.items():
                    for item in compact_shows:
                        if len(item) >= 7:
                            _, venue, _, _, totalSeats, sold, gross = item[:7]
                        else:
                            continue
                        movie_date_data[movie_title][date_str]["totalGross"] += gross
                        movie_date_data[movie_title][date_str]["totalShows"] += 1
                        movie_date_data[movie_title][date_str]["totalSeats"] += totalSeats
                        movie_date_data[movie_title][date_str]["totalSold"] += sold
                        movie_date_data[movie_title][date_str]["venues"].add(venue)
        except Exception as e:
            print(f"⚠️ Error reading {filepath}: {e}")

    index = []
    for movie_title, date_data in movie_date_data.items():
        slug = slugify(movie_title)
        sorted_dates = sorted(date_data.keys())
        per_movie_rows = []
        totalGross = 0
        totalTickets = 0
        for date_str in sorted_dates:
            d = date_data[date_str]
            totalGross += d["totalGross"]
            totalTickets += d["totalSold"]
            per_movie_rows.append([
                int(date_str),
                d["totalGross"],
                d["totalShows"],
                d["totalSeats"],
                len(d["venues"])
            ])
        # Write per-movie file
        per_movie_path = os.path.join(DATA_DIR, f"{slug}.json")
        atomic_dump(per_movie_path, {
            "last_updated": now_ist_str(),
            "data": per_movie_rows
        })
        index.append({
            "name": movie_title,
            "slug": slug,
            "totalGross": totalGross,
            "totalTickets": totalTickets
        })

    # Write index
    index_path = os.path.join(MOVIE_DIR, "index.json")
    atomic_dump(index_path, {
        "last_updated": now_ist_str(),
        "movies": index
    })
    print(f"📊 Movie database updated – {len(index)} movies")

#########################################
# MAIN
#########################################
def main():
    print("\n🚀 Sri Lanka Boxoffice Tracker Started...\n")
    target_date = get_today()
    daily_path = get_daily_file_path(target_date)

    # Load existing shows
    existing_shows = load_daily_shows(target_date)
    print(f"📂 Loaded {len(existing_shows)} existing shows from daily file")

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

    # Create session pool (share session)
    session_pool = Queue()
    for _ in range(MAX_THREADS + 2):
        session_pool.put(session)

    retry_count = {m["eventCode"]: 0 for m in expanded_movies}
    new_shows = []
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
                    new_shows.extend(rows)
                else:
                    code = movie["eventCode"]
                    retry_count[code] = retry_count.get(code, 0) + 1
                    if retry_count[code] < MAX_RETRIES_PER_EVENT:
                        next_round.append(movie)
                    else:
                        print(f"⏭️ Skipping {code} after {MAX_RETRIES_PER_EVENT} failed attempts")
        pending = next_round

    # Filter by cutoff
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

    eligible_new = [s for s in new_shows if is_within_cutoff(s)]
    print(f"✅ New shows scraped (after cutoff): {len(eligible_new)}")

    # Merge with existing
    for show in eligible_new:
        key = (show["eventCode"], show["venue"], show["sessionId"])
        existing_shows[key] = show

    # Save daily file
    save_daily_file(target_date, existing_shows)
    print(f"📁 Daily file saved: {daily_path}")

    # Update movie database
    update_movie_database()

    print("\n🎉 DONE — CUT-OFF ADD ONLY | PERMANENT DB ACTIVE\n")

if __name__ == "__main__":
    main()
