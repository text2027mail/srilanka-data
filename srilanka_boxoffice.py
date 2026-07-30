#!/usr/bin/env python3
"""
Sri Lanka Box Office Scraper (Venue API)
- Uses /showtimes/byvenue endpoint
- Adds city/district/state from venues.json
- Outputs daily, per-movie, and index files
- Daily files are compressed with venue/movie numbering
- Includes format (2D/3D) and language in compact show records
- Index includes totalShows, totalSeats, occupancy
- Rotates cloudscraper identity per venue to avoid blocking
- Logs browser config success/failure for diagnostics
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

# PATCH: only keep cloudscraper (remove curl_cffi and requests fallback)
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    print("❌ cloudscraper is required. Install: pip install cloudscraper")
    sys.exit(1)

#########################################
# CONFIG
#########################################
MAX_THREADS = 2
RETRY_PER_REQUEST = 5
SCRAPE_PASSES = 3                # increased to allow more retries
MAX_RETRIES_PER_VENUE = 3
TIMEOUT_SEC = 30
CUT_OFF_MINUTES = 500
REGION_CODE = "SNLK"
IST = ZoneInfo("Asia/Kolkata")
BASE_DELAY = 1.0                 # base delay between requests

# Paths
BASE_DIR = "srilanka"
BOXOFFICE_DIR = os.path.join(BASE_DIR, "boxoffice")
MOVIE_DIR = os.path.join(BASE_DIR, "movie")
DATA_DIR = os.path.join(MOVIE_DIR, "data")
os.makedirs(BOXOFFICE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

VENUES_FILE = os.path.join(os.path.dirname(__file__), "venues.json")

# User‑Agent pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# cloudscraper valid platforms: windows, linux, darwin, android, ios
# valid browsers: chrome, firefox
# Remove invalid combos (firefox+ios caused errors)
BROWSER_CONFIGS = [
    {"browser": "chrome", "platform": "windows", "desktop": True},
    {"browser": "chrome", "platform": "android", "desktop": False},
    {"browser": "chrome", "platform": "ios", "desktop": False},
    {"browser": "firefox", "platform": "windows", "desktop": True},
    {"browser": "firefox", "platform": "android", "desktop": False},
]

# Statistics for browser config success/failure
config_stats = defaultdict(lambda: {"success": 0, "fail": 0})

def atomic_dump(path, data, indent=2, separators=(",", ":")):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, separators=separators)
    os.replace(tmp, path)

def slugify(text):
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

def random_bms_id():
    part1 = "1"
    part2 = str(random.randint(1000000000, 9999999999))
    ts = int(time.time() * 1000) + random.randint(-5000, 5000)
    part3 = str(ts)
    return f"{part1}.{part2}.{part3}"

def load_venues():
    if not os.path.exists(VENUES_FILE):
        print(f"❌ Venues file not found: {VENUES_FILE}")
        sys.exit(1)
    try:
        with open(VENUES_FILE, "r", encoding="utf-8") as f:
            venues_list = json.load(f)
        venue_map = {}
        name_details = {}
        for v in venues_list:
            code = v.get("VenueCode")
            name = v.get("VenueName")
            if code and name:
                details = {
                    "city": v.get("city", ""),
                    "district": v.get("district", ""),
                    "state": v.get("state", ""),
                }
                venue_map[code] = details
                name_details[name] = details
        print(f"📋 Loaded {len(venue_map)} venues")
        return venue_map, name_details
    except Exception as e:
        print(f"❌ Error loading venues.json: {e}")
        sys.exit(1)

#########################################
# HEADERS & SESSION (with rotation)
#########################################
def random_user_agent():
    return random.choice(USER_AGENTS)

def build_headers(extra=None, use_mobile=False):
    ua = random_user_agent()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": random.choice(["en-GB,en;q=0.9", "en-US,en;q=0.8", "en-IN,en;q=0.9"]),
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "User-Agent": ua,
        "Referer": "https://m.bookmyshow.com/" if use_mobile else "https://lk.bookmyshow.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Origin": "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
    }
    if extra:
        headers.update(extra)
    return headers

def create_session(browser_config=None):
    """Create a new cloudscraper session with a random browser config.
    Logs success/failure per config for diagnostic purposes.
    """
    if browser_config is None:
        browser_config = random.choice(BROWSER_CONFIGS)
    config_key = f"{browser_config['browser']}_{browser_config['platform']}_{browser_config['desktop']}"
    try:
        scraper = cloudscraper.create_scraper(browser=browser_config)
        # Warm‑up request to get cookies (both desktop and mobile)
        for url in ["https://lk.bookmyshow.com/", "https://m.bookmyshow.com/"]:
            try:
                scraper.get(url, headers={"User-Agent": random_user_agent()}, timeout=10)
                time.sleep(random.uniform(0.3, 0.8))
            except:
                pass
        # Success
        config_stats[config_key]["success"] += 1
        # Uncomment the next line if you want to see each success (can be verbose)
        # print(f"  ✅ Session created with config: {config_key}")
        return scraper
    except Exception as e:
        config_stats[config_key]["fail"] += 1
        print(f"⚠️ Failed to create session with config {config_key}: {e}")
        return None

def safe_request(url, session, retries=RETRY_PER_REQUEST, use_mobile=False):
    last_err = "UNKNOWN"
    for attempt in range(retries):
        try:
            headers = build_headers(use_mobile=use_mobile)
            resp = session.get(url, headers=headers, timeout=TIMEOUT_SEC)
            if resp.status_code == 200:
                if resp.text.strip().startswith("<!DOCTYPE"):
                    return None, "HTML_RESPONSE"
                try:
                    return resp.json(), None
                except json.JSONDecodeError:
                    return None, "INVALID_JSON"
            elif resp.status_code == 404:
                return None, "HTTP_404"
            elif resp.status_code in (403, 429):
                last_err = f"HTTP_{resp.status_code}"
            else:
                last_err = f"HTTP_{resp.status_code}"
            # Exponential backoff with jitter
            sleep_time = (2 ** attempt) * BASE_DELAY + random.uniform(0, 0.5)
            time.sleep(sleep_time)
        except Exception as e:
            last_err = str(e)
            time.sleep((2 ** attempt) * BASE_DELAY + random.uniform(0, 0.5))
    return None, last_err

#########################################
# API CALL
#########################################
def get_showtimes_by_venue(venue_code, date, session, use_mobile=False):
    bms_id = random_bms_id()
    base = "https://m.bookmyshow.com" if use_mobile else "https://lk.bookmyshow.com"
    url = (f"{base}/pwa/api/de/showtimes/byvenue"
           f"?venueCode={venue_code}&dateCode={date}"
           f"&regionCode={REGION_CODE}&bmsId={bms_id}")
    return safe_request(url, session, use_mobile=use_mobile)

#########################################
# PARSE RESPONSE
#########################################
def parse_venue_response(raw, date, venue_details):
    shows = []
    try:
        show_details = raw.get("BookMyShow", {}).get("ShowDetails", [])
        if not show_details:
            return shows
        for sd in show_details:
            venue_obj = sd.get("Venues", {})
            venue_name = venue_obj.get("VenueName", "")
            city = venue_details.get("city", "")
            district = venue_details.get("district", "")
            state = venue_details.get("state", "")
            events = sd.get("Event", [])
            for event in events:
                movie_title = event.get("EventTitle", "")
                for child in event.get("ChildEvents", []):
                    event_code = child.get("EventCode", "")
                    event_format = child.get("EventDimension", "")
                    language = child.get("EventLanguage", "")
                    for st in child.get("ShowTimes", []):
                        session_id = st.get("SessionId", "")
                        show_time = st.get("ShowTime", "")
                        total_seats = 0
                        available = 0
                        for cat in st.get("Categories", []):
                            total_seats += int(cat.get("MaxSeats", 0))
                            available += int(cat.get("SeatsAvail", 0))
                        sold = total_seats - available
                        if sold < 0:
                            sold = 0
                        price = float(st.get("MinPrice", 0))
                        gross = sold * price
                        occupancy = round((sold / total_seats * 100), 2) if total_seats else 0
                        if sold < 0 or gross < 0 or available > total_seats or total_seats == 0:
                            continue
                        shows.append({
                            "movie": movie_title,
                            "format": event_format,
                            "language": language,
                            "eventCode": event_code,
                            "venue": venue_name,
                            "sessionId": str(session_id),
                            "time": show_time,
                            "totalSeats": total_seats,
                            "available": available,
                            "sold": sold,
                            "gross": gross,
                            "occupancy": occupancy,
                            "date": date,
                            "city": city,
                            "district": district,
                            "state": state,
                        })
    except Exception as e:
        print(f"  ⚠️ Parse error: {e}")
    return shows

#########################################
# SCRAPE VENUE (with session rotation)
#########################################
def scrape_venue(venue_code, date, attempt, session_pool, venue_map, use_mobile=False):
    # Get a session from the pool
    session = session_pool.get()
    raw, err = get_showtimes_by_venue(venue_code, date, session, use_mobile)
    # If the request failed, we discard this session and create a new one
    if err or not raw:
        # Replace the failed session with a fresh one
        new_session = create_session()
        if new_session:
            session_pool.put(new_session)
        else:
            # If we can't create a new session, put back the old one (better than nothing)
            session_pool.put(session)
        return venue_code, [], False
    # If success, put the same session back
    session_pool.put(session)
    vd = venue_map.get(venue_code, {})
    shows = parse_venue_response(raw, date, vd)
    return venue_code, shows, True

#########################################
# DAILY FILE MERGE (compressed)
#########################################
def load_daily_shows(date_str):
    daily_path = get_daily_file_path(date_str)
    shows = {}
    if not os.path.exists(daily_path):
        return shows
    try:
        with open(daily_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "venueMap" in data:
            venue_map = data["venueMap"]
            movie_map = data["movieMap"]
            movies = data.get("movies", {})
            for movie_num, compact_shows in movies.items():
                movie_title = movie_map.get(str(movie_num), "")
                for item in compact_shows:
                    if len(item) >= 7:
                        eventCode, venue_num, showTime, sessionId, totalSeats, sold, gross = item[:7]
                        venue_name = venue_map.get(str(venue_num), {}).get("name", "")
                        fmt = item[7] if len(item) > 7 else ""
                        lang = item[8] if len(item) > 8 else ""
                    else:
                        continue
                    key = (eventCode, venue_name, sessionId)
                    shows[key] = {
                        "movie": movie_title,
                        "format": fmt,
                        "language": lang,
                        "eventCode": eventCode,
                        "venue": venue_name,
                        "sessionId": sessionId,
                        "time": showTime,
                        "totalSeats": totalSeats,
                        "sold": sold,
                        "gross": gross,
                        "date": date_str,
                    }
        else:
            movies = data.get("movies", {})
            for movie_title, compact_shows in movies.items():
                for item in compact_shows:
                    if len(item) >= 7:
                        eventCode, venue, showTime, sessionId, totalSeats, sold, gross = item[:7]
                        fmt = item[7] if len(item) > 7 else ""
                        lang = item[8] if len(item) > 8 else ""
                    else:
                        continue
                    key = (eventCode, venue, sessionId)
                    shows[key] = {
                        "movie": movie_title,
                        "format": fmt,
                        "language": lang,
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

def save_daily_file(date_str, shows_dict, venue_details_map):
    movie_set = set()
    venue_set = set()
    for show in shows_dict.values():
        movie_set.add(show["movie"])
        venue_set.add(show["venue"])

    movie_map = {str(i+1): title for i, title in enumerate(sorted(movie_set))}
    movie_to_num = {v: k for k, v in movie_map.items()}

    venue_map = {}
    venue_to_num = {}
    for i, vname in enumerate(sorted(venue_set), start=1):
        num = str(i)
        details = venue_details_map.get(vname, {})
        venue_map[num] = {
            "name": vname,
            "city": details.get("city", ""),
            "district": details.get("district", ""),
            "state": details.get("state", "")
        }
        venue_to_num[vname] = num

    movies = defaultdict(list)
    for show in shows_dict.values():
        movie_num = movie_to_num[show["movie"]]
        venue_num = venue_to_num[show["venue"]]
        movies[movie_num].append([
            show["eventCode"],
            int(venue_num),
            show["time"],
            show["sessionId"],
            show["totalSeats"],
            show["sold"],
            show["gross"],
            show.get("format", ""),
            show.get("language", "")
        ])

    data = {
        "last_updated": now_ist_str(),
        "venueMap": venue_map,
        "movieMap": movie_map,
        "movies": dict(movies)
    }

    daily_path = get_daily_file_path(date_str)
    tmp = daily_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, daily_path)
    print(f"💾 Saved daily file (compressed): {daily_path}")

#########################################
# MOVIE DATABASE UPDATE
#########################################
def update_movie_database():
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

        shows_dict = load_daily_shows(date_str)
        for show in shows_dict.values():
            movie_title = show["movie"]
            venue = show["venue"]
            totalSeats = show["totalSeats"]
            sold = show["sold"]
            gross = show["gross"]
            d = movie_date_data[movie_title][date_str]
            d["totalGross"] += gross
            d["totalShows"] += 1
            d["totalSeats"] += totalSeats
            d["totalSold"] += sold
            d["venues"].add(venue)

    index = []
    for movie_title, date_data in movie_date_data.items():
        slug = slugify(movie_title)
        sorted_dates = sorted(date_data.keys())
        per_movie_rows = []
        totalGross = 0
        totalTickets = 0
        totalShows = 0
        totalSeats = 0
        for date_str in sorted_dates:
            d = date_data[date_str]
            totalGross += d["totalGross"]
            totalTickets += d["totalSold"]
            totalShows += d["totalShows"]
            totalSeats += d["totalSeats"]
            per_movie_rows.append([
                int(date_str),
                d["totalGross"],
                d["totalShows"],
                d["totalSeats"],
                len(d["venues"])
            ])
        per_movie_path = os.path.join(DATA_DIR, f"{slug}.json")
        atomic_dump(per_movie_path, {
            "last_updated": now_ist_str(),
            "data": per_movie_rows
        })
        occupancy = round((totalTickets / totalSeats * 100), 2) if totalSeats > 0 else 0
        index.append({
            "name": movie_title,
            "slug": slug,
            "totalGross": totalGross,
            "totalTickets": totalTickets,
            "totalShows": totalShows,
            "totalSeats": totalSeats,
            "occupancy": occupancy
        })

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
    print("\n🚀 Sri Lanka Boxoffice Tracker (Venue API) Started...\n")

    venue_map, name_details = load_venues()

    target_date = get_today()
    daily_path = get_daily_file_path(target_date)

    existing_shows = load_daily_shows(target_date)
    print(f"📂 Loaded {len(existing_shows)} existing shows from daily file")

    # Create a pool of fresh sessions (one per thread plus extra)
    session_count = MAX_THREADS + 2
    session_pool = Queue()
    created = 0
    for _ in range(session_count):
        sess = create_session()
        if sess:
            session_pool.put(sess)
            created += 1
        else:
            # Don't exit; just try to create at least one session
            print(f"⚠️ Could not create session; will retry later.")
    if created == 0:
        # If no sessions at all, try again with a fallback config
        print("❌ No sessions created; trying with default config...")
        fallback_session = cloudscraper.create_scraper()
        if fallback_session:
            session_pool.put(fallback_session)
            created = 1
        else:
            print("❌ Failed to create any session. Exiting.")
            sys.exit(1)
    print(f"🔄 Session pool ready with {created} sessions.")

    # Print browser config statistics
    print("\n📊 Browser config statistics:")
    for config, stats in config_stats.items():
        print(f"  {config}: {stats['success']} success, {stats['fail']} fails")
    print()

    # Determine mobile mode – we test with first session
    use_mobile = False
    test_session = session_pool.get()
    # Try a quick test with a known venue
    test_venue = list(venue_map.keys())[0] if venue_map else "ALCN"
    test_date = get_today()
    for mobile_flag in [False, True]:
        raw, err = get_showtimes_by_venue(test_venue, test_date, test_session, mobile_flag)
        if raw and not err:
            use_mobile = mobile_flag
            print(f"✅ Using mobile mode: {use_mobile}")
            break
    else:
        print(f"⚠️ Could not determine mobile mode; defaulting to False.")
        use_mobile = False
    session_pool.put(test_session)

    venue_codes = list(venue_map.keys())
    print(f"🏢 Total venues: {len(venue_codes)}")

    retry_count = {vc: 0 for vc in venue_codes}
    pending = venue_codes.copy()
    all_new_shows = []

    for attempt in range(1, SCRAPE_PASSES + 1):
        if not pending:
            break
        print(f"\n🔄 Scrape pass {attempt}/{SCRAPE_PASSES} – {len(pending)} venues pending")
        next_round = []
        pass_shows = []

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            futures = {pool.submit(scrape_venue, vc, target_date, attempt, session_pool, venue_map, use_mobile): vc for vc in pending}
            for job in as_completed(futures):
                vc = futures[job]
                _, shows, ok = job.result()
                if ok:
                    if shows:
                        print(f"  ✅ Venue {vc}: {len(shows)} shows")
                        pass_shows.extend(shows)
                    else:
                        print(f"  ℹ️ Venue {vc}: no shows")
                else:
                    retry_count[vc] = retry_count.get(vc, 0) + 1
                    if retry_count[vc] < MAX_RETRIES_PER_VENUE:
                        print(f"  ❌ Venue {vc}: failed (attempt {retry_count[vc]}/{MAX_RETRIES_PER_VENUE}) – will retry")
                        next_round.append(vc)
                    else:
                        print(f"  ⛔ Venue {vc}: failed permanently after {MAX_RETRIES_PER_VENUE} attempts – skipped")

        all_new_shows.extend(pass_shows)
        print(f"  Pass {attempt} collected {len(pass_shows)} shows (total so far: {len(all_new_shows)})")
        pending = next_round
        if pending:
            print(f"  {len(pending)} venues will be retried in next pass")
            # Sleep a bit longer between passes to let rate limits reset
            time.sleep(5 + random.uniform(0, 3))

    # Cut‑off filter
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

    eligible_new = [s for s in all_new_shows if is_within_cutoff(s)]
    print(f"\n✅ Total new shows scraped: {len(all_new_shows)}; after cutoff: {len(eligible_new)}")

    # Merge
    for show in eligible_new:
        key = (show["eventCode"], show["venue"], show["sessionId"])
        existing_shows[key] = show

    # Build venueDetails map
    venue_details_map = {}
    for show in existing_shows.values():
        vname = show["venue"]
        if vname not in venue_details_map:
            if vname in name_details:
                venue_details_map[vname] = name_details[vname]
            else:
                city = show.get("city", "")
                district = show.get("district", "")
                state = show.get("state", "")
                venue_details_map[vname] = {"city": city, "district": district, "state": state}

    save_daily_file(target_date, existing_shows, venue_details_map)
    print(f"📁 Daily file saved: {daily_path}")

    update_movie_database()

    print("\n🎉 DONE — CUT-OFF ADD ONLY | PERMANENT DB ACTIVE | COMPRESSED DAILY FILES\n")

if __name__ == "__main__":
    main()
