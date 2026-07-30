import requests
import json
import random
import time
import cloudscraper
from threading import local

# ----- Configuration -----
venue_code = "ALCN"
date_code = "20260731"
region_code = "snlk"

# ----- Proxy list (add your own) -----
# Each proxy dict: {"http": "http://ip:port", "https": "http://ip:port"}
# For authenticated proxies: {"http": "http://user:pass@ip:port", ...}
PROXY_LIST = [
    # {"http": "http://1.2.3.4:8080", "https": "http://1.2.3.4:8080"},
    # {"http": "http://5.6.7.8:3128", "https": "http://5.6.7.8:3128"},
]

# ----- User‑Agent pool -----
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

thread_local = local()

# ----- Random bms_id generator -----
def random_bms_id():
    part1 = "1"
    part2 = str(random.randint(1000000000, 9999999999))
    # timestamp in milliseconds + small random offset
    ts = int(time.time() * 1000) + random.randint(-5000, 5000)
    part3 = str(ts)
    return f"{part1}.{part2}.{part3}"

# ----- Identity (scraper + headers + proxy) -----
class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.proxy = random.choice(PROXY_LIST) if PROXY_LIST else None
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
        # Warm up cookies (get cf_clearance)
        try:
            self.scraper.get(
                "https://lk.bookmyshow.com/",
                headers={"User-Agent": self.ua, "Accept": "text/html"},
                timeout=10,
                proxies=self.proxy,
            )
        except:
            pass
        time.sleep(random.uniform(0.5, 1.0))  # human‑like pause

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://lk.bookmyshow.com/",
            "Origin": "https://lk.bookmyshow.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity

# ----- Fetch data with random bms_id -----
def fetch_showtimes():
    bms_id = random_bms_id()
    url = (f"https://lk.bookmyshow.com/pwa/api/de/showtimes/byvenue"
           f"?venueCode={venue_code}&dateCode={date_code}"
           f"&regionCode={region_code}&bmsId={bms_id}")

    ident = get_identity()
    try:
        # Optional: second warm‑up before API call
        time.sleep(random.uniform(0.2, 0.5))

        response = ident.scraper.get(
            url,
            headers=ident.headers(),
            proxies=ident.proxy,
            timeout=15
        )

        # Detect Cloudflare block
        txt = response.text.strip().lower()
        if not txt.startswith("{") or "<html" in txt or "cf-chl" in txt:
            reset_identity()
            raise Exception("Cloudflare block detected – resetting identity")

        if response.status_code in [403, 429]:
            reset_identity()
            raise Exception(f"HTTP {response.status_code} – resetting")

        response.raise_for_status()
        return response.json()

    except Exception as e:
        print(f"Error: {e}")
        return None

# ----- Main -----
if __name__ == "__main__":
    data = fetch_showtimes()
    if data:
        print(json.dumps(data, indent=2))
    else:
        print("Failed to fetch data.")
