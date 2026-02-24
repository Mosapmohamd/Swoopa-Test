from playwright.sync_api import sync_playwright
import time, random, os, re, json, hashlib, logging, threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Set, List, Tuple
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# =====================================================
# FASTAPI APP
# =====================================================
app = FastAPI(title="Facebook Marketplace Scraper API")

# =====================================================
# PATHS & LOGGING
# =====================================================
output_dir = os.path.join(os.path.expanduser("~"), "Desktop", "scraping")
os.makedirs(output_dir, exist_ok=True)

ALL_JSONL_PATH = os.path.join(output_dir, "all_cars.jsonl")    # All opened ads
LEADS_JSONL_PATH = os.path.join(output_dir, "lead_cars.jsonl") # Only lead ads (<= MAX_LEAD_MINUTES)
SECURITY_SKIP_PATH = os.path.join(output_dir, "security_skip.json")
LOG_PATH = os.path.join(output_dir, "scraper.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# =====================================================
# CONFIG
# =====================================================
N8N_WEBHOOK_URL = "https://webhooks.eliaracarflex.cfd/webhook-test/facebook"
N8N_WEBHOOK_TIMEOUT = 100

MAX_LEAD_MINUTES = 60          # Main condition: a car is a lead if age in minutes <= this value
MIN_PRICE_RANGE = (1, 50)      # Range of "Min price" filter values to try

SECURITY_LINK_MAX_HITS = 1
MAX_NO_PROGRESS_ROUNDS = 4     # If we have no new links for this many rounds, stop the city
MAX_MIN_PRICE_CHANGES_PER_CITY = 2  # Max number of Min Price changes per city before stopping

SCRAPER_INTERVAL_SECONDS = 60  # How often the scraper runs in the background (seconds)

# Avoid running the scraper more than once at the same time inside this process
SCRAPER_LOCK = threading.Lock()

# Background thread control (inside same process)
BACKGROUND_THREAD_STARTED = False
BACKGROUND_THREAD_LOCK = threading.Lock()


def test_speed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Execution time: {end - start:.2f} sec")
        print("===" * 15)
        return result
    return wrapper


marketplace_links = [
    ("montreal", "https://www.facebook.com/marketplace/montreal/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("quebec", "https://www.facebook.com/marketplace/quebec/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("london", "https://www.facebook.com/marketplace/london_ontario/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"), 
    ("Toronto", "https://www.facebook.com/marketplace/toronto/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),   
    ("Barrie", "https://www.facebook.com/marketplace/barrie/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Sudbury", "https://www.facebook.com/marketplace/sudbury/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Sault Ste. Marie", "https://www.facebook.com/marketplace/106087732763236/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Timmins", "https://www.facebook.com/marketplace/114723638540069/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("North Bay","https://www.facebook.com/marketplace/105535069479513/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Windsor", "https://www.facebook.com/marketplace/windsor/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Sarnia","https://www.facebook.com/marketplace/106099112755478/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Thunder Bay","https://www.facebook.com/marketplace/111551465530472/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Peterborough","https://www.facebook.com/marketplace/107401009289940/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Ottawa","https://www.facebook.com/marketplace/106021666096708/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),
    ("Kitchener","https://www.facebook.com/marketplace/104045032964460/vehicles/?sortBy=creation_time_descend&topLevelVehicleType=car_truck&exact=false"),


]

SECURITY_PATTERNS = [
    "unusual login", "new login", "security check", "verify your account",
    "suspicious activity", "unreadme", "please review",
    "checkpoint required", "confirm your identity", "checkpoint",
]

BAD_TITLE_WORDS = [
    "chats", "chat", "messages", "messenger", "inbox",
    "notifications", "home", "marketplace", "facebook",
    "log in", "login", "sign up", "create new listing",
    "seller details", "send message", "follow", "share", "report",
    "see more", "see less", "notifications", "Notifications",
]

CAR_KEYWORDS = [
    "km", "mi", "miles", "kilometers", "engine", "transmission", "automatic", "manual",
    "clean", "maintained", "maintenance", "roof", "interior", "exterior", "tires",
    "wheels", "option", "optional", "feature", "condition", "service", "oil",
    "brakes", "tire", "wheel", "mileage", "odometer", "runs", "drives", "vehicle",
    "car", "truck", "suv", "van", "sedan", "coupe", "hatchback", "accident", "damage",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1600, "height": 900},
    {"width": 1920, "height": 1080},
]

# ==============================
# FACEBOOK STORAGE STATE (cookies)
# ==============================
#===================james=================
STORAGE_STATE = {
    "cookies": [
        {
            "name": "datr",
            "value": "cB6eac3Qe9YSOeZzh2wfgGtH",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1806530160.782025,
            "httpOnly": True,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "wd",
            "value": "1264x752",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1772574967,
            "httpOnly": False,
            "secure": True,
            "sameSite": "Lax",
        },
        {
            "name": "dpr",
            "value": "1.0000000298023224",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1772574981,
            "httpOnly": False,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "c_user",
            "value": "61587310108017",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1803506185.406976,
            "httpOnly": False,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "sb",
            "value": "fx6eacPk18QkEo8x79eEhPiZ",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1806530181.337522,
            "httpOnly": True,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "ps_l",
            "value": "1",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1806530183.219685,
            "httpOnly": True,
            "secure": True,
            "sameSite": "Lax",
        },
        {
            "name": "ps_n",
            "value": "1",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1806530183.219852,
            "httpOnly": True,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "fr",
            "value": "220mOM39TOrqWbR2h.AWeqtJsDVxyYM3jkxgdz5fEvNoexC-PW8K4su1nPmOyA3gXde1I.Bpnh6I..AAA.0.0.Bpnh6I.AWca2RBseBbo-Gip_5XTubMVeIw",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1779746185.407057,
            "httpOnly": True,
            "secure": True,
            "sameSite": "None",
        },
        {
            "name": "xs",
            "value": "23%3A93oypF-P5a5PEA%3A2%3A1771970176%3A-1%3A-1%3A%3AAcyiQheQpvo_JQ9xiljTOm_oQy1ZnebDZI-mBpgptw",
            "domain": ".facebook.com",
            "path": "/",
            "expires": 1803506185.407098,
            "httpOnly": True,
            "secure": True,
            "sameSite": "None",
        },
    ],

    "origins": [
        {
            "origin": "https://www.facebook.com",
            "localStorage": [
                {
                    "name": "screen_time_period_logging_facebook",
                    "value": "{\"actorId\":\"61587310108017\",\"currentIntervalLastTick\":1771970184,\"currentIntervalStart\":1771970184,\"initRemoteAgg\":{\"timestamp\":1771876527,\"today_s\":0},\"intervals\":[],\"lastCallbackAttempt\":1771970184,\"localStorageId\":\"71645813\",\"syncedL24HS\":null,\"syncedTodayS\":null}"
                },
                {
                    "name": "last_headload_time",
                    "value": "1771970184777",
                },
                {
                    "name": "hb_timestamp",
                    "value": "1771970167616",
                },
                {
                    "name": "mw_encrypted_backups_restore_upsell_first_impression_time_key",
                    "value": "1771970185797",
                },
                {
                    "name": "signal_flush_timestamp",
                    "value": "1771970167633",
                },
                {
                    "name": "Session",
                    "value": "so3d0s:1771970222069",
                },
                {
                    "name": "banzai:last_storage_flush",
                    "value": "1771970167420.8",
                },
            ],
        }
    ],
}

# =====================================================
# UTILS
# =====================================================
def human_delay(a=0.25, b=0.75):
    """Random short sleep to simulate more human-like actions."""
    time.sleep(random.uniform(a, b))


def is_bad_title(title: str) -> bool:
    """Heuristics to decide if a title looks like a non-car / noise title."""
    t = (title or "").strip().lower()
    if not t:
        return True
    if len(t) < 3:
        return True
    if any(w in t for w in BAD_TITLE_WORDS):
        return True
    return False


def sanitize_storage_state(state: dict) -> dict:
    """Clean storage_state: remove invalid expires entries etc."""
    clean = {"cookies": [], "origins": state.get("origins", [])}
    for c in state.get("cookies", []):
        c2 = dict(c)
        if "expires" in c2 and (c2["expires"] is None or c2["expires"] <= 0):
            c2.pop("expires", None)
        clean["cookies"].append(c2)
    return clean


def enable_speed_routes(context):
    """Block images/media/fonts to speed up scraping."""
    def handler(route):
        r = route.request
        if r.resource_type in ("image", "media", "font"):
            return route.abort()
        return route.continue_()
    context.route("**/*", handler)

def send_lead_to_n8n(lead: dict) -> bool:
    try:
        r = requests.post(
            N8N_WEBHOOK_URL,
            json=lead,
            timeout=N8N_WEBHOOK_TIMEOUT,
        )
        r.raise_for_status()
        logging.info("📤 Lead sent to n8n")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to send lead to n8n: {e}")
        return False

def append_jsonl(path: str, record: dict):
    """Append a single JSON object as a line to a .jsonl file."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        logging.error(f"❌ Failed writing JSONL: {e}")


def load_seen_links_from_jsonl(path: str) -> Set[str]:
    """Load all 'Link' values from a jsonl file into a set."""
    seen = set()
    if not os.path.exists(path):
        return seen
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    link = obj.get("Link")
                    if link:
                        seen.add(link)
                except:
                    continue
    except:
        pass
    return seen


def load_security_skip(path: str) -> Dict[str, int]:
    """Load the dict of 'link -> number of security hits' from JSON."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return {str(k): int(v) for k, v in obj.items()}
    except:
        pass
    return {}


def save_security_skip(path: str, skip: Dict[str, int]):
    """Save the 'link -> security hits' dict as JSON."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(skip, f, ensure_ascii=False, indent=2)
    except:
        pass


def is_checkpoint_text(text: str) -> bool:
    """Check if text contains any of the security / checkpoint patterns."""
    t = (text or "").lower()
    return any(p in t for p in SECURITY_PATTERNS)


def page_is_checkpoint(page) -> bool:
    """Detect if the current page looks like a security/checkpoint page."""
    try:
        body = page.inner_text("body")[:2500]
        return is_checkpoint_text(body)
    except Exception as e:
        logging.warning(f"page_is_checkpoint error: {e}")
        return False


def refresh_page(page):
    """Hard refresh using Ctrl+Shift+R."""
    try:
        page.keyboard.down("Control")
        page.keyboard.down("Shift")
        page.keyboard.press("R")
        page.keyboard.up("Shift")
        page.keyboard.up("Control")
        human_delay(1.2, 2.0)
    except Exception as e:
        logging.warning(f"Refresh error: {e}")


def change_min_price(page) -> bool:
    """
    Change the "Min price" filter in the marketplace sidebar.

    Strategy:
      - Scroll to top so the filter is visible.
      - Try several selectors (aria-label, placeholder, etc.).
      - If found, clear input, type random price, press Enter.
    """
    try:
        price = random.randint(*MIN_PRICE_RANGE)

        # Scroll to top to bring filters into view
        try:
            page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass
        human_delay(0.4, 0.8)

        # Give time for filters to load
        page.wait_for_timeout(1500)

        selectors = [
            'input[aria-label="Minimum range"]',
            'input[aria-label*="Minimum"]',
            'input[placeholder="Min."]',
            'input[placeholder*="Min"]',
        ]

        input_box = None
        for sel in selectors:
            try:
                loc = page.locator(sel)
                count = loc.count()
                logging.info(f"[MinPrice] Trying selector '{sel}', found {count} elements.")
                if count > 0:
                    loc.first.wait_for(state="visible", timeout=8000)
                    input_box = loc.first
                    break
            except Exception as e:
                logging.info(f"[MinPrice] Selector '{sel}' failed: {e}")
                continue

        if input_box is None:
            logging.warning("[MinPrice] Could not find min price input with any selector.")
            try:
                shot_path = os.path.join(output_dir, "min_price_not_found.png")
                page.screenshot(path=shot_path, full_page=True)
                logging.warning(f"[MinPrice] Saved NOT FOUND screenshot to: {shot_path}")
            except Exception:
                pass
            return False

        try:
            input_box.scroll_into_view_if_needed()
            human_delay(0.15, 0.3)
        except Exception:
            pass

        input_box.click()
        human_delay(0.1, 0.25)

        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.type(str(price), delay=40)
        page.keyboard.press("Enter")

        human_delay(0.8, 1.2)
        logging.info(f"✅ Min price changed to {price}")
        return True

    except Exception as e:
        logging.warning(f"[MinPrice] error: {e}")
        try:
            shot_path = os.path.join(output_dir, "min_price_debug.png")
            page.screenshot(path=shot_path, full_page=True)
            logging.warning(f"[MinPrice] Saved DEBUG screenshot to: {shot_path}")
        except Exception:
            pass
        return False


# =====================================================
# TIME PARSING
# =====================================================
def parse_facebook_time(text: str) -> Optional[datetime]:
    """
    Parse relative Facebook time text into a datetime.
    Supports English, French, and some Arabic patterns, e.g.:
      - "Listed 9 minutes ago in Montréal"
      - "9 min"
      - "il y a 3 minutes"
      - "منذ 3 دقائق"
    """
    now = datetime.now()
    if not text:
        return None

    t = text.strip().lower()

    m_listed = re.search(
        r"listed\s+(\d+)\s+(minute|minutes|min|hour|hours|hr|hrs|day|days)\s+ago",
        t
    )
    if m_listed:
        val = int(m_listed.group(1))
        unit = m_listed.group(2)
        if unit.startswith("min"):
            return now - timedelta(minutes=val)
        if unit.startswith("hour") or unit in ("h", "hr", "hrs"):
            return now - timedelta(hours=val)
        if unit.startswith("day") or unit == "d":
            return now - timedelta(days=val)

    m_ago = re.search(
        r"(\d+)\s+(minute|minutes|min|hour|hours|hr|hrs|day|days)\s+ago",
        t
    )
    if m_ago:
        val = int(m_ago.group(1))
        unit = m_ago.group(2)
        if unit.startswith("min"):
            return now - timedelta(minutes=val)
        if unit.startswith("hour") or unit in ("h", "hr", "hrs"):
            return now - timedelta(hours=val)
        if unit.startswith("day") or unit == "d":
            return now - timedelta(days=val)

    m_en_min = re.search(r"(\d+)\s*(min|mins|minute|minutes|m)\b", t)
    if m_en_min:
        return now - timedelta(minutes=int(m_en_min.group(1)))

    m_en_hr = re.search(r"(\d+)\s*(h|hr|hrs|hour|hours)\b", t)
    if m_en_hr:
        return now - timedelta(hours=int(m_en_hr.group(1)))

    m_en_day = re.search(r"(\d+)\s*(d|day|days)\b", t)
    if m_en_day:
        return now - timedelta(days=int(m_en_day.group(1)))

    m_fr = re.search(
        r"il y a\s+(\d+)\s*(min|mins|minute|minutes|h|heure|heures|j|jour|jours)",
        t
    )
    if m_fr:
        val = int(m_fr.group(1))
        unit = m_fr.group(2)
        if unit in ("min", "mins", "minute", "minutes"):
            return now - timedelta(minutes=val)
        if unit in ("h", "heure", "heures"):
            return now - timedelta(hours=val)
        if unit in ("j", "jour", "jours"):
            return now - timedelta(days=val)

    m_ar = re.search(r"منذ\s+(\d+)\s*(دقيقة|دقائق|دقيقه|ساعة|ساعات|يوم|أيام)", t)
    if m_ar:
        val = int(m_ar.group(1))
        unit = m_ar.group(2)
        if unit.startswith("دقي"):
            return now - timedelta(minutes=val)
        if unit.startswith("ساع"):
            return now - timedelta(hours=val)
        if unit.startswith("يوم") or unit.startswith("أيا"):
            return now - timedelta(days=val)

    if "just now" in t:
        return now
    if "yesterday" in t:
        return now - timedelta(days=1)

    return None


def is_car_description(text: str) -> bool:
    """Heuristic check whether a text looks like a car description."""
    if not text:
        return False
    if is_checkpoint_text(text):
        return False
    t = text.lower()
    keyword_count = sum(1 for kw in CAR_KEYWORDS if kw in t)
    if not (50 < len(text) < 2000):
        return False
    return keyword_count >= 1


def dedupe_texts(texts: List[str]) -> List[str]:
    """Remove near-duplicate texts by normalizing and deduplicating."""
    seen = set()
    out = []
    for s in texts:
        k = re.sub(r"\W+", "", s.lower())
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out


def ensure_page_open(context, page):
    """Make sure we have an open page; create a new one if closed."""
    if page is None or page.is_closed():
        return context.new_page()
    return page


# =====================================================
# SELLER / TITLE / PRICE / ODOMETER / DESC
# =====================================================
def extract_seller_name_from_ad_page(page) -> str:
    """
    Try to extract the seller's name from the ad page.
    Uses profile links and some heuristics to avoid common non-name texts.
    """
    BAD = [
        "seller details", "seller", "marketplace", "facebook",
        "message", "send message", "follow", "see more", "see less",
        "view profile", "notifications", "home", "report", "share",
        "create new listing", "listing", "listings",
        "active listing", "active listings",
    ]

    def norm(x: str) -> str:
        return re.sub(r"\s+", " ", (x or "").strip())

    def bad(x: str) -> bool:
        s = norm(x).lower()
        if not s or len(s) < 3:
            return True
        if any(b in s for b in BAD):
            return True
        if re.search(r"\b(just now|yesterday|\d+\s*(minute|hour|day)s?)\b", s):
            return True
        return False

    def looks_like_name(x: str) -> bool:
        x = norm(x)
        if bad(x):
            return False
        parts = x.split()
        return 1 <= len(parts) <= 4 and len(x) <= 40

    selectors = [
        'a[href*="/marketplace/profile/"] span[dir="auto"]',
        'a[href*="/profile.php"] span[dir="auto"]',
        'a[href*="/people/"] span[dir="auto"]',
        'a[href*="/marketplace/profile/"] span',
        'a[href*="/profile.php"] span',
        'a[href*="/people/"] span',
    ]

    try:
        for sel in selectors:
            els = page.query_selector_all(sel)
            for el in els[:10]:
                try:
                    txt = norm(el.inner_text())
                    if looks_like_name(txt):
                        return txt
                except:
                    continue
    except:
        pass

    # Fallback: scan a bunch of span[dir="auto"] and pick best name-like candidate
    try:
        cands = []
        for sp in page.query_selector_all('span[dir="auto"]')[:180]:
            try:
                txt = norm(sp.inner_text())
                if looks_like_name(txt):
                    cands.append(txt)
            except:
                continue
        if cands:
            cands = list(dict.fromkeys(cands))
            cands.sort(key=lambda x: (abs(len(x.split()) - 2), abs(len(x) - 16)))
            return cands[0]
    except:
        pass

    return "N/A"


PRICE_RE = re.compile(
    r"(?:\bCA\$|\bC\$|\bCAD\b|\$)\s*[\d]{1,3}(?:[,\s]\d{3})*(?:\.\d{1,2})?",
    re.IGNORECASE
)


def _parse_price(text: str) -> Optional[str]:
    """Extract a normalized price string from text (e.g. '$10,200' → '$10,200')."""
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    return re.sub(r"\s+", "", m.group(0))


def extract_price_from_ad_page(page) -> str:
    """
    Try to extract the price from the ad page by:
      1) Looking at elements following the <h1> title.
      2) Looking for any element containing a price pattern.
      3) As a fallback, scanning the body text.
    """
    try:
        # 1) Elements after the first h1
        candidates = page.locator(
            "xpath=(//h1)[1]/following::*[self::span or self::div or self::strong][normalize-space()]"
        ).filter(has_text=PRICE_RE)

        n = min(candidates.count(), 80)
        for i in range(n):
            txt = (candidates.nth(i).text_content() or "").strip()
            p = _parse_price(txt)
            if p:
                return p

        # 2) Any element containing a price pattern
        any_price = page.locator(
            "xpath=//*[self::span or self::div or self::strong][normalize-space()]"
        ).filter(has_text=PRICE_RE).first

        if any_price.count() > 0:
            txt = (any_price.text_content() or "").strip()
            p = _parse_price(txt)
            if p:
                return p

        # 3) Fallback: scan body text
        body = (page.text_content("body") or "")[:10000]
        p = _parse_price(body)
        return p or "N/A"

    except Exception:
        return "N/A"


def extract_true_title(page) -> str:
    """Extract a clean title for the ad, preferring h1 > span[dir='auto'] then plain h1."""
    try:
        loc = page.locator("h1 span[dir='auto']").first
        if loc.count() > 0:
            t = (loc.text_content() or "").strip()
            t = re.sub(r"\s+", " ", t)
            if 1 <= len(t) <= 160:
                return t

        h1 = page.locator("h1").first
        if h1.count() > 0:
            t = (h1.text_content() or "").strip()
            t = re.sub(r"\s+", " ", t)
            if 1 <= len(t) <= 160:
                return t
    except:
        pass
    return "N/A"


def _extract_odometer_from_text(text: str) -> Optional[str]:
    """
    Extract odometer (mileage) from raw text.

    Supports patterns like:
      - "134 000 km"
      - "261 000km"
      - "147000 km"
      - "85,000 km"
      - "120000 miles"

    Skips lines containing L/100km (fuel consumption).
    """
    if not text:
        return None

    txt = text.lower()

    # Remove lines that are clearly fuel consumption (e.g. "8.3 L/100km")
    cleaned_lines = []
    for line in txt.splitlines():
        if "l/100" in line:
            continue
        cleaned_lines.append(line)
    txt = " ".join(cleaned_lines)

    matches = re.findall(
        r"(\d[\d\s.,]{2,})\s*(km|kms|kilometers?|kilomètres?|mi|miles?)\b",
        txt
    )

    candidates = []
    for num_raw, unit_raw in matches:
        num_str = re.sub(r"[^\d]", "", num_raw)
        if not num_str:
            continue
        try:
            val = int(num_str)
        except ValueError:
            continue

        # Filter out obviously wrong values
        if val < 1000:
            continue
        if val > 1_000_000:
            continue

        unit = "km" if "km" in unit_raw or "kilom" in unit_raw else "mi"
        candidates.append((val, unit))

    if not candidates:
        return None

    # Use the largest candidate as the most likely odometer value
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_val, best_unit = candidates[0]
    return f"{best_val} {best_unit}"


def extract_odometer(page) -> str:
    """
    Extract odometer value from:
      1) A 'driven ... km' element on the page.
      2) The main description block.
      3) As a fallback, the entire body text.
    """
    # 1) "driven xxx km" in DOM
    try:
        for sp in page.query_selector_all("span[dir='auto']"):
            txt = (sp.inner_text() or "").strip()
            if txt.lower().startswith("driven"):
                m = re.search(r"([\d.,\s]+)\s*(km|kms|kilometers?|mi|miles?)\b", txt.lower())
                if m:
                    num = re.sub(r"[^\d]", "", m.group(1))
                    unit = "km" if "km" in m.group(2) or "kilometer" in m.group(2) else "mi"
                    if num:
                        return f"{num} {unit}"
    except Exception:
        pass

    # 2) Description block
    try:
        el = page.query_selector('div[data-ad-preview="message"]')
        if el:
            desc = el.inner_text() or ""
            od = _extract_odometer_from_text(desc)
            if od:
                return od
    except Exception:
        pass

    # 3) Full body text
    try:
        body_text = (page.text_content("body") or "")[:15000]
    except Exception:
        body_text = ""

    od = _extract_odometer_from_text(body_text)
    return od or "N/A"


def click_see_more_buttons(page):
    """Click a few 'See more' buttons to expand truncated text."""
    try:
        btns = page.query_selector_all(
            'div[role="button"]:has-text("See more"),button:has-text("See more")'
        )
        for btn in btns[:4]:
            try:
                if btn.is_visible():
                    btn.scroll_into_view_if_needed()
                    human_delay(0.10, 0.25)
                    btn.click()
                    human_delay(0.15, 0.35)
            except:
                continue
    except:
        pass


def extract_description(page) -> str:
    """Extract the best candidate for the ad's description."""
    try:
        human_delay(0.25, 0.55)
        click_see_more_buttons(page)

        el = page.query_selector('div[data-ad-preview="message"]')
        if el:
            txt = re.sub(r"\s+", " ", (el.inner_text() or "").strip())
            if is_car_description(txt):
                return txt

        cands = []
        for sp in page.query_selector_all('span[dir="auto"]'):
            try:
                t = re.sub(r"\s+", " ", (sp.inner_text() or "").strip())
                if len(t) < 30:
                    continue
                if is_car_description(t):
                    cands.append(t)
            except:
                continue
        cands = dedupe_texts(cands)
        if cands:
            cands.sort(
                key=lambda x: (
                    sum(1 for kw in CAR_KEYWORDS if kw in x.lower()),
                    len(x)
                ),
                reverse=True
            )
            return cands[0].replace("See more", "").replace("See less", "").strip()

        html = page.content().lower()
        if any(p in html for p in SECURITY_PATTERNS):
            return "SECURITY_CHECK_REQUIRED"
    except:
        pass
    return "N/A"


def extract_creation_time_from_ad_page(page) -> Tuple[Optional[datetime], Optional[str]]:
    """
    Try to parse a relative creation time from multiple elements
    (span, div, strong) up to a limit.
    """
    try:
        selectors = ["span", "div", "strong"]
        seen_texts = set()
        for sel in selectors:
            for el in page.query_selector_all(sel)[:400]:
                try:
                    txt = (el.inner_text() or "").strip()
                    if not txt or txt in seen_texts:
                        continue
                    seen_texts.add(txt)
                    parsed = parse_facebook_time(txt)
                    if parsed:
                        return parsed, txt
                except:
                    continue
    except:
        pass
    return None, None


# =====================================================
# LEAD LOGIC
# =====================================================
def is_valid_lead(row: dict) -> bool:
    """
    Decide if a scraped row is a lead.
    Currently: only age in minutes matters (<= MAX_LEAD_MINUTES).
    """
    age_min = row.get("AgeMinutes")
    if age_min is None:
        return False
    return age_min <= MAX_LEAD_MINUTES


# =====================================================
# OPEN AD + AGE
# =====================================================
def open_ad_and_check_age(context, ad_page, link: str) -> Tuple[str, Dict, object]:
    """
    Open a specific ad link in a page, extract core fields (title, price,
    description, odometer, seller, time) and compute age in minutes.
    Returns:
      - status: "ok", "checkpoint", or "error"
      - data: dict with extracted info
      - ad_page: the updated page object (may be re-opened on error)
    """
    base = {
        "title": "N/A",
        "title_looks_bad": False,
        "price": "N/A",
        "creation_time": None,
        "creation_source": "unknown",
        "age_minutes": None,
        "raw_time_text": None,
        "odometer": "N/A",
        "seller": "N/A",
        "description": "N/A",
    }

    for attempt in range(2):
        try:
            ad_page = ensure_page_open(context, ad_page)
            ad_page.set_default_timeout(30000)

            human_delay(0.2, 0.55)
            ad_page.goto(link, wait_until="domcontentloaded", timeout=60000)

            ad_page.locator("h1").first.wait_for(state="attached", timeout=20000)
            human_delay(0.25, 0.55)

            if page_is_checkpoint(ad_page):
                base["description"] = "SECURITY_CHECK_REQUIRED"
                logging.warning(f"Checkpoint on ad page -> {link}")
                return "checkpoint", base, ad_page

            ct, raw_text = extract_creation_time_from_ad_page(ad_page)
            base["raw_time_text"] = raw_text

            if ct:
                base["creation_time"] = ct
                base["creation_source"] = "parsed"
                base["age_minutes"] = (datetime.now() - ct).total_seconds() / 60.0
            else:
                base["creation_time"] = None
                base["creation_source"] = "unknown"
                base["age_minutes"] = None
                logging.info(f"[TIME] No parsed time for {link}")

            base["title"] = extract_true_title(ad_page)
            base["title_looks_bad"] = is_bad_title(base["title"])
            base["price"] = extract_price_from_ad_page(ad_page)
            base["description"] = extract_description(ad_page)
            if base["description"] == "SECURITY_CHECK_REQUIRED":
                logging.warning(f"SECURITY_CHECK in description for {link}")
                return "checkpoint", base, ad_page

            base["odometer"] = extract_odometer(ad_page)
            base["seller"] = extract_seller_name_from_ad_page(ad_page)

            logging.info(f"[OK] Ad collected: {link}")
            return "ok", base, ad_page

        except Exception as e:
            msg = str(e).lower()
            logging.error(f"Ad page error (attempt {attempt+1}) for {link}: {e}")

            if "has been closed" in msg:
                try:
                    ad_page = context.new_page()
                except Exception as e2:
                    logging.error(f"Failed to reopen ad_page: {e2}")

            human_delay(0.8, 1.2)

    return "error", base, ad_page


# =====================================================
# FEED LINKS
# =====================================================
def get_feed_item_links_in_order(city_page, limit=100, reset_to_top=False) -> List[str]:
    """
    Collect a list of marketplace item links in the current city feed,
    in the order they appear in the DOM.
    """
    try:
        if reset_to_top:
            city_page.evaluate("window.scrollTo(0, 0)")
            human_delay(0.4, 0.8)

        hrefs = city_page.evaluate("""
            () => Array.from(document.querySelectorAll("a[href*='/marketplace/item/']"))
                .map(a => a.getAttribute("href"))
                .filter(Boolean)
        """)

        out = []
        for href in hrefs:
            link = "https://www.facebook.com" + href.split("?")[0]
            if link not in out:
                out.append(link)
            if len(out) >= limit:
                break
        return out
    except Exception as e:
        logging.warning(f"Error getting feed links: {e}")
        return []


# =====================================================
# SCRAPE CITY (main city loop)
# =====================================================
def scrape_city(context, city_page, ad_page, city: str, url: str,
               seen_links: Set[str], security_skip: Dict[str, int]) -> object:
    """
    Main loop for scraping one city:

      1) Open city feed.
      2) Pull item links (newest-first setting).
      3) For each unseen link:
           - Open ad, extract data.
           - Record in all_cars.jsonl.
           - If it's a lead (AgeMinutes <= MAX_LEAD_MINUTES) -> record in leads.
         Stop scanning earlier once we see several non-lead ads in a row,
         then adjust min price to push the feed down and repeat.
    """
    city_page = ensure_page_open(context, city_page)
    city_page.goto(url, wait_until="domcontentloaded", timeout=60000)
    human_delay(1.0, 1.6)
    refresh_page(city_page)

    if page_is_checkpoint(city_page):
        logging.warning(f"⚠️ Feed checkpoint/security -> stop city {city}.")
        return ad_page

    no_progress_rounds = 0
    min_price_changes = 0

    # Number of consecutive non-lead ads allowed before changing Min Price
    MAX_CONSECUTIVE_NON_LEAD = 3

    while True:
        if page_is_checkpoint(city_page):
            logging.warning(f"⚠️ Feed became checkpoint/security -> stop city {city}.")
            return ad_page

        links = get_feed_item_links_in_order(city_page, limit=140, reset_to_top=True)

        if not links:
            no_progress_rounds += 1
            logging.info(f"[{city}] No ads at all. round={no_progress_rounds}")
            if no_progress_rounds >= MAX_NO_PROGRESS_ROUNDS:
                logging.info(f"✅ No ads -> stop city {city}.")
                return ad_page
            human_delay(0.8, 1.2)
            continue

        logging.info(f"[{city}] {len(links)} ads detected on page.")

        processed_any = False
        consecutive_non_lead = 0

        for idx, link in enumerate(links):
            if link in seen_links:
                continue
            if security_skip.get(link, 0) >= SECURITY_LINK_MAX_HITS:
                continue

            status, page_data, ad_page = open_ad_and_check_age(context, ad_page, link)

            if status == "checkpoint":
                security_skip[link] = security_skip.get(link, 0) + 1
                save_security_skip(SECURITY_SKIP_PATH, security_skip)
                logging.warning(f"⚠️ Security check for {link[:70]}... skip.")
                continue

            if status == "error":
                continue

            if status == "ok":
                processed_any = True

                ct = page_data.get("creation_time")
                age_min = page_data.get("age_minutes")

                row = {
                    "City": city,
                    "Title": page_data.get("title", "N/A"),
                    "TitleLooksBad": page_data.get("title_looks_bad", False),
                    "Price": page_data.get("price", "N/A"),
                    "Odometer": page_data.get("odometer", "N/A"),
                    "Seller": page_data.get("seller", "N/A"),
                    "Description": page_data.get("description", "N/A"),
                    "CreationTime": ct.strftime("%Y-%m-%d %H:%M") if ct else "N/A",
                    "CreationTimeSource": page_data.get("creation_source", "unknown"),
                    "RawTimeText": page_data.get("raw_time_text"),
                    "AgeMinutes": age_min,
                    "Link": link,
                }
                row["_key"] = hashlib.md5(link.encode("utf-8")).hexdigest()

                # Record every ad in ALL_JSONL
                append_jsonl(ALL_JSONL_PATH, row)

                # If it is a lead: store and keep scanning for more leads
                if is_valid_lead(row):
                    append_jsonl(LEADS_JSONL_PATH, row)
                    if send_lead_to_n8n(row):
                            seen_links.add(link)
                    else:
                            logging.warning("⚠️ Lead not delivered to n8n (will retry)")

                    consecutive_non_lead = 0
                    logging.info(
                            f"🔥 LEAD (Age≈{age_min:.1f} min) [{city}] {row['Title'][:60]} | "
                            f"{row['Price']} | Odometer={row['Odometer']} | {link}"
                    )
                    continue

                # Not a lead → increase consecutive non-lead counter
                consecutive_non_lead += 1
                logging.info(
                    f"⏱️ NOT A LEAD (#{consecutive_non_lead}) -> Age={age_min}, "
                    f"Odometer={row['Odometer']}, "
                    f"Title='{row['Title'][:50]}', "
                    f"Link={link}"
                )

                # If we reached the threshold (e.g. 3 older ads in a row), treat as boundary
                if consecutive_non_lead >= MAX_CONSECUTIVE_NON_LEAD:
                    min_price_changes += 1
                    if min_price_changes > MAX_MIN_PRICE_CHANGES_PER_CITY:
                        logging.info(
                            f"❌ Too many Min Price changes ({min_price_changes}) -> stop city {city}."
                        )
                        return ad_page

                    if not change_min_price(city_page):
                        logging.info(
                            f"❌ Failed to change Min Price -> stop city {city}."
                        )
                        return ad_page

                    # After boundary, break out of the for-loop and restart from top with new Min Price
                    break

                # If we haven't hit the threshold yet, just continue to the next link
                continue

        # End of "for links"
        if not processed_any:
            no_progress_rounds += 1
        else:
            no_progress_rounds = 0

        if no_progress_rounds >= MAX_NO_PROGRESS_ROUNDS:
            logging.info(f"❌ No progress for {no_progress_rounds} rounds -> stop city {city}.")
            return ad_page

        human_delay(0.6, 1.0)


# =====================================================
# MAIN SCRAPER (run once)
# =====================================================
@test_speed
def run_scraper():
    """
    Run the scraper once over all configured cities.
    Uses SCRAPER_LOCK to avoid concurrent runs in this process.
    """
    seen_links = load_seen_links_from_jsonl(LEADS_JSONL_PATH)
    security_skip = load_security_skip(SECURITY_SKIP_PATH)

    logging.info(f"Loaded {len(seen_links)} seen (lead) links.")
    logging.info(f"Loaded {len(security_skip)} security-skip links.")
    logging.info(f"All cars output: {ALL_JSONL_PATH}")
    logging.info(f"Leads output: {LEADS_JSONL_PATH}")
    logging.info(f"Logs: {LOG_PATH}")

    user_agent = random.choice(USER_AGENTS)
    viewport = random.choice(VIEWPORTS)

    logging.info(f"Using UA: {user_agent}")
    logging.info(f"Using viewport: {viewport}")

    with SCRAPER_LOCK:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
            )

            context = browser.new_context(
                storage_state=sanitize_storage_state(STORAGE_STATE),
                viewport=viewport,
                locale="en-US",
                user_agent=user_agent,
            )

            enable_speed_routes(context)

            ad_page = context.new_page()

            for city, url in marketplace_links:
                logging.info(f"\n===== Scraping {city} =====")
                city_page = context.new_page()

                try:
                    ad_page = scrape_city(context, city_page, ad_page, city, url, seen_links, security_skip)
                except Exception as e:
                    logging.error(f"City error {city}: {e}")

                try:
                    if not city_page.is_closed():
                        city_page.close()
                except:
                    pass

            try:
                if ad_page and not ad_page.is_closed():
                    ad_page.close()
            except:
                pass

            browser.close()

    logging.info("\nDone.")


# =====================================================
# BACKGROUND SCHEDULER (every 60 sec)
# =====================================================
def background_scraper_loop():
    """
    Background loop:
      - calls run_scraper()
      - waits SCRAPER_INTERVAL_SECONDS
      - repeats forever
    """
    while True:
        try:
            logging.info("🕒 Background scraper tick -> running run_scraper()")
            run_scraper()
        except Exception as e:
            logging.exception(f"[background_scraper_loop] Error in run_scraper: {e}")
        time.sleep(SCRAPER_INTERVAL_SECONDS)


def start_background_scraper_if_needed():
    """
    Ensure we only start one background thread per process.
    """
    global BACKGROUND_THREAD_STARTED

    if BACKGROUND_THREAD_STARTED:
        return

    with BACKGROUND_THREAD_LOCK:
        if BACKGROUND_THREAD_STARTED:
            return

        t = threading.Thread(target=background_scraper_loop, daemon=True)
        t.start()
        BACKGROUND_THREAD_STARTED = True
        logging.info("🚀 Started background scraper thread.")


@app.on_event("startup")
def startup_event():
    """
    FastAPI startup event:
      - kicks off the background scraper loop once per process.
    """
    start_background_scraper_if_needed()


# =====================================================
# FASTAPI ENDPOINTS
# =====================================================
@app.get("/")
def read_root():
    """Root endpoint: basic info about the service and output paths."""
    return {
        "message": "Facebook Marketplace Scraper API",
        "endpoints": {
            "/scrape_facebook": "GET - Return current leads (no scraping)",
            "/health": "GET - Health check",
        },
        "output_paths": {
            "all_cars": ALL_JSONL_PATH,
            "leads": LEADS_JSONL_PATH,
            "log": LOG_PATH,
        },
    }


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    Also triggers starting the background scraper if it wasn't started for some reason.
    """
    start_background_scraper_if_needed()
    return {"status": "healthy", "service": "fb_marketplace_scraper"}


@app.get("/scrape_facebook")
def scrape_facebook():
    """
    Return current leads without triggering a manual scraper run.
    Leads are read from lead_cars.jsonl, which is maintained
    by the background scraper loop.
    """
    leads = []
    try:
        if os.path.exists(LEADS_JSONL_PATH):
            with open(LEADS_JSONL_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        leads.append(json.loads(line))
    except Exception as e:
        logging.error(f"Error reading leads file: {e}")
        raise HTTPException(status_code=500, detail="Error reading leads file")

    return JSONResponse(content={
        "success": True,
        "leads_count": len(leads),
        "leads": leads,
    })
