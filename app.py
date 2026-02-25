from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Query
import requests
import json
import re
import warnings
from datetime import datetime, timezone
from fastapi.responses import FileResponse
import pandas as pd
import time
import uuid

warnings.filterwarnings("ignore")
app = FastAPI(title=" Scraping API")


class TextInput(BaseModel):
    text: str


# =============================
# CONFIGURATION
# =============================

# ---------- Kijiji ----------

URLKIJII = "https://www.kijiji.ca/b-cars-trucks/sudbury/c174l1700245"

PARAMSKIJII = {
    "address": "Spanish, ON",
    "for-sale-by": "ownr",
    "ll": "46.1947959,-82.3422779",
    "price": "0__",
    "radius": "988.0",
    "view": "list",
}

HEADERSKIJII = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html",
}

COOKIESKIJII = {
    "kjses": "a3ada55c-3dda-4d3b-a2f1-5a2dc3e6d11e",
}

# ---------- AutoTrader ----------

URL = "https://www.autotrader.ca/lst"

PARAMS = {
    "atype": "C",
    "custtype": "P",
    "cy": "CA",
    "damaged_listing": "exclude",
    "desc": "1",
    "lat": "46.20007",
    "lon": "-82.34984",
    "offer": "U",
    "size": "40",
    "sort": "age",
    "ustate": "N,U",
    "zip": "Spanish, ON",
    "zipr": "1000",
}

HEADERS = {
    "Host": "www.autotrader.ca",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Not_A Brand";v="99", "Chromium";v="142"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Priority": "u=0, i",
}

COOKIES = {
    "as24Visitor": "c3c760d9-0878-408d-a19b-2180d1931375",
    # لو حابب حط هنا باقي الـ cookies اللي أخدتهم من المتصفح
}

# ---------- Swoopa ----------

SWOOPA_ACCOUNTS = {
    "primary": {
        "url": "https://backend.getswoopa.com/api/marketplace/",
        "detail_url_template": "https://backend.getswoopa.com/api/marketplace/{id}/",
        "headers": {
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzcyMDMzMjY3LCJpYXQiOjE3NzE5NDY4NjcsImp0aSI6ImQ5ZjY3MDE5YmE3MTRmMWI4ZjZkNDA4NzQ4MWJjZGQxIiwidXNlcl9pZCI6Ijk1MjE2In0.g7oKVdCBs1NgN5mMxLDgN-inApnbCM9rNSjoI2d_M4g",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": "https://app.getswoopa.com",
            "Referer": "https://app.getswoopa.com/",
            "User-Agent": "Mozilla/5.0",
        }
    },
    "secondary": {
        "url": "https://backend.getswoopa.com/api/marketplace/",
        "detail_url_template": "https://backend.getswoopa.com/api/marketplace/{id}/",
        "headers": {
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzcyMDMyMjgxLCJpYXQiOjE3NzE5NDU4ODEsImp0aSI6IjEwZjdjYzA3MDBiMzQ4YTlhYjgwZTU0MzQyMzAzYzAyIiwidXNlcl9pZCI6Ijk3OTE3In0.Vvn_NbTD5RrDvc_GYyTu_AXFLw41V9J6OVLND58D2z4",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": "https://app.getswoopa.com",
            "Referer": "https://app.getswoopa.com/",
            "User-Agent": "Mozilla/5.0",
        }
    }
}

# =============================
# HELPER FUNCTIONS
# =============================

def parse_kijiji_date(date_str):
    """Parse Kijiji ISO-ish timestamps into timezone-aware datetime."""
    if not date_str:
        return None
    try:
        return datetime.strptime(
            date_str, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.strptime(
            date_str, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)


def fetch_swoopa_listing_info(listing_id: str, account_config: dict) -> dict | None:
    """
    يجيب JSON تفاصيل إعلان واحد من Swoopa (info endpoint).
    ويطبع شوية حاجات في الترمنال عشان نعرف المشكلة فين.
    """
    detail_template = account_config.get("detail_url_template")
    if not detail_template:
        print("❌ No detail_url_template in config")
        return None

    detail_url = detail_template.format(id=listing_id)
    print(f"\n➡️ DETAIL URL for {listing_id}: {detail_url}")

    try:
        resp = requests.get(detail_url, headers=account_config["headers"], timeout=15)
        print("   STATUS:", resp.status_code)

        if resp.status_code != 200:
            print("   BODY (first 200 chars):", resp.text[:200])
            return None

        data = resp.json()
        print("   KEYS:", list(data.keys()))
        print("   listing_description preview:",
              (data.get("listing_description") or "")[:80])
        return data

    except requests.RequestException as e:
        print("   ERROR:", e)
        return None


def find_autos_listings(obj, results=None):
    """Recursively find all 'AutosListing:*' nodes inside Kijiji JSON."""
    if results is None:
        results = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.startswith("AutosListing:"):
                results[k] = v
            else:
                find_autos_listings(v, results)

    elif isinstance(obj, list):
        for item in obj:
            find_autos_listings(item, results)

    return results


# =============================
# FASTAPI ENDPOINTS
# =============================

@app.get("/")
def read_root():
    return {
        "message": "Scraping API",
        "endpoints": {
            "/scrape_autotrader": "GET - Scrape Autotrader listings",
            "/scrape_kijiji": "GET -  Scrape Kijiji listings",
            "/fetch-marketplace-primary": "GET -  Scrape primary marketplace listings",
            "/fetch-marketplace-secondary": "GET -  Scrape secondary marketplace listings",
            "/check-scammer": "POST - Check if text indicates a real person or dealer",
        },
    }


# ---------- Autotrader ----------

@app.get("/scrape_autotrader")
def scrape_autotrader():
    """
    Scrape Autotrader listings and return structured data.
    """
    try:
        response = requests.get(
            URL,
            params=PARAMS,
            headers=HEADERS,
            cookies=COOKIES,
            verify=False,
            timeout=30,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=f"Request failed with status code: {response.status_code}",
            )

        html = response.text
        # Parse embedded JSON
        match = re.search(
            r'<script[^>]+type="application/json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )

        if not match:
            raise HTTPException(
                status_code=500,
                detail="Embedded JSON not found in response",
            )

        json_text = match.group(1).replace("&quot;", '"')
        data = json.loads(json_text)

        # Extract data
        page_props = data["props"]["pageProps"]
        number_of_results = page_props["numberOfResults"]
        cars = page_props["listings"]

        results = []

        for car in cars:
            vehicle = car.get("vehicle", {})
            price_data = car.get("price", {})
            location = car.get("location", {})

            make = vehicle.get("make", "")
            model = vehicle.get("model", "")
            year = vehicle.get("modelYear", "")
            mileage = vehicle.get("mileageInKm")

            price = price_data.get("priceFormatted", "")
            city = location.get("city", "")
            url = car.get("url", "")

            image = car["images"][0] if car.get("images") else None
            description = (
                car.get("description", "").split("<br")[0]
                if car.get("description")
                else ""
            )

            title = f"{year} {make} {model}".strip()

            car_data = {
                "title": title,
                "price": price,
                "city": city,
                "mileage_km": mileage,
                "image": image,
                "url": url,
                "description": description,
                "make": make,
                "model": model,
                "year": year,
            }

            results.append(car_data)

        return {
            "success": True,
            "total_results": number_of_results,
            "scraped_count": len(results),
            "source": "AutoTrader",
            "cars": results,
        }

    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Request timeout")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Request error: {str(e)}")
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"JSON parsing error: {str(e)}",
        )
    except KeyError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Missing expected data field: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}",
        )


# ---------- Kijiji ----------

@app.get("/scrape_kijiji")
def scrape_kijiji():
    r = requests.get(
        URLKIJII,
        params=PARAMSKIJII,
        headers=HEADERSKIJII,
        cookies=COOKIESKIJII,
        timeout=30,
    )

    if r.status_code != 200:
        raise HTTPException(500, "Request failed")

    match = re.search(
        r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
        r.text,
        re.DOTALL,
    )

    if not match:
        raise HTTPException(500, "Embedded JSON not found")

    raw_json = (
        match.group(1)
        .replace("&quot;", '"')
        .replace("&amp;", "&")
        .strip()
    )

    data = json.loads(raw_json)

    listings_map = find_autos_listings(data)
    now = datetime.now(timezone.utc)

    results = []

    for listing in listings_map.values():
        attributes = listing.get("attributes", {}).get("all", [])

        def get_attr(name):
            for a in attributes:
                if a.get("canonicalName") == name:
                    vals = a.get("canonicalValues")
                    return vals[0] if vals else None
            return None

        activation = parse_kijiji_date(listing.get("activationDate"))
        sorting = parse_kijiji_date(listing.get("sortingDate"))
        amount = listing.get("price", {}).get("amount")

        if isinstance(amount, (int, float)):
            price = amount // 100
        else:
            price = amount

        results.append(
            {
                "title": listing.get("title"),
                "description": listing.get("description"),
                "price": price,
                "currency": "CAD",
                "url": listing.get("url"),
                "images": listing.get("imageUrls") or [],
                "brand": get_attr("carmake"),
                "model": get_attr("carmodel"),
                "year": get_attr("caryear"),
                "mileage_km": get_attr("carmileageinkms"),
                "body_type": get_attr("carbodytype"),
                "color": get_attr("carcolor"),
                "doors": get_attr("noofdoors"),
                "fuel_type": get_attr("carfueltype"),
                "transmission": get_attr("cartransmission"),
                "activation_date": activation.isoformat() if activation else None,
                "sorting_date": sorting.isoformat() if sorting else None,
                "time_since_activation": (
                    str(now - activation) if activation else None
                ),
            }
        )

    results.sort(key=lambda x: x["sorting_date"] or "", reverse=True)

    return {
        "count": len(results),
        "cars": results,
    }


# ---------- Swoopa: primary & secondary ----------

@app.get("/fetch-marketplace-primary")
def fetch_marketplace_primary(
    pages: int = Query(1, ge=1, le=100),
    account: str = Query("primary"),
    with_description: bool = True,
):
    """
    يجيب listings من Swoopa لحساب primary
    + يضيف listing_description من info/<id> لو with_description=True
    """
    if account not in SWOOPA_ACCOUNTS:
        raise HTTPException(status_code=400, detail="Invalid Swoopa account")

    swoopa = SWOOPA_ACCOUNTS[account]
    url = swoopa["url"]
    headers = swoopa["headers"]

    all_results = []

    # 1) جلب صفحات الـ listings
    for _ in range(pages):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Swoopa {account} request failed: {str(e)}",
            )

        try:
            data = r.json()
        except ValueError:
            raise HTTPException(
                status_code=502,
                detail=f"Swoopa {account} returned non-JSON response",
            )

        all_results.extend(data.get("results", []))

        url = data.get("next")
        if not url:
            break

        time.sleep(1)

    # 2) جلب listing_description لكل إعلان من info/<id> وإضافته
    if with_description:
        enriched = []
        for item in all_results:
            listing_id = item.get("id")
            desc = None

            if listing_id:
                info_data = fetch_swoopa_listing_info(listing_id, swoopa)
                if info_data:
                    # من الـ screenshot واضح إن اسم الفيلد هو "listing_description"
                    desc = info_data.get("listing_description")

            item["listing_description"] = desc
            enriched.append(item)

        all_results = enriched

    return {
        "count": len(all_results),
        "results": all_results,
    }


@app.get("/fetch-marketplace-secondary")
def fetch_marketplace_secondary(
    pages: int = Query(1, ge=1, le=100),
    account: str = Query("secondary"),
    with_description: bool = True,
):
    """
    نفس فكرة primary لكن على حساب secondary.
    """
    if account not in SWOOPA_ACCOUNTS:
        raise HTTPException(status_code=400, detail="Invalid Swoopa account")

    swoopa = SWOOPA_ACCOUNTS[account]
    url = swoopa["url"]
    headers = swoopa["headers"]

    all_results = []

    # 1) جلب صفحات الـ listings
    for _ in range(pages):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Swoopa {account} request failed: {str(e)}",
            )

        try:
            data = r.json()
        except ValueError:
            raise HTTPException(
                status_code=502,
                detail=f"Swoopa {account} returned non-JSON response",
            )

        all_results.extend(data.get("results", []))

        url = data.get("next")
        if not url:
            break

        time.sleep(1)

    # 2) إضافة listing_description من info/<id>
    if with_description:
        enriched = []
        for item in all_results:
            listing_id = item.get("id")
            desc = None

            if listing_id:
                info_data = fetch_swoopa_listing_info(listing_id, swoopa)
                if info_data:
                    desc = info_data.get("listing_description")

            item["listing_description"] = desc
            enriched.append(item)

        all_results = enriched

    return {
        "count": len(all_results),
        "results": all_results,
    }


# ---------- Health Check ----------

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "autotrader_scraper"}
