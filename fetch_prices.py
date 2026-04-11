#!/usr/bin/env python3
"""
SG vs JB Petrol Price Fetcher
==============================
Pulls from 3 sources and writes prices.json:
  1. frankfurter.app     — live MYR/SGD FX rate (no key needed)
  2. data.gov.my API     — official MY weekly fuel prices (no key needed)
  3. petrolprice.sg      — SG prices (PRIMARY scrape)
     motorist.sg         — SG prices (AUTO-FALLBACK if primary fails)

Run daily via cron or GitHub Actions.
Output: prices.json  (consumed by your frontend)
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timezone
import sys

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

GRADE_ALIASES = {
    "92":             "ron92",  "92 petrol":      "ron92",  "ron 92": "ron92",
    "95":             "ron95",  "95 petrol":      "ron95",  "ron 95": "ron95",
    "98":             "ron98",  "98 petrol":      "ron98",  "ron 98": "ron98",
    "premium":        "premium","premium petrol": "premium",
    "diesel":         "diesel",
}

BRAND_CANONICAL = {
    "esso":         "Esso",
    "shell":        "Shell",
    "spc":          "SPC",
    "caltex":       "Caltex",
    "sinopec":      "Sinopec",
    "smart energy": "Smart Energy",
    "smartenergy":  "Smart Energy",
    "petron":       "Petron",
}

def canonical_brand(raw):
    return BRAND_CANONICAL.get(raw.strip().lower(), raw.strip().title())

def parse_price(raw):
    cleaned = re.sub(r"[^\d.]", "", str(raw).strip())
    if not cleaned:
        return None
    try:
        val = float(cleaned)
        return val if 1.0 < val < 20.0 else None
    except ValueError:
        return None

def resolve_grade(raw):
    key = raw.strip().lower()
    if key in GRADE_ALIASES:
        return GRADE_ALIASES[key]
    for alias, grade in GRADE_ALIASES.items():
        if alias in key:
            return grade
    return None

def sort_grades(prices):
    for grade in prices:
        prices[grade].sort(key=lambda x: x["price"])
    return prices

def print_sg_summary(prices, source):
    for grade, items in prices.items():
        if items:
            c = items[0]
            print(f"  [SG/{source}] {grade}: cheapest SGD {c['price']} ({c['brand']}), {len(items)} brands")


# ─────────────────────────────────────────────────────────────
# 1. FX RATE
# ─────────────────────────────────────────────────────────────
def fetch_fx():
    url = "https://api.frankfurter.app/latest?from=MYR&to=SGD"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    rate = data["rates"]["SGD"]
    print(f"  [FX] 1 MYR = {rate} SGD  (date: {data['date']})")
    return {"rate": rate, "date": data["date"], "source": "frankfurter.app"}


# ─────────────────────────────────────────────────────────────
# 2. MALAYSIA PRICES
# ─────────────────────────────────────────────────────────────
def fetch_my_prices():
    url = "https://api.data.gov.my/data-catalogue?id=fuelprice&limit=1&sort=-date"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        raise ValueError("data.gov.my returned empty response")
    latest = rows[0]
    print(f"  [MY] date={latest['date']}  RON95={latest.get('ron95')}  RON97={latest.get('ron97')}  diesel={latest.get('diesel')}")
    return {
        "date": latest["date"],
        "ron95": float(latest.get("ron95", 0)),
        "ron97": float(latest.get("ron97", 0)),
        "diesel": float(latest.get("diesel", 0)),
        "source": "api.data.gov.my",
        "note": "Unsubsidised rates. RON95 subsidised rate (RM1.99) not applicable to SG-registered vehicles."
    }


# ─────────────────────────────────────────────────────────────
# 3a. PRIMARY SCRAPER — petrolprice.sg
# ─────────────────────────────────────────────────────────────
def scrape_petrolprice_sg():
    """
    Table structure (Apr 2026):
    thead: [Fuel Type | Esso | Shell | SPC | Caltex | Sinopec | Smart Energy | Lowest Today]
    tbody: [92 Petrol | $3.43 | N/A  | $3.39 | ...]
    """
    url = "https://petrolprice.sg/"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find table that has brand names in header
    target = None
    for table in soup.find_all("table"):
        ths = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any(b in ths for b in ("esso", "shell", "spc", "caltex", "sinopec")):
            target = table
            break

    if not target:
        raise ValueError("petrolprice.sg: price table not found")

    # Map column index → brand
    col_brand = {}
    for i, th in enumerate(target.find("thead").find_all("th")):
        brand = canonical_brand(th.get_text(strip=True))
        if brand in BRAND_CANONICAL.values():
            col_brand[i] = brand

    if not col_brand:
        raise ValueError("petrolprice.sg: no brand columns found in header")

    prices = {g: [] for g in set(GRADE_ALIASES.values())}
    for row in target.find("tbody").find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        grade = resolve_grade(cells[0].get_text(strip=True))
        if not grade:
            continue
        for col_idx, brand in col_brand.items():
            if col_idx < len(cells):
                price = parse_price(cells[col_idx].get_text(strip=True))
                if price:
                    prices[grade].append({"brand": brand, "price": price})

    if not prices.get("ron95"):
        raise ValueError("petrolprice.sg: ron95 data empty after parse — structure likely changed")

    sort_grades(prices)
    print_sg_summary(prices, "petrolprice.sg")
    return prices


# ─────────────────────────────────────────────────────────────
# 3b. FALLBACK SCRAPER — motorist.sg
# ─────────────────────────────────────────────────────────────
def scrape_motorist_sg():
    """
    motorist.sg table structure (Apr 2026):
    Section "Compare Pump Prices"
    - First col: Grade (92 / 95 / 98 / Premium / Diesel)
    - Remaining cols: brands rendered as images (alt text) or plain text
    - Known column order: Esso, Shell, SPC, Caltex, Sinopec

    Brand images have empty alt text on motorist.sg, so we rely on
    known column order as the primary detection method, with text
    fallback if they ever add readable headers.
    """
    # Known brand column order for motorist.sg as of Apr 2026
    MOTORIST_BRAND_ORDER = ["Esso", "Shell", "SPC", "Caltex", "Sinopec"]

    url = "https://www.motorist.sg/petrol-prices"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the price table — look for table containing grade rows
    target = None
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if cells and resolve_grade(cells[0].get_text(strip=True)):
                target = table
                break
        if target:
            break

    if not target:
        raise ValueError("motorist.sg: price table not found")

    # Try to read brand names from thead (text or img alt)
    brands = []
    thead = target.find("thead")
    if thead:
        header_cells = thead.find_all("th")[1:]  # skip grade col
        for th in header_cells:
            img = th.find("img")
            text = (img.get("alt", "") if img else "") or th.get_text(strip=True)
            cb = canonical_brand(text) if text.strip() else None
            brands.append(cb if (cb in BRAND_CANONICAL.values()) else None)

    # If header parsing got nothing useful, fall back to hardcoded order
    if not any(brands):
        brands = MOTORIST_BRAND_ORDER
        print("  [motorist.sg] using hardcoded brand column order (images have no alt text)")
    else:
        # Fill any None gaps with hardcoded order
        for i, b in enumerate(brands):
            if b is None and i < len(MOTORIST_BRAND_ORDER):
                brands[i] = MOTORIST_BRAND_ORDER[i]
        print(f"  [motorist.sg] detected brands: {brands}")

    prices = {g: [] for g in set(GRADE_ALIASES.values())}
    for row in target.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        grade = resolve_grade(cells[0].get_text(strip=True))
        if not grade:
            continue
        for i, cell in enumerate(cells[1:]):  # skip grade label
            if i >= len(brands) or not brands[i]:
                continue
            price = parse_price(cell.get_text(strip=True))
            if price:
                prices[grade].append({"brand": brands[i], "price": price})

    if not prices.get("ron95"):
        raise ValueError("motorist.sg: ron95 data empty after parse")

    sort_grades(prices)
    print_sg_summary(prices, "motorist.sg")
    return prices


# ─────────────────────────────────────────────────────────────
# 3. SG PRICES — try primary, auto-fallback to secondary
# ─────────────────────────────────────────────────────────────
def fetch_sg_prices():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    primary_err_msg = None

    print("  Trying petrolprice.sg (primary)...")
    try:
        prices = scrape_petrolprice_sg()
        return {
            "date": today,
            "prices": prices,
            "source": "petrolprice.sg",
            "fallback_used": False
        }
    except Exception as e:
        primary_err_msg = str(e)
        print(f"  petrolprice.sg FAILED: {primary_err_msg}")
        print("  Auto-switching to motorist.sg (fallback)...")

    try:
        prices = scrape_motorist_sg()
        return {
            "date": today,
            "prices": prices,
            "source": "motorist.sg",
            "fallback_used": True,
            "fallback_reason": primary_err_msg
        }
    except Exception as fallback_err:
        raise RuntimeError(
            f"Both SG scrapers failed.\n"
            f"  Primary   (petrolprice.sg): {primary_err_msg}\n"
            f"  Fallback  (motorist.sg):    {fallback_err}"
        )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fx": None,
        "malaysia": None,
        "singapore": None,
        "errors": []
    }

    print("── FX rate ─────────────────────────────────")
    try:
        output["fx"] = fetch_fx()
    except Exception as e:
        msg = f"FX fetch failed: {e}"
        print(f"  ERROR: {msg}")
        output["errors"].append(msg)
        output["fx"] = {"rate": 0.316, "date": "fallback", "source": "hardcoded-fallback"}

    print("── Malaysia prices ─────────────────────────")
    try:
        output["malaysia"] = fetch_my_prices()
    except Exception as e:
        msg = f"MY fetch failed: {e}"
        print(f"  ERROR: {msg}")
        output["errors"].append(msg)

    print("── Singapore prices ────────────────────────")
    try:
        output["singapore"] = fetch_sg_prices()
        if output["singapore"].get("fallback_used"):
            output["errors"].append(
                f"petrolprice.sg scraper broke — used motorist.sg fallback. "
                f"Reason: {output['singapore'].get('fallback_reason', 'unknown')}"
            )
    except Exception as e:
        msg = f"SG scrape failed (both sources): {e}"
        print(f"  ERROR: {msg}")
        output["errors"].append(msg)

    with open("prices.json", "w") as f:
        json.dump(output, f, indent=2)

    print("\n── Done ────────────────────────────────────")
    if output["singapore"]:
        src = output["singapore"]["source"]
        fb = " ⚠ (fallback)" if output["singapore"].get("fallback_used") else ""
        print(f"SG data source: {src}{fb}")
    if output["errors"]:
        print(f"Errors/warnings ({len(output['errors'])}):")
        for e in output["errors"]:
            print(f"  - {e}")

    # Only hard-fail if SG data is completely missing
    if output["singapore"] is None:
        sys.exit(1)

if __name__ == "__main__":
    main()
