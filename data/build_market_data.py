"""
build_market_data.py
────────────────────
Generates a WFP-style Kenya food prices dataset based on published FAO/WFP
price bulletins and market monitoring reports (2016-2024), then processes it
into seasonal indices, volatility statistics, and trend parameters used by
the Farm Price Predictor.

Sources used to calibrate the data:
  - WFP Kenya Market Price Monitoring Bulletins (2016-2024)
  - FAO GIEWS East Africa Food Price Bulletins
  - Kenya Ministry of Agriculture crop calendar data
  - FEWS NET Kenya Livelihoods Baselines
  - WFP VAM food price database (published summaries)

Output: data/market_data.json
"""

import json
import math
import random
from datetime import date, timedelta

random.seed(42)   # reproducible

# ── Crop Definitions ──────────────────────────────────────────────────────────
# Each crop entry contains:
#   base_price_2016  : KES price at start of series (Jan 2016)
#   annual_trend     : average annual price increase (e.g. 0.06 = 6% p.a.)
#   seasonal_raw     : 12 raw monthly factors (Jan-Dec), pre-normalised
#                      derived from FAO/WFP Kenya seasonal calendars
#   volatility       : intra-month random noise factor
#   unit             : unit of measurement
#   rainfall_beta    : price sensitivity to 1-unit rainfall deficit
#   events           : dict of (year,month) → price multiplier shocks
#   markets          : list of market names with their price premium factors
CROPS = {
    "maize": {
        "name": "Maize (White Grain)",
        "unit": "per 90kg bag",
        "base_price_2016": 2600,
        "annual_trend": 0.065,
        # Lean season Mar–Aug (long rains planting), cheap Oct–Dec (harvest)
        "seasonal_raw": [1.18, 1.22, 1.15, 1.05, 1.00, 0.90, 0.83, 0.85, 0.93, 0.95, 1.00, 1.10],
        "volatility": 0.04,
        "rainfall_beta": 0.22,   # 1-unit deficit → +22% price
        "events": {
            (2017,  2): 1.28,  # 2016/17 drought – price spike
            (2017,  3): 1.35,
            (2017,  4): 1.42,
            (2021,  6): 1.25,  # 2021 La Niña drought
            (2021,  7): 1.30,
            (2021,  8): 1.35,
            (2022,  3): 1.20,  # Ukraine war grain squeeze
            (2022,  4): 1.25,
            (2022,  9): 1.15,
            (2023,  2): 0.92,  # Bumper long rains harvest
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.08,
            "Kisumu":   0.96,
            "Eldoret":  0.93,
            "Nakuru":   0.95,
            "Garissa":  1.18,
        },
    },
    "wheat": {
        "name": "Wheat Flour",
        "unit": "per 90kg bag",
        "base_price_2016": 2800,
        "annual_trend": 0.055,
        # Kenya imports ~80% of wheat; less seasonal variation
        "seasonal_raw": [1.02, 1.05, 1.08, 1.06, 1.03, 0.96, 0.90, 0.88, 0.93, 0.99, 1.02, 1.03],
        "volatility": 0.025,
        "rainfall_beta": 0.08,
        "events": {
            (2022,  3): 1.30,  # Ukraine war wheat shock
            (2022,  4): 1.40,
            (2022,  5): 1.45,
            (2022,  6): 1.42,
            (2022,  7): 1.38,
            (2022,  8): 1.32,
            (2023,  1): 1.18,
            (2023,  6): 1.10,
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.05,
            "Kisumu":   1.02,
            "Eldoret":  0.97,
            "Nakuru":   0.98,
            "Garissa":  1.12,
        },
    },
    "rice": {
        "name": "Rice (Imported, Long Grain)",
        "unit": "per 25kg bag",
        "base_price_2016": 1800,
        "annual_trend": 0.05,
        # Rice is mostly imported; mild seasonal pattern
        "seasonal_raw": [1.05, 1.08, 1.10, 1.08, 1.00, 0.95, 0.88, 0.85, 0.90, 0.97, 1.00, 1.02],
        "volatility": 0.022,
        "rainfall_beta": 0.05,
        "events": {
            (2020,  4): 1.15,  # COVID supply chain
            (2020,  5): 1.18,
            (2022,  5): 1.12,  # global food crisis
            (2023,  9): 1.08,  # India export restrictions
            (2023, 10): 1.12,
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  0.97,
            "Kisumu":   1.03,
            "Eldoret":  1.05,
            "Nakuru":   1.04,
            "Garissa":  1.14,
        },
    },
    "tomatoes": {
        "name": "Tomatoes",
        "unit": "per crate (64kg)",
        "base_price_2016": 1600,
        "annual_trend": 0.07,
        # Very high seasonal variation; peak supply Dec-Mar (long rains)
        # and Jun-Jul (short rains); peak scarcity Apr-May and Sep-Oct
        "seasonal_raw": [0.75, 0.72, 0.78, 0.98, 1.18, 1.32, 1.38, 1.28, 1.15, 1.02, 0.88, 0.80],
        "volatility": 0.10,
        "rainfall_beta": 0.35,
        "events": {
            (2017,  5): 1.20,
            (2021,  4): 1.25,
            (2021,  5): 1.30,
            (2022,  4): 1.28,
            (2023,  4): 0.80,  # glut from irrigation expansion
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.10,
            "Kisumu":   0.90,
            "Eldoret":  0.92,
            "Nakuru":   0.88,
            "Garissa":  1.25,
        },
    },
    "onions": {
        "name": "Onions",
        "unit": "per 50kg bag",
        "base_price_2016": 2200,
        "annual_trend": 0.06,
        # Main harvest Jul-Sep; lean season Jan-Apr
        "seasonal_raw": [1.20, 1.25, 1.22, 1.15, 1.05, 0.92, 0.82, 0.80, 0.85, 0.92, 1.00, 1.10],
        "volatility": 0.055,
        "rainfall_beta": 0.20,
        "events": {
            (2019,  2): 1.18,
            (2021,  2): 1.22,
            (2021,  3): 1.25,
            (2022,  2): 1.20,
            (2023,  8): 0.85,
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.08,
            "Kisumu":   0.95,
            "Eldoret":  0.93,
            "Nakuru":   0.90,
            "Garissa":  1.20,
        },
    },
    "potatoes": {
        "name": "Irish Potatoes",
        "unit": "per 50kg bag",
        "base_price_2016": 1400,
        "annual_trend": 0.055,
        # Two seasons: main harvest Jul-Sep, second harvest Jan-Feb
        # Price peaks Apr-Jun and Oct-Dec
        "seasonal_raw": [0.90, 0.88, 0.95, 1.08, 1.15, 1.18, 0.82, 0.78, 0.85, 0.98, 1.08, 1.12],
        "volatility": 0.07,
        "rainfall_beta": 0.18,
        "events": {
            (2020, 11): 1.22,
            (2021,  5): 1.20,
            (2021, 11): 1.18,
            (2022,  5): 1.15,
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.12,
            "Kisumu":   0.98,
            "Eldoret":  0.88,
            "Nakuru":   0.85,
            "Garissa":  1.28,
        },
    },
    "beans": {
        "name": "Dry Beans (Red)",
        "unit": "per 90kg bag",
        "base_price_2016": 5500,
        "annual_trend": 0.07,
        # Main harvest Oct-Nov (short rains), second Mar-Apr (long rains)
        "seasonal_raw": [1.22, 1.28, 1.20, 1.08, 0.95, 0.88, 0.85, 0.88, 1.00, 1.08, 1.15, 1.20],
        "volatility": 0.06,
        "rainfall_beta": 0.28,
        "events": {
            (2017,  3): 1.30,
            (2017,  4): 1.35,
            (2021,  2): 1.25,
            (2021,  3): 1.28,
            (2022,  4): 1.22,
            (2023, 10): 0.88,  # good short rains harvest
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.06,
            "Kisumu":   0.94,
            "Eldoret":  0.92,
            "Nakuru":   0.95,
            "Garissa":  1.16,
        },
    },
    "sorghum": {
        "name": "Sorghum",
        "unit": "per 90kg bag",
        "base_price_2016": 2400,
        "annual_trend": 0.05,
        # Drought-tolerant; harvest Oct-Dec; grown in ASAL regions
        "seasonal_raw": [1.12, 1.16, 1.10, 1.05, 1.00, 0.94, 0.87, 0.88, 0.92, 0.96, 1.00, 1.06],
        "volatility": 0.035,
        "rainfall_beta": 0.15,
        "events": {
            (2021,  7): 1.18,
            (2022,  3): 1.12,
        },
        "markets": {
            "Nairobi":  1.00,
            "Mombasa":  1.06,
            "Kisumu":   0.92,
            "Eldoret":  0.94,
            "Nakuru":   0.95,
            "Garissa":  0.88,
        },
    },
}

# ── Generate monthly price series ─────────────────────────────────────────────

def normalise_seasonal(raw):
    """Normalise seasonal factors so mean = 1.0."""
    mean = sum(raw) / len(raw)
    return [r / mean for r in raw]

def build_series(crop_key, crop):
    """Return list of (year, month, market, price_kes) records."""
    seasonal = normalise_seasonal(crop["seasonal_raw"])
    records = []

    start = date(2016, 1, 1)
    end   = date(2024, 12, 1)
    cur   = start

    while cur <= end:
        yr, mo = cur.year, cur.month
        years_elapsed = (yr - 2016) + (mo - 1) / 12.0
        trend_factor = (1 + crop["annual_trend"]) ** years_elapsed

        for market, mkt_factor in crop["markets"].items():
            base = crop["base_price_2016"] * trend_factor * seasonal[mo - 1] * mkt_factor
            # apply event shock if any
            shock = crop["events"].get((yr, mo), 1.0)
            # gaussian noise
            noise = 1 + random.gauss(0, crop["volatility"])
            price = max(50, round(base * shock * noise, 0))
            records.append({
                "date": cur.isoformat(),
                "year": yr,
                "month": mo,
                "market": market,
                "commodity": crop["name"],
                "unit": crop["unit"],
                "currency": "KES",
                "price": price,
            })

        cur = date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)

    return records

# ── Compute analytics from the generated series ───────────────────────────────

def compute_analytics(crop_key, crop, records):
    """
    From the full price series, compute:
      - seasonal_index[12]  : averaged monthly price ratios (Jan-Dec)
      - annual_avg_prices   : list of (year, avg_price)
      - overall_avg         : grand average price (Nairobi retail)
      - volatility          : std dev / mean across Nairobi monthly prices
      - rainfall_beta       : sensitivity coefficient (from crop definition)
      - price_range         : (p10, p50, p90) for Nairobi
      - annual_inflation    : compound annual growth rate across series
    """
    nairobi = [r for r in records if r["market"] == "Nairobi"]
    nairobi.sort(key=lambda r: (r["year"], r["month"]))

    prices_by_month = {m: [] for m in range(1, 13)}
    yearly = {}
    all_prices = []

    for r in nairobi:
        prices_by_month[r["month"]].append(r["price"])
        yearly.setdefault(r["year"], []).append(r["price"])
        all_prices.append(r["price"])

    # Monthly averages → seasonal index
    monthly_avgs = {m: sum(v) / len(v) for m, v in prices_by_month.items()}
    grand_avg = sum(monthly_avgs.values()) / 12
    seasonal_index = [round(monthly_avgs[m] / grand_avg, 4) for m in range(1, 13)]

    # Annual averages
    annual_avg = {yr: sum(v) / len(v) for yr, v in yearly.items()}

    # CAGR
    y_sorted = sorted(annual_avg.keys())
    cagr = ((annual_avg[y_sorted[-1]] / annual_avg[y_sorted[0]]) ** (1 / (y_sorted[-1] - y_sorted[0])) - 1)

    # Volatility (coefficient of variation)
    mean = sum(all_prices) / len(all_prices)
    std  = math.sqrt(sum((p - mean) ** 2 for p in all_prices) / len(all_prices))
    cv   = std / mean

    # Percentiles
    sp = sorted(all_prices)
    p10 = sp[int(0.10 * len(sp))]
    p50 = sp[int(0.50 * len(sp))]
    p90 = sp[int(0.90 * len(sp))]

    return {
        "seasonal_index": seasonal_index,
        "annual_avg_prices": {str(yr): round(avg, 0) for yr, avg in annual_avg.items()},
        "overall_avg": round(grand_avg, 0),
        "volatility": round(cv, 4),
        "rainfall_beta": crop["rainfall_beta"],
        "price_range": {"p10": round(p10, 0), "p50": round(p50, 0), "p90": round(p90, 0)},
        "annual_inflation_cagr": round(cagr, 4),
        "market_premiums": crop["markets"],
    }

# ── Main ──────────────────────────────────────────────────────────────────────

print("Generating Kenya agricultural price dataset (WFP-style)...")
print(f"Period: Jan 2016 – Dec 2024  |  Markets: 6  |  Crops: {len(CROPS)}")
print()

all_records = []
analytics   = {}
csv_rows    = ["date,year,month,market,commodity,unit,currency,price"]

for key, crop in CROPS.items():
    records = build_series(key, crop)
    all_records.extend(records)
    analytics[key] = compute_analytics(key, crop, records)

    for r in records:
        csv_rows.append(
            f"{r['date']},{r['year']},{r['month']},{r['market']},"
            f"\"{r['commodity']}\",\"{r['unit']}\",{r['currency']},{r['price']}"
        )

    meta = analytics[key]
    print(f"  {key:12s}  avg KES {meta['overall_avg']:>7,.0f}  "
          f"CAGR {meta['annual_inflation_cagr']*100:.1f}%  "
          f"CV {meta['volatility']*100:.1f}%  "
          f"seasonal_range [{min(meta['seasonal_index']):.2f} – {max(meta['seasonal_index']):.2f}]")

# Write CSV
import os
os.makedirs("data", exist_ok=True)
with open("data/wfp_kenya_prices.csv", "w") as f:
    f.write("\n".join(csv_rows))

print(f"\nCSV  → data/wfp_kenya_prices.csv  ({len(csv_rows)-1} rows)")

# Build market_data.json for the predictor
market_data = {}
for key, crop_def in CROPS.items():
    a = analytics[key]
    market_data[key] = {
        "name": crop_def["name"],
        "unit": crop_def["unit"],
        "seasonal_index": a["seasonal_index"],
        "overall_avg": a["overall_avg"],
        "volatility": a["volatility"],
        "rainfall_beta": a["rainfall_beta"],
        "price_range": a["price_range"],
        "annual_inflation_cagr": a["annual_inflation_cagr"],
        "annual_avg_prices": a["annual_avg_prices"],
        "market_premiums": a["market_premiums"],
        # Derived: 3-year recent average (2022-2024) as default reference price
        "recent_avg": round(
            sum(float(v) for yr, v in a["annual_avg_prices"].items() if int(yr) >= 2022)
            / max(1, sum(1 for yr in a["annual_avg_prices"] if int(yr) >= 2022)),
            0
        ),
    }

with open("data/market_data.json", "w") as f:
    json.dump(market_data, f, indent=2)

print(f"JSON → data/market_data.json  ({len(market_data)} crops)")
print("\nDone.")
