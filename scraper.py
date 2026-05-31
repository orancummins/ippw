#!/usr/bin/env python3
"""
Irish Property Price Register (PPR) scraper.

Downloads the official per-year CSV files from propertypriceregister.ie and
stores all results in SQLite.  Each CSV contains the full nationwide dataset
for that year (~50-70k rows); rows are filtered to the requested county.

Usage:
    python scraper.py                         # Dublin, all years 2010-present
    python scraper.py --county Cork           # different county
    python scraper.py --county All            # store all counties
    python scraper.py --start-year 2022       # from 2022 only
    python scraper.py --db custom.db          # custom database file
"""

import csv
import io
import time
import sqlite3
import logging
import argparse
from datetime import datetime, timezone

import requests

try:
    import truststore
    truststore.inject_into_ssl()  # use macOS/Windows/Linux system CA trust store
except ImportError:
    pass  # fall back to certifi; install 'truststore' if you hit SSL errors

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.propertypriceregister.ie"
CSV_URL = (
    BASE_URL
    + "/website/npsra/PPR/npsra-ppr.nsf/Downloads/PPR-{year}.csv/$FILE/PPR-{year}.csv"
)
SEED_URL = f"{BASE_URL}/website/npsra/PPR/npsra-ppr.nsf/PPR-By-Date?OpenForm"
USER_AGENT = "Mozilla/5.0"

DEFAULT_DB = "ppr.db"
DEFAULT_DELAY = 1.0   # seconds between year downloads; be polite to the server

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    date_of_sale          TEXT    NOT NULL,   -- DD/MM/YYYY as stored by PPR
    address               TEXT    NOT NULL,
    county                TEXT    NOT NULL,
    eircode               TEXT,
    price_eur             REAL    NOT NULL,
    not_full_market_price INTEGER NOT NULL DEFAULT 0,  -- 1 = Yes
    vat_exclusive         INTEGER NOT NULL DEFAULT 0,  -- 1 = Yes
    description           TEXT,
    property_size         TEXT,
    year                  INTEGER NOT NULL,
    scraped_at            TEXT    NOT NULL    -- UTC ISO-8601
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup
    ON properties(date_of_sale, address, county, price_eur);
CREATE INDEX IF NOT EXISTS idx_county ON properties(county);
CREATE INDEX IF NOT EXISTS idx_year   ON properties(year);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # Drop old schema if it has the old unid-based table
    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)")}
    if cols and "unid" in cols:
        log.info("Migrating old schema (dropping unid-based table) …")
        conn.execute("DROP TABLE IF EXISTS properties")
        conn.execute("DROP INDEX IF EXISTS idx_date")
        conn.execute("DROP INDEX IF EXISTS idx_county")
        conn.execute("DROP INDEX IF EXISTS idx_year")
    conn.executescript(SCHEMA)
    conn.commit()
    log.info("Database ready: %s", db_path)
    return conn


# ---------------------------------------------------------------------------
# CSV download + parsing
# ---------------------------------------------------------------------------

# CSV columns (as of 2024):
# Date of Sale (dd/mm/yyyy), Address, County, Eircode, Price (€),
# Not Full Market Price, VAT Exclusive, Description of Property,
# Property Size Description

def _download_csv(session: requests.Session, year: int) -> list[dict]:
    url = CSV_URL.format(year=year)
    try:
        resp = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to download CSV for %d: %s", year, exc)
        return []

    # The CSV is Windows-1252 encoded
    text = resp.content.decode("windows-1252", errors="replace")
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    scraped_at = datetime.now(timezone.utc).isoformat()
    for row in reader:
        price_raw = row.get("Price (\u20ac)", "").replace("\u20ac", "").replace(",", "").strip()
        try:
            price_eur = float(price_raw)
        except ValueError:
            log.debug("Skipping row with unparseable price: %r", price_raw)
            continue
        rows.append({
            "date_of_sale":          row.get("Date of Sale (dd/mm/yyyy)", "").strip(),
            "address":               row.get("Address", "").strip(),
            "county":                row.get("County", "").strip(),
            "eircode":               row.get("Eircode", "").strip() or None,
            "price_eur":             price_eur,
            "not_full_market_price": 1 if row.get("Not Full Market Price", "").strip().lower() == "yes" else 0,
            "vat_exclusive":         1 if row.get("VAT Exclusive", "").strip().lower() == "yes" else 0,
            "description":           row.get("Description of Property", "").strip() or None,
            "property_size":         row.get("Property Size Description", "").strip() or None,
            "year":                  year,
            "scraped_at":            scraped_at,
        })
    return rows


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

_INSERT_SQL = """
INSERT OR IGNORE INTO properties
    (date_of_sale, address, county, eircode, price_eur,
     not_full_market_price, vat_exclusive, description,
     property_size, year, scraped_at)
VALUES
    (:date_of_sale, :address, :county, :eircode, :price_eur,
     :not_full_market_price, :vat_exclusive, :description,
     :property_size, :year, :scraped_at)
"""


def _save(conn: sqlite3.Connection, records: list[dict]) -> tuple[int, int]:
    """Upsert records; returns (inserted, already_existed)."""
    before = conn.execute("SELECT changes()").fetchone()[0]
    cursor = conn.cursor()
    inserted = 0
    skipped = 0
    for rec in records:
        cursor.execute(_INSERT_SQL, rec)
        if cursor.rowcount:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(
    county: str,
    start_year: int,
    end_year: int,
    db_path: str,
    delay: float,
) -> None:
    conn = init_db(db_path)
    session = requests.Session()
    total_inserted = total_skipped = 0

    # Seed session cookies
    try:
        session.get(SEED_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    except requests.RequestException as exc:
        log.error("Could not seed session: %s", exc)
        return

    years = list(range(start_year, end_year + 1))
    n_years = len(years)
    bar_width = 30

    county_label = county if county != "All" else "all counties"
    print(f"\nScraping {county_label} property sales {start_year}–{end_year} → {db_path}")
    print(f"{'─' * 60}")

    start_time = datetime.now(timezone.utc)

    for idx, year in enumerate(years, 1):
        # Progress bar
        filled = int(bar_width * (idx - 1) / n_years)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"\r[{bar}] {idx-1}/{n_years}  Downloading {year} CSV …", end="", flush=True)

        records = _download_csv(session, year)

        # Filter by county unless "All" requested
        if county != "All":
            records = [r for r in records if r["county"].lower() == county.lower()]

        if records:
            ins, skip = _save(conn, records)
            total_inserted += ins
            total_skipped += skip
            status = f"{len(records):>6,} records  (+{ins:,} new, {skip:,} dup)"
        else:
            status = "     0 records"

        # Overwrite line with completed result
        filled = int(bar_width * idx / n_years)
        bar = "█" * filled + "░" * (bar_width - filled)
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        avg = elapsed / idx
        eta_s = avg * (n_years - idx)
        eta = f"ETA {int(eta_s//60)}m{int(eta_s%60):02d}s" if idx < n_years else "done"
        print(f"\r[{bar}] {idx}/{n_years}  {year}: {status}  [{eta}]")

        if idx < n_years:
            time.sleep(delay)

    print(f"{'─' * 60}")
    elapsed_total = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(
        f"Finished in {int(elapsed_total//60)}m{int(elapsed_total%60):02d}s  |  "
        f"New records: {total_inserted:,}  |  Duplicates skipped: {total_skipped:,}\n"
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape the Irish Property Price Register into SQLite.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--county",
        default="Dublin",
        help="County name exactly as it appears on the PPR website.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2010,
        dest="start_year",
        help="First year to scrape (PPR data begins 2010).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=datetime.now().year,
        dest="end_year",
        help="Last year to scrape (inclusive).",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help="Path to the SQLite database file.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to wait between requests (be polite to the server).",
    )
    args = parser.parse_args()

    if args.start_year < 2010:
        parser.error("PPR data starts in 2010; --start-year must be >= 2010")
    if args.end_year < args.start_year:
        parser.error("--end-year must be >= --start-year")

    run(
        county=args.county,
        start_year=args.start_year,
        end_year=args.end_year,
        db_path=args.db,
        delay=args.delay,
    )


if __name__ == "__main__":
    _cli()
