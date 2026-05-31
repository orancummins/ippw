#!/usr/bin/env python3
"""
PPR Web App — Irish Property Price Register browser

Loads PPR-ALL.csv into SQLite (ppr_web.db) on first run, then serves
a query/filter UI on http://localhost:2012
"""

import csv
import io
import sqlite3
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, g, jsonify, render_template_string, request
import requests
try:
    import truststore
    truststore.inject_into_ssl()  # use macOS / Windows system trust store
except ImportError:
    pass  # truststore not installed — fall back to certifi / default SSL

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CSV_PATH = Path(__file__).parent / "PPR-ALL.csv"
DB_PATH  = Path(__file__).parent / "ppr_web.db"
PORT     = 2012

PPR_REFRESH_SOURCES = [
  "https://www.propertypriceregister.ie/website/npsra/PPR/npsra-ppr.nsf/Downloads/PPR-ALL.csv/$FILE/PPR-ALL.csv",
  "https://www.propertypriceregister.ie/website/npsra/PPR/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip",
]
REFRESH_USER_AGENT = "Mozilla/5.0"

_refresh_lock = threading.Lock()
_refresh_state = {
  "running": False,
  "stage": "Idle",
  "last_result": None,
  "last_error": None,
  "last_success_at": None,
  "updated_at": None,
}

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS properties (
    id                    INTEGER PRIMARY KEY,
    date_of_sale          TEXT,
    year                  INTEGER,
    month                 INTEGER,
    address               TEXT,
    county                TEXT,
    eircode               TEXT,
    price_eur             REAL,
    not_full_market_price INTEGER DEFAULT 0,
    vat_exclusive         INTEGER DEFAULT 0,
    description           TEXT,
    property_size         TEXT
);

CREATE INDEX IF NOT EXISTS idx_county ON properties(county);
CREATE INDEX IF NOT EXISTS idx_year   ON properties(year);
CREATE INDEX IF NOT EXISTS idx_price  ON properties(price_eur);
CREATE INDEX IF NOT EXISTS idx_desc   ON properties(description);
CREATE INDEX IF NOT EXISTS idx_size   ON properties(property_size);

CREATE VIRTUAL TABLE IF NOT EXISTS addr_fts USING fts5(
    address,
    content = properties,
    content_rowid = id
);
"""

INSERT_SQL = """
INSERT INTO properties
    (date_of_sale, year, month, address, county, eircode,
     price_eur, not_full_market_price, vat_exclusive, description, property_size)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
"""

# ---------------------------------------------------------------------------
# CSV → SQLite import
# ---------------------------------------------------------------------------

def _norm_description(raw: str) -> str:
    """Collapse Irish-language variants to English equivalents."""
    r = raw.strip()
    if not r:
        return r
    lower = r.lower()
    if "nua" in lower or "new" in lower:
        return "New Dwelling house /Apartment"
    if "atháimhe" in lower or "second" in lower or "ath" in lower:
        return "Second-Hand Dwelling house /Apartment"
    return r


def _norm_size(raw: str) -> str:
    """Collapse Irish-language size variants to English equivalents."""
    r = raw.strip()
    if not r:
        return r
    lower = r.lower()
    if "38" in lower and "125" in lower:
        return "38–125 sq metres"
    if "125" in lower:
        return "greater than 125 sq metres"
    if "38" in lower:
        return "less than 38 sq metres"
    return r


def load_db(
    csv_path: Path = CSV_PATH,
    db_path: Path = DB_PATH,
    *,
    skip_if_populated: bool = True,
) -> None:
    """Import a PPR CSV file into SQLite."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)

    count = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
    if skip_if_populated and count > 0:
        print(f"[ppr] DB already has {count:,} records — skipping CSV import.", flush=True)
        conn.close()
        return

    if not csv_path.exists():
        conn.close()
        raise FileNotFoundError(f"PPR CSV not found at {csv_path}")

    print(f"[ppr] Importing {csv_path.name} ({csv_path.stat().st_size // 1_048_576} MB) …", flush=True)

    batch: list[tuple] = []
    total = errs = 0

    with open(csv_path, encoding="windows-1252", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            raw_price = row.get("Price (\u20ac)", "").replace("\u20ac", "").replace(",", "").strip()
            try:
                price = float(raw_price)
            except ValueError:
                errs += 1
                continue

            date = row.get("Date of Sale (dd/mm/yyyy)", "").strip()
            try:
                parts = date.split("/")
                year, month = int(parts[2]), int(parts[1])
            except Exception:
                year = month = None

            batch.append((
                date, year, month,
                row.get("Address", "").strip(),
                row.get("County", "").strip(),
                row.get("Eircode", "").strip() or None,
                price,
                1 if row.get("Not Full Market Price", "").strip().lower() == "yes" else 0,
                1 if row.get("VAT Exclusive", "").strip().lower() == "yes" else 0,
                _norm_description(row.get("Description of Property", "")),
                _norm_size(row.get("Property Size Description", "")),
            ))
            total += 1

            if len(batch) >= 10_000:
                conn.executemany(INSERT_SQL, batch)
                conn.commit()
                print(f"  {total:,} rows …\r", end="", flush=True)
                batch = []

    if batch:
        conn.executemany(INSERT_SQL, batch)
        conn.commit()

    print(f"\n[ppr] {total:,} rows imported ({errs} skipped). Building FTS index …", flush=True)
    conn.execute("INSERT INTO addr_fts(addr_fts) VALUES('rebuild')")
    conn.commit()
    print("[ppr] Ready.", flush=True)
    conn.close()


def _refresh_set_state(**updates) -> None:
    with _refresh_lock:
        _refresh_state.update(updates)
        _refresh_state["updated_at"] = datetime.now(timezone.utc).isoformat()


def _refresh_get_state() -> dict:
    with _refresh_lock:
        return dict(_refresh_state)


def _download_latest_csv(target: Path) -> str:
    """Download the latest full PPR file, supporting CSV and ZIP sources."""
    errors: list[str] = []

    with requests.Session() as session:
        for url in PPR_REFRESH_SOURCES:
            _refresh_set_state(stage=f"Downloading source file from {url}")
            try:
                resp = session.get(url, headers={"User-Agent": REFRESH_USER_AGENT}, timeout=(20, 300))
                resp.raise_for_status()
            except Exception as exc:
                errors.append(f"{url}: {exc}")
                continue

            data: bytes
            if url.lower().endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        if not names:
                            raise RuntimeError("ZIP did not contain a CSV file")
                        data = zf.read(names[0])
                except Exception as exc:
                    errors.append(f"{url}: could not extract ZIP ({exc})")
                    continue
            else:
                data = resp.content

            if len(data) < 1024:
                errors.append(f"{url}: downloaded file was unexpectedly small")
                continue

            tmp = target.with_suffix(target.suffix + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(target)
            return url

    raise RuntimeError("Could not download PPR source file: " + " | ".join(errors))


def _refresh_worker() -> None:
    try:
        source_url = _download_latest_csv(CSV_PATH)

        _refresh_set_state(stage="Rebuilding local database")
        tmp_db = DB_PATH.with_name(DB_PATH.stem + ".refresh.db")
        if tmp_db.exists():
            tmp_db.unlink()

        load_db(csv_path=CSV_PATH, db_path=tmp_db, skip_if_populated=False)

        # Remove the temp DB's WAL/SHM (load_db checkpointed them, but
        # clean up so the renamed file is entirely self-contained).
        for suffix in ("-wal", "-shm"):
            p = Path(str(tmp_db) + suffix)
            if p.exists():
                p.unlink()

        # Also clear the live DB's old WAL/SHM so they don't confuse SQLite
        # after the atomic rename below.
        for suffix in ("-wal", "-shm"):
            p = Path(str(DB_PATH) + suffix)
            if p.exists():
                p.unlink()

        tmp_db.replace(DB_PATH)

        conn = sqlite3.connect(str(DB_PATH))
        total = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        conn.close()

        _refresh_set_state(
            running=False,
            stage=f"Refresh complete ({total:,} rows)",
            last_result="success",
            last_error=None,
            last_success_at=datetime.now(timezone.utc).isoformat(),
            source_url=source_url,
        )
    except Exception as exc:
        _refresh_set_state(
            running=False,
            stage="Refresh failed",
            last_result="error",
            last_error=str(exc),
        )


# ---------------------------------------------------------------------------
# DB connection per request
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop("db", None)
    if db:
        db.close()


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

ALLOWED_SORT = {"date_of_sale", "price_eur", "county", "address", "year"}


def _build_where(args) -> tuple[str, list]:
    where, params = [], []

    county = args.get("county", "").strip()
    if county:
        where.append("county = ?")
        params.append(county)

    year_from = args.get("year_from", "").strip()
    year_to   = args.get("year_to",   "").strip()
    if year_from:
        where.append("year >= ?"); params.append(int(year_from))
    if year_to:
        where.append("year <= ?"); params.append(int(year_to))

    price_min = args.get("price_min", "").strip()
    price_max = args.get("price_max", "").strip()
    if price_min:
        where.append("price_eur >= ?"); params.append(float(price_min))
    if price_max:
        where.append("price_eur <= ?"); params.append(float(price_max))

    desc = args.get("description", "").strip()
    if desc:
        where.append("description = ?"); params.append(desc)

    size = args.get("property_size", "").strip()
    if size:
        where.append("property_size = ?"); params.append(size)

    if args.get("not_fmp") == "1":
        where.append("not_full_market_price = 1")

    if args.get("vat") == "1":
        where.append("vat_exclusive = 1")

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    return clause, params


def _build_query(args, count_only=False) -> tuple[str, list]:
    address_q = args.get("address", "").strip()
    where_clause, params = _build_where(args)

    if address_q:
        # FTS match + optional extra filters
        if count_only:
            select = "SELECT COUNT(*)"
        else:
            select = "SELECT p.*"
        sql = f"""
            {select}
            FROM addr_fts f
            JOIN properties p ON p.id = f.rowid
            WHERE f.address MATCH ?
        """
        fts_params = [f'"{address_q}"*']  # prefix match, quoted for safety
        if where_clause:
            # Append AND conditions (skip the WHERE keyword we already have)
            sql += " AND " + where_clause.lstrip(" WHERE ")
            fts_params.extend(params)
        return sql, fts_params
    else:
        if count_only:
            return f"SELECT COUNT(*) FROM properties{where_clause}", params
        else:
            return f"SELECT * FROM properties{where_clause}", params


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.post("/api/refresh")
def api_refresh_start():
  with _refresh_lock:
    if _refresh_state["running"]:
      return jsonify({"ok": False, "error": "Refresh is already running."}), 409
    _refresh_state.update({
      "running": True,
      "stage": "Starting refresh",
      "last_error": None,
      "updated_at": datetime.now(timezone.utc).isoformat(),
    })

  threading.Thread(target=_refresh_worker, daemon=True).start()
  return jsonify({"ok": True, "message": "Refresh started."})


@app.get("/api/refresh/status")
def api_refresh_status():
  return jsonify(_refresh_get_state())

@app.get("/api/meta")
def api_meta():
    db = get_db()
    counties = [r[0] for r in db.execute(
        "SELECT DISTINCT county FROM properties WHERE county != '' ORDER BY county"
    ).fetchall()]
    years = [r[0] for r in db.execute(
        "SELECT DISTINCT year FROM properties WHERE year IS NOT NULL ORDER BY year"
    ).fetchall()]
    descs = [r[0] for r in db.execute(
        "SELECT DISTINCT description FROM properties WHERE description != '' ORDER BY description"
    ).fetchall()]
    sizes = [r[0] for r in db.execute(
        "SELECT DISTINCT property_size FROM properties WHERE property_size != '' ORDER BY property_size"
    ).fetchall()]
    total = db.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
    return jsonify({
        "total":        total,
        "counties":     counties,
        "years":        years,
        "descriptions": descs,
        "sizes":        sizes,
    })


@app.get("/api/search")
def api_search():
    db   = get_db()
    args = request.args

    # ----- diagnostic logging ------------------------------------------------
    try:
        import sqlite3 as _sq3
        _prop_count = db.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        _fts_count  = db.execute("SELECT COUNT(*) FROM addr_fts").fetchone()[0]
        print(f"[search-diag] sqlite_version={_sq3.sqlite_version}  properties={_prop_count:,}  addr_fts={_fts_count:,}  args={dict(args)}", flush=True)
    except Exception as _e:
        print(f"[search-diag] count error: {_e}", flush=True)

    _addr_diag = args.get("address", "").strip()
    if _addr_diag:
        try:
            _raw2 = _sq3.connect(str(DB_PATH))
            # Try quoted prefix: "word"*
            _q1 = _raw2.execute(
                "SELECT COUNT(*) FROM addr_fts WHERE address MATCH ?",
                [f'"{_addr_diag}"*']
            ).fetchone()[0]
            # Try unquoted prefix: word*
            _q2 = _raw2.execute(
                "SELECT COUNT(*) FROM addr_fts WHERE address MATCH ?",
                [f'{_addr_diag}*']
            ).fetchone()[0]
            # Try plain equality
            _q3 = _raw2.execute(
                "SELECT COUNT(*) FROM addr_fts WHERE address MATCH ?",
                [_addr_diag]
            ).fetchone()[0]
            # Sample rowids from FTS
            _rowids = [r[0] for r in _raw2.execute(
                "SELECT rowid FROM addr_fts WHERE address MATCH ? LIMIT 5",
                [f'{_addr_diag}*']
            ).fetchall()]
            # Check those rowids exist in properties
            _exists = [_raw2.execute("SELECT COUNT(*) FROM properties WHERE id=?", [rid]).fetchone()[0] for rid in _rowids]
            _raw2.close()
            print(f"[search-diag] quoted_prefix={_q1}  unquoted_prefix={_q2}  plain={_q3}  sample_rowids={_rowids}  rowids_exist={_exists}", flush=True)
        except Exception as _e:
            print(f"[search-diag] fts variant test error: {_e}", flush=True)
    # -------------------------------------------------------------------------

    # Total matching count
    count_sql, count_params = _build_query(args, count_only=True)
    total = db.execute(count_sql, count_params).fetchone()[0]
    print(f"[search-diag] total={total}", flush=True)

    # Pagination
    page     = max(1, int(args.get("page", 1)))
    per_page = min(200, max(10, int(args.get("per_page", 50))))
    offset   = (page - 1) * per_page

    # Sort
    sort_col = args.get("sort", "date_of_sale")
    sort_dir = "ASC" if args.get("dir", "desc") == "asc" else "DESC"
    if sort_col not in ALLOWED_SORT:
        sort_col = "date_of_sale"

    # Translate date_of_sale -> chronological multi-column sort (text is DD/MM/YYYY)
    if sort_col == "date_of_sale":
        order_by = (
            f"year {sort_dir}, month {sort_dir}, "
            f"CAST(SUBSTR(date_of_sale, 1, 2) AS INTEGER) {sort_dir}"
        )
    else:
        order_by = f"{sort_col} {sort_dir}"

    # Data page
    data_sql, data_params = _build_query(args, count_only=False)
    data_sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    data_params = list(data_params) + [per_page, offset]
    rows = db.execute(data_sql, data_params).fetchall()

    # Aggregate stats (skip if result set too large — would be slow)
    stats: dict = {}
    if total <= 500_000:
        agg_sql, agg_params = _build_query(args, count_only=False)
        agg = db.execute(
            f"SELECT MIN(price_eur), MAX(price_eur), AVG(price_eur) FROM ({agg_sql})",
            agg_params,
        ).fetchone()
        if agg and agg[2] is not None:
            stats = {
                "min": agg[0],
                "max": agg[1],
                "avg": round(agg[2], 2),
            }

    return jsonify({
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, (total + per_page - 1) // per_page),
        "stats":    stats,
        "results":  [dict(r) for r in rows],
    })


# ---------------------------------------------------------------------------
# Stats endpoint
# ---------------------------------------------------------------------------

@app.get("/api/stats")
def api_stats():
    db   = get_db()
    args = request.args

    # Build the filtered base query as a subquery we can reuse
    base_sql, base_params = _build_query(args, count_only=False)
    sub = f"({base_sql})"

    # ── Headline KPIs ────────────────────────────────────────────────────────
    kpi = db.execute(f"""
        SELECT
            COUNT(*)                        AS n,
            COALESCE(SUM(price_eur), 0)     AS total_value,
            COALESCE(AVG(price_eur), 0)     AS avg_price,
            COALESCE(MIN(price_eur), 0)     AS min_price,
            COALESCE(MAX(price_eur), 0)     AS max_price,
            COALESCE(AVG(CASE WHEN LOWER(description) LIKE 'new%'    THEN 1.0 ELSE 0.0 END), 0) AS pct_new,
            COALESCE(AVG(not_full_market_price), 0) AS pct_nfmp,
            COALESCE(AVG(vat_exclusive), 0)         AS pct_vat
        FROM {sub}
    """, base_params).fetchone()

    n = kpi["n"]
    kpis = {
        "count":       n,
        "total_value": kpi["total_value"],
        "avg_price":   kpi["avg_price"],
        "min_price":   kpi["min_price"],
        "max_price":   kpi["max_price"],
        "pct_new":     kpi["pct_new"]  * 100,
        "pct_nfmp":    kpi["pct_nfmp"] * 100,
        "pct_vat":     kpi["pct_vat"]  * 100,
    }

    # Median (only when row count is sane)
    if 0 < n <= 1_500_000:
        offset = n // 2
        med = db.execute(
            f"SELECT price_eur FROM {sub} ORDER BY price_eur LIMIT 1 OFFSET ?",
            base_params + [offset],
        ).fetchone()
        kpis["median_price"] = med["price_eur"] if med else 0
    else:
        kpis["median_price"] = 0

    # ── Yearly trend ─────────────────────────────────────────────────────────
    yearly = db.execute(f"""
        SELECT year, COUNT(*) AS n, AVG(price_eur) AS avg_price, SUM(price_eur) AS total_value
        FROM {sub}
        WHERE year IS NOT NULL
        GROUP BY year ORDER BY year
    """, base_params).fetchall()

    yearly_data = [
        {"year": r["year"], "count": r["n"],
         "avg_price": r["avg_price"], "total_value": r["total_value"]}
        for r in yearly
    ]

    # ── Monthly seasonality (across all years in selection) ─────────────────
    monthly = db.execute(f"""
        SELECT month, COUNT(*) AS n, AVG(price_eur) AS avg_price
        FROM {sub}
        WHERE month IS NOT NULL
        GROUP BY month ORDER BY month
    """, base_params).fetchall()
    monthly_data = [
        {"month": r["month"], "count": r["n"], "avg_price": r["avg_price"]}
        for r in monthly
    ]

    # ── County breakdown ─────────────────────────────────────────────────────
    counties = db.execute(f"""
        SELECT county, COUNT(*) AS n, AVG(price_eur) AS avg_price, SUM(price_eur) AS total_value
        FROM {sub}
        WHERE county != ''
        GROUP BY county
        ORDER BY n DESC
    """, base_params).fetchall()
    county_data = [
        {"county": r["county"], "count": r["n"],
         "avg_price": r["avg_price"], "total_value": r["total_value"]}
        for r in counties
    ]

    # ── New vs Second-hand split ─────────────────────────────────────────────
    type_mix = db.execute(f"""
        SELECT
            SUM(CASE WHEN LOWER(description) LIKE 'new%'         THEN 1 ELSE 0 END) AS new_n,
            SUM(CASE WHEN LOWER(description) LIKE 'second-hand%' THEN 1 ELSE 0 END) AS used_n,
            SUM(CASE WHEN description = '' OR description IS NULL THEN 1 ELSE 0 END) AS unknown_n
        FROM {sub}
    """, base_params).fetchone()

    # ── Price distribution (histogram buckets) ───────────────────────────────
    bucket_defs = [
        ("< €100k",      0,        100_000),
        ("€100k–200k",   100_000,  200_000),
        ("€200k–300k",   200_000,  300_000),
        ("€300k–400k",   300_000,  400_000),
        ("€400k–500k",   400_000,  500_000),
        ("€500k–750k",   500_000,  750_000),
        ("€750k–1M",     750_000,  1_000_000),
        ("€1M–2M",       1_000_000, 2_000_000),
        ("€2M–5M",       2_000_000, 5_000_000),
        ("≥ €5M",        5_000_000, None),
    ]
    bucket_cases = []
    for i, (_lbl, lo, hi) in enumerate(bucket_defs):
        cond = f"price_eur >= {lo}" + (f" AND price_eur < {hi}" if hi is not None else "")
        bucket_cases.append(f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) AS b{i}")
    hist_row = db.execute(
        f"SELECT {', '.join(bucket_cases)} FROM {sub}", base_params
    ).fetchone()
    histogram = [
        {"label": bucket_defs[i][0], "count": hist_row[f"b{i}"] or 0}
        for i in range(len(bucket_defs))
    ]

    # ── Top 10 most expensive sales ─────────────────────────────────────────
    top = db.execute(
        f"SELECT * FROM {sub} ORDER BY price_eur DESC LIMIT 10",
        base_params,
    ).fetchall()
    top_expensive = [dict(r) for r in top]

    # ── Million-Euro club: count of ≥€1M sales by year ──────────────────────
    million = db.execute(f"""
        SELECT year, COUNT(*) AS n
        FROM {sub}
        WHERE price_eur >= 1000000 AND year IS NOT NULL
        GROUP BY year ORDER BY year
    """, base_params).fetchall()
    million_data = [{"year": r["year"], "count": r["n"]} for r in million]

    # ── Top Eircode routing areas ───────────────────────────────────────────
    routing = db.execute(f"""
        SELECT UPPER(SUBSTR(eircode, 1, 3)) AS routing, COUNT(*) AS n, AVG(price_eur) AS avg_price
        FROM {sub}
        WHERE eircode IS NOT NULL AND LENGTH(eircode) >= 3
        GROUP BY routing
        ORDER BY n DESC
        LIMIT 15
    """, base_params).fetchall()
    routing_data = [
        {"routing": r["routing"], "count": r["n"], "avg_price": r["avg_price"]}
        for r in routing
    ]

    # ── Repeat-sale addresses (potential flips) ─────────────────────────────
    repeats = db.execute(f"""
        SELECT COUNT(*) AS n FROM (
            SELECT address, county FROM {sub}
            WHERE address != ''
            GROUP BY address, county HAVING COUNT(*) >= 2
        )
    """, base_params).fetchone()
    repeat_count = repeats["n"] or 0

    # Top 10 most-frequently-sold addresses
    top_repeats = db.execute(f"""
        SELECT address, county, COUNT(*) AS times_sold,
               MIN(price_eur) AS first_price, MAX(price_eur) AS last_price,
               MIN(date_of_sale) AS first_date, MAX(date_of_sale) AS last_date
        FROM {sub}
        WHERE address != ''
        GROUP BY address, county
        HAVING COUNT(*) >= 2
        ORDER BY times_sold DESC, last_price DESC
        LIMIT 10
    """, base_params).fetchall()
    top_repeats_data = [dict(r) for r in top_repeats]

    # ── Property size mix ───────────────────────────────────────────────────
    size_mix = db.execute(f"""
        SELECT property_size, COUNT(*) AS n
        FROM {sub}
        WHERE property_size IS NOT NULL AND property_size != ''
        GROUP BY property_size
        ORDER BY n DESC
    """, base_params).fetchall()
    size_data = [{"size": r["property_size"], "count": r["n"]} for r in size_mix]

    # ── Day-of-month sale concentration (e.g. end-of-month bias) ────────────
    dom = db.execute(f"""
        SELECT CAST(SUBSTR(date_of_sale, 1, 2) AS INTEGER) AS dom, COUNT(*) AS n
        FROM {sub}
        WHERE date_of_sale != ''
        GROUP BY dom ORDER BY dom
    """, base_params).fetchall()
    dom_data = [{"dom": r["dom"], "count": r["n"]} for r in dom if r["dom"]]

    # ── Average price per county over time ───────────────────────────────────
    # One series per county, x-axis = year.  Limit to top 12 counties by volume
    # so the chart stays readable.
    top_county_names = [c["county"] for c in county_data[:12]]
    county_year_data: dict[str, list] = {}
    if top_county_names:
        placeholders = ",".join(["?"] * len(top_county_names))
        rows = db.execute(f"""
            SELECT county, year, AVG(price_eur) AS avg_price, COUNT(*) AS n
            FROM {sub}
            WHERE year IS NOT NULL AND county IN ({placeholders})
            GROUP BY county, year
            ORDER BY county, year
        """, base_params + top_county_names).fetchall()
        for r in rows:
            county_year_data.setdefault(r["county"], []).append({
                "year":      r["year"],
                "avg_price": r["avg_price"],
                "count":     r["n"],
            })

    return jsonify({
        "kpis":          kpis,
        "yearly":        yearly_data,
        "monthly":       monthly_data,
        "counties":      county_data,
        "type_mix":      {
            "new":     type_mix["new_n"]     or 0,
            "used":    type_mix["used_n"]    or 0,
            "unknown": type_mix["unknown_n"] or 0,
        },
        "histogram":     histogram,
        "top_expensive": top_expensive,
        "million":       million_data,
        "routing":       routing_data,
        "repeats": {
            "total_addresses_sold_multiple_times": repeat_count,
            "top": top_repeats_data,
        },
        "size_mix":      size_data,
        "day_of_month":  dom_data,
        "county_over_time": county_year_data,
    })



# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PPR Browser</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%2315803d'/%3E%3Cpath fill='none' stroke='white' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round' d='M2.25 12l8.954-8.955c.44-.439 1.152-.439 1.591 0L21.75 12M4.5 9.75v10.125c0 .621.504 1.125 1.125 1.125H9.75v-4.875c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125V21h4.125c.621 0 1.125-.504 1.125-1.125V9.75M8.25 21h8.25'/%3E%3C/svg%3E">
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body { font-family: system-ui, sans-serif; }
  .sort-btn { cursor: pointer; user-select: none; }
  .sort-btn:hover { text-decoration: underline; }
  #results-table tbody tr:nth-child(even) { background: #f8fafc; }
  .badge { display:inline-block; padding:1px 6px; border-radius:9999px; font-size:.7rem; font-weight:600; }
  .badge-new  { background:#dcfce7; color:#166534; }
  .badge-used { background:#e0f2fe; color:#075985; }
  .badge-nfmp { background:#fef3c7; color:#92400e; }
  .badge-vat  { background:#ede9fe; color:#5b21b6; }
  th { white-space: nowrap; }
  .tab-btn { display:flex; align-items:center; gap:.375rem; padding:.5rem 1rem; border-bottom: 2px solid transparent; cursor: pointer; font-weight: 500; color: #6b7280; user-select:none; transition:color .15s; }
  .tab-btn:hover { color:#374151; }
  .tab-btn.active { border-color: #15803d; color: #15803d; }
  .kpi-card { background:white; border-radius:.5rem; padding:1rem; box-shadow:0 1px 2px rgba(0,0,0,.05); }
  .kpi-label { font-size:.7rem; text-transform:uppercase; letter-spacing:.05em; color:#6b7280; font-weight:600; }
  .kpi-value { font-size:1.5rem; font-weight:700; color:#111827; margin-top:.25rem; }
  .kpi-sub   { font-size:.75rem; color:#6b7280; margin-top:.125rem; }
  .chart-card { background:white; border-radius:.5rem; padding:1rem; box-shadow:0 1px 2px rgba(0,0,0,.05); }
  .chart-card h3 { font-weight:600; color:#374151; font-size:.95rem; margin-bottom:.5rem; }
  .chart-wrap { position:relative; height:280px; }
  /* ── Filter controls ─────────────────────────────── */
  .fc-label { display:block; font-size:.7rem; font-weight:600; color:#6b7280; margin-bottom:.25rem; text-transform:uppercase; letter-spacing:.04em; }
  .fc {
    display:block; width:100%; padding:.4rem .65rem;
    font-size:.8125rem; line-height:1.5; color:#374151;
    background:#fff; border:1px solid #d1d5db;
    border-radius:.375rem;
    box-shadow:inset 0 1px 2px rgba(0,0,0,.04);
    transition:border-color .15s,box-shadow .15s;
    -webkit-appearance:none; appearance:none;
    box-sizing:border-box;
  }
  .fc:focus { outline:none; border-color:#16a34a; box-shadow:0 0 0 2px rgba(22,163,74,.2); }
  select.fc {
    background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2.5' stroke-linecap='round'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E");
    background-repeat:no-repeat; background-position:right .5rem center;
    padding-right:1.75rem;
  }
  input[type=number].fc::-webkit-inner-spin-button { opacity:.5; }

  /* ── About modal ──────────────────────────────── */
  #about-modal-backdrop {
    position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:900;
    overflow-y:auto;
    padding:3rem 1rem 3rem;
    opacity:0; transition:opacity .2s;
    pointer-events:none;
  }
  #about-modal-backdrop.open { opacity:1; pointer-events:auto; }
  #about-modal {
    background:#fff; border-radius:1rem;
    max-width:580px; width:100%; margin:0 auto;
    transform:translateY(12px) scale(.98); transition:transform .2s;
    box-shadow:0 25px 60px rgba(0,0,0,.3);
  }
  #about-modal-backdrop.open #about-modal { transform:translateY(0) scale(1); }
  .about-hero {
    background: linear-gradient(135deg, #14532d 0%, #166534 45%, #15803d 100%);
    border-radius:1rem 1rem 0 0;
    padding:2rem 2rem 1.75rem;
    color:#fff;
    position:relative;
    overflow:hidden;
  }
  .about-hero::before {
    content:'';
    position:absolute; inset:0;
    background:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.04'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E");
  }
  .about-feature {
    display:flex; gap:1rem; align-items:flex-start;
    padding:.875rem 0; border-bottom:1px solid #f1f5f9;
  }
  .about-feature:last-child { border-bottom:none; }
  .about-icon {
    width:2.5rem; height:2.5rem; border-radius:.625rem;
    display:flex; align-items:center; justify-content:center;
    font-size:1.25rem; flex-shrink:0;
  }
</style>
</head>
<body class="bg-gray-100 min-h-screen">

<!-- Header -->
<header class="bg-green-800 text-white px-6 py-3 flex items-center gap-4 shadow">
  <div class="flex items-center gap-2.5">
    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.75" stroke="currentColor" class="w-6 h-6 text-green-200 shrink-0">
      <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12l8.954-8.955c.44-.439 1.152-.439 1.591 0L21.75 12M4.5 9.75v10.125c0 .621.504 1.125 1.125 1.125H9.75v-4.875c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125V21h4.125c.621 0 1.125-.504 1.125-1.125V9.75M8.25 21h8.25" />
    </svg>
    <span class="text-lg font-bold tracking-tight">PPR Browser</span>
    <span class="text-green-300 text-sm hidden sm:inline">Irish Property Price Register</span>
  </div>
  <div class="ml-auto flex items-center gap-3">
    <button id="btn-refresh" class="bg-white/10 hover:bg-white/20 text-white text-xs font-semibold px-3 py-1.5 rounded transition flex items-center gap-1.5">
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-3.5 h-3.5">
        <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
      </svg>
      Refresh
    </button>
    <span id="refresh-status" class="text-xs text-green-200">Idle</span>
    <div id="total-badge" class="text-sm text-green-200"></div>
    <button id="btn-about" title="About PPR Browser"
      class="bg-white/10 hover:bg-white/20 text-white w-8 h-8 rounded-full flex items-center justify-center transition">
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.75" stroke="currentColor" class="w-4.5 h-4.5 w-[18px] h-[18px]">
        <path stroke-linecap="round" stroke-linejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
      </svg>
    </button>
  </div>
</header>

<div class="flex h-[calc(100vh-52px)]">

  <!-- Filter sidebar -->
  <aside class="w-60 min-w-[15rem] bg-white border-r border-gray-200 overflow-y-auto flex flex-col">
    <div class="flex flex-col gap-3 p-4 flex-1">
      <h2 class="font-bold text-gray-500 text-xs uppercase tracking-widest">Filters</h2>

      <div>
        <label class="fc-label">Address</label>
        <input id="f-address" type="text" placeholder="e.g. Grafton Street" class="fc">
      </div>

      <div>
        <label class="fc-label">County</label>
        <select id="f-county" class="fc">
          <option value="">All counties</option>
        </select>
      </div>

      <div>
        <label class="fc-label">Year</label>
        <div class="flex gap-2">
          <select id="f-year-from" class="fc flex-1" style="padding-right:.5rem">
            <option value="">From</option>
          </select>
          <select id="f-year-to" class="fc flex-1" style="padding-right:.5rem">
            <option value="">To</option>
          </select>
        </div>
      </div>

      <div>
        <label class="fc-label">Price (€)</label>
        <div class="flex flex-col gap-1.5">
          <select id="f-price-min" class="fc">
            <option value="">Min — any</option>
            <option value="50000">€50k</option>
            <option value="100000">€100k</option>
            <option value="150000">€150k</option>
            <option value="200000">€200k</option>
            <option value="250000">€250k</option>
            <option value="300000">€300k</option>
            <option value="350000">€350k</option>
            <option value="400000">€400k</option>
            <option value="500000">€500k</option>
            <option value="750000">€750k</option>
            <option value="1000000">€1M</option>
            <option value="2000000">€2M</option>
          </select>
          <select id="f-price-max" class="fc">
            <option value="">Max — any</option>
            <option value="100000">€100k</option>
            <option value="150000">€150k</option>
            <option value="200000">€200k</option>
            <option value="250000">€250k</option>
            <option value="300000">€300k</option>
            <option value="350000">€350k</option>
            <option value="400000">€400k</option>
            <option value="500000">€500k</option>
            <option value="750000">€750k</option>
            <option value="1000000">€1M</option>
            <option value="2000000">€2M</option>
            <option value="5000000">€5M</option>
          </select>
        </div>
      </div>

      <div>
        <label class="fc-label">Property type</label>
        <select id="f-description" class="fc">
          <option value="">All types</option>
        </select>
      </div>

      <div>
        <label class="fc-label">Size</label>
        <select id="f-size" class="fc">
          <option value="">All sizes</option>
        </select>
      </div>

      <div class="flex flex-col gap-1.5 pt-0.5">
        <label class="flex items-center gap-2 text-xs text-gray-600 cursor-pointer select-none">
          <input id="f-nfmp" type="checkbox" class="accent-green-700 w-3.5 h-3.5">
          Not full market price
        </label>
        <label class="flex items-center gap-2 text-xs text-gray-600 cursor-pointer select-none">
          <input id="f-vat" type="checkbox" class="accent-green-700 w-3.5 h-3.5">
          VAT exclusive
        </label>
      </div>

      <div class="border-t border-gray-100 pt-3">
        <label class="fc-label">Results per page</label>
        <select id="f-per-page" class="fc">
          <option value="25">25</option>
          <option value="50" selected>50</option>
          <option value="100">100</option>
          <option value="200">200</option>
        </select>
      </div>
    </div>

    <div class="p-4 border-t border-gray-100 flex flex-col gap-2">
      <button id="btn-search"
        class="w-full bg-green-700 hover:bg-green-800 text-white font-semibold py-2 rounded text-sm transition">
        Search
      </button>
      <button id="btn-reset"
        class="w-full border border-gray-200 hover:bg-gray-50 text-gray-500 font-medium py-1.5 rounded text-sm transition">
        Reset
      </button>
    </div>
  </aside>

  <!-- Main content -->
  <main class="flex-1 overflow-auto p-4 flex flex-col gap-3">

    <!-- Tabs -->
    <div class="flex border-b border-gray-200 bg-white rounded-t-lg shadow-sm px-3">
      <div id="tab-search" class="tab-btn active">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-4 h-4">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        Search
      </div>
      <div id="tab-stats" class="tab-btn">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-4 h-4">
          <path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
        </svg>
        Stats
      </div>
    </div>

    <!-- ═══════════ SEARCH VIEW ═══════════ -->
    <div id="view-search" class="flex flex-col gap-3">

    <!-- Stats bar -->
    <div id="stats-bar" class="bg-white rounded-lg shadow-sm p-3 flex flex-wrap gap-6 text-sm text-gray-700 hidden">
      <span><span class="font-semibold text-gray-900" id="stat-count">—</span> results</span>
      <span>Min: <span class="font-semibold text-gray-900" id="stat-min">—</span></span>
      <span>Max: <span class="font-semibold text-gray-900" id="stat-max">—</span></span>
      <span>Avg: <span class="font-semibold text-gray-900" id="stat-avg">—</span></span>
    </div>

    <!-- Loading -->
    <div id="loading" class="text-center text-gray-500 py-12 hidden">
      <svg class="animate-spin inline w-8 h-8 text-green-600" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>
      </svg>
      <p class="mt-2">Searching…</p>
    </div>

    <!-- Results table -->
    <div id="results-wrap" class="bg-white rounded-lg shadow-sm overflow-auto hidden">
      <table id="results-table" class="w-full text-sm border-collapse">
        <thead class="bg-gray-50 border-b text-left text-gray-600">
          <tr>
            <th class="px-3 py-2 sort-btn" data-col="date_of_sale">Date ↕</th>
            <th class="px-3 py-2">Address</th>
            <th class="px-3 py-2 sort-btn" data-col="county">County ↕</th>
            <th class="px-3 py-2">Eircode</th>
            <th class="px-3 py-2 sort-btn text-right" data-col="price_eur">Price ↕</th>
            <th class="px-3 py-2">Type</th>
            <th class="px-3 py-2">Size</th>
            <th class="px-3 py-2">Flags</th>
          </tr>
        </thead>
        <tbody id="results-body"></tbody>
      </table>
    </div>

    <!-- Empty state -->
    <div id="empty" class="text-center text-gray-400 py-16 hidden">
      <p class="text-4xl mb-2">🔍</p>
      <p class="text-lg">No results found</p>
      <p class="text-sm mt-1">Try adjusting your filters</p>
    </div>

    <!-- Intro state -->
    <div id="intro" class="text-center text-gray-400 py-16">
      <p class="text-4xl mb-3">🏠</p>
      <p class="text-lg text-gray-500">Set filters and click <strong>Search</strong></p>
      <p class="text-sm mt-1" id="intro-total"></p>
    </div>

    <!-- Pagination -->
    <div id="pagination" class="flex items-center gap-2 justify-center flex-wrap hidden">
      <button id="pg-prev" class="px-3 py-1.5 rounded border text-sm hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed">← Prev</button>
      <span id="pg-info" class="text-sm text-gray-600"></span>
      <button id="pg-next" class="px-3 py-1.5 rounded border text-sm hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed">Next →</button>
    </div>

    </div><!-- /view-search -->

    <!-- ═══════════ STATS VIEW ═══════════ -->
    <div id="view-stats" class="hidden flex-col gap-4">

      <!-- Loading stats -->
      <div id="stats-loading" class="text-center text-gray-500 py-12 hidden">
        <svg class="animate-spin inline w-8 h-8 text-green-600" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>
        </svg>
        <p class="mt-2">Crunching numbers…</p>
      </div>

      <div id="stats-content" class="flex flex-col gap-4 hidden">

        <!-- Headline KPIs -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div class="kpi-card"><div class="kpi-label">Transactions</div>
            <div class="kpi-value" id="k-count">—</div></div>
          <div class="kpi-card"><div class="kpi-label">Total value</div>
            <div class="kpi-value" id="k-total">—</div></div>
          <div class="kpi-card"><div class="kpi-label">Average price</div>
            <div class="kpi-value" id="k-avg">—</div></div>
          <div class="kpi-card"><div class="kpi-label">Median price</div>
            <div class="kpi-value" id="k-median">—</div></div>
          <div class="kpi-card"><div class="kpi-label">Highest sale</div>
            <div class="kpi-value" id="k-max">—</div>
            <div class="kpi-sub"   id="k-max-sub"></div></div>
          <div class="kpi-card"><div class="kpi-label">% New build</div>
            <div class="kpi-value" id="k-new">—</div></div>
          <div class="kpi-card"><div class="kpi-label">% Not full mkt price</div>
            <div class="kpi-value" id="k-nfmp">—</div></div>
          <div class="kpi-card"><div class="kpi-label">% VAT exclusive</div>
            <div class="kpi-value" id="k-vat">—</div></div>
        </div>

        <!-- Row 1: yearly trend + transactions/year -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="chart-card">
            <h3>Average price by year</h3>
            <div class="chart-wrap"><canvas id="c-yearly-price"></canvas></div>
          </div>
          <div class="chart-card">
            <h3>Transactions by year</h3>
            <div class="chart-wrap"><canvas id="c-yearly-count"></canvas></div>
          </div>
        </div>

        <!-- Row 2: county + new vs used -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="chart-card lg:col-span-2">
            <h3>Transactions by county</h3>
            <div class="chart-wrap"><canvas id="c-counties"></canvas></div>
          </div>
          <div class="chart-card">
            <h3>New vs Second-hand</h3>
            <div class="chart-wrap"><canvas id="c-typemix"></canvas></div>
          </div>
        </div>

        <!-- Row 3: histogram + monthly seasonality -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="chart-card">
            <h3>Price distribution</h3>
            <div class="chart-wrap"><canvas id="c-histogram"></canvas></div>
          </div>
          <div class="chart-card">
            <h3>Seasonality — sales by month of year</h3>
            <div class="chart-wrap"><canvas id="c-monthly"></canvas></div>
          </div>
        </div>

        <!-- Row 4: million-euro club + day-of-month -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="chart-card">
            <h3>The Million-Euro Club — sales ≥ €1M by year</h3>
            <div class="chart-wrap"><canvas id="c-million"></canvas></div>
          </div>
          <div class="chart-card">
            <h3>Day-of-month bias (when contracts close)</h3>
            <div class="chart-wrap"><canvas id="c-dom"></canvas></div>
          </div>
        </div>

        <!-- Row 5: Eircode routing -->
        <div class="chart-card">
          <h3>Top Eircode routing areas (most active postcodes)</h3>
          <div class="chart-wrap" style="height:320px"><canvas id="c-routing"></canvas></div>
        </div>

        <!-- Row 6: Avg price by county -->
        <div class="chart-card">
          <h3>Average price by county</h3>
          <div class="chart-wrap" style="height:340px"><canvas id="c-county-avg"></canvas></div>
        </div>

        <!-- Row 7: Avg price per county over time -->
        <div class="chart-card">
          <h3>Average price per county over time
            <span class="text-xs font-normal text-gray-500">(top 12 by volume)</span>
          </h3>
          <div class="chart-wrap" style="height:380px"><canvas id="c-county-over-time"></canvas></div>
        </div>

        <!-- Tables: top expensive + most flipped -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="chart-card">
            <h3>Top 10 most expensive sales</h3>
            <div class="overflow-auto">
              <table class="w-full text-sm">
                <thead class="text-left text-gray-500 border-b">
                  <tr><th class="py-1 pr-2">Date</th><th class="py-1 pr-2">Address</th><th class="py-1 pr-2">County</th><th class="py-1 text-right">Price</th></tr>
                </thead>
                <tbody id="t-top-expensive"></tbody>
              </table>
            </div>
          </div>
          <div class="chart-card">
            <h3>Most-frequently-sold addresses 🔁
              <span id="t-repeats-total" class="text-xs font-normal text-gray-500"></span>
            </h3>
            <div class="overflow-auto">
              <table class="w-full text-sm">
                <thead class="text-left text-gray-500 border-b">
                  <tr><th class="py-1 pr-2">Address</th><th class="py-1 pr-2">County</th><th class="py-1 text-center">×</th><th class="py-1 pr-2">First → last price</th></tr>
                </thead>
                <tbody id="t-repeats"></tbody>
              </table>
            </div>
          </div>
        </div>

      </div><!-- /stats-content -->

      <div id="stats-empty" class="text-center text-gray-400 py-16 hidden">
        <p class="text-4xl mb-2">📊</p>
        <p class="text-lg">No data for this filter combination</p>
      </div>

    </div><!-- /view-stats -->

  </main>
</div>

<!-- About modal -->
<div id="about-modal-backdrop">
  <div id="about-modal" role="dialog" aria-modal="true" aria-labelledby="about-title">

    <!-- Hero -->
    <div class="about-hero">
      <div class="relative z-10">
        <div class="mb-4 w-14 h-14 bg-white/15 rounded-2xl flex items-center justify-center">
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="white" class="w-8 h-8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12l8.954-8.955c.44-.439 1.152-.439 1.591 0L21.75 12M4.5 9.75v10.125c0 .621.504 1.125 1.125 1.125H9.75v-4.875c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125V21h4.125c.621 0 1.125-.504 1.125-1.125V9.75M8.25 21h8.25" />
          </svg>
        </div>
        <h1 id="about-title" class="text-2xl font-extrabold tracking-tight mb-1">PPR Browser</h1>
        <p class="text-green-200 text-sm font-medium">Irish Property Price Register — local explorer</p>
        <p class="text-green-100/80 text-xs mt-3 leading-relaxed max-w-md">
          Every residential property sale in Ireland, reported to the Revenue Commissioners
          and published by the Property Services Regulatory Authority, in one fast searchable interface.
        </p>
      </div>
    </div>

    <!-- Body -->
    <div class="px-6 py-5">

      <!-- Data badge -->
      <div class="bg-green-50 border border-green-100 rounded-lg px-4 py-2.5 mb-5 flex items-center gap-3">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.75" stroke="currentColor" class="w-5 h-5 text-green-700 shrink-0">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25zM6.75 12h.008v.008H6.75V12zm0 3h.008v.008H6.75V15zm0 3h.008v.008H6.75V18z" />
        </svg>
        <div>
          <p class="text-xs font-semibold text-green-800 uppercase tracking-wide">Source data</p>
          <p class="text-sm text-green-900">
            <strong id="about-total">—</strong> transactions &nbsp;·&nbsp; 2010 to present
            &nbsp;·&nbsp; Updated via
            <a href="https://www.propertypriceregister.ie" target="_blank"
               class="underline">propertypriceregister.ie</a>
          </p>
        </div>
      </div>

      <!-- Features -->
      <div class="divide-y divide-gray-100">

        <div class="about-feature">
          <div class="about-icon bg-blue-50">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="#3b82f6" class="w-5 h-5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
          </div>
          <div>
            <p class="font-semibold text-gray-800 text-sm">Search &amp; Filter</p>
            <p class="text-xs text-gray-500 mt-0.5 leading-relaxed">
              Filter by address keyword, county, year range, price band, property type, and size.
              Sort results by date or price. Paginate through thousands of matches instantly.
            </p>
          </div>
        </div>

        <div class="about-feature">
          <div class="about-icon bg-purple-50">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="#a855f7" class="w-5 h-5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
            </svg>
          </div>
          <div>
            <p class="font-semibold text-gray-800 text-sm">Stats &amp; Charts</p>
            <p class="text-xs text-gray-500 mt-0.5 leading-relaxed">
              Explore price trends by year and county, transaction volumes, new vs second-hand mix,
              price distributions, the Million-Euro Club, seasonal patterns, and more.
              All charts respond to your active filters.
            </p>
          </div>
        </div>

        <div class="about-feature">
          <div class="about-icon bg-amber-50">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="#f59e0b" class="w-5 h-5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
            </svg>
          </div>
          <div>
            <p class="font-semibold text-gray-800 text-sm">Always up to date</p>
            <p class="text-xs text-gray-500 mt-0.5 leading-relaxed">
              Click <em>Refresh</em> in the header to download the latest PPR export
              and rebuild the local database in the background — no restart needed.
            </p>
          </div>
        </div>

        <div class="about-feature">
          <div class="about-icon bg-gray-100">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="#6b7280" class="w-5 h-5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M9 17.25v1.007a3 3 0 01-.879 2.122L7.5 21h9l-.621-.621A3 3 0 0115 18.257V17.25m6-12V15a2.25 2.25 0 01-2.25 2.25H5.25A2.25 2.25 0 013 15V5.25m18 0A2.25 2.25 0 0018.75 3H5.25A2.25 2.25 0 003 5.25m18 0H3" />
            </svg>
          </div>
          <div>
            <p class="font-semibold text-gray-800 text-sm">Runs locally</p>
            <p class="text-xs text-gray-500 mt-0.5 leading-relaxed">
              A lightweight Python/Flask app storing data in a local SQLite database.
              Nothing leaves your machine. Source at
              <a href="https://github.com/orancummins/ippw" target="_blank"
                 class="text-blue-600 underline">github.com/orancummins/ippw</a>.
            </p>
          </div>
        </div>

      </div>

      <!-- Footer -->
      <div class="mt-6 pt-4 border-t border-gray-100 flex items-center justify-between gap-4">
        <label class="flex items-center gap-2 text-xs text-gray-400 cursor-pointer select-none">
          <input id="about-hide-future" type="checkbox" class="accent-green-700">
          Don’t show on startup
        </label>
        <button id="about-close"
          class="shrink-0 bg-green-700 hover:bg-green-800 text-white font-semibold px-6 py-2 rounded-lg text-sm transition">
          Got it
        </button>
      </div>
    </div>
  </div>
</div>

<script>
const fmt = n => n == null ? '—' : '€' + Math.round(n).toLocaleString('en-IE');
const fmtCount = n => n.toLocaleString('en-IE');

let state = { page: 1, sort: 'date_of_sale', dir: 'desc' };
let refreshPollTimer = null;
let refreshWasRunning = false;

// ── Load metadata ────────────────────────────────────────────────────────────
async function loadMeta() {
  const res = await fetch('api/meta');
  const d   = await res.json();

  const countyEl = document.getElementById('f-county');
  countyEl.innerHTML = '<option value="">All counties</option>';

  const [yf, yt] = ['f-year-from','f-year-to'].map(id => document.getElementById(id));
  yf.innerHTML = '<option value="">From</option>';
  yt.innerHTML = '<option value="">To</option>';

  const descEl = document.getElementById('f-description');
  descEl.innerHTML = '<option value="">All types</option>';

  const sizeEl = document.getElementById('f-size');
  sizeEl.innerHTML = '<option value="">All sizes</option>';

  document.getElementById('total-badge').textContent =
    fmtCount(d.total) + ' total records in DB';
  document.getElementById('intro-total').textContent =
    fmtCount(d.total) + ' records across all counties (2010–2026)';

  d.counties.forEach(c => {
    const o = document.createElement('option');
    o.value = o.textContent = c;
    countyEl.appendChild(o);
  });

  d.years.forEach(y => {
    [yf, yt].forEach(sel => {
      const o = document.createElement('option');
      o.value = o.textContent = y;
      sel.appendChild(o);
    });
  });

  d.descriptions.forEach(v => {
    if (!v) return;
    const o = document.createElement('option');
    o.value = v;
    o.textContent = v.replace('Dwelling house /Apartment','').trim();
    descEl.appendChild(o);
  });

  d.sizes.forEach(v => {
    if (!v) return;
    const o = document.createElement('option');
    o.value = o.textContent = v;
    sizeEl.appendChild(o);
  });
}

// ── Collect filter params ────────────────────────────────────────────────────
function getParams(page) {
  const p = new URLSearchParams();
  const v = id => document.getElementById(id).value.trim();
  const c = id => document.getElementById(id).checked;

  if (v('f-address'))     p.set('address',       v('f-address'));
  if (v('f-county'))      p.set('county',        v('f-county'));
  if (v('f-year-from'))   p.set('year_from',     v('f-year-from'));
  if (v('f-year-to'))     p.set('year_to',       v('f-year-to'));
  if (v('f-price-min'))   p.set('price_min',     v('f-price-min'));
  if (v('f-price-max'))   p.set('price_max',     v('f-price-max'));
  if (v('f-description')) p.set('description',   v('f-description'));
  if (v('f-size'))        p.set('property_size', v('f-size'));
  if (c('f-nfmp'))        p.set('not_fmp', '1');
  if (c('f-vat'))         p.set('vat', '1');

  p.set('per_page', v('f-per-page') || '50');
  p.set('page',     page || state.page);
  p.set('sort',     state.sort);
  p.set('dir',      state.dir);
  return p;
}

// ── Search ───────────────────────────────────────────────────────────────────
async function search(page) {
  state.page = page || 1;
  const params = getParams(state.page);

  show('loading');
  hide('results-wrap','empty','stats-bar','pagination','intro');

  let data;
  try {
    const res = await fetch('api/search?' + params);
    data = await res.json();
  } catch(e) {
    hide('loading');
    alert('Search failed: ' + e);
    return;
  }
  hide('loading');
  searchLoaded = true;

  // Stats
  if (data.total > 0) {
    document.getElementById('stat-count').textContent = fmtCount(data.total);
    document.getElementById('stat-min').textContent   = fmt(data.stats.min);
    document.getElementById('stat-max').textContent   = fmt(data.stats.max);
    document.getElementById('stat-avg').textContent   = fmt(data.stats.avg);
    show('stats-bar');
  }

  if (data.results.length === 0) {
    show('empty');
    return;
  }

  // Table
  const tbody = document.getElementById('results-body');
  tbody.innerHTML = '';
  data.results.forEach(r => {
    const isNew  = r.description && r.description.toLowerCase().includes('new');
    const typeBadge = r.description
      ? `<span class="badge ${isNew ? 'badge-new' : 'badge-used'}">${isNew ? 'New' : 'Used'}</span>`
      : '';
    const flags = [
      r.not_full_market_price ? '<span class="badge badge-nfmp">NFMP</span>' : '',
      r.vat_exclusive         ? '<span class="badge badge-vat">VAT excl.</span>' : '',
    ].filter(Boolean).join(' ');

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="px-3 py-1.5 text-gray-500 whitespace-nowrap">${r.date_of_sale || ''}</td>
      <td class="px-3 py-1.5 max-w-xs truncate" title="${(r.address||'').replace(/"/g,'&quot;')}">${r.address || ''}</td>
      <td class="px-3 py-1.5 whitespace-nowrap">${r.county || ''}</td>
      <td class="px-3 py-1.5 text-gray-400 whitespace-nowrap">${r.eircode || ''}</td>
      <td class="px-3 py-1.5 text-right font-mono font-medium whitespace-nowrap">${fmt(r.price_eur)}</td>
      <td class="px-3 py-1.5">${typeBadge}</td>
      <td class="px-3 py-1.5 text-gray-500 text-xs">${r.property_size || ''}</td>
      <td class="px-3 py-1.5">${flags}</td>
    `;
    tbody.appendChild(tr);
  });

  show('results-wrap');

  // Pagination
  if (data.pages > 1) {
    document.getElementById('pg-info').textContent =
      `Page ${data.page} of ${data.pages}`;
    document.getElementById('pg-prev').disabled = data.page <= 1;
    document.getElementById('pg-next').disabled = data.page >= data.pages;
    show('pagination');
  }
}

// ── Sort ─────────────────────────────────────────────────────────────────────
document.querySelectorAll('.sort-btn').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (state.sort === col) {
      state.dir = state.dir === 'desc' ? 'asc' : 'desc';
    } else {
      state.sort = col;
      state.dir  = col === 'price_eur' ? 'desc' : 'asc';
    }
    search(1);
  });
});

// ── Pagination buttons ───────────────────────────────────────────────────────
document.getElementById('pg-prev').addEventListener('click', () => search(state.page - 1));
document.getElementById('pg-next').addEventListener('click', () => search(state.page + 1));

// ── Search / Reset buttons ───────────────────────────────────────────────────
// Apply filters to BOTH tabs: run the active one immediately,
// mark the other dirty so it reloads when switched to.
function applyFilters() {
  searchLoaded = false;
  statsLoaded  = false;
  if (activeTab === 'stats') {
    loadStats();
  } else {
    search(1);
  }
}
document.getElementById('btn-search').addEventListener('click', applyFilters);
document.getElementById('btn-reset').addEventListener('click', () => {
  ['f-address','f-price-min','f-price-max'].forEach(id =>
    document.getElementById(id).value = '');
  ['f-county','f-year-from','f-year-to','f-description','f-size'].forEach(id =>
    document.getElementById(id).selectedIndex = 0);
  ['f-nfmp','f-vat'].forEach(id =>
    document.getElementById(id).checked = false);
  hide('results-wrap','empty','stats-bar','pagination','loading','stats-content','stats-empty');
  show('intro');
  statsLoaded  = false;
  searchLoaded = false;
});

// Enter key triggers search on both tabs
document.getElementById('f-address').addEventListener('keydown', e => {
  if (e.key === 'Enter') applyFilters();
});

// ── Source file refresh ─────────────────────────────────────────────────────
async function updateRefreshStatus() {
  try {
    const res = await fetch('api/refresh/status');
    const s = await res.json();

    const btn = document.getElementById('btn-refresh');
    const label = document.getElementById('refresh-status');
    btn.disabled = !!s.running;
    btn.classList.toggle('opacity-60', !!s.running);
    btn.classList.toggle('cursor-not-allowed', !!s.running);

    if (s.running) {
      label.textContent = s.stage || 'Refreshing...';
    } else if (s.last_result === 'success') {
      label.textContent = 'Last refresh successful';
    } else if (s.last_result === 'error') {
      label.textContent = 'Refresh failed';
    } else {
      label.textContent = 'Idle';
    }

    if (refreshWasRunning && !s.running && s.last_result === 'success') {
      await loadMeta();
      applyFilters();
    }

    refreshWasRunning = !!s.running;

    if (!s.running && refreshPollTimer) {
      clearInterval(refreshPollTimer);
      refreshPollTimer = null;
    }
  } catch (err) {
    const label = document.getElementById('refresh-status');
    label.textContent = 'Refresh status unavailable';
  }
}

async function startRefresh() {
  if (!confirm('Download latest source file from propertypriceregister.ie and rebuild local database now?')) {
    return;
  }

  try {
    const res = await fetch('api/refresh', { method: 'POST' });
    const payload = await res.json();
    if (!res.ok || !payload.ok) {
      alert(payload.error || payload.message || 'Could not start refresh.');
      return;
    }
  } catch (err) {
    alert('Could not start refresh: ' + err);
    return;
  }

  await updateRefreshStatus();
  if (!refreshPollTimer) {
    refreshPollTimer = setInterval(updateRefreshStatus, 3000);
  }
}

document.getElementById('btn-refresh').addEventListener('click', startRefresh);

// ── Tabs ─────────────────────────────────────────────────────────────────────
let activeTab    = 'search';
let statsLoaded  = false;
let searchLoaded = false;
const charts = {};

function setTab(name) {
  activeTab = name;
  document.getElementById('tab-search').classList.toggle('active', name === 'search');
  document.getElementById('tab-stats').classList.toggle('active',  name === 'stats');
  document.getElementById('view-search').classList.toggle('hidden', name !== 'search');
  document.getElementById('view-search').classList.toggle('flex',   name === 'search');
  document.getElementById('view-stats').classList.toggle('hidden',  name !== 'stats');
  document.getElementById('view-stats').classList.toggle('flex',    name === 'stats');
  if (name === 'stats'  && !statsLoaded)  loadStats();
  if (name === 'search' && !searchLoaded) search(1);
}
document.getElementById('tab-search').addEventListener('click', () => setTab('search'));
document.getElementById('tab-stats') .addEventListener('click', () => setTab('stats'));

// ── Stats ────────────────────────────────────────────────────────────────────
async function loadStats() {
  show('stats-loading'); hide('stats-content','stats-empty');

  const params = new URLSearchParams();
  const v = id => document.getElementById(id).value.trim();
  const c = id => document.getElementById(id).checked;
  if (v('f-address'))     params.set('address',       v('f-address'));
  if (v('f-county'))      params.set('county',        v('f-county'));
  if (v('f-year-from'))   params.set('year_from',     v('f-year-from'));
  if (v('f-year-to'))     params.set('year_to',       v('f-year-to'));
  if (v('f-price-min'))   params.set('price_min',     v('f-price-min'));
  if (v('f-price-max'))   params.set('price_max',     v('f-price-max'));
  if (v('f-description')) params.set('description',   v('f-description'));
  if (v('f-size'))        params.set('property_size', v('f-size'));
  if (c('f-nfmp'))        params.set('not_fmp', '1');
  if (c('f-vat'))         params.set('vat', '1');

  let d;
  try {
    const res = await fetch('api/stats?' + params);
    d = await res.json();
  } catch(e) {
    hide('stats-loading');
    alert('Stats failed: ' + e);
    return;
  }
  hide('stats-loading');

  if (!d.kpis || d.kpis.count === 0) {
    show('stats-empty');
    statsLoaded = true;
    return;
  }
  show('stats-content');
  statsLoaded = true;

  // ── KPI cards ──
  const k = d.kpis;
  document.getElementById('k-count' ).textContent = fmtCount(k.count);
  document.getElementById('k-total' ).textContent = fmtBig(k.total_value);
  document.getElementById('k-avg'   ).textContent = fmt(k.avg_price);
  document.getElementById('k-median').textContent = fmt(k.median_price);
  document.getElementById('k-max'   ).textContent = fmt(k.max_price);
  document.getElementById('k-new'   ).textContent = k.pct_new.toFixed(1) + '%';
  document.getElementById('k-nfmp'  ).textContent = k.pct_nfmp.toFixed(1) + '%';
  document.getElementById('k-vat'   ).textContent = k.pct_vat.toFixed(1) + '%';
  if (d.top_expensive[0]) {
    const t = d.top_expensive[0];
    document.getElementById('k-max-sub').textContent = `${t.address} (${t.county}, ${t.date_of_sale})`;
  }

  const palette = ['#15803d','#0369a1','#b45309','#7c3aed','#be185d','#0891b2','#65a30d','#c2410c','#4338ca','#0f766e'];

  // Helpers
  const destroy = id => { if (charts[id]) charts[id].destroy(); };
  const mk = (id, cfg) => { destroy(id); charts[id] = new Chart(document.getElementById(id), cfg); };

  // ── Yearly avg price ──
  mk('c-yearly-price', {
    type: 'line',
    data: {
      labels: d.yearly.map(r => r.year),
      datasets: [{
        label: 'Avg price (€)',
        data:  d.yearly.map(r => Math.round(r.avg_price)),
        borderColor: '#15803d',
        backgroundColor: '#15803d22',
        tension: 0.25, fill: true, pointRadius: 3,
      }]
    },
    options: chartOpts({ y: { ticks: { callback: v => '€' + (v/1000).toFixed(0) + 'k' } } })
  });

  // ── Yearly transactions ──
  mk('c-yearly-count', {
    type: 'bar',
    data: {
      labels: d.yearly.map(r => r.year),
      datasets: [{ label: 'Transactions', data: d.yearly.map(r => r.count), backgroundColor: '#0369a1' }]
    },
    options: chartOpts({ y: { ticks: { callback: v => v.toLocaleString() } } })
  });

  // ── County transactions ──
  mk('c-counties', {
    type: 'bar',
    data: {
      labels: d.counties.map(r => r.county),
      datasets: [{ label: 'Transactions', data: d.counties.map(r => r.count), backgroundColor: '#15803d' }]
    },
    options: chartOpts({ y: { ticks: { callback: v => v.toLocaleString() } } })
  });

  // ── New vs used pie ──
  mk('c-typemix', {
    type: 'doughnut',
    data: {
      labels: ['New build', 'Second-hand', 'Unknown'],
      datasets: [{
        data: [d.type_mix.new, d.type_mix.used, d.type_mix.unknown],
        backgroundColor: ['#15803d','#0369a1','#9ca3af'],
      }]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }
  });

  // ── Histogram ──
  mk('c-histogram', {
    type: 'bar',
    data: {
      labels: d.histogram.map(r => r.label),
      datasets: [{ label: 'Sales', data: d.histogram.map(r => r.count), backgroundColor: '#7c3aed' }]
    },
    options: chartOpts({ y: { ticks: { callback: v => v.toLocaleString() } } })
  });

  // ── Monthly seasonality ──
  const monthLabels = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  mk('c-monthly', {
    type: 'bar',
    data: {
      labels: d.monthly.map(r => monthLabels[r.month-1] || r.month),
      datasets: [{ label: 'Sales', data: d.monthly.map(r => r.count), backgroundColor: '#b45309' }]
    },
    options: chartOpts()
  });

  // ── Million-Euro club ──
  mk('c-million', {
    type: 'bar',
    data: {
      labels: d.million.map(r => r.year),
      datasets: [{ label: 'Sales ≥ €1M', data: d.million.map(r => r.count), backgroundColor: '#be185d' }]
    },
    options: chartOpts()
  });

  // ── Day-of-month ──
  mk('c-dom', {
    type: 'bar',
    data: {
      labels: d.day_of_month.map(r => r.dom),
      datasets: [{ label: 'Sales', data: d.day_of_month.map(r => r.count), backgroundColor: '#0891b2' }]
    },
    options: chartOpts()
  });

  // ── Routing areas ──
  mk('c-routing', {
    type: 'bar',
    data: {
      labels: d.routing.map(r => r.routing),
      datasets: [
        { label: 'Sales', data: d.routing.map(r => r.count), backgroundColor: '#15803d', yAxisID: 'y' },
        { label: 'Avg price (€)', data: d.routing.map(r => Math.round(r.avg_price)), backgroundColor: '#c2410c', yAxisID: 'y1', type: 'line', borderColor: '#c2410c' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y:  { beginAtZero: true, position: 'left',  ticks: { callback: v => v.toLocaleString() } },
        y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false }, ticks: { callback: v => '€' + (v/1000).toFixed(0) + 'k' } }
      },
      plugins: { legend: { position: 'bottom' } }
    }
  });

  // ── Avg price by county ──
  const sortedCounty = [...d.counties].sort((a,b) => b.avg_price - a.avg_price);
  mk('c-county-avg', {
    type: 'bar',
    data: {
      labels: sortedCounty.map(r => r.county),
      datasets: [{
        label: 'Average price (€)',
        data: sortedCounty.map(r => Math.round(r.avg_price)),
        backgroundColor: sortedCounty.map((_,i) => palette[i % palette.length]),
      }]
    },
    options: chartOpts({ y: { ticks: { callback: v => '€' + (v/1000).toFixed(0) + 'k' } } })
  });

  // ── Avg price per county over time (multi-line) ──
  const cot = d.county_over_time || {};
  const allYears = Array.from(new Set(
    Object.values(cot).flat().map(r => r.year)
  )).sort((a,b) => a - b);
  const cotCounties = Object.keys(cot);
  mk('c-county-over-time', {
    type: 'line',
    data: {
      labels: allYears,
      datasets: cotCounties.map((county, i) => {
        const byYear = Object.fromEntries(cot[county].map(r => [r.year, r.avg_price]));
        return {
          label: county,
          data:  allYears.map(y => byYear[y] != null ? Math.round(byYear[y]) : null),
          borderColor: palette[i % palette.length],
          backgroundColor: palette[i % palette.length] + '22',
          tension: 0.25, pointRadius: 2, spanGaps: true, fill: false,
        };
      })
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: { beginAtZero: false, ticks: { callback: v => '€' + (v/1000).toFixed(0) + 'k' } }
      },
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}` } }
      }
    }
  });

  // ── Top expensive table ──
  const te = document.getElementById('t-top-expensive');
  te.innerHTML = '';
  d.top_expensive.forEach(r => {
    const tr = document.createElement('tr');
    tr.className = 'border-b last:border-0';
    tr.innerHTML = `
      <td class="py-1 pr-2 text-gray-500 whitespace-nowrap">${r.date_of_sale || ''}</td>
      <td class="py-1 pr-2 max-w-xs truncate" title="${(r.address||'').replace(/"/g,'&quot;')}">${r.address || ''}</td>
      <td class="py-1 pr-2 whitespace-nowrap">${r.county || ''}</td>
      <td class="py-1 text-right font-mono font-semibold">${fmt(r.price_eur)}</td>
    `;
    te.appendChild(tr);
  });

  // ── Repeats table ──
  document.getElementById('t-repeats-total').textContent =
    `(${fmtCount(d.repeats.total_addresses_sold_multiple_times)} addresses sold ≥2×)`;
  const tr2 = document.getElementById('t-repeats');
  tr2.innerHTML = '';
  d.repeats.top.forEach(r => {
    const change = r.first_price && r.last_price
      ? ` <span class="text-xs ${r.last_price > r.first_price ? 'text-green-700' : 'text-red-700'}">(${r.last_price > r.first_price ? '+' : ''}${(((r.last_price-r.first_price)/r.first_price)*100).toFixed(0)}%)</span>`
      : '';
    const row = document.createElement('tr');
    row.className = 'border-b last:border-0';
    row.innerHTML = `
      <td class="py-1 pr-2 max-w-xs truncate" title="${(r.address||'').replace(/"/g,'&quot;')}">${r.address || ''}</td>
      <td class="py-1 pr-2 whitespace-nowrap">${r.county || ''}</td>
      <td class="py-1 text-center font-semibold">${r.times_sold}</td>
      <td class="py-1 pr-2 whitespace-nowrap font-mono text-xs">${fmt(r.first_price)} → ${fmt(r.last_price)}${change}</td>
    `;
    tr2.appendChild(row);
  });
}

function chartOpts(scales) {
  return {
    responsive: true, maintainAspectRatio: false,
    scales: Object.assign({ y: { beginAtZero: true } }, scales || {}),
    plugins: { legend: { display: false } }
  };
}

function fmtBig(n) {
  if (n == null) return '—';
  if (n >= 1e9) return '€' + (n/1e9).toFixed(2) + 'B';
  if (n >= 1e6) return '€' + (n/1e6).toFixed(2) + 'M';
  if (n >= 1e3) return '€' + (n/1e3).toFixed(0) + 'k';
  return fmt(n);
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function show(...ids) { ids.forEach(id => document.getElementById(id).classList.remove('hidden')); }
function hide(...ids) { ids.forEach(id => document.getElementById(id).classList.add('hidden')); }

loadMeta();
updateRefreshStatus();

// ── About modal ──────────────────────────────────────────────────────────────
function openAbout() {
  document.getElementById('about-modal-backdrop').classList.add('open');
}
function closeAbout() {
  const bd = document.getElementById('about-modal-backdrop');
  bd.classList.remove('open');
  if (document.getElementById('about-hide-future').checked) {
    localStorage.setItem('ppr-about-seen', '1');
  }
}
document.getElementById('about-close').addEventListener('click', closeAbout);
document.getElementById('btn-about').addEventListener('click', openAbout);
document.getElementById('about-modal-backdrop').addEventListener('click', e => {
  if (e.target === e.currentTarget) closeAbout();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeAbout();
});

// Populate total in modal once meta loads
const _origLoadMeta = loadMeta;
loadMeta = async function() {
  const res = await _origLoadMeta.apply(this, arguments);
  const badge = document.getElementById('total-badge').textContent;
  const m = badge.match(/[\d,]+/);
  if (m) document.getElementById('about-total').textContent = m[0] + ' transactions';
  return res;
};

// Show on first visit
if (!localStorage.getItem('ppr-about-seen')) {
  openAbout();
}
</script>
</body>
</html>
"""


SETUP_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PPR Browser — Setting up</title>
<script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 min-h-screen flex items-center justify-center">
<div class="bg-white rounded-xl shadow-lg p-10 max-w-md w-full text-center">
  <div class="text-5xl mb-4">🏠</div>
  <h1 class="text-xl font-bold text-gray-800 mb-1">Setting up PPR Browser</h1>
  <p class="text-sm text-gray-500 mb-6">Downloading data from propertypriceregister.ie…<br>This only happens once and takes a minute or two.</p>
  <div class="w-full bg-gray-100 rounded-full h-2 mb-4 overflow-hidden">
    <div id="bar" class="bg-green-600 h-2 rounded-full transition-all duration-500" style="width:5%"></div>
  </div>
  <p id="stage" class="text-sm text-gray-600 font-medium mb-1">Starting…</p>
  <p id="error" class="text-sm text-red-600 hidden mt-3"></p>
</div>
<script>
let pct = 5;
const stages = {
  'Downloading': 20,
  'Rebuilding': 70,
  'Refresh complete': 100,
};
async function poll() {
  try {
    const r = await fetch('api/refresh/status');
    const s = await r.json();
    const stage = s.stage || '';
    document.getElementById('stage').textContent = stage;
    for (const [key, val] of Object.entries(stages)) {
      if (stage.startsWith(key)) { pct = Math.max(pct, val); break; }
    }
    if (!s.running) pct = Math.max(pct, 90);
    document.getElementById('bar').style.width = pct + '%';
    if (!s.running && s.last_result === 'success') {
      document.getElementById('bar').style.width = '100%';
      document.getElementById('stage').textContent = 'Done! Loading…';
      setTimeout(() => location.reload(), 800);
      return;
    }
    if (!s.running && s.last_result === 'error') {
      const el = document.getElementById('error');
      el.textContent = 'Error: ' + (s.last_error || 'unknown');
      el.classList.remove('hidden');
      document.getElementById('stage').textContent = 'Setup failed.';
      return;
    }
  } catch(e) {}
  setTimeout(poll, 2000);
}
poll();
</script>
</body>
</html>
"""


@app.get("/")
def index():
    conn = sqlite3.connect(str(DB_PATH))
    try:
        count = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
    except Exception:
        count = 0
    finally:
        conn.close()

    state = _refresh_get_state()
    if count == 0 and state.get("last_result") != "success":
        return render_template_string(SETUP_HTML)
    return render_template_string(HTML)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ensure schema exists and check if we have data.
    _boot_conn = sqlite3.connect(str(DB_PATH))
    _boot_conn.executescript(SCHEMA)
    _boot_count = _boot_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]

    if _boot_count > 0:
        # Sanity-check FTS: if properties has rows but addr_fts is empty
        # (e.g. WAL wasn't checkpointed during a previous DB swap), rebuild now.
        try:
            _fts_count = _boot_conn.execute("SELECT COUNT(*) FROM addr_fts").fetchone()[0]
        except Exception:
            _fts_count = 0
        if _fts_count == 0:
            print("[ppr] FTS index empty \u2014 rebuilding from existing data \u2026", flush=True)
            _boot_conn.execute("INSERT INTO addr_fts(addr_fts) VALUES('rebuild')")
            _boot_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _boot_conn.commit()
            print("[ppr] FTS rebuild complete.", flush=True)

    _boot_conn.close()

    if _boot_count == 0:
        print("[ppr] No data found — starting background download from propertypriceregister.ie …", flush=True)
        _refresh_set_state(
            running=True,
            stage="Downloading source file from propertypriceregister.ie",
            last_error=None,
            last_result=None,
        )
        threading.Thread(target=_refresh_worker, daemon=True).start()
    else:
        print(f"[ppr] DB has {_boot_count:,} records — ready.", flush=True)

    print(f"[ppr] Starting on http://localhost:{PORT}", flush=True)
    app.run(host="0.0.0.0", port=PORT, debug=False)
