"""
Price Monitor ETL  ·  search-driven

Airflow owns the data + credentials; the price_monitor container is a stateless
scraper triggered per run. Flow:

  1. extract_products      — Zoho CRM Products (EAN, name, brand, PVL, IVA, lab)
  2. extract_sales_volume  — BIFarma SO units per product (pick what to monitor)
  3. select_products       — join + keep the top-selling products that have an EAN
  4. scrape_*              — POST the product JSON to the search-driven spiders
                             (atida, farmavazquez) and trigger primor's category
                             crawl, via Scrapyd schedule.json; pull raw items
  5. match_and_load        — search results are already EAN-keyed; primor's raw
                             dump is matched offline by name+brand (match_score);
                             load competitor_prices (by EAN) into ClickHouse

Why search-driven: we drive each lookup from a product whose EAN we already know
(from Zoho), so competitor prices come back keyed to your catalog even when the
competitor never publishes an EAN. match_score is a trust gate — high = accept,
low = ignore, so a wrong top hit can't set a price.

Tunable via Airflow Variables:
  scrapyd_base_url                 (http://scrapyd:6800)
  price_monitor_schedule           ("0 5 * * *")
  price_monitor_max_products       ("200")  — cap the monitored set (top sellers)
  price_monitor_match_threshold    ("0.6")  — min match_score to accept a price
  price_monitor_primor_categories  (JSON list of primor category URLs to crawl)
  clickhouse_url / _db / _user / _password
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID = "zoho_crm"
SQL_BIFARMA_CONN_ID = "BIFarma_db"

SCRAPYD_URL = Variable.get("scrapyd_base_url", default_var="http://scrapyd:6800")
PROJECT = "price_monitor"
SEARCH_SPIDERS = ["competitor_atida", "competitor_farmavazquez"]
CATEGORY_SPIDER = "competitor_primor"

BASE_TMP = "/opt/airflow/data/staging/price_monitor"
POLL_INTERVAL_S = 15
POLL_TIMEOUT_S = 60 * 45  # generous: search spiders do one request per product

MAX_PRODUCTS = int(Variable.get("price_monitor_max_products", default_var="200"))
MATCH_THRESHOLD = float(Variable.get("price_monitor_match_threshold", default_var="0.6"))

CLICKHOUSE_URL = Variable.get("clickhouse_url", default_var="http://clickhouse:8123")
CLICKHOUSE_DB = Variable.get("clickhouse_db", default_var="price_monitor")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="price_monitor")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="changeme_pm")

# Zoho Products field map (confirmed via ecoLabs_ETL / calculo_fee_ETL).
Z_CN = "Product_Code"
Z_EAN = "EAN"
Z_NAME = "Product_Name"
Z_BRAND = "Marca"

_CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.competitor_prices
(
    scraped_at DateTime, run_id String, ean String, site LowCardinality(String),
    competitor_name String, competitor_url String, price Nullable(Float64),
    currency LowCardinality(String), in_stock UInt8, match_score Float32
) ENGINE = MergeTree PARTITION BY toYYYYMM(scraped_at) ORDER BY (ean, site, scraped_at)
""".strip()


# ─────────────────────────────────────────────────────────────
# Helpers (module-level: pure, unit-testable)
# ─────────────────────────────────────────────────────────────

def _run_dir(ctx) -> str:
    import os
    dag_run = ctx.get("dag_run")
    run_id = str(dag_run.run_id) if dag_run is not None else "default"
    safe = run_id.replace(":", "_").replace("+", "_")
    d = os.path.join(BASE_TMP, safe)
    os.makedirs(d, exist_ok=True)
    return d


def _tokens(s):
    import re
    return [t for t in re.sub(r"[^\w]+", " ", (s or "").lower()).split() if len(t) > 1]


def _match_score(query_name, candidate_name):
    """Fraction of the product's tokens present in the candidate name (0..1)."""
    a, b = set(_tokens(query_name)), set(_tokens(candidate_name))
    if not a or not b:
        return 0.0
    return round(len(a & b) / len(a), 3)


def _clean_str(v):
    s = str(v).strip()
    return "" if s.lower() in ("", "nan", "none") else s


def _ean_key(v):
    """Normalise an EAN to a plain digit string, or '' if not usable."""
    import pandas as pd
    n = pd.to_numeric(str(v).strip(), errors="coerce")
    if pd.isna(n):
        return ""
    return str(int(n))


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="price_monitor_ETL",
    description="Zoho products -> search competitors (Scrapyd) -> competitor_prices by EAN in ClickHouse",
    schedule=Variable.get("price_monitor_schedule", default_var="0 5 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["scraping", "prices", "zoho"],
)
def price_monitor_etl():

    # ── 1. Zoho Products ──
    @task
    def extract_products() -> str:
        """Page through Zoho CRM Products; keep {ean, cn, name, brand}; write JSON; return path."""
        import os, json
        from time import sleep
        from airflow.hooks.base import BaseHook
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector
        from airflow.operators.python import get_current_context

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        products, page = [], 0
        while True:
            try:
                batch = crm.fetch_module_data("Products", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            for p in batch:
                ean = _ean_key(p.get(Z_EAN) or p.get(Z_CN))
                name = _clean_str(p.get(Z_NAME))
                if not ean or not name:
                    continue
                products.append({
                    "ean": ean,
                    "cn": _clean_str(p.get(Z_CN)),
                    "name": name,
                    "brand": _clean_str(p.get(Z_BRAND)),
                })
            page += 1
            sleep(0.3)
        print(f"Zoho: {len(products)} products with a usable EAN + name")

        run_dir = _run_dir(get_current_context())
        out = os.path.join(run_dir, "zoho_products.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(products, f)
        return out

    # ── 2. BIFarma sell-out volume (to prioritise what we monitor) ──
    @task
    def extract_sales_volume() -> str:
        """SO units per CN over the last 12 months (ECO group); write JSON {cn: units}; return path."""
        import os, json
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsSQL import SQLConnection

        run_dir = _run_dir(get_current_context())
        out = os.path.join(run_dir, "sales_volume.json")
        try:
            ctx = get_current_context()
            ds = ctx["logical_date"]
            end = ds.year * 100 + ds.month
            start = (ds.year - 1) * 100 + ds.month  # rolling 12 months
            sql = f"""
                SELECT pr.codproducto AS CN, SUM(ISNULL(T1.cantidad, 0)) AS SO
                FROM dbo.bench_dwComprasVentasMesS T1
                INNER JOIN dbo.tbi_productosERS pr
                    ON T1.idendeS = pr.idendeS AND T1.idproducto = pr.idproducto
                WHERE T1.anyomes > {start} AND T1.anyomes <= {end}
                  AND T1.idendes IN (SELECT idendes FROM tme_delegaciones WHERE grupoCompras = 'ECO')
                GROUP BY pr.codproducto
            """
            conn = BaseHook.get_connection(SQL_BIFARMA_CONN_ID)
            db = SQLConnection(
                db_host=conn.host, db_port=conn.port or 1433, db_database=conn.schema,
                db_username=conn.login, db_password=conn.password, dialect="mssql", driver="pymssql",
            )
            with db:
                df = db.fech_dataframe(sql)
            vol = {_clean_str(r["CN"]): float(r["SO"] or 0) for _, r in df.iterrows()}
            print(f"BIFarma: SO volume for {len(vol)} products")
        except Exception as e:
            print(f"⚠️ sales volume unavailable ({e!r}) — proceeding without prioritisation")
            vol = {}
        with open(out, "w", encoding="utf-8") as f:
            json.dump(vol, f)
        return out

    # ── 3. Pick the monitored set (top sellers with an EAN) ──
    @task
    def select_products(products_file: str, volume_file: str) -> str:
        """Join products+volume, keep the top MAX_PRODUCTS by SO units; write JSON; return path."""
        import os, json
        from airflow.operators.python import get_current_context

        with open(products_file, encoding="utf-8") as f:
            products = json.load(f)
        with open(volume_file, encoding="utf-8") as f:
            volume = json.load(f)

        for p in products:
            p["_so"] = volume.get(p.get("cn", ""), 0.0)
        if volume:
            products.sort(key=lambda p: p["_so"], reverse=True)
        else:
            print("No volume signal — keeping products in Zoho order")
        monitored = products[:MAX_PRODUCTS]
        # payload the spiders need: ean, name, brand
        payload = [{"ean": p["ean"], "name": p["name"], "brand": p["brand"]} for p in monitored]

        run_dir = _run_dir(get_current_context())
        out = os.path.join(run_dir, "monitored_products.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        print(f"Monitoring {len(payload)} products (cap {MAX_PRODUCTS}); "
              f"top SO example: {monitored[0]['name'] if monitored else '-'}")
        return out

    # ── 4a. Search-driven spiders (one Scrapyd job per spider, products as arg) ──
    @task
    def scrape_search(spider: str, monitored_file: str) -> dict:
        """Trigger a search-driven spider with the product list; poll; pull items."""
        import os, json, time, requests
        from airflow.operators.python import get_current_context

        run_dir = _run_dir(get_current_context())
        with open(monitored_file, encoding="utf-8") as f:
            products_json = f.read()

        jobid = _schedule(spider, {"products": products_json})
        _wait(jobid)
        return _pull(spider, jobid, run_dir)

    # ── 4b. primor category crawl (raw dump) ──
    @task
    def scrape_primor() -> dict:
        """Trigger primor's category crawl (search is robots-blocked); poll; pull raw items."""
        import json
        from airflow.operators.python import get_current_context

        run_dir = _run_dir(get_current_context())
        categories = Variable.get(
            "price_monitor_primor_categories",
            default_var='["https://www.primor.eu/es_es/perfumes-de-mujer"]',
        )
        jobid = _schedule(CATEGORY_SPIDER, {"categories": categories})
        _wait(jobid)
        return _pull(CATEGORY_SPIDER, jobid, run_dir)

    # ── 5. Match to EAN + load competitor_prices to ClickHouse ──
    @task
    def match_and_load(search_results: list[dict], primor_result: dict, monitored_file: str) -> int:
        import os, json
        from datetime import datetime, timezone
        import requests
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        run_dir = _run_dir(ctx)
        dag_run = ctx.get("dag_run")
        run_id = str(dag_run.run_id) if dag_run is not None else "default"
        scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        with open(monitored_file, encoding="utf-8") as f:
            monitored = json.load(f)

        rows = []

        # 5a. Search spiders: items already carry input_ean + match_score.
        for res in search_results:
            if not res or not res.get("items_file"):
                continue
            for rec in _read_jsonl(res["items_file"]):
                if not rec.get("matched") or rec.get("price") is None:
                    continue
                if float(rec.get("match_score") or 0) < MATCH_THRESHOLD:
                    continue
                rows.append(_row(scraped_at, run_id, rec.get("input_ean"), rec.get("site"),
                                 rec.get("name"), rec.get("url"), rec.get("price"),
                                 rec.get("in_stock"), rec.get("match_score")))

        # 5b. primor: raw listings matched offline to the monitored catalog by name+brand.
        if primor_result and primor_result.get("items_file"):
            listings = [r for r in _read_jsonl(primor_result["items_file"]) if r.get("price") is not None]
            matched = _match_catalog(monitored, listings)
            for m in matched:
                rows.append(_row(scraped_at, run_id, m["ean"], "primor",
                                 m["name"], m["url"], m["price"], m.get("in_stock"), m["score"]))

        if not rows:
            print("No competitor prices met the match threshold — nothing loaded.")
            return 0

        headers = {"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD}
        requests.post(CLICKHOUSE_URL, params={"query": _CLICKHOUSE_DDL.format(db=CLICKHOUSE_DB)},
                      headers=headers, timeout=30).raise_for_status()
        body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        resp = requests.post(
            CLICKHOUSE_URL,
            params={"query": f"INSERT INTO {CLICKHOUSE_DB}.competitor_prices FORMAT JSONEachRow",
                    "input_format_skip_unknown_fields": 1},
            data=body, headers=headers, timeout=120,
        )
        resp.raise_for_status()
        by_site = {}
        for r in rows:
            by_site[r["site"]] = by_site.get(r["site"], 0) + 1
        print(f"Loaded {len(rows)} competitor prices into {CLICKHOUSE_DB}.competitor_prices: {by_site}")
        return len(rows)

    # ── Wire ──
    products = extract_products()
    volume = extract_sales_volume()
    monitored = select_products(products, volume)
    search_results = scrape_search.partial(monitored_file=monitored).expand(spider=SEARCH_SPIDERS)
    primor_result = scrape_primor()
    match_and_load(search_results, primor_result, monitored)


# ─────────────────────────────────────────────────────────────
# Scrapyd + load helpers (module-level so tasks stay thin)
# ─────────────────────────────────────────────────────────────

def _schedule(spider, data):
    import requests
    payload = {"project": PROJECT, "spider": spider}
    payload.update(data)
    r = requests.post(f"{SCRAPYD_URL}/schedule.json", data=payload, timeout=60)
    r.raise_for_status()
    body = r.json()
    if body.get("status") != "ok":
        raise RuntimeError(f"[{spider}] schedule.json failed: {body}")
    print(f"[{spider}] scheduled jobid={body['jobid']}")
    return body["jobid"]


def _wait(jobid):
    import time, requests
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while True:
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {jobid} not finished after {POLL_TIMEOUT_S}s")
        lj = requests.get(f"{SCRAPYD_URL}/listjobs.json", params={"project": PROJECT}, timeout=30).json()
        if jobid in {j["id"] for j in lj.get("finished", [])}:
            return
        time.sleep(POLL_INTERVAL_S)


def _pull(spider, jobid, run_dir):
    import os, requests
    url = f"{SCRAPYD_URL}/items/{PROJECT}/{spider}/{jobid}.jl"
    resp = requests.get(url, timeout=120)
    out = os.path.join(run_dir, f"{spider}.jsonl")
    if resp.status_code == 200 and resp.content:
        with open(out, "wb") as f:
            f.write(resp.content)
        n = resp.content.count(b"\n")
        print(f"[{spider}] pulled {n} items -> {out}")
        return {"spider": spider, "jobid": jobid, "items_file": out, "count": n}
    print(f"[{spider}] no items at {url} (HTTP {resp.status_code})")
    return {"spider": spider, "jobid": jobid, "items_file": "", "count": 0}


def _read_jsonl(path):
    import json
    if not path:
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def _row(scraped_at, run_id, ean, site, name, url, price, in_stock, score):
    return {
        "scraped_at": scraped_at, "run_id": run_id, "ean": str(ean or ""), "site": site or "",
        "competitor_name": name or "", "competitor_url": url or "",
        "price": price, "currency": "EUR", "in_stock": 1 if in_stock else 0,
        "match_score": float(score or 0),
    }


def _match_catalog(monitored, listings):
    """For each monitored product, find the best primor listing by name+brand overlap."""
    out = []
    for p in monitored:
        target = f"{p.get('brand', '')} {p.get('name', '')}"
        best, best_score = None, 0.0
        for lst in listings:
            score = _match_score(target, lst.get("name"))
            if score > best_score:
                best, best_score = lst, score
        if best and best_score >= MATCH_THRESHOLD:
            out.append({
                "ean": p["ean"], "name": best.get("name"), "url": best.get("url"),
                "price": best.get("price"), "in_stock": best.get("in_stock"), "score": best_score,
            })
    print(f"primor: matched {len(out)}/{len(monitored)} monitored products (>= {MATCH_THRESHOLD})")
    return out


price_monitor_etl()
