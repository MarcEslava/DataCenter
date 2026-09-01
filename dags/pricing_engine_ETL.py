"""
Pricing Engine ETL  ·  Phase 3

Turns pricing_2.ipynb into a scheduled DAG. Consumes:
  - competitor_prices (ClickHouse, EAN-keyed) — from price_monitor_ETL
  - product cost/margin from Zoho CRM Products (PVL, IVA, Dto_Book_3)
  - sell-out units from BIFarma (for €/margin totals)

Pricing rule (from the notebook, lines 218-229), with a margin floor:
  PUC(No IVA) = PVL * (1 - Dto_Book_3)                 # unit cost after lab discount
  PUC(IVA+R)  = PUC(No IVA) * (1 + IVA + RE)           # cost incl. tax (RE derived from IVA)
  PVPr Min    = min competitor price (> 0)             # cheapest competitor for that EAN
  PVPr teoric = PVPr Min
  candidate   = MROUND(PVPr teoric, 0.5) - 0.05        # match cheapest, psychological rounding
  floor       = PUC(IVA+R) * (1 + MIN_MARGIN_PCT)      # never price below this
  PVPr Final  = max(candidate, floor)                  # margin floor applied
  below_floor = candidate < floor                      # matching would lose money -> flag for review
  Margen €/%  = PVPr Final - PUC(IVA+R) (and /PVPr Final)

Output: price_monitor.price_recommendations in ClickHouse (PowerBI reads it).
BI/pricing rules will be refined later (own workstream).

Variables:
  pricing_schedule           ("0 7 * * *")
  pricing_min_margin_pct     ("0")     # floor = cost * (1 + this); 0 = never below cost
  clickhouse_* (shared with price_monitor)
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ZOHO_CONN_ID = "zoho_crm"
SQL_BIFARMA_CONN_ID = "BIFarma_db"

CLICKHOUSE_URL = Variable.get("clickhouse_url", default_var="http://clickhouse:8123")
CLICKHOUSE_DB = Variable.get("clickhouse_db", default_var="price_monitor")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="price_monitor")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="changeme_pm")

MIN_MARGIN_PCT = float(Variable.get("pricing_min_margin_pct", default_var="0"))

BASE_TMP = "/opt/airflow/data/staging/pricing_engine"

# Zoho Products fields (Decimal_1/IVA2 confirmed in calculo_fee; book discount per ecoLabs).
Z_CN = "Product_Code"
Z_EAN = "EAN"
Z_NAME = "Product_Name"
Z_BRAND = "Marca"
Z_PVL = "Decimal_1"
Z_IVA = "IVA2"
Z_BOOK3_CANDIDATES = ["Dto_Book_3", "Descuento_Book_3", "Dto_Book3", "Book_3"]

# Recargo de equivalencia by IVA %% (standard Spanish table).
RE_BY_IVA = {21: 0.052, 10: 0.014, 5: 0.0062, 4: 0.005, 0: 0.0}

_CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.price_recommendations
(
    computed_at DateTime, run_id String, ean String, cn String, name String, brand String,
    pvl Nullable(Float64), dto_book3 Nullable(Float64), iva Nullable(Float64), re Nullable(Float64),
    puc_no_iva Nullable(Float64), puc_iva_r Nullable(Float64),
    competitor_min Nullable(Float64), n_competitors UInt8, cheapest_site LowCardinality(String),
    pvpr_teoric Nullable(Float64), pvpr_final Nullable(Float64),
    margin_eur Nullable(Float64), margin_pct Nullable(Float64), below_floor UInt8,
    so_units Nullable(Float64), sell_total Nullable(Float64), margin_total Nullable(Float64)
) ENGINE = MergeTree PARTITION BY toYYYYMM(computed_at) ORDER BY (ean, computed_at)
""".strip()


# ─────────────────────────────────────────────────────────────
# Helpers (pure, module-level)
# ─────────────────────────────────────────────────────────────

def _run_dir(ctx) -> str:
    import os
    dag_run = ctx.get("dag_run")
    run_id = str(dag_run.run_id) if dag_run is not None else "default"
    safe = run_id.replace(":", "_").replace("+", "_")
    d = os.path.join(BASE_TMP, safe)
    os.makedirs(d, exist_ok=True)
    return d


def _clean(v):
    s = str(v).strip()
    return "" if s.lower() in ("", "nan", "none") else s


def _num(v):
    import pandas as pd
    n = pd.to_numeric(str(v).replace(",", ".").replace("%", "").strip(), errors="coerce")
    return None if pd.isna(n) else float(n)


def _ean_key(v):
    import pandas as pd
    n = pd.to_numeric(str(v).strip(), errors="coerce")
    return "" if pd.isna(n) else str(int(n))


def _rate(v):
    """Normalise a discount/percentage to a 0..1 rate (33 -> 0.33, 0.33 -> 0.33)."""
    n = _num(v)
    if n is None:
        return 0.0
    return n / 100.0 if n > 1 else n


def _mround05(x):
    """Excel MROUND(x, 0.5): nearest multiple of 0.5, half away from zero."""
    import math
    if x is None:
        return None
    return math.floor(x * 2 + 0.5) / 2.0


def _re_for_iva(iva_pct):
    if iva_pct is None:
        return 0.0
    return RE_BY_IVA.get(int(round(iva_pct)), 0.0)


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="pricing_engine_ETL",
    description="Competitor prices (ClickHouse) + Zoho cost -> recommended prices + margins",
    schedule=Variable.get("pricing_schedule", default_var="0 7 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["pricing", "zoho", "clickhouse"],
)
def pricing_engine_etl():

    # ── 1. Cheapest competitor price per EAN (latest per site) ──
    @task
    def extract_competitor_min() -> str:
        """min of the latest competitor price per EAN across sites; write JSON; return path."""
        import os, json, requests
        from airflow.operators.python import get_current_context

        headers = {"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD}
        sql = (
            "SELECT ean, min(lp) AS competitor_min, count() AS n, argMin(site, lp) AS cheapest_site "
            "FROM (SELECT ean, site, argMax(price, scraped_at) AS lp "
            f"      FROM {CLICKHOUSE_DB}.competitor_prices WHERE price > 0 GROUP BY ean, site) "
            "GROUP BY ean FORMAT JSONEachRow"
        )
        r = requests.get(CLICKHOUSE_URL, params={"query": sql}, headers=headers, timeout=60)
        r.raise_for_status()
        comp = {}
        for line in r.text.splitlines():
            if line.strip():
                rec = json.loads(line)
                comp[str(rec["ean"])] = {
                    "min": float(rec["competitor_min"]),
                    "n": int(rec["n"]),
                    "site": rec.get("cheapest_site", ""),
                }
        print(f"Competitor prices for {len(comp)} EANs")
        out = os.path.join(_run_dir(get_current_context()), "competitor_min.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(comp, f)
        return out

    # ── 2. Product cost/margin from Zoho ──
    @task
    def extract_product_cost() -> str:
        """Zoho Products -> {ean: {cn, name, brand, pvl, iva, dto_book3}}; write JSON; return path."""
        import os, json
        from time import sleep
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsZohoInput import ZohoTokenManager, ZohoCRMConnector

        conn = BaseHook.get_connection(ZOHO_CONN_ID)
        extra = conn.extra_dejson
        token_mgr = ZohoTokenManager(str(conn.login or ""), str(conn.password or ""))
        access_token = token_mgr.get_refresh_token(extra["refresh_token"])

        crm = ZohoCRMConnector(access_token)
        out_map, page, book_hits = {}, 0, 0
        while True:
            try:
                batch = crm.fetch_module_data("Products", params={"page": page, "per_page": 200})
            except Exception:
                break
            if not batch:
                break
            for p in batch:
                ean = _ean_key(p.get(Z_EAN) or p.get(Z_CN))
                if not ean:
                    continue
                book3 = None
                for k in Z_BOOK3_CANDIDATES:
                    if _clean(p.get(k)):
                        book3 = _num(p.get(k))
                        break
                if book3 is not None:
                    book_hits += 1
                out_map[ean] = {
                    "cn": _clean(p.get(Z_CN)),
                    "name": _clean(p.get(Z_NAME)),
                    "brand": _clean(p.get(Z_BRAND)),
                    "pvl": _num(p.get(Z_PVL)),
                    "iva": _num(p.get(Z_IVA)),
                    "dto_book3": book3,
                }
            page += 1
            sleep(0.3)
        print(f"Zoho cost for {len(out_map)} products; {book_hits} had a Book-3 discount "
              f"(fields tried: {Z_BOOK3_CANDIDATES})")
        out = os.path.join(_run_dir(get_current_context()), "product_cost.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(out_map, f)
        return out

    # ── 3. Sell-out units per CN (for €/margin totals) ──
    @task
    def extract_sales_volume() -> str:
        import os, json
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsSQL import SQLConnection

        out = os.path.join(_run_dir(get_current_context()), "sales_volume.json")
        vol = {}
        try:
            ctx = get_current_context()
            ds = ctx["logical_date"]
            end = ds.year * 100 + ds.month
            start = (ds.year - 1) * 100 + ds.month
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
            db = SQLConnection(db_host=conn.host, db_port=conn.port or 1433, db_database=conn.schema,
                               db_username=conn.login, db_password=conn.password,
                               dialect="mssql", driver="pymssql")
            with db:
                df = db.fech_dataframe(sql)
            vol = {_clean(r["CN"]): float(r["SO"] or 0) for _, r in df.iterrows()}
            print(f"SO volume for {len(vol)} products")
        except Exception as e:
            print(f"⚠️ sales volume unavailable ({e!r}) — €/margin totals will be blank")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(vol, f)
        return out

    # ── 4. Compute recommendations + load to ClickHouse ──
    @task
    def compute_and_load(competitor_file: str, cost_file: str, volume_file: str) -> int:
        import json, requests
        from datetime import datetime, timezone
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        run_id = str(dag_run.run_id) if dag_run is not None else "default"
        computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        with open(competitor_file, encoding="utf-8") as f:
            competitor = json.load(f)
        with open(cost_file, encoding="utf-8") as f:
            cost = json.load(f)
        with open(volume_file, encoding="utf-8") as f:
            volume = json.load(f)

        rows, priced, no_cost, flagged = [], 0, 0, 0
        for ean, comp in competitor.items():
            c = cost.get(ean)
            comp_min = comp.get("min")
            if comp_min is None or comp_min <= 0:
                continue

            pvl = (c or {}).get("pvl")
            iva_pct = (c or {}).get("iva")
            dte = _rate((c or {}).get("dto_book3"))
            iva_rate = (iva_pct / 100.0) if iva_pct is not None else None
            re_rate = _re_for_iva(iva_pct)

            if c is None or pvl is None or iva_rate is None:
                no_cost += 1
                puc_no_iva = puc_iva_r = None
                floor = 0.0
            else:
                puc_no_iva = pvl * (1 - dte)
                puc_iva_r = puc_no_iva * (1 + iva_rate + re_rate)
                floor = puc_iva_r * (1 + MIN_MARGIN_PCT)

            candidate = _mround05(comp_min) - 0.05
            if candidate < 0:
                candidate = 0.0
            below = puc_iva_r is not None and candidate < floor
            pvpr_final = max(candidate, floor) if puc_iva_r is not None else candidate
            if below:
                flagged += 1

            margin_eur = (pvpr_final - puc_iva_r) if puc_iva_r is not None else None
            margin_pct = (margin_eur / pvpr_final) if (margin_eur is not None and pvpr_final) else None

            cn = (c or {}).get("cn", "")
            so = volume.get(cn)
            rows.append({
                "computed_at": computed_at, "run_id": run_id, "ean": ean, "cn": cn,
                "name": (c or {}).get("name", ""), "brand": (c or {}).get("brand", ""),
                "pvl": pvl, "dto_book3": _rate((c or {}).get("dto_book3")) if c else None,
                "iva": iva_rate, "re": re_rate,
                "puc_no_iva": puc_no_iva, "puc_iva_r": puc_iva_r,
                "competitor_min": comp_min, "n_competitors": comp.get("n", 0),
                "cheapest_site": comp.get("site", ""),
                "pvpr_teoric": comp_min, "pvpr_final": pvpr_final,
                "margin_eur": margin_eur, "margin_pct": margin_pct, "below_floor": 1 if below else 0,
                "so_units": so,
                "sell_total": (so * pvpr_final) if so is not None else None,
                "margin_total": (so * margin_eur) if (so is not None and margin_eur is not None) else None,
            })
            priced += 1

        print(f"Priced {priced} products ({no_cost} missing cost -> floor skipped, "
              f"{flagged} below margin floor / flagged)")
        if not rows:
            print("Nothing to load.")
            return 0

        headers = {"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD}
        requests.post(CLICKHOUSE_URL, params={"query": _CLICKHOUSE_DDL.format(db=CLICKHOUSE_DB)},
                      headers=headers, timeout=30).raise_for_status()
        body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        resp = requests.post(
            CLICKHOUSE_URL,
            params={"query": f"INSERT INTO {CLICKHOUSE_DB}.price_recommendations FORMAT JSONEachRow",
                    "input_format_skip_unknown_fields": 1},
            data=body, headers=headers, timeout=120,
        )
        resp.raise_for_status()
        print(f"Loaded {len(rows)} recommendations into {CLICKHOUSE_DB}.price_recommendations")
        return len(rows)

    competitor = extract_competitor_min()
    cost = extract_product_cost()
    volume = extract_sales_volume()
    compute_and_load(competitor, cost, volume)


pricing_engine_etl()
