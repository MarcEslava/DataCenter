"""
Inventory ETL

Snapshots stock + trailing-12-month sales per pharmacy × product from BIFarma,
values the stock at BIFarma's average purchase cost, and loads it to ClickHouse
so the Pricing Cockpit can show inventory value, category composition, and the
inventory-vs-sales composition (over/under-stock signal).

Source (BIFarma_db, MSSQL) — same tables calculo_fee_ETL / pricing_engine_ETL use:
  - dbo.tbi_productosERS       product master per office: stockActual, codproducto, idsuperfamilia, codlab
  - dbo.bench_dwComprasVentasMesS  monthly compras/ventas fact (anyomes, cantidad, importe, *compra)
  - dbo.tme_delegaciones       office/pharmacy (idendeS, delegacion, grupoCompras='ECO')
  - dbo.vteco_familias         family names -> nombresuperfamiliaeco (= SUPER FAMILIA / category)

Grain: per pharmacy (idendeS) × product (codproducto).  Aggregation to network/category
is done in the dashboard queries, so the drill-down by pharmacy stays available.

Valuation (decided with the user): stock valued at average PURCHASE cost from BIFarma,
  coste_ud   = SUM(importecompra) / SUM(cantidadcompra)   over the 12m window
  valor_inv  = stockActual * coste_ud
Products with stock but no purchases in the window get a null cost (flagged, not valued) —
this is the "dead stock" tail; refine later with a cost column off pr if one exists.

Output: price_monitor.inventory in ClickHouse.

Variables:
  inventory_schedule          ("0 6 3 * *")   # monthly, after the month closes
  inventory_sales_months      ("12")          # trailing window for sales/cost
  clickhouse_* / BIFarma_db    (shared with the other pricing DAGs)
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SQL_BIFARMA_CONN_ID = "BIFarma_db"

CLICKHOUSE_URL = Variable.get("clickhouse_url", default_var="http://clickhouse:8123")
CLICKHOUSE_DB = Variable.get("clickhouse_db", default_var="price_monitor")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="price_monitor")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="changeme_pm")

SALES_MONTHS = int(Variable.get("inventory_sales_months", default_var="12"))

BASE_TMP = "/opt/airflow/data/staging/inventory"

_CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.inventory
(
    snapshot_at DateTime, run_id String,
    idende Int32, id_delegacion String, delegacion String,
    cn String, ean String, producto String, id_lab String, laboratorio String,
    id_superfamilia String, cat String,
    stock_uds Float64, coste_ud Nullable(Float64), valor_inv Nullable(Float64),
    ventas_uds_12m Float64, ventas_eur_12m Float64,
    compra_uds_12m Float64, compra_eur_12m Float64
) ENGINE = MergeTree PARTITION BY toYYYYMM(snapshot_at) ORDER BY (cat, cn, idende)
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
    n = pd.to_numeric(v, errors="coerce")
    return None if pd.isna(n) else float(n)


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="inventory_ETL",
    description="BIFarma stock + 12m sales per pharmacy -> valued inventory in ClickHouse",
    schedule=Variable.get("inventory_schedule", default_var="0 6 3 * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["inventory", "bifarma", "clickhouse"],
)
def inventory_etl():

    # ── 1. Extract stock + 12m sales/cost per pharmacy × product ──
    @task
    def extract_inventory() -> str:
        import os, json
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context
        from utils.clsSQL import SQLConnection

        ctx = get_current_context()
        ds = ctx["logical_date"]
        end = ds.year * 100 + ds.month                       # trailing window ends at logical month
        # start is exclusive: (start, end]  -> SALES_MONTHS full months
        sm = SALES_MONTHS
        start_year = ds.year - (sm // 12) - (1 if ds.month <= (sm % 12) else 0)
        start_month = (ds.month - (sm % 12) - 1) % 12 + 1
        start = start_year * 100 + start_month

        # Driven from the product master (LEFT JOIN the fact) so stock with no recent
        # sales -- dead stock -- is still captured. The window filter lives in the ON
        # clause to preserve unmatched product rows.
        sql = f"""
            SELECT
                de.idendeS                        AS idende,
                de.iddelegacion                   AS id_delegacion,
                de.delegacion                     AS delegacion,
                pr.codproducto                    AS cn,
                pr.desproducto                    AS producto,
                pr.codlab                         AS id_lab,
                pr.deslab                         AS laboratorio,
                pr.idsuperfamilia                 AS id_superfamilia,
                f.nombresuperfamiliaeco           AS cat,
                MIN(pr.stockActual)               AS stock_uds,
                SUM(ISNULL(T1.cantidad, 0))       AS ventas_uds,
                SUM(ISNULL(T1.importe, 0))        AS ventas_eur,
                SUM(ISNULL(T1.cantidadcompra, 0)) AS compra_uds,
                SUM(ISNULL(T1.importecompra, 0))  AS compra_eur
            FROM dbo.tbi_productosERS pr
            INNER JOIN dbo.tme_delegaciones de ON pr.idendeS = de.idendeS
            INNER JOIN dbo.vteco_familias  f   ON f.idfamiliaeco = pr.idfamilia
            LEFT  JOIN dbo.bench_dwComprasVentasMesS T1
                   ON T1.idendeS = pr.idendeS AND T1.idproducto = pr.idproducto
                  AND T1.anyomes > {start} AND T1.anyomes <= {end}
            WHERE de.grupoCompras = 'ECO'
            GROUP BY
                de.idendeS, de.iddelegacion, de.delegacion,
                pr.codproducto, pr.desproducto, pr.codlab, pr.deslab,
                pr.idsuperfamilia, f.nombresuperfamiliaeco
            HAVING MIN(pr.stockActual) > 0 OR SUM(ISNULL(T1.cantidad, 0)) > 0
        """
        conn = BaseHook.get_connection(SQL_BIFARMA_CONN_ID)
        db = SQLConnection(db_host=conn.host, db_port=conn.port or 1433, db_database=conn.schema,
                           db_username=conn.login, db_password=conn.password,
                           dialect="mssql", driver="pymssql")
        with db:
            df = db.fech_dataframe(sql)
        print(f"Extracted {len(df)} pharmacy×product rows (window {start}<anyomes<={end})")

        out = os.path.join(_run_dir(ctx), "inventory_raw.jsonl")
        df.to_json(out, orient="records", lines=True)      # JSONL -> streamable in chunks below
        return out

    # ── 2. Value the stock and load to ClickHouse (vectorized + chunked) ──
    @task
    def transform_and_load(raw_file: str) -> int:
        import requests
        import numpy as np, pandas as pd
        from datetime import datetime, timezone
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        run_id = str(dag_run.run_id) if dag_run is not None else "default"
        snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        def transform(df):
            num = lambda c: pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            s = lambda c: df[c].astype(str).str.strip()        # BIFarma CHARs are space-padded -> strip
            stock = num("stock_uds"); v_uds = num("ventas_uds"); v_eur = num("ventas_eur")
            c_uds = num("compra_uds"); c_eur = num("compra_eur")
            keep = (stock > 0) | (v_uds > 0)                   # HAVING already did this; belt-and-braces
            coste_ud = (c_eur / c_uds).where(c_uds > 0)        # NaN where no purchases -> null in JSON
            valor_inv = (stock * coste_ud).where(stock > 0)
            return pd.DataFrame({
                "snapshot_at": snapshot_at, "run_id": run_id,
                "idende": pd.to_numeric(df["idende"], errors="coerce").fillna(0).astype(int),
                "id_delegacion": s("id_delegacion"), "delegacion": s("delegacion"),
                "cn": s("cn"), "ean": "",                       # EAN mapping added later (tbi_sinonimos/Zoho)
                "producto": s("producto"),
                "id_lab": s("id_lab"), "laboratorio": s("laboratorio"),
                "id_superfamilia": s("id_superfamilia"), "cat": s("cat"),
                "stock_uds": stock, "coste_ud": coste_ud, "valor_inv": valor_inv,
                "ventas_uds_12m": v_uds, "ventas_eur_12m": v_eur,
                "compra_uds_12m": c_uds, "compra_eur_12m": c_eur,
            })[keep].reset_index(drop=True)

        headers = {"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD}
        requests.post(CLICKHOUSE_URL, params={"query": _CLICKHOUSE_DDL.format(db=CLICKHOUSE_DB)},
                      headers=headers, timeout=30).raise_for_status()
        # Point-in-time snapshot, not a history: replace the table each run (no accumulation).
        requests.post(CLICKHOUSE_URL, params={"query": f"TRUNCATE TABLE {CLICKHOUSE_DB}.inventory"},
                      headers=headers, timeout=30).raise_for_status()

        # Stream the raw JSONL in chunks: transform + insert each chunk, never holding it all.
        loaded = valued = dead = 0
        total = 0.0
        CHUNK = 100_000
        for out in pd.read_json(raw_file, lines=True, chunksize=CHUNK):
            out = transform(out)
            if out.empty:
                continue
            valued += int(out["valor_inv"].notna().sum())
            dead += int(((out["valor_inv"].isna()) & (out["stock_uds"] > 0)).sum())
            total += float(out["valor_inv"].sum())
            body = out.to_json(orient="records", lines=True).encode("utf-8")
            resp = requests.post(
                CLICKHOUSE_URL,
                params={"query": f"INSERT INTO {CLICKHOUSE_DB}.inventory FORMAT JSONEachRow",
                        "input_format_skip_unknown_fields": 1},
                data=body, headers=headers, timeout=180,
            )
            resp.raise_for_status()
            loaded += len(out)
            print(f"  loaded {loaded:,} rows so far")

        if not loaded:
            print("Nothing to load.")
            return 0
        print(f"Loaded {loaded:,} inventory rows into {CLICKHOUSE_DB}.inventory "
              f"({valued:,} valued, {dead:,} stock w/o cost) · valor total ≈ {total:,.0f} €")
        return loaded

    transform_and_load(extract_inventory())


inventory_etl()
