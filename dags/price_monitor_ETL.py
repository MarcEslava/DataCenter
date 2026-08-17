"""
Price Monitor ETL

Triggers the competitor-price spiders that run in the standalone `scrapyd`
container, waits for each crawl to finish, pulls the scraped items into a
per-run staging directory, then detects significant price moves versus the
previous run and emails an alert.

Architecture
------------
The Scrapy project runs as a long-lived Scrapyd daemon (service `scrapyd`,
port 6800) that Airflow talks to over HTTP:

    schedule.json   -> start a spider, returns a jobid
    listjobs.json   -> poll until the job moves to "finished"
    /items/...jl    -> download that job's scraped items (Scrapyd items_dir)

Change detection (snapshot diff)
--------------------------------
There is intentionally NO database yet — the analytical store (Snowflake, or a
dedicated container) is a separate piece of work. For alerting we don't need
one: we compare THIS run's pulled items against the PREVIOUS run's staging
snapshot (by site + url) and alert on moves >= a threshold. Full price history
lands later, when the analytical `load` step is added.

Tunable via Airflow Variables:
    scrapyd_base_url                    (default http://scrapyd:6800)
    price_monitor_schedule              (default "0 6 * * *")
    price_monitor_alert_threshold_pct   (default "5")
    price_monitor_alert_recipients      (default "meslava@ecoceutics.com", comma-separated)
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SCRAPYD_URL = Variable.get("scrapyd_base_url", default_var="http://scrapyd:6800")
PROJECT = "price_monitor"
SPIDERS = ["competitor_primor", "competitor_farmavazquez", "competitor_atida"]

BASE_TMP = "/opt/airflow/data/staging/price_monitor"
POLL_INTERVAL_S = 15
POLL_TIMEOUT_S = 60 * 30  # 30 min safety cap per spider

ALERT_THRESHOLD_PCT = float(Variable.get("price_monitor_alert_threshold_pct", default_var="5"))
ALERT_RECIPIENTS = Variable.get("price_monitor_alert_recipients", default_var="meslava@ecoceutics.com")

# Analytical store (ClickHouse). Interim home for price history until Snowflake.
CLICKHOUSE_URL = Variable.get("clickhouse_url", default_var="http://clickhouse:8123")
CLICKHOUSE_DB = Variable.get("clickhouse_db", default_var="price_monitor")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="price_monitor")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="changeme_pm")

# Kept in sync with clickhouse/init/01_schema.sql; run on load so the target is
# self-healing even if the container's init step was skipped.
_CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.price_history
(
    scraped_at DateTime, run_id String, site LowCardinality(String),
    sku String, url String, name String, price Nullable(Float64),
    currency LowCardinality(String), in_stock UInt8
) ENGINE = MergeTree PARTITION BY toYYYYMM(scraped_at) ORDER BY (site, url, scraped_at)
""".strip()


# ─────────────────────────────────────────────────────────────
# Helpers (module-level so they can be unit-tested without Airflow)
# ─────────────────────────────────────────────────────────────

def _run_dir(ctx) -> str:
    import os
    dag_run = ctx.get("dag_run")
    run_id = str(dag_run.run_id) if dag_run is not None else "default"
    safe = run_id.replace(":", "_").replace("+", "_")
    d = os.path.join(BASE_TMP, safe)
    os.makedirs(d, exist_ok=True)
    return d


def _load_run_dir(path: str) -> dict:
    """Read every *.jsonl in a run dir -> {(site, url): record}. Skips rows
    without a url or price so detection only compares real observations."""
    import os
    import glob
    import json
    out = {}
    for fp in glob.glob(os.path.join(path, "*.jsonl")):
        with open(fp, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                url, price = rec.get("url"), rec.get("price")
                if not url or price is None:
                    continue
                out[(rec.get("site"), url)] = rec
    return out


def _previous_run_dir(base: str, current_dir: str):
    """Most recently modified run dir under `base` that isn't the current one."""
    import os
    if not os.path.isdir(base):
        return None
    cur = os.path.abspath(current_dir)
    dirs = [
        os.path.join(base, n) for n in os.listdir(base)
        if os.path.isdir(os.path.join(base, n)) and os.path.abspath(os.path.join(base, n)) != cur
    ]
    return max(dirs, key=os.path.getmtime) if dirs else None


def _compute_alerts(current: dict, previous: dict, threshold_pct: float) -> list:
    """Products present in both snapshots whose price moved >= threshold_pct."""
    alerts = []
    for key, cur in current.items():
        prev = previous.get(key)
        if not prev:
            continue
        old, new = prev.get("price"), cur.get("price")
        if old in (None, 0) or new is None or new == old:
            continue
        change_pct = (new - old) / old * 100.0
        if abs(change_pct) >= threshold_pct:
            site, url = key
            alerts.append({
                "site": site,
                "url": url,
                "name": cur.get("name") or prev.get("name"),
                "old_price": old,
                "new_price": new,
                "change_pct": round(change_pct, 2),
            })
    alerts.sort(key=lambda a: abs(a["change_pct"]), reverse=True)
    return alerts


def _alerts_html(alerts: list, threshold_pct: float, prev_label: str) -> str:
    rows = ""
    for a in alerts:
        up = a["change_pct"] > 0
        arrow = "▲" if up else "▼"
        color = "#c0392b" if up else "#27ae60"  # red up (competitor raised), green down
        rows += (
            f"<tr>"
            f"<td>{a['site']}</td>"
            f"<td><a href='{a['url']}'>{a['name']}</a></td>"
            f"<td style='text-align:right'>{a['old_price']:.2f} €</td>"
            f"<td style='text-align:right'>{a['new_price']:.2f} €</td>"
            f"<td style='text-align:right;color:{color}'>{arrow} {a['change_pct']:+.2f}%</td>"
            f"</tr>"
        )
    return (
        f"<p>Se han detectado <b>{len(alerts)}</b> cambio(s) de precio "
        f"&ge; {threshold_pct:g}% respecto a la ejecución anterior "
        f"(<code>{prev_label}</code>):</p>"
        f"<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>"
        f"<tr><th>Competidor</th><th>Producto</th><th>Precio ant.</th>"
        f"<th>Precio nuevo</th><th>Cambio</th></tr>"
        f"{rows}</table>"
    )


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="price_monitor_ETL",
    description="Crawl competitor prices via Scrapyd, detect significant moves, alert by email",
    schedule=Variable.get("price_monitor_schedule", default_var="0 6 * * *"),  # daily 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=3,
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["scraping", "prices"],
)
def price_monitor_etl():

    # ── EXTRACT: one mapped task per spider (trigger -> poll -> pull) ──
    @task
    def crawl_and_pull(spider: str) -> dict:
        """Start one spider on Scrapyd, wait for it to finish, download its items."""
        import os
        import time
        import requests
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        run_dir = _run_dir(ctx)

        r = requests.post(
            f"{SCRAPYD_URL}/schedule.json",
            data={"project": PROJECT, "spider": spider},
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != "ok":
            raise RuntimeError(f"[{spider}] schedule.json failed: {payload}")
        jobid = payload["jobid"]
        print(f"[{spider}] scheduled jobid={jobid}")

        deadline = time.monotonic() + POLL_TIMEOUT_S
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(f"[{spider}] job {jobid} not finished after {POLL_TIMEOUT_S}s")
            lj = requests.get(
                f"{SCRAPYD_URL}/listjobs.json", params={"project": PROJECT}, timeout=30
            ).json()
            if jobid in {j["id"] for j in lj.get("finished", [])}:
                break
            time.sleep(POLL_INTERVAL_S)
        print(f"[{spider}] job {jobid} finished")

        items_url = f"{SCRAPYD_URL}/items/{PROJECT}/{spider}/{jobid}.jl"
        resp = requests.get(items_url, timeout=60)
        out_file = os.path.join(run_dir, f"{spider}.jsonl")
        if resp.status_code == 200 and resp.content:
            with open(out_file, "wb") as f:
                f.write(resp.content)
            count = resp.content.count(b"\n")
            print(f"[{spider}] pulled {count} items -> {out_file}")
        else:
            print(f"[{spider}] no items at {items_url} (HTTP {resp.status_code})")
            out_file = ""
            count = 0
        return {"spider": spider, "jobid": jobid, "items_file": out_file, "count": count}

    # ── LOAD: append this run's observations to ClickHouse (analytics) ──
    @task
    def load_to_clickhouse(results: list[dict]) -> int:
        """Insert the current run's products into ClickHouse price_history."""
        import json
        import requests
        from datetime import datetime, timezone
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        run_dir = _run_dir(ctx)
        dag_run = ctx.get("dag_run")
        run_id = str(dag_run.run_id) if dag_run is not None else "default"

        records = _load_run_dir(run_dir)
        if not records:
            print("Nothing to load into ClickHouse this run.")
            return 0

        headers = {"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD}
        # self-healing schema
        requests.post(
            CLICKHOUSE_URL,
            params={"query": _CLICKHOUSE_DDL.format(db=CLICKHOUSE_DB)},
            headers=headers,
            timeout=30,
        ).raise_for_status()

        scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows = [
            {
                "scraped_at": scraped_at,
                "run_id": run_id,
                "site": site or "",
                "sku": str(rec.get("sku") or ""),
                "url": url,
                "name": rec.get("name") or "",
                "price": rec.get("price"),
                "currency": rec.get("currency") or "",
                "in_stock": 1 if rec.get("in_stock") else 0,
            }
            for (site, url), rec in records.items()
        ]
        body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        resp = requests.post(
            CLICKHOUSE_URL,
            params={
                "query": f"INSERT INTO {CLICKHOUSE_DB}.price_history FORMAT JSONEachRow",
                "input_format_skip_unknown_fields": 1,
            },
            data=body,
            headers=headers,
            timeout=120,
        )
        resp.raise_for_status()
        print(f"Loaded {len(rows)} rows into {CLICKHOUSE_DB}.price_history (run {run_id})")
        return len(rows)

    # ── DETECT: diff this run vs the previous run's snapshot ──
    @task
    def detect_changes(results: list[dict]) -> dict:
        """Compare current pulled items to the previous run; return alert list + context."""
        import os
        from airflow.operators.python import get_current_context

        for r in results:
            if r:
                print(f"{r['spider']}: {r['count']} items")

        ctx = get_current_context()
        run_dir = _run_dir(ctx)
        current = _load_run_dir(run_dir)

        prev_dir = _previous_run_dir(BASE_TMP, run_dir)
        if not prev_dir:
            print("No previous run to compare against — baseline stored, no alerts this run.")
            return {"alerts": [], "prev_label": None}

        previous = _load_run_dir(prev_dir)
        prev_label = os.path.basename(prev_dir)
        print(f"Comparing {len(current)} current vs {len(previous)} previous products (prev={prev_label})")

        alerts = _compute_alerts(current, previous, ALERT_THRESHOLD_PCT)
        print(f"{len(alerts)} product(s) moved >= {ALERT_THRESHOLD_PCT}%")
        for a in alerts[:20]:
            print(f"  [{a['site']}] {a['name']}: {a['old_price']} -> {a['new_price']} ({a['change_pct']:+.2f}%)")
        return {"alerts": alerts, "prev_label": prev_label}

    # ── NOTIFY: email only if there are significant moves ──
    @task
    def notify(detection: dict) -> None:
        alerts = detection.get("alerts") or []
        if not alerts:
            print("No significant price changes — no email sent.")
            return

        from utils.clsZohoMailing import ZohoMailer

        recipients = [a.strip() for a in ALERT_RECIPIENTS.split(",") if a.strip()]
        html = _alerts_html(alerts, ALERT_THRESHOLD_PCT, detection.get("prev_label") or "—")
        subject = f"[Price Monitor] {len(alerts)} cambio(s) de precio ≥ {ALERT_THRESHOLD_PCT:g}%"
        ZohoMailer().send(
            to=[{"address": r, "name": ""} for r in recipients],
            subject=subject,
            html_body=html,
        )
        print(f"Alert email sent to {recipients}: {len(alerts)} change(s)")

    crawled = crawl_and_pull.expand(spider=SPIDERS)
    load_to_clickhouse(crawled)           # analytics store (independent branch)
    detection = detect_changes(crawled)   # alerting (snapshot diff)
    notify(detection)

price_monitor_etl()
