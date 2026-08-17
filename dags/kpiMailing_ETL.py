r"""
KPI Mailing ETL

Airflow port of the eco-mailing Laravel app (`kpi:send-all`). Once a month it
fans out over every eligible pharmacy, builds its bilingual (ES/CA) KPI report
PDF from the Bifarma SQL Server DW, and emails it as an attachment via
ZeptoMail. Pharmacies missing the month's data get a "datos incompletos" email
instead (no PDF).

Structure (mirrors the PHP flow):
  resolve_period       — from/to/fromAcum (defaults to previous month, YYMM ints)
  extract_pharmacies   — eligible pharmacies, one payload each (emails grouped)
  send_report.expand() — per pharmacy: query → build PDF once → email all its addresses

Reusable pieces:
  utils.clsPdf.PdfBuilder   — WeasyPrint + Jinja2 HTML→PDF (generic)
  utils.kpi_mailing         — verbatim SQL + data shaping + translators
  utils.clsZohoMailing      — ZeptoMail send with base64 attachments
  templates/kpi/*.j2        — report + email bodies (ports of the Blade views)
  assets/kpi/               — logo + Montserrat fonts

SAFETY: `dry_run` defaults to True — it builds everything and logs, but does not
send. Flip it to False (or set `test_recipient` to redirect all mail to one
address) when you are ready to send for real.

PHARMACY SELECTION: like the original PHP, this INCLUDES only the
"transformadas" pharmacies (`whereIn('idendeS', $codesTransformadas)`). The
misleading comment in the PHP notwithstanding, this include-only behaviour is
intended and confirmed correct (see SELECT_ONLY_TRANSFORMADAS).
"""
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

KPI_CONN_ID       = "bifarma_origen"          # mssql — default catalog MUST be `bifarma`
KPI_AGREG_CONN_ID = "bifarma_agreg_db"    # mssql — the "transformadas" flag DB

# Include only the "transformadas" pharmacies — confirmed intended behaviour.
SELECT_ONLY_TRANSFORMADAS = True

HERE         = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(HERE, "templates", "kpi")
ASSET_DIR    = os.path.join(HERE, "assets", "kpi")
FONT_DIR     = os.path.join(ASSET_DIR, "fonts")
LOGO_PATH    = os.path.join(ASSET_DIR, "logo.png")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────
# Connection helpers
# ─────────────────────────────────────────────────────────────

def _mssql(conn_id: str):
    """Build an mssql SQLConnection (pymssql) from an Airflow connection."""
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    c = BaseHook.get_connection(conn_id)
    return SQLConnection(
        db_host=c.host, db_port=c.port or 1433,
        db_database=c.schema, db_username=c.login,
        db_password=c.password, dialect="mssql", driver="pymssql",
    )


def _locale_for(ccaa) -> str:
    return "ca" if str(ccaa or "").upper() == "CATA" else "es"


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="kpiMailing_ETL",
    description="Monthly per-pharmacy KPI report PDF emailed via ZeptoMail (port of eco-mailing)",
    schedule=Variable.get("kpi_mailing_schedule", default_var="55 7 5 * *"),  # 07:55 on the 5th
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_tasks=8,
    tags=["kpi", "mailing", "bifarma", "reports"],
    params={
        "from": None,          # YYMM int; default = previous month
        "to": None,            # YYMM int; default = previous month
        "dry_run": True,       # build + log only, do NOT send
        "test_recipient": "",  # if set, redirect ALL mail here
    },
)
def kpi_mailing_etl():

    @task
    def resolve_period(**context) -> dict:
        """Compute (from, to, fromAcum) as YYMM ints. Port of the command's default logic."""
        p = context["params"]
        frm, to = p.get("from"), p.get("to")

        if not frm or not to:
            # Carbon::now()->subMonth() → previous month relative to the run date.
            logical = context.get("logical_date") or datetime.now()
            y, m = logical.year, logical.month
            m -= 1
            if m == 0:
                m, y = 12, y - 1
            anyomes = (y % 100) * 100 + m
            frm = frm or anyomes
            to = to or anyomes

        frm, to = int(frm), int(to)
        from_acum = (to // 100) * 100 + 1

        if frm > to or frm < 100:
            raise ValueError(f"Invalid period range: from={frm} to={to}")

        print(f"[kpi] period from={frm} to={to} fromAcum={from_acum}")
        return {"from": frm, "to": to, "from_acum": from_acum}

    @task
    def extract_pharmacies(period: dict, **context) -> list[dict]:
        """Eligible pharmacies — one payload per pharmacy (all its emails grouped)."""
        import pandas as pd

        agreg = _mssql(KPI_AGREG_CONN_ID)
        with agreg as adb:
            transformadas = adb.fech_dataframe(
                "SELECT identidad, iddelegacion FROM tme_delegacionesRAMTransformadas "
                "WHERE swTransformada = 'SI'"
            )
        ident_list = [str(x) for x in transformadas["identidad"].dropna().unique().tolist()]

        db = _mssql(KPI_CONN_ID)
        with db as bdb:
            if ident_list:
                in_clause = ", ".join("'" + i.replace("'", "''") + "'" for i in ident_list)
                trans_ids = bdb.fech_dataframe(
                    f"SELECT idendeS FROM bifarma.dbo.tme_delegaciones WHERE identidad IN ({in_clause})"
                )["idendeS"].dropna().astype(int).unique().tolist()
            else:
                trans_ids = []

            where = ""
            if SELECT_ONLY_TRANSFORMADAS:
                if not trans_ids:
                    print("[kpi] no transformadas pharmacies found — nothing to send")
                    return []
                ids_clause = ", ".join(str(int(i)) for i in trans_ids)
                where = f"AND idendeS IN ({ids_clause})"

            farmacias = bdb.fech_dataframe(f"""
                SELECT DISTINCT idendeS, email, CCAA, fUltDatos
                FROM bifarma.dbo.tme_delegaciones
                WHERE email IS NOT NULL
                  AND email <> ''
                  AND activo = 1
                  AND grupoFarmacias <> 'FUNID'
                  {where}
            """)

        # cutoff = start of the current (run) month
        logical = context.get("logical_date") or datetime.now()
        cutoff = datetime(logical.year, logical.month, 1)

        payloads: list[dict] = []
        for _, row in farmacias.iterrows():
            f_ult = row.get("fUltDatos")
            f_ult_dt = pd.to_datetime(f_ult, errors="coerce") if f_ult is not None else None
            missing = (f_ult_dt is None) or pd.isna(f_ult_dt) or (f_ult_dt < cutoff)

            emails = []
            for e in str(row["email"]).split(";"):
                e = e.strip()
                if e and e not in emails:
                    emails.append(e)
            if not emails:
                continue

            # One payload per pharmacy — the PDF is built once and sent to all emails.
            payloads.append({
                "idendeS": int(row["idendeS"]),
                "emails": emails,
                "ccaa": row.get("CCAA"),
                "missingData": bool(missing),
                **period,
            })

        total_emails = sum(len(p["emails"]) for p in payloads)
        print(f"[kpi] {len(payloads)} pharmacies / {total_emails} emails "
              f"({sum(1 for p in payloads if p['missingData'])} missing-data pharmacies)")
        return payloads

    @task(max_active_tis_per_dag=8)
    def send_report(payload: dict, **context) -> str:
        """Per pharmacy: build the report (or no-data) email ONCE, send to all its emails."""
        from pathlib import Path
        from utils.clsPdf import PdfBuilder, data_uri
        from utils.clsZohoMailing import ZohoMailer
        from utils import kpi_mailing as km

        params = context["params"]
        dry_run = bool(params.get("dry_run", True))
        test_recipient = (params.get("test_recipient") or "").strip()

        store_id = payload["idendeS"]
        frm, to, from_acum = payload["from"], payload["to"], payload["from_acum"]
        locale = _locale_for(payload["ccaa"])
        is_catalan = locale == "ca"
        # test_recipient short-circuits to a single address for the whole pharmacy.
        recipients = [test_recipient] if test_recipient else payload["emails"]

        builder = PdfBuilder(template_dir=TEMPLATE_DIR, base_url=TEMPLATE_DIR)
        logo = data_uri(LOGO_PATH)
        font_dir = Path(FONT_DIR).as_uri()

        mes_ano = km.format_month_range(to, to, locale)

        # ---- build the message body + attachment once for this pharmacy ----
        db = _mssql(KPI_CONN_ID)
        with db as bdb:
            nombre_farmacia = km.get_nombre_farmacia(bdb, store_id)

            if payload["missingData"]:
                subject = "Datos incompletos – Bifarmaeco"
                html = builder.render_html("email_nodata.html.j2", {
                    "locale": locale, "nombreFarmacia": nombre_farmacia,
                    "mesRango": mes_ano, "logo": logo,
                })
                attachments = None
            else:
                subject = ("Informe de Situació Farmàcia" if is_catalan
                           else "Informe Situación Farmacia") + f" — {mes_ano}"
                ctx = km.build_report_context(bdb, store_id, frm, to, from_acum,
                                              locale, nombre_farmacia)
                ctx["logo"] = logo
                ctx["font_dir"] = font_dir
                pdf_bytes = builder.render("report.html.j2", ctx)

                filename = (f"INFORME COMPLET – {mes_ano}.pdf" if is_catalan
                            else f"INFORME COMPLETO – {mes_ano}.pdf")
                html = builder.render_html("email_report.html.j2", {
                    "mesAno": mes_ano,
                    "mesRango": km.format_month_range(from_acum, to, locale),
                    "nombreFarmacia": nombre_farmacia, "locale": locale, "logo": logo,
                })
                attachments = [{"content": pdf_bytes, "name": filename,
                                "mime_type": "application/pdf"}]

        # ---- deliver the single built message to every recipient ----
        tag = "MISSING-DATA" if payload["missingData"] else "OK"
        if dry_run:
            size = 0 if not attachments else len(attachments[0]["content"])
            print(f"[kpi][DRY-RUN] {tag} farmacia {store_id} → {recipients} "
                  f"| subject={subject!r} | pdf_bytes={size}")
            return f"dry-run:{store_id}:{len(recipients)}"

        mailer = ZohoMailer()
        for recipient in recipients:
            mailer.send(
                to=[{"address": recipient, "name": nombre_farmacia}],
                subject=subject, html_body=html, attachments=attachments,
            )
            print(f"[kpi][SENT] {tag} farmacia {store_id} → {recipient}")
        return f"sent:{store_id}:{len(recipients)}"

    period = resolve_period()
    send_report.expand(payload=extract_pharmacies(period))


kpi_mailing_etl()
