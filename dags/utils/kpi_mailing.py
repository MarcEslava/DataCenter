"""
KPI mailing — query + data-shaping logic.

Faithful 1:1 port of the eco-mailing Laravel app
(KpiQueryService / KpiPdfBuilderService / Translators). Given a
``utils.clsSQL.SQLConnection`` to the Bifarma SQL Server, it reproduces the
exact queries and the exact per-row shaping the PHP produced, then assembles
the context dict consumed by ``templates/kpi/report.html.j2``.

The SQL is kept verbatim from the PHP. Unqualified ``dbo.`` references resolve
against the connection's default catalog, which MUST be ``bifarma`` (as
DB_DATABASE was in the Laravel config). ``bifarma.dbo.*`` and
``BifarmaCentral.dbo.*`` are cross-catalog references on the same instance.

NOTE ON FIDELITY: some of the original label matching is fragile / order- and
data-dependent (e.g. rows are re-translated in place and the retail block break
tests a value that may already be translated). This port deliberately preserves
that behaviour — the shared mutable rows are mutated in the same order as the
PHP so, given the same database, the output is byte-for-byte equivalent.
"""

from __future__ import annotations

from typing import Optional

# ─────────────────────────────────────────────────────────────
# Translators (ports of App\Services\Translators\*)
# ─────────────────────────────────────────────────────────────


class _Translator:
    map_es: dict[str, str] = {}
    map_ca: dict[str, str] = {}
    non_monetary_base: list[str] = []

    def __init__(self, locale: str = "es"):
        self.locale = locale

    def translate(self, key: str) -> str:
        key = (key or "").strip()
        m = self.map_ca if self.locale == "ca" else self.map_es
        return m.get(key, key)

    def is_monetary(self, original_indicator: str) -> bool:
        return original_indicator not in self.non_monetary_base


class IndicatorTranslator(_Translator):
    map_es = {
        "SO Paraf./metro lineal": "SO Paraf./metro lineal",
        "Marge € total": "Margen € total",
        "Margen € libre": "Margen € libre",
        "Número tiquets": "Número tiquets",
        "Ticket Medio": "Ticket Medio",
        "Unitats": "Unidades",
        "Cistella mitjana": "Cesta media",
    }
    map_ca = {
        "SO Paraf./metro lineal": "SO Paraf./metre lineal",
        "Marge € total": "Marge € total",
        "Margen € libre": "Marge € lliure",
        "Número tiquets": "Número tiquets",
        "Ticket Medio": "Tiquet mig",
        "Unitats": "Unitats",
        "Cistella mitjana": "Cistella mitjana",
    }
    non_monetary_base = ["Número tiquets", "Unidades", "Cesta media"]


class SalesAndPurchasesTranslator(_Translator):
    map_es = {
        "SO Receta (1)": "SO Receta (1)",
        "SO Libre (2)": "SO Libre (2)",
        "SO Total (1+2)": "SO Total (1+2)",
        "SO Especialidades": "SO Especialidades",
        "SO Espec. Caras": "SO Espec. Caras",
        "SO Paraf. Consell (A)": "SO Paraf. Consejo (A)",
        "SO OTC (B)": "SO OTC (B)",
        "SO Paraf. Autocuidado (C)": "SO Paraf. Autocuidado (C)",
        "SO Paraf. Total (A+B+C)": "SO Paraf. Total (A+B+C)",
        "SO EFGs-ECO": "SO EFGs-ECO",
        "SO EFGs-NO ECO": "SO EFGs-NO ECO",
        "SO EFGs-Total": "SO EFGs-Total",
        "SO MEX": "SO MEX",
    }
    map_ca = {
        "SO Receta (1)": "SO Recepta (1)",
        "SO Libre (2)": "SO Lliure (2)",
        "SO Total (1+2)": "SO Total (1+2)",
        "SO Especialidades": "SO Especialitats",
        "SO Espec. Caras": "SO Espec. Cares",
        "SO Paraf. Consell (A)": "SO Paraf. Consell (A)",
        "SO OTC (B)": "SO OTC (B)",
        "SO Paraf. Autocuidado (C)": "SO Paraf. Autocura (C)",
        "SO Paraf. Total (A+B+C)": "SO Paraf. Total (A+B+C)",
        "SO EFGs-ECO": "SO EFGs-ECO",
        "SO EFGs-NO ECO": "SO EFGs-NO ECO",
        "SO EFGs-Total": "SO EFGs-Total",
        "SO MEX": "SO MEX",
    }


class StockTranslator(_Translator):
    map_es = {
        "Estoc total | s/ Ventas TAM": "Stock total | s/ Ventas TAM",
        "Estoc muerto | s/ Estoc Total": "Stock muerto | s/ Stock Total",
        "Estoc caducado | s/ Estoc Total": "Stock caducado | s/ Stock Total",
        "Estoc muerto | s/ Ventas TAM": "Stock muerto | s/ Ventas TAM",
        "Núm. productos con fecha caducidad": "Núm. productos con fecha caducidad",
        "Estoc Paraf. s/ Ventas Paraf. TAM": "Stock Paraf. s/ Ventas Paraf. TAM",
        "Estoc Paraf. (Unid.)/m. lineal (Transf.)": "Stock Paraf. (Unid.)/m. lineal (Transf.)",
        "Estoc Paraf. (€/)/m. lineal (Transf.)": "Stock Paraf. (€/)/m. lineal (Transf.)",
        "Metros lineales farmacia (Transf.)": "Metros lineales farmacia (Transf.)",
    }
    map_ca = {
        "Estoc total | s/ Ventas TAM": "Estoc total | s/ Vendes TAM",
        "Estoc muerto | s/ Estoc Total": "Estoc mort | s/ Estoc Total",
        "Estoc caducado | s/ Estoc Total": "Estoc caducat | s/ Estoc Total",
        "Estoc muerto | s/ Ventas TAM": "Estoc mort | s/ Vendes TAM",
        "Núm. productos con fecha caducidad": "Núm. productes amb data de caducitat",
        "Estoc Paraf. s/ Ventas Paraf. TAM": "Estoc Paraf. s/ Vendes Paraf. TAM",
        "Estoc Paraf. (Unid.)/m. lineal (Transf.)": "Estoc Paraf. (Unit.)/m. lineal (Transf.)",
        "Estoc Paraf. (€/)/m. lineal (Transf.)": "Estoc Paraf. (€/)/m. lineal (Transf.)",
        "Metros lineales farmacia (Transf.)": "Metres lineals farmàcia (Transf.)",
    }
    non_monetary_base = [
        "Metros lineales farmacia (Transf.)",
        "Núm. productos con fecha caducidad",
        "Estoc Paraf. (Unid.)/m. lineal (Transf.)",
    ]


class SuperFamiliesTranslator(_Translator):
    map_es = {
        "VETERINARIA": "VETERINARIA", "DIETETICA": "DIETÉTICA", "BEBES": "BEBÉS",
        "DERMATOLOGIA": "DERMATOLOGÍA", "SIST. MUSCULAR Y OSEO": "SIST. MUSCULAR Y ÓSEO",
        "OJOS": "OJOS", "SERVICIOS": "SERVICIOS", "SIST. NERVIOSO": "SIST. NERVIOSO",
        "SIST. DIGESTIVO": "SIST. DIGESTIVO", "PEDIATRIA": "PEDIATRÍA",
        "SIST. CIRCULATORIO": "SIST. CIRCULATORIO", "GINECOLOGIA": "GINECOLOGÍA",
        "SALUD SEXUAL": "SALUD SEXUAL", "OIDOS": "OÍDOS", "SIST. URINARIO": "SIST. URINARIO",
        "VITALIDAD": "VITALIDAD", "OPTICA": "ÓPTICA", "TERAPIAS ALTERNATIVAS": "TERAPIAS ALTERNATIVAS",
        "ORTOPEDIA": "ORTOPEDIA", "BOTIQUIN": "BOTIQUÍN", "TEMPORADA": "TEMPORADA",
        "HIGIENE": "HIGIENE", "SIST. RESPIRATORIO": "SIST. RESPIRATORIO", "COSMETICA": "COSMÉTICA",
    }
    map_ca = {
        "VETERINARIA": "VETERINÀRIA", "DIETETICA": "DIETÈTICA", "BEBES": "BEBÈS",
        "DERMATOLOGIA": "DERMATOLOGIA", "SIST. MUSCULAR Y OSEO": "SIST. MUSCULAR I OSSI",
        "OJOS": "ULLS", "SERVICIOS": "SERVEIS", "SIST. NERVIOSO": "SIST. NERVIÓS",
        "SIST. DIGESTIVO": "SIST. DIGESTIU", "PEDIATRIA": "PEDIATRIA",
        "SIST. CIRCULATORIO": "SIST. CIRCULATORI", "GINECOLOGIA": "GINECOLOGIA",
        "SALUD SEXUAL": "SALUT SEXUAL", "OIDOS": "OÏDES", "SIST. URINARIO": "SIST. URINARI",
        "VITALIDAD": "VITALITAT", "OPTICA": "ÒPTICA", "TERAPIAS ALTERNATIVAS": "TERÀPIES ALTERNATIVES",
        "ORTOPEDIA": "ORTOPÈDIA", "BOTIQUIN": "FARMACIOLA", "TEMPORADA": "TEMPORADA",
        "HIGIENE": "HIGIENE", "SIST. RESPIRATORIO": "SIST. RESPIRATORI", "COSMETICA": "COSMÈTICA",
    }


class PurchaseTranslator(_Translator):
    map_es = {
        "ALLIANCE": "ALLIANCE", "COMPRA DIRECTA": "COMPRA DIRECTA",
        "FEDERACIO": "FEDERACIÓN", "RESTO MAYORISTAS": "RESTO MAYORISTAS",
    }
    map_ca = {
        "ALLIANCE": "ALLIANCE", "COMPRA DIRECTA": "COMPRA DIRECTA",
        "FEDERACIO": "FEDERACIÓ", "RESTO MAYORISTAS": "RESTA MAJORISTES",
    }


# ─────────────────────────────────────────────────────────────
# Date / number helpers (ports of Carbon formatting)
# ─────────────────────────────────────────────────────────────

_MONTHS = {
    "es": ["", "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
           "agosto", "septiembre", "octubre", "noviembre", "diciembre"],
    "ca": ["", "gener", "febrer", "març", "abril", "maig", "juny", "juliol",
           "agost", "setembre", "octubre", "novembre", "desembre"],
}


def _ucfirst(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s


def _format_anyomes(anyomes: int, locale: str) -> str:
    """'F Y' for a YYMM int, e.g. 2607 -> 'julio 2026'."""
    year = 2000 + anyomes // 100
    month = anyomes % 100
    months = _MONTHS.get(locale, _MONTHS["es"])
    return f"{months[month]} {year}"


def format_month_range(frm: int, to: int, locale: str = "es") -> str:
    """Port of getFormattedMonthRange(): ucfirst, ' - ' joined when frm != to."""
    frm_fmt = _ucfirst(_format_anyomes(frm, locale))
    to_fmt = _ucfirst(_format_anyomes(to, locale))
    return frm_fmt if frm == to else f"{frm_fmt} - {to_fmt}"


def _num(v) -> float:
    """Coerce a possibly-None/Decimal DB value to float (None -> 0.0)."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ─────────────────────────────────────────────────────────────
# Queries (verbatim ports of KpiQueryService)
# ─────────────────────────────────────────────────────────────


def _records(db, sql: str) -> list[dict]:
    df = db.fech_dataframe(sql)
    return df.to_dict("records")


def get_nombre_farmacia(db, idende_s: int) -> str:
    df = db.fech_dataframe(
        f"SELECT delegacion FROM bifarma.dbo.tme_delegaciones WHERE idendeS = {int(idende_s)}"
    )
    if len(df) and df.iloc[0, 0] is not None:
        return str(df.iloc[0, 0])
    return "Farmacia desconocida"


def get_ccaa_farmacia(db, idende_s: int) -> Optional[str]:
    df = db.fech_dataframe(
        f"SELECT CCAA FROM bifarma.dbo.tme_delegaciones WHERE idendeS = {int(idende_s)}"
    )
    if len(df) and df.iloc[0, 0] is not None:
        return str(df.iloc[0, 0])
    return None


def fetch_main_kpis(db, idende_s: int, frm: int, to: int, comparativa_id: int = 1) -> list[dict]:
    valorfarmant = "kpi.valorfarmant" if comparativa_id == 1 else "kpi.valorfarmant2"
    valordfarmant = "kpi.valordfarmant" if comparativa_id == 1 else "kpi.valordfarmant2"
    sql = f"""
        SELECT
            v.orden AS Orden,
            kpi.kpi AS KPI,
            v.concepto AS Indicador,
            SUM(kpi.valorfarmact) /
                NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM(kpi.valordfarmact) END, 0) AS ValorAct,
            SUM({valorfarmant}) /
                NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM({valordfarmant}) END, 0) AS ValorAnt,
            (
                SUM(kpi.valorfarmact) / NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM(kpi.valordfarmact) END, 0)
                -
                SUM({valorfarmant}) / NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM({valordfarmant}) END, 0)
            ) AS VariacionAbsoluta,
            (
                (
                    SUM(kpi.valorfarmact) / NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM(kpi.valordfarmact) END, 0)
                    -
                    SUM({valorfarmant}) / NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM({valordfarmant}) END, 0)
                )
                / NULLIF(
                    SUM({valorfarmant}) / NULLIF(CASE v.sn WHEN 'S' THEN 1 WHEN 'D' THEN SUM({valordfarmant}) END, 0)
                , 0) * 100
            ) AS VariacionPorcentaje
        FROM bifarma.dbo.dwCMKPIsMes kpi
        JOIN dbo.tme_validaciones v
            ON kpi.kpi = v.codigo
            AND v.tipo = 'VKPI'
            AND (v.sn = 'D' OR v.sn = 'S')
        WHERE
            kpi.idendeS = {int(idende_s)}
            AND kpi.anyomesS BETWEEN {int(frm)} AND {int(to)}
        GROUP BY
            v.orden, kpi.kpi, v.concepto, v.sn
        ORDER BY
            v.orden ASC
    """
    rows = _records(db, sql)
    for r in rows:
        for k in ("ValorAct", "ValorAnt", "VariacionAbsoluta", "VariacionPorcentaje"):
            r[k] = _num(r.get(k))
    return rows


def fetch_stock_indicadores(db, idende_s: int) -> list[dict]:
    sql = f"""
        SELECT
            v.orden AS Orden,
            v.codigo AS KPI,
            v.concepto AS Indicador,
            ISNULL(SUM(kpi.valorfarmact), 0) AS ValorPVP,
            ISNULL(SUM(kpi.valordfarmact), 0) AS ValorReferencia,
            CASE
                WHEN SUM(kpi.valordfarmact) IS NULL OR SUM(kpi.valordfarmact) = 0 THEN 0
                ELSE (SUM(kpi.valorfarmact) / SUM(kpi.valordfarmact)) * 100
            END AS Porcentaje
        FROM BifarmaCentral.dbo.tme_validaciones v
        LEFT JOIN bifarma.dbo.dwCMKPIsMes kpi
            ON v.codigo = kpi.kpi
            AND kpi.idendeS = {int(idende_s)}
        WHERE
            v.tipo = 'VKPI'
            AND v.sn = 'E'
        GROUP BY
            v.orden, v.codigo, v.concepto
        ORDER BY
            v.orden ASC
    """
    rows = _records(db, sql)
    for r in rows:
        for k in ("ValorPVP", "ValorReferencia", "Porcentaje"):
            r[k] = _num(r.get(k))
    return rows


def fetch_compras_por_proveedor(db, idende_s: int, frm: int, to: int) -> list[dict]:
    sql = f"""
        WITH TotalCompras AS (
            SELECT SUM(importecompra) AS Total
            FROM dbo.dwComprasMesProvUnif
            WHERE idendeS = {int(idende_s)}
            AND anyomesS BETWEEN {int(frm)} AND {int(to)}
        )
        SELECT
            T1.nombreproveedorunif AS ProveedorUnificado,
            SUM(T1.importecompra) AS ImporteCompra,
            CASE
                WHEN TC.Total = 0 THEN 0
                ELSE (SUM(T1.importecompra) / TC.Total) * 100
            END AS PorcentajeTotal
        FROM dbo.dwComprasMesProvUnif T1
        CROSS JOIN TotalCompras TC
        WHERE T1.idendeS = {int(idende_s)}
        AND T1.anyomesS BETWEEN {int(frm)} AND {int(to)}
        GROUP BY
            T1.nombreproveedorunif, TC.Total
        ORDER BY
            ImporteCompra DESC
    """
    rows = _records(db, sql)
    for r in rows:
        for k in ("ImporteCompra", "PorcentajeTotal"):
            r[k] = _num(r.get(k))
    return rows


def fetch_fidelizacion(db, idende_s: int, frm: int, to: int) -> Optional[dict]:
    sql = f"""
        SELECT
            de.idendeS AS IdendeS,
            fc.numClientes,
            fc.numClientesEmail,
            fc.numClientesMovil,
            fc.numClientesRGDP,
            fc.numClientesVentasTAM,
            SUM(fnc.nuevosClientes) as nuevosClientes,
            SUM(fvm.importePFTarjeta) as importePFTarjeta,
            SUM(fvm.importePF) as importePF
        FROM bifarma.dbo.tme_delegaciones de
        JOIN bifarma.dbo.fideldwClientes fc ON de.idendeS = fc.idendeS
        JOIN bifarma.dbo.fidelNuevosClientes fnc ON de.idendeS = fnc.idendeS
        JOIN bifarma.dbo.fidelDwVentasMes fvm ON de.idendeS = fvm.idendeS AND fnc.anyomes = fvm.anyomes
        WHERE
            de.idendeS = {int(idende_s)}
            AND fnc.anyomes >= {int(frm)}
            AND fnc.anyomes <= {int(to)}
            AND fvm.anyomes >= {int(frm)}
            AND fvm.anyomes <= {int(to)}
        GROUP BY
            de.idendeS, fc.numClientes, fc.numClientesEmail, fc.numClientesMovil,
            fc.numClientesRGDP, fc.numClientesVentasTAM
    """
    rows = _records(db, sql)
    return rows[0] if rows else None


def fetch_crecimiento_superfamilias(db, idende_s: int, frm: int, to: int) -> dict:
    sql = f"""
        WITH Total AS (
            SELECT SUM(valorfarmact) AS TotalValorAct
            FROM bifarma.dbo.dwCMKPISuperFamMes
            WHERE idendeS = {int(idende_s)} AND anyomesS BETWEEN {int(frm)} AND {int(to)}
        )
        SELECT
            kpi.nombresuperfamilia AS SuperFamilia,
            SUM(kpi.valorfarmact) AS ValorAct,
            SUM(kpi.valorfarmant) AS ValorAnt,
            SUM(kpi.valorfarmact) - SUM(kpi.valorfarmant) AS VarValor,
            CASE
                WHEN SUM(kpi.valorfarmant) = 0 THEN 0
                ELSE (SUM(kpi.valorfarmact) - SUM(kpi.valorfarmant)) / SUM(kpi.valorfarmant) * 100
            END AS PctVarValor,
            CASE
                WHEN Total.TotalValorAct = 0 THEN 0
                ELSE SUM(kpi.valorfarmact) / Total.TotalValorAct * 100
            END AS PorcentajeSobreTotal
        FROM bifarma.dbo.dwCMKPISuperFamMes kpi
        CROSS JOIN Total
        WHERE kpi.idendeS = {int(idende_s)} AND kpi.anyomesS BETWEEN {int(frm)} AND {int(to)}
        GROUP BY kpi.nombresuperfamilia, Total.TotalValorAct
        ORDER BY VarValor DESC
    """
    rows = _records(db, sql)
    for r in rows:
        for k in ("ValorAct", "ValorAnt", "VarValor", "PctVarValor", "PorcentajeSobreTotal"):
            r[k] = _num(r.get(k))
    return {"todas": rows}


# ─────────────────────────────────────────────────────────────
# Shaping (ports of KpiPdfBuilderService private methods)
# ─────────────────────────────────────────────────────────────


def _split_main_kpis(kpis: list[dict], sales_tr: SalesAndPurchasesTranslator):
    block1, block2, block3 = [], [], []
    in_block1, in_block2 = True, False
    total_row = None

    for row in kpis:
        label = (row["Indicador"] or "").strip()
        row["Indicador"] = sales_tr.translate(label)

        if label == "SO Total (1+2)":
            total_row = row
            continue

        if in_block1:
            block1.append(row)
            if label == "SO Libre (2)" and total_row:
                block1.append(total_row)
            if label == "SO Paraf. Total (A+B+C)":
                in_block1 = False

        if label == "SO EFGs-ECO":
            in_block2 = True
        if in_block2:
            block2.append(row)
            if label == "SO EFGs-Total":
                in_block2 = False

        if label == "SO MEX":
            block3.append(row)

    return block1, block2, block3


def _extract_retail(kpis: list[dict], indicator_tr: IndicatorTranslator) -> list[dict]:
    retail = []
    in_retail = False
    for row in kpis:
        label = (row["Indicador"] or "").strip()
        if label == "SO Paraf./metro lineal":
            in_retail = True
        if in_retail:
            row["Indicador"] = indicator_tr.translate(label)
            retail.append(row)
            if label == "Cesta media":
                break
    return retail


def _format_fidelizacion(data: Optional[dict], locale: str) -> list[dict]:
    if not data:
        return []
    num_clientes = _num(data.get("numClientes"))
    importe_pf = _num(data.get("importePF"))

    pct_tarjeta = (_num(data.get("importePFTarjeta")) / importe_pf * 100) if importe_pf > 0 else 0
    pct_email = (_num(data.get("numClientesEmail")) / num_clientes * 100) if num_clientes > 0 else 0
    pct_rgpd = (_num(data.get("numClientesRGDP")) / num_clientes * 100) if num_clientes > 0 else 0
    pct_activos = (_num(data.get("numClientesVentasTAM")) / num_clientes * 100) if num_clientes > 0 else 0

    from utils.clsPdf import number_format as nf

    if locale == "ca":
        indicadores = [
            ("% Parafarmàcia amb targeta", nf(pct_tarjeta, 1) + " %"),
            ("Nº Clients totals", nf(num_clientes, 0)),
            ("Nº Clients nous", nf(_num(data.get("nuevosClientes")), 0)),
            ("% clients Email", nf(pct_email, 1) + " %"),
            ("% clients RGPD", nf(pct_rgpd, 1) + " %"),
            ("% clients Actius", nf(pct_activos, 1) + " %"),
        ]
    else:
        indicadores = [
            ("% Parafarmacia con tarjeta", nf(pct_tarjeta, 1) + " %"),
            ("Nº Clientes totales", nf(num_clientes, 0)),
            ("Nº Clientes nuevos", nf(_num(data.get("nuevosClientes")), 0)),
            ("% clientes Email", nf(pct_email, 1) + " %"),
            ("% clientes RGPD", nf(pct_rgpd, 1) + " %"),
            ("% clientes Activos", nf(pct_activos, 1) + " %"),
        ]
    return [{"Indicador": k, "MesActual": v} for k, v in indicadores]


def build_all(db, store_id: int, frm: int, to: int, locale: str, translators: dict,
              nombre_farmacia: str, include_stock: bool = True) -> dict:
    """Port of KpiPdfBuilderService::buildAll() for a single period range.

    Optimisations vs. the PHP (output-equivalent):
      * ``nombre_farmacia`` is fetched once by the caller and passed in (the PHP
        re-queried it on every buildAll).
      * ``include_stock=False`` skips the stock query for period ranges whose
        stock is discarded (buildUnifiedCompletePdf only uses the actual month's
        stock — the accumulated build's stock was fetched and thrown away).
    """
    from utils.clsPdf import number_format as nf

    indicator_tr: IndicatorTranslator = translators["indicator"]
    stock_tr: StockTranslator = translators["stock"]
    superfam_tr: SuperFamiliesTranslator = translators["superfam"]
    purchase_tr: PurchaseTranslator = translators["purchase"]
    # buildAll re-instantiates the sales translator from the pharmacy's CCAA.
    sales_tr = SalesAndPurchasesTranslator(locale)

    all_kpis = fetch_main_kpis(db, store_id, frm, to)
    block1, block2, block3 = _split_main_kpis(all_kpis, sales_tr)   # mutates all_kpis rows
    stock_indicators = fetch_stock_indicadores(db, store_id) if include_stock else []
    retail = _extract_retail(all_kpis, indicator_tr)                 # reads/mutates shared rows
    purchases = fetch_compras_por_proveedor(db, store_id, frm, to)
    families = fetch_crecimiento_superfamilias(db, store_id, frm, to)
    fidelizacion = fetch_fidelizacion(db, store_id, frm, to)

    formatted_fidelizacion = _format_fidelizacion(fidelizacion, locale)

    unit = " uts" if locale == "ca" else " uds"

    formatted_retail = []
    for row in retail:
        if (row["Indicador"] or "").strip() == "Tiquet mig":
            continue
        label = (row["Indicador"] or "").strip()
        is_monetary = indicator_tr.is_monetary(label)
        row["Indicador"] = indicator_tr.translate(label)
        suffix = " €" if is_monetary else unit
        dec = 2 if is_monetary else 0
        row["ValorFormatted"] = nf(row["ValorAct"], dec) + suffix
        row["VariacionAbsFormatted"] = nf(row["VariacionAbsoluta"], dec) + suffix
        formatted_retail.append(row)

    formatted_purchases = []
    for row in purchases:
        row["ProveedorUnificado"] = purchase_tr.translate((row["ProveedorUnificado"] or "").strip())
        formatted_purchases.append(row)

    formatted_stock = []
    for row in stock_indicators:
        indicator = (row["Indicador"] or "").strip()
        row["Indicador"] = stock_tr.translate(indicator)
        is_monetary = stock_tr.is_monetary(indicator)
        suffix = " €" if is_monetary else unit
        dec = 2 if is_monetary else 0
        row["ValorPVPFormatted"] = nf(row["ValorPVP"], dec) + suffix
        formatted_stock.append(row)

    formatted_families = []
    for row in (families.get("todas") or []):
        row["SuperFamilia"] = superfam_tr.translate((row["SuperFamilia"] or "").strip())
        formatted_families.append(row)

    return {
        "block1": block1,
        "block2": block2,
        "block3": block3,
        "purchases": formatted_purchases,
        "stockData": formatted_stock,
        "retailData": formatted_retail,
        "fidelizacionData": formatted_fidelizacion,
        "familiesData": formatted_families,
        "nombreFarmacia": nombre_farmacia,
    }


def build_report_context(db, store_id: int, frm: int, to: int, from_acum: int, locale: str,
                         nombre_farmacia: Optional[str] = None) -> dict:
    """Port of buildUnifiedCompletePdf(): assemble the full template context."""
    translators = {
        "indicator": IndicatorTranslator(locale),
        "stock": StockTranslator(locale),
        "superfam": SuperFamiliesTranslator(locale),
        "purchase": PurchaseTranslator(locale),
    }
    if nombre_farmacia is None:
        nombre_farmacia = get_nombre_farmacia(db, store_id)
    # Only the actual month's stock is rendered; skip it for the accumulated build.
    actual = build_all(db, store_id, to, to, locale, translators, nombre_farmacia, include_stock=True)
    acumulado = build_all(db, store_id, from_acum, to, locale, translators, nombre_farmacia, include_stock=False)

    return {
        "ventasCompras": {
            "actual": {
                "kpisBlock1": actual["block1"], "kpisBlock2": actual["block2"],
                "kpisBlock3": actual["block3"], "purchases": actual["purchases"],
            },
            "acumulado": {
                "kpisBlock1": acumulado["block1"], "kpisBlock2": acumulado["block2"],
                "kpisBlock3": acumulado["block3"], "purchases": acumulado["purchases"],
            },
        },
        "retail": {"actual": actual["retailData"], "acumulado": acumulado["retailData"]},
        "fidelizacion": {"actual": actual["fidelizacionData"], "acumulado": acumulado["fidelizacionData"]},
        "familias": {"actual": actual["familiesData"], "acumulado": acumulado["familiesData"]},
        "stockIndicators": actual["stockData"],
        "mesAno": format_month_range(to, to, locale),
        "mesRango": format_month_range(from_acum, to, locale),
        "nombreFarmacia": actual["nombreFarmacia"],
        "locale": locale,
    }
