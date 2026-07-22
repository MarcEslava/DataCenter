"""
EcoBuy ETL  ·  exact port of the Talend job `Ejecuta_Fases_ecobuy` (Carga_Incremental)

Per-pharmacy incremental sync  ecoextract (Farmatic mirror, Spanish)  →  ecobuy (normalized).
See docs/ecoBuy_talend_mapping.md for the full source→target mapping this file implements.

Flow (per unit from `SELECT id, id_ecobuy FROM Unit WHERE ecobuy <> 0`):
  Phase 1 (master)       : providers, superfamilies, families, laboratories, iva, iva_groups (+ defaults)
  Phase 2 (transactions) : products, orders, purchases + details, receptions + details,
                           product_lists + products   (incremental on `_updated >= Fecha_Desde`)
  Post-load fixups        : num_products recount, orphan-lab delete, pharmacy_configs.date_import stamp.

Upsert = INSERT … ON DUPLICATE KEY UPDATE, so the ecobuy tables need the natural unique keys:
  providers(pharmacy_id, import_id) · superfamilies(pharmacy_id, origin_id) ·
  families(pharmacy_id, origin_id) · laboratories(pharmacy_id, code) · iva(pharmacy_id, import_id) ·
  iva_groups(pharmacy_id, import_id) · products(pharmacy_id, code) · purchases(pharmacy_id, import_id) ·
  purchase_details(pharmacy_id, line_id) · receptions(pharmacy_id, import_id) ·
  reception_detail(pharmacy_id, reception_id, line_num) · orders(pharmacy_id, order_id, line_id) ·
  product_lists(pharmacy_id, import_id) · product_lists_products(pharmacy_id, product_lists_import_id, product_import_id)

⚠️ PROTOTYPE — exact port first, optimize later.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SRC_CONN_ID = "ecoextract_db"     # Farmatic mirror (Spanish tables) — read
DST_CONN_ID = "ecobuy_db"         # normalized ecobuy — read + write

IVA_RATE        = {"01": 0, "02": 4.5, "03": 11.4, "04": 26.2}   # XGrup_IdGrupoIva → iva %
DEFAULT_CODE    = "99999"          # "SIN ASIGNAR" lab / superfamily code
SFAM_DEFAULT_ID = 171422           # orders.superfamily_id fallback (from the Talend tMap)
UPSERT_CHUNK    = int(Variable.get("ecobuy_upsert_chunk", default_var="5000"))  # rows per statement

# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────

def _db(conn_id):
    from airflow.hooks.base import BaseHook
    from utils.clsSQL import SQLConnection
    c = BaseHook.get_connection(conn_id)
    db = SQLConnection(
        db_host=c.host, db_port=c.port or 3306, db_database=c.schema,
        db_username=c.login, db_password=c.password, dialect="mysql", driver="pymysql",
    )
    db.connect()
    return db

def _read(db, sql):
    return db.fech_dataframe(sql)

def _exec(db, sql):
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text(sql))

def _upsert(db, table, rows, cols, update_cols, chunk=UPSERT_CHUNK):
    """INSERT … ON DUPLICATE KEY UPDATE. rows = list[dict] keyed by cols.
    Executed in `chunk`-row batches inside one transaction (atomic per table,
    but each statement stays small enough for max_allowed_packet)."""
    from sqlalchemy import text
    if not rows:
        return 0
    collist = ", ".join(f"`{c}`" for c in cols)
    ph      = ", ".join(f":{c}" for c in cols)
    upd     = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols)
    stmt    = text(f"INSERT INTO `{table}` ({collist}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}")
    with db.engine.begin() as conn:
        for i in range(0, len(rows), chunk):
            conn.execute(stmt, rows[i:i + chunk])
    return len(rows)

def _val(row, key, default=None):
    v = row.get(key)
    return default if v is None or (isinstance(v, str) and v == "") else v

def _int0(v):
    try:
        return 0 if v in (None, "") else int(float(v))
    except (TypeError, ValueError):
        return 0

def _lookup(df, key_col, val_col):
    """Build {str(key): val} from a target dataframe."""
    if df is None or df.empty:
        return {}
    return {str(k): v for k, v in zip(df[key_col], df[val_col])}

def _to_records(df):
    """DataFrame → list[dict] with NaN/NaT → None (safe for executemany)."""
    if df is None or df.empty:
        return []
    return df.astype(object).where(df.notna(), None).to_dict("records")

# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id="ecobuy_ETL",
    description="Incremental ecoextract → ecobuy per pharmacy (exact Talend port)",
    schedule=Variable.get("ecobuy_schedule", default_var="0 2 * * *"),  # nightly 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=2,
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
)
def ecobuy_etl():

    @task
    def extract_units() -> list[dict]:
        """Pharmacies enrolled in ecobuy: SELECT id, id_ecobuy FROM Unit WHERE ecobuy <> 0."""
        s = _db(SRC_CONN_ID)
        try:
            df = _read(s, "SELECT id, id_ecobuy FROM Unit WHERE ecobuy <> 0")
        finally:
            s.close()
        units = [{"id_unit": int(r["id"]), "id_ecobuy": int(r["id_ecobuy"])} for _, r in df.iterrows()]
        print(f"{len(units)} ecobuy units")
        return units

    @task
    def process_master(unit: dict) -> dict:
        """Phase 1 — master/dimension upserts for one unit."""
        s, t = _db(SRC_CONN_ID), _db(DST_CONN_ID)
        try:
            u_ex   = unit["id_unit"]          # id_UNIT_ex
            eco_ex = unit["id_ecobuy"]         # id_ecobuy_ex
            farma  = unit["id_ecobuy"]         # id_API_farma (ecobuy pharmacy_id == Unit.id_ecobuy)

            # ── default rows (idempotent) ──
            _exec(t, f"INSERT IGNORE INTO laboratories (pharmacy_id, code, name, address, location, region, CP, latitude, longitude, phone, fax) "
                     f"VALUES ({farma}, '{DEFAULT_CODE}', 'SIN ASIGNAR', '.','.','.',0,0,0,'.','.')")
            _exec(t, f"INSERT IGNORE INTO superfamilies (pharmacy_id, origin_id, description) "
                     f"VALUES ({farma}, '{DEFAULT_CODE}', 'Sin Asignar')")
            _exec(s, f"INSERT INTO FamiliaAux (unit, idFamilia, IdSuperFamilia) "
                     f"SELECT {u_ex}, Familia.IdFamilia, '{DEFAULT_CODE}' FROM Familia "
                     f"LEFT JOIN FamiliaAux ON Familia.IdFamilia=FamiliaAux.IdFamilia "
                     f"WHERE Familia.unit={u_ex} AND FamiliaAux.idFamilia IS NULL")

            # ── providers ← Proveedor ──
            prov = _read(s, f"SELECT IDPROVEEDOR, IF(FIS_NIF='','0R',FIS_NIF) AS Nif, FIS_NOMBRE, _updated "
                            f"FROM Proveedor JOIN Unit ON Proveedor.unit=Unit.id "
                            f"WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy={eco_ex}")
            rows = [{"import_id": _int0(r["IDPROVEEDOR"]), "pharmacy_id": farma, "cif": r["Nif"],
                     "name": r["FIS_NOMBRE"], "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in prov.iterrows()]
            n = _upsert(t, "providers", rows, ["import_id","pharmacy_id","cif","name","created_at","updated_at"],
                        ["cif","name","updated_at"])
            print(f"[{farma}] providers: {n}")

            # ── superfamilies ← SuperFamilia ──
            sfam = _read(s, f"SELECT IdSuperFamilia, Descripcion, _updated FROM SuperFamilia "
                            f"JOIN Unit ON SuperFamilia.unit=Unit.id "
                            f"WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy={eco_ex}")
            rows = [{"pharmacy_id": farma, "origin_id": r["IdSuperFamilia"], "description": r["Descripcion"],
                     "status": 1, "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in sfam.iterrows()]
            _upsert(t, "superfamilies", rows, ["pharmacy_id","origin_id","description","status","created_at","updated_at"],
                    ["description","updated_at"])

            # ── families ← Familia (+ FamiliaAux → superfamily) ──
            sfam_map = _lookup(_read(t, f"SELECT id, origin_id FROM superfamilies WHERE pharmacy_id={farma}"),
                               "origin_id", "id")
            fam = _read(s, f"SELECT f.IdFamilia, f.Descripcion, f._updated, fa.IdSuperFamilia "
                           f"FROM Familia f JOIN Unit ON f.unit=Unit.id "
                           f"LEFT JOIN FamiliaAux fa ON fa.IdFamilia=f.IdFamilia AND fa.unit=f.unit "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy={eco_ex}")
            rows = [{"pharmacy_id": farma,
                     "superfamily_id": sfam_map.get(str(_val(r,"IdSuperFamilia",DEFAULT_CODE)), sfam_map.get(DEFAULT_CODE)),
                     "origin_id": str(r["IdFamilia"]), "description": r["Descripcion"], "status": 1,
                     "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in fam.iterrows()]
            _upsert(t, "families", rows,
                    ["pharmacy_id","superfamily_id","origin_id","description","status","created_at","updated_at"],
                    ["superfamily_id","description","status","updated_at"])

            # ── laboratories ← Laboratorio (numeric) + LABOR (alpha) ──
            lab_cols = ["pharmacy_id","code","name","address","location","region","cp","phone","fax","created_at","updated_at"]
            labn = _read(s, f"SELECT Codigo, Nombre, Direccion, Ciudad, Provincia, CodigoPostal, Telefonos, Fax, _updated "
                            f"FROM Laboratorio JOIN Unit ON Laboratorio.unit=Unit.id "
                            f"WHERE Unit.id_ecobuy IS NOT NULL AND LEFT(Codigo,1) NOT BETWEEN 'A' AND 'Z' AND Unit.id={u_ex}")
            rows = [{"pharmacy_id": farma, "code": r["Codigo"], "name": r["Nombre"], "address": r["Direccion"],
                     "location": r["Ciudad"], "region": r["Provincia"], "cp": _val(r,"CodigoPostal","0"),
                     "phone": r["Telefonos"], "fax": r["Fax"], "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in labn.iterrows()]
            laba = _read(s, "SELECT CODIGO, IFNULL(TIPO,'1') AS TIPO, "
                            "CASE WHEN NOMBRE='' OR NOMBRE IS NULL THEN '.' ELSE NOMBRE END AS NOMBRE, "
                            "CASE WHEN DIRECCION='' OR DIRECCION IS NULL THEN '.' ELSE DIRECCION END AS DIRECCION, "
                            "CASE WHEN CIUDAD='' OR CIUDAD IS NULL THEN '.' ELSE CIUDAD END AS CIUDAD, "
                            "CASE WHEN PROVINCIA='' OR PROVINCIA IS NULL THEN '.' ELSE PROVINCIA END AS PROVINCIA, "
                            "CASE WHEN TELEFONO='' OR TELEFONO IS NULL THEN '.' ELSE TELEFONO END AS TELEFONO, "
                            "CASE WHEN FAX='' OR FAX IS NULL THEN '.' ELSE FAX END AS FAX "
                            "FROM LABOR WHERE LEFT(CODIGO,1) BETWEEN 'A' AND 'Z'")
            now = datetime.now()
            rows += [{"pharmacy_id": farma, "code": r["CODIGO"], "name": r["NOMBRE"], "address": r["DIRECCION"],
                      "location": r["CIUDAD"], "region": r["PROVINCIA"], "cp": "0", "phone": r["TELEFONO"],
                      "fax": r["FAX"], "created_at": now, "updated_at": now}
                     for _, r in laba.iterrows()]
            _upsert(t, "laboratories", rows, lab_cols,
                    ["name","address","location","region","cp","phone","fax","updated_at"])

            # ── iva ← Tablaiva ── (import_id: composite of the source type ids)
            iva = _read(s, f"SELECT IdTipoArt, IdTipoCli, IdTipoPro, Descripcion, Piva, PReq, _updated "
                           f"FROM Tablaiva JOIN Unit ON Tablaiva.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Tablaiva.unit={u_ex}")
            rows = [{"pharmacy_id": farma, "import_id": f"{r['IdTipoArt']}-{r['IdTipoCli']}-{r['IdTipoPro']}",
                     "id_tipo_art": r["IdTipoArt"], "id_tipo_cli": r["IdTipoCli"], "id_tipo_pro": r["IdTipoPro"],
                     "description": r["Descripcion"], "iva": r["Piva"], "recargo_equivalencia": r["PReq"],
                     "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in iva.iterrows()]
            _upsert(t, "iva", rows,
                    ["pharmacy_id","import_id","id_tipo_art","id_tipo_cli","id_tipo_pro","description","iva","recargo_equivalencia","created_at","updated_at"],
                    ["description","iva","recargo_equivalencia","updated_at"])

            # ── iva_groups ← Grupoiva ──
            ivg = _read(s, f"SELECT IdGrupoIva, Descripcion, Tipo, _updated FROM Grupoiva "
                           f"JOIN Unit ON Grupoiva.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Grupoiva.unit={u_ex}")
            rows = [{"pharmacy_id": farma, "import_id": str(r["IdGrupoIva"]), "id_grupo_iva": r["IdGrupoIva"],
                     "descripcion": r["Descripcion"], "tipo": r["Tipo"],
                     "created_at": r["_updated"], "updated_at": r["_updated"]}
                    for _, r in ivg.iterrows()]
            _upsert(t, "iva_groups", rows,
                    ["pharmacy_id","import_id","id_grupo_iva","descripcion","tipo","created_at","updated_at"],
                    ["descripcion","tipo","updated_at"])

            print(f"[{farma}] master done")
            return unit
        finally:
            s.close(); t.close()

    @task
    def process_transactions(unit: dict) -> dict:
        """Phase 2 — transactional upserts for one unit (incremental on _updated)."""
        import pandas as pd
        s, t = _db(SRC_CONN_ID), _db(DST_CONN_ID)
        try:
            u_ex   = unit["id_unit"]
            eco_ex = unit["id_ecobuy"]
            farma  = unit["id_ecobuy"]

            # watermark from pharmacy_configs (Talend stamps this at the end of the run)
            wdf = _read(t, f"SELECT date_import FROM pharmacy_configs WHERE pharmacy_id={farma}")
            fecha_desde = str(wdf.iloc[0]["date_import"]) if not wdf.empty and wdf.iloc[0]["date_import"] else "2000-01-01 00:00:00"
            print(f"[{farma}] Fecha_Desde={fecha_desde}")

            # default lab id (id_LAB_farma) + lookup maps
            lab_def = _read(t, f"SELECT id FROM laboratories WHERE pharmacy_id={farma} AND code='{DEFAULT_CODE}'")
            id_lab  = int(lab_def.iloc[0]["id"]) if not lab_def.empty else 0
            fam_map = _lookup(_read(t, f"SELECT id, origin_id FROM families WHERE pharmacy_id={farma}"), "origin_id", "id")
            lab_map = _lookup(_read(t, f"SELECT id, code FROM laboratories WHERE pharmacy_id={farma}"), "code", "id")

            def _empty_default(series, default):
                """Series with None/'' replaced by default (vectorized _val)."""
                return series.where(series.notna() & (series.astype(str) != ""), default)

            # ── products ← Articu ── (incremental on _updated; vectorized)
            # Only changed articles are re-upserted; the target `products` table accumulates,
            # so the prod_map/pfam/plab lookups below stay complete for all products.
            art = _read(s, f"SELECT idArticu, Descripcion, XFam_idFamilia, "
                           f"IF(Laboratorio='','{id_lab}', IFNULL(Laboratorio,'{id_lab}')) AS Labora1, "
                           f"Pvp, PvpAux, StockActual, XGrup_idGrupoIva, _updated "
                           f"FROM Articu JOIN Unit ON Articu.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy={eco_ex} "
                           f"AND Articu._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "code": art["idArticu"], "description": art["Descripcion"],
                "pvp": art["Pvp"], "pvp_aux": art["PvpAux"],
                "stock": pd.to_numeric(art["StockActual"], errors="coerce").fillna(0).astype(int),
                "family_import_id": art["XFam_idFamilia"].astype(str).map(fam_map),
                "laboratory_import_id": art["Labora1"].astype(str).map(lab_map).fillna(id_lab),
                "iva": art["XGrup_idGrupoIva"].astype(str).map(IVA_RATE).fillna(0),
                "created_at": art["_updated"], "updated_at": art["_updated"],
            })
            _upsert(t, "products", _to_records(o),
                    ["pharmacy_id","code","description","pvp","pvp_aux","stock","family_import_id","laboratory_import_id","iva","created_at","updated_at"],
                    ["description","pvp","pvp_aux","stock","family_import_id","laboratory_import_id","iva","updated_at"])

            # products lookup, read ONCE and reused everywhere below
            prod_full = _read(t, f"SELECT id, code, family_import_id, laboratory_import_id FROM products WHERE pharmacy_id={farma}")
            prod_map = _lookup(prod_full, "code", "id")
            pfam     = _lookup(prod_full, "code", "family_import_id")
            plab     = _lookup(prod_full, "code", "laboratory_import_id")

            # ── purchases ← Pedido ──
            ped = _read(s, f"SELECT IdPedido, Tipo, Fecha, ImportePuc, ImportePvp, NLineas, XProv_IdProveedor, _updated "
                           f"FROM Pedido JOIN Unit ON Pedido.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Pedido.unit={u_ex} AND Pedido._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "user_id": "0", "type": ped["Tipo"], "status": 4, "name": "",
                "copy_id": "", "custom_days_purchase": 0, "import_id": ped["IdPedido"], "purchase_date": ped["Fecha"],
                "puc": ped["ImportePuc"], "pvp": ped["ImportePvp"], "lines": ped["NLineas"], "delegate": "",
                "provider_import_id": ped["XProv_IdProveedor"], "created_at": ped["_updated"],
            })
            _upsert(t, "purchases", _to_records(o),
                    ["pharmacy_id","user_id","type","status","name","copy_id","custom_days_purchase","import_id","purchase_date","puc","pvp","lines","delegate","provider_import_id","created_at"],
                    ["type","purchase_date","puc","pvp","lines","provider_import_id"])
            purch_map = _lookup(_read(t, f"SELECT id, import_id FROM purchases WHERE pharmacy_id={farma}"), "import_id", "id")

            # ── purchase_details ← LineaPedido ──
            lp = _read(s, f"SELECT IdPedido, IdLinea, XArt_IdArticu, Unidades, Puc, Pvp, _updated "
                          f"FROM LineaPedido JOIN Unit ON LineaPedido.unit=Unit.id "
                          f"WHERE Unit.id_ecobuy IS NOT NULL AND LineaPedido.unit={u_ex} AND LineaPedido._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "purchase_id": lp["IdPedido"].astype(str).map(purch_map),
                "purchase_import_id": lp["IdPedido"], "product_id": lp["XArt_IdArticu"].astype(str).map(prod_map),
                "quantity": _empty_default(lp["Unidades"], "0"), "line_id": lp["IdLinea"],
                "puc": lp["Puc"], "pvp": lp["Pvp"], "product_code": lp["XArt_IdArticu"],
                "created_at": lp["_updated"], "origin": "ecoextract", "status": 1,
            })
            _upsert(t, "purchase_details", _to_records(o),
                    ["pharmacy_id","purchase_id","purchase_import_id","product_id","quantity","line_id","puc","pvp","product_code","created_at","origin","status"],
                    ["purchase_id","product_id","quantity","puc","pvp","product_code"])

            # ── receptions ← Recep ──
            prov_map = _lookup(_read(t, f"SELECT id, import_id FROM providers WHERE pharmacy_id={farma}"), "import_id", "id")
            rec = _read(s, f"SELECT IdRecepcion, XCPro_IdCondicion, XProv_IdProveedor, Fecha, _updated "
                           f"FROM Recep JOIN Unit ON Recep.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND Recep.unit={u_ex} AND Recep._updated>='{fecha_desde}'")
            prov_key = _empty_default(rec["XProv_IdProveedor"], "9999")
            o = pd.DataFrame({
                "import_id": rec["IdRecepcion"], "pharmacy_id": farma,
                "import_purchase_condition_id": rec["XCPro_IdCondicion"].astype(str),
                "provider_id": prov_key.astype(str).map(prov_map), "import_provider_id": prov_key,
                "reception_date": rec["Fecha"], "created_at": rec["_updated"],
            })
            _upsert(t, "receptions", _to_records(o),
                    ["import_id","pharmacy_id","import_purchase_condition_id","provider_id","import_provider_id","reception_date","created_at"],
                    ["import_purchase_condition_id","provider_id","import_provider_id","reception_date"])
            rec_map = _lookup(_read(t, f"SELECT id, import_id FROM receptions WHERE pharmacy_id={farma}"), "import_id", "id")

            # ── reception_detail ← Linearecep ──
            lr = _read(s, f"SELECT IdRecepcion, IdNLinea, XArt_IdArticu, Importe, ImportePuc, ImportePvp, palbaran, "
                          f"Pedidas, Recibidas, _updated FROM Linearecep JOIN Unit ON Linearecep.unit=Unit.id "
                          f"WHERE Unit.id_ecobuy IS NOT NULL AND Linearecep.unit={u_ex} AND Linearecep._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "reception_id": lr["IdRecepcion"].astype(str).map(rec_map),
                "product_id": lr["XArt_IdArticu"].astype(str).map(prod_map), "product_code": lr["XArt_IdArticu"],
                "line_num": lr["IdNLinea"], "line_amount": lr["Importe"], "product_puc": lr["ImportePuc"],
                "product_pvp": lr["ImportePvp"], "product_final_price": lr["palbaran"],
                "quantity_ordered": lr["Pedidas"].mask(lr["Pedidas"].astype(str) == "0", "1"),
                "quantity_recived": lr["Recibidas"], "created_at": lr["_updated"], "origin": "1",
            })
            _upsert(t, "reception_detail", _to_records(o),
                    ["pharmacy_id","reception_id","product_id","product_code","line_num","line_amount","product_puc","product_pvp","product_final_price","quantity_ordered","quantity_recived","created_at","origin"],
                    ["product_id","line_amount","product_puc","product_pvp","product_final_price","quantity_ordered","quantity_recived"])

            # ── orders ← Lineaventa (Venta header joined in SQL for date_insert) ──
            lv = _read(s, f"SELECT lv.IdVenta, lv.IdNLinea, lv.Codigo, lv.Descripcion, lv.Cantidad, lv.PVP, "
                          f"lv.ImporteBruto, lv.DescuentoLinea, lv.ImporteNeto, lv.TipoLinea, lv.TipoAportacion, "
                          f"lv._updated, v.FechaHora FROM Lineaventa lv JOIN Unit ON lv.unit=Unit.id "
                          f"LEFT JOIN Venta v ON v.unit=lv.unit AND v.IdVenta=lv.IdVenta "
                          f"WHERE Unit.id_ecobuy IS NOT NULL AND lv.unit={u_ex} AND lv._updated>='{fecha_desde}'")
            lv = lv[lv["Codigo"].astype(str).isin(prod_map)]     # orders FILTER: only matched products.code
            code = lv["Codigo"].astype(str)
            o = pd.DataFrame({
                "pharmacy_id": farma, "product_id": code.map(prod_map), "family_id": code.map(pfam),
                "superfamily_id": SFAM_DEFAULT_ID, "laboratory_id": code.map(plab).fillna(id_lab), "shop_id": 0,
                "order_id": lv["IdVenta"], "date_insert": lv["FechaHora"], "client_id": str(farma), "user_import_id": 0,
                "line_id": pd.to_numeric(lv["IdNLinea"], errors="coerce").fillna(0).astype(int),
                "product_code": lv["Codigo"], "product_description": lv["Descripcion"],
                "amount": pd.to_numeric(lv["Cantidad"], errors="coerce").fillna(0).astype(int),
                "pvp": lv["PVP"], "total_gross": lv["ImporteBruto"], "total_discount": lv["DescuentoLinea"],
                "total_net": lv["ImporteNeto"], "line_type": lv["TipoLinea"],
                "input_type": _empty_default(lv["TipoAportacion"], "0"), "created_at": lv["_updated"],
            })
            _upsert(t, "orders", _to_records(o),
                    ["pharmacy_id","product_id","family_id","superfamily_id","laboratory_id","shop_id","order_id","date_insert","client_id","user_import_id","line_id","product_code","product_description","amount","pvp","total_gross","total_discount","total_net","line_type","input_type","created_at"],
                    ["product_id","family_id","superfamily_id","laboratory_id","date_insert","amount","pvp","total_gross","total_discount","total_net","line_type","input_type"])

            # ── product_lists ← ListaArticu ──
            la = _read(s, f"SELECT IdLista, Descripcion, NumElem, Tipo, _updated FROM ListaArticu "
                          f"JOIN Unit ON ListaArticu.unit=Unit.id "
                          f"WHERE Unit.id_ecobuy IS NOT NULL AND ListaArticu.unit={u_ex} AND ListaArticu._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "import_id": pd.to_numeric(la["IdLista"], errors="coerce").fillna(0).astype(int),
                "description": la["Descripcion"],
                "numero_productos": pd.to_numeric(la["NumElem"], errors="coerce").fillna(0).astype(int),
                "import_status": pd.to_numeric(la["Tipo"], errors="coerce").fillna(0).astype(int),
                "status": 1, "created_at": la["_updated"],
            })
            _upsert(t, "product_lists", _to_records(o),
                    ["pharmacy_id","import_id","description","numero_productos","import_status","status","created_at"],
                    ["description","numero_productos","import_status"])
            plist_map = _lookup(_read(t, f"SELECT id, import_id FROM product_lists WHERE pharmacy_id={farma}"), "import_id", "id")

            # ── product_lists_products ← ItemListaArticu ──
            ila = _read(s, f"SELECT XItem_IdLista, XItem_IdArticu, _updated FROM ItemListaArticu "
                           f"JOIN Unit ON ItemListaArticu.unit=Unit.id "
                           f"WHERE Unit.id_ecobuy IS NOT NULL AND ItemListaArticu.unit={u_ex} AND ItemListaArticu._updated>='{fecha_desde}'")
            o = pd.DataFrame({
                "pharmacy_id": farma, "product_lists_id": ila["XItem_IdLista"].astype(str).map(plist_map),
                "product_lists_import_id": ila["XItem_IdLista"], "product_import_id": ila["XItem_IdArticu"],
                "product_id": ila["XItem_IdArticu"].astype(str).map(prod_map), "created_at": ila["_updated"],
            })
            _upsert(t, "product_lists_products", _to_records(o),
                    ["pharmacy_id","product_lists_id","product_lists_import_id","product_import_id","product_id","created_at"],
                    ["product_lists_id","product_id"])

            # ── per-unit post-load (inherently per-unit; lab fixups hoisted to finalize) ──
            # source-side synonym fix on Lineaventa
            _exec(s, f"UPDATE Lineaventa SET Codigo=IFNULL((SELECT MAX(IdArticu) FROM Sinonimo "
                     f"WHERE Sinonimo.Sinonimo=Lineaventa.Codigo AND Sinonimo.unit=Lineaventa.unit), Lineaventa.Codigo) "
                     f"WHERE Lineaventa.unit={u_ex}")
            # advance the watermark for this pharmacy
            _exec(t, f"UPDATE pharmacy_configs SET date_import='{datetime.now():%Y-%m-%d %H:%M:%S}' WHERE pharmacy_id={farma}")
            print(f"[{farma}] transactions done")
            return unit
        finally:
            s.close(); t.close()

    @task(trigger_rule="all_done")
    def finalize(results: list[dict]) -> None:
        """Per-pharmacy lab fixups, run once (set-based) over all processed pharmacies."""
        farmas = sorted({u["id_ecobuy"] for u in (results or []) if u})
        if not farmas:
            print("finalize: no pharmacies processed")
            return
        ids = ",".join(str(f) for f in farmas)
        t = _db(DST_CONN_ID)
        try:
            _exec(t, f"UPDATE reception_detail SET import_detail_id=id WHERE pharmacy_id IN ({ids})")
            _exec(t, f"DELETE A FROM laboratories A WHERE A.pharmacy_id IN ({ids}) AND LEFT(A.`code`,1)='0' "
                     f"AND A.id NOT IN (SELECT B.laboratory_import_id FROM products B WHERE B.pharmacy_id=A.pharmacy_id)")
            _exec(t, f"UPDATE laboratories B SET B.num_products=(SELECT count(1) FROM products A "
                     f"WHERE A.pharmacy_id=B.pharmacy_id AND A.laboratory_import_id=B.id) WHERE B.pharmacy_id IN ({ids})")
            print(f"finalize: fixups applied for {len(farmas)} pharmacy(ies)")
        finally:
            t.close()

    # ── Wire: per-unit master → transactions → finalize once ──
    units  = extract_units()
    master = process_master.expand(unit=units)
    tx     = process_transactions.expand(unit=master)
    finalize(tx)


ecobuy_etl()
