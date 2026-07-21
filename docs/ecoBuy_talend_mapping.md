# ecoBuy — Talend → Airflow port reference (exact mapping)

Extracted from the Talend job `Ejecuta_Fases_ecobuy` (`Carga_Incremental/Nueva_Carga`),
child jobs `ecoBuy_Mapeo_1` (master) and `ecoBuy_Mapeo_2` (transactions).

## Architecture

| | Source | Target |
|---|---|---|
| DB | **`ecoextract`** (Farmatic mirror, Spanish tables) | **`ecobuy`** (normalized, English) |
| Host | `10.20.10.6` — **via SSH tunnel** (context `ssh_1`/`ssh_2`) | `10.20.10.4:3306` — direct |
| Row scope | filtered by `unit` + `_updated` watermark | keyed by `pharmacy_id` |

**Per-unit contexts** (set by orchestrator from `SELECT id, id_ecobuy FROM Unit WHERE ecobuy=2`):
- `id_UNIT_ex` = ecoextract `Unit.id` (source filter)
- `id_ecobuy_ex` = ecoextract `Unit.id_ecobuy`
- `id_API_farma` = ecobuy `pharmacy_id` (target)
- `id_LAB_farma` = default laboratory fallback id (e.g. 3138)
- `Fecha_Desde` = incremental watermark (`_updated >= Fecha_Desde`)

**Upsert pattern:** each entity reads the source rows AND the existing target rows, a `tMap`
joins them on the natural key, and splits into an **INS** flow (no target match → bulk insert)
and an **UPD** flow (match → update). Loading is `tMysqlOutputBulk` + `…BulkExec` (LOAD DATA).

---

## Phase 1 — Mapeo_1 (master data)

### providers  ← `Proveedor`
```sql
SELECT IDPROVEEDOR, IF(FIS_NIF="","0R",FIS_NIF) AS Nif, FIS_NOMBRE, Unit.id_ecobuy, _updated
FROM Proveedor JOIN Unit ON Proveedor.unit=Unit.id
WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy = {id_ecobuy_ex}
```
→ `providers(import_id=IDPROVEEDOR, pharmacy_id=id_API_farma, cif=Nif, name=FIS_NOMBRE, created_at/updated_at=_updated)`

### superfamilies  ← `SuperFamilia`
`SELECT SuperFamilia.*, Unit.id_ecobuy … WHERE Unit.id_ecobuy={id_ecobuy_ex}`
→ `superfamilies(pharmacy_id=id_API_farma, origin_id=IdSuperFamilia, description=Descripcion, status, created_at/updated_at=_updated)`

### families  ← `Familia` + `FamiliaAux` + lookup `superfamilies`
`SELECT Familia.*, Unit.id_ecobuy … ` joined to `FamiliaAux` (IdFamilia→IdSuperFamilia) and to target `superfamilies` (to resolve `superfamily_id`).
→ `families(pharmacy_id=id_API_farma, superfamily_id=<superfamilies.id>, origin_id=IdFamilia, description=Descripcion, status=1, created_at/updated_at=_updated)`

### laboratories  ← `Laboratorio` (numeric codes) + `LABOR` (alpha codes)
- Numeric: `SELECT Laboratorio.*, Unit.id_ecobuy … WHERE LEFT(Codigo,1) NOT BETWEEN 'A' AND 'Z' AND Unit.id={id_UNIT_ex}`
- Alpha (`LABOR`, with COALESCE-to-'.' cleanup): `SELECT CODIGO, IFNULL(TIPO,'1'), CASE WHEN NOMBRE='' … END, … FROM LABOR WHERE LEFT(CODIGO,1) BETWEEN 'A' AND 'Z'`
→ `laboratories(pharmacy_id=id_API_farma, code=Codigo/CODIGO, name, address, location, region, cp, phone, fax, created_at/updated_at)`  (cp forced `0` on alpha/update)

### iva  ← `Tablaiva`
`SELECT Tablaiva.*, Unit.id_ecobuy … WHERE Tablaiva.unit={id_UNIT_ex}`
→ `iva(pharmacy_id=id_API_farma, import_id=Numeric.sequence("s1",1000,100), id_tipo_art=IdTipoArt, id_tipo_cli=IdTipoCli, id_tipo_pro=IdTipoPro, description=Descripcion, iva=Piva, recargo_equivalencia=PReq, created_at/updated_at=_updated)`

### iva_groups  ← `Grupoiva`  (target DB name is `eco-buy`.`iva_groups`)
`SELECT Grupoiva.*, Unit.id_ecobuy … WHERE Grupoiva.unit={id_UNIT_ex}`
→ `iva_groups(pharmacy_id=id_API_farma, import_id=seq, id_grupo_iva=IdGrupoIva, descripcion=Descripcion, tipo=Tipo, created_at/updated_at=_updated)`

### Default rows / fixups (tDBRow, run once per unit)
```sql
INSERT INTO laboratories (pharmacy_id, code, name, address, location, region, CP, latitude, longitude, phone, fax)
  VALUES ({id_API_farma}, '99999', 'SIN ASIGNAR', '.','.','.',0,0,0,'.','.');
INSERT INTO superfamilies (pharmacy_id, origin_id, description)
  VALUES ({id_API_farma}, '99999', 'Sin Asignar');
-- fill missing FamiliaAux with superfamily 99999
INSERT INTO FamiliaAux (unit, idFamilia, IdSuperFamilia)
  SELECT {id_UNIT_ex}, Familia.IdFamilia, '99999'
  FROM Familia LEFT JOIN FamiliaAux ON Familia.IdFamilia=FamiliaAux.IdFamilia
  WHERE Familia.unit={id_UNIT_ex} AND FamiliaAux.idFamilia IS NULL;
```

---

## Phase 2 — Mapeo_2 (transactions, incremental `_updated >= Fecha_Desde`)

### products  ← `Articu` (+ lookups families, laboratories)
```sql
SELECT unit, idArticu, Descripcion, Presentacion, Situacion, XFam_idFamilia,
  IF(Laboratorio='', '{id_LAB_farma}', IFNULL(Laboratorio,'{id_LAB_farma}')) AS Labora1,
  Laboratorio, Pvp, PvpAux, Puc, Pmc, StockActual, StockMinimo, StockMaximo, LoteOptimo,
  FechaUltimaEntrada, FechaUltimaSalida, FechaCaducidad, ProveedorHabitual, CarteraHabitual,
  XGrup_idGrupoIva, Efp, Receta, Unit.id_ecobuy, _updated
FROM Articu JOIN Unit ON Articu.unit=Unit.id
WHERE Unit.id_ecobuy IS NOT NULL AND Unit.id_ecobuy = {id_ecobuy_ex}
```
→ `products(pharmacy_id=id_API_farma, code=IdArticu, description, pvp=Pvp, pvp_aux=PvpAux,
  stock = StockActual=="" ? 0 : int(StockActual),
  family_import_id = <families.id lookup>,
  laboratory_import_id = <laboratories.id lookup> == 0 ? id_LAB_farma : <laboratories.id>,
  created_at/updated_at=_updated,
  iva = XGrup_IdGrupoIva: '01'→0, '02'→4.5, '03'→11.4, '04'→26.2, else 0)`
INS path also sets `ean`, `laboratory_internal_id` (blank).

### orders  ← `Lineaventa` + `Venta` (header) + lookups products, families, superfamilies
```sql
SELECT Lineaventa.*, Unit.id_ecobuy FROM Lineaventa JOIN Unit …
WHERE Lineaventa.unit={id_UNIT_ex} AND Lineaventa._updated >= '{Fecha_Desde}'
-- header: SELECT Venta.*, Unit.id_ecobuy FROM Venta … WHERE Venta.unit={id_UNIT_ex}
```
FILTER: only rows where `products.code` matched.
→ `orders(pharmacy_id=id_API_farma, product_id=<products.id or null>, family_id=products.family_import_id,
  superfamily_id = <SFamilies.id> ?? 171422, laboratory_id=<products.laboratory_import_id or id_LAB_farma>,
  shop_id=0, order_id=IdVenta, date_insert=Venta.FechaHora, client_id=id_API_farma, user_import_id=0,
  line_id=int(IdNLinea), product_code=Codigo, product_description=Descripcion, amount=int(Cantidad),
  pvp=PVP, total_gross=ImporteBruto, total_discount=DescuentoLinea, total_net=ImporteNeto,
  line_type=TipoLinea, input_type = TipoAportacion=="" ? "0" : TipoAportacion, created_at=_updated)`

### purchases  ← `Pedido`
```sql
SELECT Pedido.*, Unit.id_ecobuy FROM Pedido JOIN Unit …
WHERE Pedido.unit={id_UNIT_ex} AND Pedido._updated >= '{Fecha_Desde}'
```
→ `purchases(pharmacy_id=id_API_farma, user_id="0", type=Tipo, status=4, name="", copy_id="",
  custom_days_purchase=0, import_id=IdPedido, purchase_date=Fecha, puc=ImportePuc, pvp=ImportePvp,
  lines=NLineas, delegate="", provider_import_id=XProv_IdProveedor, created_at=_updated)`

### purchase_details  ← `LineaPedido` + lookups purchases, products
```sql
SELECT LineaPedido.*, Unit.id_ecobuy FROM LineaPedido JOIN Unit …
WHERE LineaPedido.unit={id_UNIT_ex} AND LineaPedido._updated >= '{Fecha_Desde}'
```
→ `purchase_details(pharmacy_id=id_API_farma, purchase_id=<purchases.id>,
  purchase_import_id=<purchases.import_id> ?? "-", product_id=<products.id>,
  quantity=Unidades ?? "0", line_id=IdLinea, puc=Puc, pvp=Pvp, product_code=XArt_IdArticu,
  created_at=_updated, origin="ecoextract", status=1)`

### receptions  ← `Recep` + lookup providers
```sql
SELECT Recep.*, Unit.id_ecobuy FROM Recep JOIN Unit …
WHERE Recep.unit={id_UNIT_ex} AND Recep._updated >= '{Fecha_Desde}'
```
→ `receptions(import_id=IdRecepcion, pharmacy_id=id_API_farma, import_purchase_condition_id=XCPro_IdCondicion,
  provider_id=<providers.id>, import_provider_id = XProv_IdProveedor=="" ? "9999" : XProv_IdProveedor,
  reception_date=Fecha, created_at=_updated)`

### reception_detail  ← `Linearecep` + lookups receptions, products
```sql
SELECT Linearecep.*, Unit.id_ecobuy FROM Linearecep JOIN Unit …
WHERE Linearecep.unit={id_UNIT_ex} AND Linearecep._updated >= '{Fecha_Desde}'
```
→ `reception_detail(pharmacy_id=id_API_farma, reception_id=<receptions.id>, product_id=<products.id>,
  product_code=XArt_IdArticu, line_num=IdNLinea, line_amount=Importe, product_puc=ImportePuc,
  product_pvp=ImportePvp, product_final_price=palbaran, quantity_ordered = Pedidas=="0" ? "1" : Pedidas,
  quantity_recived=Recibidas, created_at=_updated, origin="1")`

### product_lists  ← `ListaArticu`
```sql
SELECT ListaArticu.*, Unit.id_ecobuy FROM ListaArticu JOIN Unit …
WHERE ListaArticu.unit={id_UNIT_ex} AND ListaArticu._updated >= '{Fecha_Desde}'
```
→ `product_lists(pharmacy_id=id_API_farma, import_id=IdLista, description=Descripcion,
  numero_productos=NumElem ?? 0, import_status=Tipo, status, created_at=_updated)`

### product_lists_products  ← `ItemListaArticu` + lookups product_lists, products
```sql
SELECT ItemListaArticu.*, Unit.id_ecobuy FROM ItemListaArticu JOIN Unit …
WHERE ItemListaArticu.unit={id_UNIT_ex} AND ItemListaArticu._updated >= '{Fecha_Desde}'
```
→ `product_lists_products(pharmacy_id=id_API_farma, product_lists_id=<product_lists.id>,
  product_lists_import_id=XItem_IdLista, product_import_id=XItem_IdArticu, product_id=<products.id>, created_at=_updated)`

### Post-load SQL (tDBRow, after the loads)
```sql
UPDATE reception_detail SET import_detail_id=id WHERE pharmacy_id={id_API_farma};
UPDATE pharmacy_configs SET date_import='{now YYYY/MM/DD HH:mm:ss}' WHERE pharmacy_id={id_API_farma};
DELETE A.* FROM laboratories A WHERE A.pharmacy_id={id_API_farma} AND LEFT(A.code,1)='0'
  AND A.id NOT IN (SELECT B.laboratory_import_id FROM products B WHERE B.pharmacy_id={id_API_farma});
UPDATE laboratories B SET B.num_products=(SELECT count(1) FROM products A
  WHERE A.pharmacy_id={id_API_farma} AND A.laboratory_import_id=B.id) WHERE B.pharmacy_id={id_API_farma};
-- source-side synonym fix (runs on ecoextract):
UPDATE Lineaventa SET Codigo=IFNULL((SELECT MAX(IdArticu) FROM Sinonimo
  WHERE Sinonimo.Sinonimo=Lineaventa.Codigo AND Sinonimo.unit=Lineaventa.unit), Lineaventa.Codigo)
  WHERE Lineaventa.unit={id_UNIT_ex};
```

### IVA code → rate map (used in products.iva)
`'01'→0, '02'→4.5, '03'→11.4, '04'→26.2, else 0`

---

## Notes for the Airflow port
- Driver: `SELECT id, id_ecobuy FROM Unit WHERE ecobuy=2` → dynamic-map per unit (like `novedades_SKU`).
- Source (`ecoextract`) reached via the existing `db-tunnel` (no `tSshTunnel` needed).
- Two connections: `ecoextract` (source, MySQL) and `ecobuy` (target, MySQL).
- Watermark `Fecha_Desde` per unit — needs a home (Airflow Variable, or a control table).
- `import_id` sequences (`Numeric.sequence("s1",1000,100)`) for iva/iva_groups — replace with a real sequence/auto id.
- Bulk `LOAD DATA` → replace with pandas `to_sql` / executemany batch upsert.
