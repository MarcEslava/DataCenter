import zipfile, io, re, xml.etree.ElementTree as ET
import pandas as pd

template = 'dags/templates/parafarmacia_template.xlsx'

# xlsxwriter generates our data sheets
buf = io.BytesIO()
writer = pd.ExcelWriter(buf, engine='xlsxwriter')
pd.DataFrame({'A':[1],'B':[2]}).to_excel(writer, sheet_name='Acuerdo book', index=False)
pd.DataFrame({'A':[1]}).to_excel(writer, sheet_name='Por Farmacia', index=False)
pd.DataFrame({'A':[1]}).to_excel(writer, sheet_name='Por Producto', index=False)
pd.DataFrame({'A':[1]}).to_excel(writer, sheet_name='Base Fee', index=False)
writer.close()
buf.seek(0)

NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
out = io.BytesIO()

with zipfile.ZipFile(buf, 'r') as zx, zipfile.ZipFile(template, 'r') as zt, zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zout:

    # --- shared strings: template first (TD sheets ref these indices), xlsxwriter appended ---
    tpl_ss = ET.fromstring(zt.read('xl/sharedStrings.xml'))
    xw_ss  = ET.fromstring(zx.read('xl/sharedStrings.xml'))
    tpl_count = len(tpl_ss.findall(f'{{{NS}}}si'))
    merged_sis = list(tpl_ss.findall(f'{{{NS}}}si')) + list(xw_ss.findall(f'{{{NS}}}si'))
    merged_count = len(merged_sis)
    merged_ss = (
        f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<sst xmlns="{NS}" count="{merged_count}" uniqueCount="{merged_count}">'
        + ''.join(ET.tostring(si, encoding='unicode') for si in merged_sis)
        + '</sst>'
    ).encode('utf-8')

    # Shift shared string indices in a worksheet by offset
    def _shift_ss(xml_bytes, offset):
        s = xml_bytes.decode('utf-8')
        result, pos = [], 0
        for m in re.finditer(r'<c\b([^>]*\bt="s"[^>]*)>(.*?)</c>', s, re.DOTALL):
            result.append(s[pos:m.start()])
            inner = re.sub(r'<v>(\d+)</v>', lambda x: f'<v>{int(x.group(1))+offset}</v>', m.group(2))
            result.append(f'<c{m.group(1)}>{inner}</c>')
            pos = m.end()
        result.append(s[pos:])
        return ''.join(result).encode('utf-8')

    # Map xlsxwriter sheet names to their file paths
    xw_wb_rels = zx.read('xl/_rels/workbook.xml.rels').decode()
    xw_wb_xml  = zx.read('xl/workbook.xml').decode()
    xw_sheet_files = {}
    for rid, tgt in re.findall(r'Id="(rId\d+)"[^>]+Target="(worksheets/sheet\d+\.xml)"', xw_wb_rels):
        xw_sheet_files[rid] = tgt
    xw_sheet_names = {}
    for name, rid in re.findall(r'name="([^"]+)"[^>]+r:id="(rId\d+)"', xw_wb_xml):
        if rid in xw_sheet_files:
            xw_sheet_names[name] = xw_sheet_files[rid]

    # Add Por Farmacia, Por Producto, Base Fee to template
    tpl_wb_rels = zt.read('xl/_rels/workbook.xml.rels').decode()
    tpl_wb_xml  = zt.read('xl/workbook.xml').decode()
    tpl_max_rid = max(int(m) for m in re.findall(r'Id="rId(\d+)"', tpl_wb_rels))
    tpl_max_sid = max(int(m) for m in re.findall(r'sheetId="(\d+)"', tpl_wb_xml))
    REL_WS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet'
    CT_WS  = 'application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml'

    extra_sheets = [('Por Farmacia', 4), ('Por Producto', 5), ('Base Fee', 6)]
    new_sheet_xml = new_rel_xml = new_ct_xml = ''
    for i, (sname, fnum) in enumerate(extra_sheets):
        rid   = f'rId{tpl_max_rid + 1 + i}'
        sid   = tpl_max_sid + 1 + i
        fname = f'worksheets/sheet{fnum}.xml'
        new_sheet_xml += f'<sheet name="{sname}" sheetId="{sid}" r:id="{rid}"/>'
        new_rel_xml   += f'<Relationship Id="{rid}" Type="{REL_WS}" Target="{fname}"/>'
        new_ct_xml    += f'<Override PartName="/xl/{fname}" ContentType="{CT_WS}"/>'

    # Copy template as-is, only patching what's necessary
    for item in zt.namelist():
        data = zt.read(item)

        if item == 'xl/worksheets/sheet1.xml':
            # Replace Acuerdo book data with xlsxwriter version
            xw_file = xw_sheet_names.get('Acuerdo book')
            if xw_file:
                data = _shift_ss(zx.read(f'xl/{xw_file}'), tpl_count)

        elif item == 'xl/sharedStrings.xml':
            data = merged_ss

        elif item == 'xl/workbook.xml':
            data = data.decode()
            data = data.replace('</sheets>', new_sheet_xml + '</sheets>')
            data = data.encode()

        elif item == 'xl/_rels/workbook.xml.rels':
            data = data.decode()
            data = data.replace('</Relationships>', new_rel_xml + '</Relationships>')
            data = data.encode()

        elif item == '[Content_Types].xml':
            data = data.decode()
            data = data.replace('</Types>', new_ct_xml + '</Types>')
            data = data.encode()

        elif item == 'xl/pivotCache/pivotCacheDefinition1.xml':
            # Only set refreshOnLoad — don't touch anything else
            s = data.decode('utf-8')
            if 'refreshOnLoad' not in s:
                s = re.sub(r'(<pivotCacheDefinition\b)', r'\1 refreshOnLoad="1"', s)
            data = s.encode('utf-8')

        # Pivot table XMLs: keep completely untouched

        zout.writestr(item, data)

    # Add extra data sheets from xlsxwriter
    for sname, fnum in extra_sheets:
        xw_file = xw_sheet_names.get(sname)
        if xw_file:
            data = _shift_ss(zx.read(f'xl/{xw_file}'), tpl_count)
            zout.writestr(f'xl/worksheets/sheet{fnum}.xml', data)

out.seek(0)
with open('test_pivot3.xlsx', 'wb') as f:
    f.write(out.read())
print('Done')
