"""Small Excel workbook with two sheets, independent of DuckDB's writer."""

from __future__ import annotations

import tempfile
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring
from zipfile import ZIP_DEFLATED, ZipFile

from tenzir_test import fixture

SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _worksheet(rows: list[list[str | float | bool | None]]) -> bytes:
    worksheet = Element("worksheet", xmlns=SPREADSHEET_NS)
    data = SubElement(worksheet, "sheetData")
    for row_index, values in enumerate(rows, start=1):
        row = SubElement(data, "row", r=str(row_index))
        for column, value in enumerate(values):
            if value is None:
                continue
            cell = SubElement(row, "c", r=f"{chr(ord('A') + column)}{row_index}")
            if isinstance(value, str):
                cell.set("t", "inlineStr")
                SubElement(SubElement(cell, "is"), "t").text = value
            else:
                if isinstance(value, bool):
                    cell.set("t", "b")
                    value = int(value)
                SubElement(cell, "v").text = str(value)
    return tostring(worksheet, encoding="utf-8", xml_declaration=True)


@fixture()
def xlsx():
    with tempfile.TemporaryDirectory(prefix="tenzir-test-xlsx-") as directory:
        root = Path(directory).resolve()
        home = root / "home"
        home.mkdir()
        workbook = root / "assets.xlsx"
        with ZipFile(workbook, "w", compression=ZIP_DEFLATED) as archive:
            archive.writestr(
                "[Content_Types].xml",
                """<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>""",
            )
            archive.writestr(
                "_rels/.rels",
                """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
            )
            archive.writestr(
                "xl/workbook.xml",
                f"""<workbook xmlns="{SPREADSHEET_NS}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="Assets" sheetId="1" r:id="rId1"/><sheet name="Indicators" sheetId="2" r:id="rId2"/></sheets>
</workbook>""",
            )
            archive.writestr(
                "xl/_rels/workbook.xml.rels",
                """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>""",
            )
            archive.writestr(
                "xl/worksheets/sheet1.xml",
                _worksheet(
                    [
                        ["asset", "owner", "risk_score", "active"],
                        ["edge-01", "secops", 7.5, True],
                        ["edge-02", None, 3.0, False],
                    ]
                ),
            )
            archive.writestr(
                "xl/worksheets/sheet2.xml",
                _worksheet(
                    [
                        ["indicator", "kind"],
                        ["198.51.100.7", "ip"],
                        ["example.test", "domain"],
                    ]
                ),
            )
        # A clean home proves the bundled reader works without a downloaded
        # extension in the user's DuckDB cache.
        yield {"XLSX_PATH": str(workbook), "HOME": str(home)}
