"""Minimal .xlsx reader using only the Python standard library (no openpyxl)."""

from __future__ import annotations

import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_NS_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_NS_OFFICE_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def _tag(ns: str, local: str) -> str:
    return f"{{{ns}}}{local}"


def _col_letters_to_index(col: str) -> int:
    n = 0
    for ch in col.upper():
        n = n * 26 + (ord(ch) - 64)
    return n


def _parse_cell_ref(ref: str) -> Tuple[int, int]:
    m = re.match(r"^([A-Z]+)(\d+)$", (ref or "").upper())
    if not m:
        raise ValueError(f"Invalid cell ref: {ref!r}")
    return _col_letters_to_index(m.group(1)), int(m.group(2))


def _parse_range(cell_range: str) -> Tuple[int, int, int, int]:
    """Return min_col, min_row, max_col, max_row (1-based)."""
    part = cell_range.strip().upper()
    if ":" in part:
        a, b = part.split(":", 1)
    else:
        a = b = part
    c1, r1 = _parse_cell_ref(a)
    c2, r2 = _parse_cell_ref(b)
    return min(c1, c2), min(r1, r2), max(c1, c2), max(r1, r2)


def _read_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        data = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(data)
    out: List[str] = []
    for si in root.findall(_tag(_NS_MAIN, "si")):
        parts: List[str] = []
        t = si.find(_tag(_NS_MAIN, "t"))
        if t is not None and t.text is not None:
            parts.append(t.text)
        else:
            for r in si.findall(_tag(_NS_MAIN, "r")):
                rt = r.find(_tag(_NS_MAIN, "t"))
                if rt is not None and rt.text:
                    parts.append(rt.text)
        out.append("".join(parts))
    return out


def _resolve_sheet_path(zf: zipfile.ZipFile, sheet_name: str) -> str:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets = wb.find(_tag(_NS_MAIN, "sheets"))
    if sheets is None:
        raise RuntimeError("workbook.xml has no sheets")
    target_rid: Optional[str] = None
    for sh in sheets.findall(_tag(_NS_MAIN, "sheet")):
        name = sh.get("name") or ""
        if name.upper() == sheet_name.upper():
            target_rid = sh.get(f"{{{_NS_OFFICE_REL}}}id") or sh.get("id")
            break
    if not target_rid:
        names = [sh.get("name") for sh in sheets.findall(_tag(_NS_MAIN, "sheet"))]
        raise RuntimeError(f"Sheet {sheet_name!r} not found. Available: {names}")

    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    sheet_target: Optional[str] = None
    for rel in rels.findall(_tag(_NS_REL, "Relationship")):
        if rel.get("Id") == target_rid:
            sheet_target = rel.get("Target")
            break
    if not sheet_target:
        raise RuntimeError(f"Could not resolve sheet path for {sheet_name!r}")
    if not sheet_target.startswith("xl/"):
        sheet_target = "xl/" + sheet_target.lstrip("/")
    return sheet_target


def _cell_value(c: ET.Element, shared: List[str]) -> Any:
    cell_type = c.get("t")
    if cell_type == "s":
        v = c.find(_tag(_NS_MAIN, "v"))
        if v is None or v.text is None:
            return None
        try:
            return shared[int(v.text)]
        except (ValueError, IndexError):
            return v.text
    if cell_type == "inlineStr":
        is_el = c.find(_tag(_NS_MAIN, "is"))
        if is_el is None:
            return None
        t = is_el.find(_tag(_NS_MAIN, "t"))
        return t.text if t is not None else None
    v = c.find(_tag(_NS_MAIN, "v"))
    if v is None or v.text is None:
        return None
    text = v.text.strip()
    try:
        if "." in text or "e" in text.lower():
            return float(text)
        return int(text)
    except ValueError:
        return text


def read_sheet_range(
    workbook_path: Path,
    sheet_name: str,
    cell_range: str,
) -> List[List[Any]]:
    """
    Read a rectangular range from a .xlsx sheet.
    Returns rows as lists of cell values (left to right).
    """
    min_col, min_row, max_col, max_row = _parse_range(cell_range)
    width = max_col - min_col + 1
    grid: Dict[Tuple[int, int], Any] = {}

    with zipfile.ZipFile(workbook_path, "r") as zf:
        shared = _read_shared_strings(zf)
        sheet_path = _resolve_sheet_path(zf, sheet_name)
        root = ET.fromstring(zf.read(sheet_path))
        sheet_data = root.find(_tag(_NS_MAIN, "sheetData"))
        if sheet_data is None:
            return [[None] * width for _ in range(max_row - min_row + 1)]

        for row_el in sheet_data.findall(_tag(_NS_MAIN, "row")):
            for c in row_el.findall(_tag(_NS_MAIN, "c")):
                ref = c.get("r")
                if not ref:
                    continue
                col_i, row_i = _parse_cell_ref(ref)
                if row_i < min_row or row_i > max_row or col_i < min_col or col_i > max_col:
                    continue
                grid[(row_i, col_i)] = _cell_value(c, shared)

    rows_out: List[List[Any]] = []
    for r in range(min_row, max_row + 1):
        rows_out.append([grid.get((r, c)) for c in range(min_col, max_col + 1)])
    return rows_out
