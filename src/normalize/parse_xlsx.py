"""Parse XLSX files into structural elements."""

from __future__ import annotations

from pathlib import Path

import openpyxl

from .elements import Element, HeadingElement, KVPairElement, TableRowElement


def _is_likely_kv_layout(ws, max_sample: int = 20) -> bool:
    """Detect if a sheet uses a two-column key-value layout.

    Heuristic: exactly 2 non-empty columns, first column values are mostly
    unique strings, second column has values.
    """
    col_count = ws.max_column
    if col_count != 2:
        return False

    keys = []
    for row in ws.iter_rows(min_row=1, max_row=min(ws.max_row, max_sample), values_only=True):
        k, v = row
        if k is not None:
            keys.append(str(k).strip())

    if len(keys) < 2:
        return False
    unique_ratio = len(set(keys)) / len(keys)
    return unique_ratio > 0.8


def _find_header_row(ws, max_scan: int = 10) -> tuple[int, list[str]]:
    """Find the first non-empty row to use as column headers."""
    for row_idx, row in enumerate(
        ws.iter_rows(min_row=1, max_row=min(ws.max_row, max_scan), values_only=True),
        start=1,
    ):
        values = [str(c).strip() if c is not None else "" for c in row]
        non_empty = sum(1 for v in values if v)
        if non_empty >= 2:
            return row_idx, values
    return 1, []


def parse_xlsx(path: Path) -> list[Element]:
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    elements: list[Element] = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        elements.append(
            HeadingElement(text=sheet_name, level=1, sheet_name=sheet_name)
        )

        if _is_likely_kv_layout(ws):
            for row in ws.iter_rows(values_only=True):
                k = str(row[0]).strip() if row[0] is not None else ""
                v = str(row[1]).strip() if row[1] is not None else ""
                if k:
                    elements.append(
                        KVPairElement(key=k, value=v, sheet_name=sheet_name)
                    )
        else:
            header_row_idx, headers = _find_header_row(ws)

            for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
                if row_idx <= header_row_idx:
                    continue

                cells = [str(c).strip() if c is not None else "" for c in row]

                if not any(cells):
                    continue

                non_empty_count = sum(1 for c in cells if c)
                is_section_break = non_empty_count == 1 and cells[0] != ""

                elements.append(
                    TableRowElement(
                        cells=cells,
                        headers=headers,
                        is_section_break=is_section_break,
                        sheet_name=sheet_name,
                        row_index=row_idx,
                    )
                )

    wb.close()
    return elements
