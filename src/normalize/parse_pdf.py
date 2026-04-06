"""Parse PDF files into structural elements."""

from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

from .elements import Element, HeadingElement, KVPairElement, TableRowElement, TextElement

_KV_PATTERN = re.compile(r"^(.+?):\s+(.+)$")

_HEADING_PATTERN = re.compile(
    r"^[A-Z][A-Z &/;'\-]{2,}$"
)


def _is_heading_line(line: str) -> bool:
    """Detect structural headings: ALL CAPS lines like 'LIFE INSURANCE', 'AD&D;'."""
    return bool(_HEADING_PATTERN.match(line.strip()))


def _is_major_heading(line: str) -> bool:
    """Detect top-level structural breaks like 'Plan Design - Executives'."""
    stripped = line.strip()
    if " - " in stripped and len(stripped) < 100:
        parts = stripped.split(" - ", 1)
        return len(parts) == 2 and len(parts[0]) > 2 and len(parts[1]) > 2
    return False


def _classify_line(line: str, page_number: int) -> Element:
    """Classify a single text line into the appropriate element type."""
    stripped = line.strip()

    if _is_major_heading(stripped):
        return HeadingElement(text=stripped, level=1, page_number=page_number)

    if _is_heading_line(stripped):
        return HeadingElement(text=stripped, level=2, page_number=page_number)

    kv_match = _KV_PATTERN.match(stripped)
    if kv_match:
        key, value = kv_match.group(1).strip(), kv_match.group(2).strip()
        return KVPairElement(key=key, value=value, page_number=page_number)

    return TextElement(text=stripped, page_number=page_number)


def parse_pdf(path: Path) -> list[Element]:
    elements: list[Element] = []

    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []

            for table_data in tables:
                if not table_data or len(table_data) < 2:
                    continue

                headers = [str(c).strip() if c else "" for c in table_data[0]]

                for row_idx, row in enumerate(table_data[1:], start=1):
                    cells = [str(c).strip() if c else "" for c in row]
                    if not any(cells):
                        continue

                    non_empty = sum(1 for c in cells if c)
                    is_section_break = non_empty == 1 and cells[0] != ""

                    elements.append(
                        TableRowElement(
                            cells=cells,
                            headers=headers,
                            is_section_break=is_section_break,
                            page_number=page_num,
                            row_index=row_idx,
                        )
                    )

            text = page.extract_text() or ""
            if tables:
                for table_data in tables:
                    for row in table_data:
                        for cell in row:
                            if cell:
                                text = text.replace(str(cell), "", 1)

            for line in text.split("\n"):
                line = line.strip()
                if line:
                    elements.append(_classify_line(line, page_num))

    return elements
