"""Structural elements extracted from documents.

Every parser produces a flat list of these elements. The segmenter then
merges them into composite chunks based on structural boundaries and
token budgets.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Element:
    """Base structural element from a parsed document."""

    element_type: str
    text: str
    metadata: dict = field(default_factory=dict)


@dataclass
class TextElement(Element):
    """A paragraph or block of narrative text."""

    def __init__(self, text: str, **metadata):
        super().__init__(element_type="text", text=text, metadata=metadata)


@dataclass
class HeadingElement(Element):
    """A heading or title."""

    level: int = 1

    def __init__(self, text: str, level: int = 1, **metadata):
        super().__init__(element_type="heading", text=text, metadata=metadata)
        self.level = level


@dataclass
class TableRowElement(Element):
    """A single row from a table or spreadsheet."""

    headers: list[str] = field(default_factory=list)
    cells: list[str] = field(default_factory=list)
    is_section_break: bool = False

    def __init__(
        self,
        cells: list[str],
        headers: list[str] | None = None,
        is_section_break: bool = False,
        **metadata,
    ):
        text = self._build_text(cells, headers)
        super().__init__(element_type="table_row", text=text, metadata=metadata)
        self.headers = headers or []
        self.cells = cells
        self.is_section_break = is_section_break

    @staticmethod
    def _build_text(cells: list[str], headers: list[str] | None) -> str:
        if headers and len(headers) == len(cells):
            pairs = [f"{h}: {v}" for h, v in zip(headers, cells) if v.strip()]
            return " | ".join(pairs)
        return " | ".join(c for c in cells if c.strip())


@dataclass
class KVPairElement(Element):
    """A key-value pair."""

    key: str = ""
    value: str = ""

    def __init__(self, key: str, value: str, **metadata):
        super().__init__(
            element_type="kv_pair", text=f"{key}: {value}", metadata=metadata
        )
        self.key = key
        self.value = value
