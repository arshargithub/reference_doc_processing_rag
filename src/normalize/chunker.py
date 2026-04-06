"""Merge structural elements into composite chunks.

Walks through the flat element list and accumulates elements into chunks,
flushing when a structural boundary is hit or the token budget is exceeded.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import tiktoken

from .elements import Element, HeadingElement, KVPairElement, TableRowElement, TextElement

_encoder = tiktoken.get_encoding("cl100k_base")

DEFAULT_TOKEN_BUDGET = 512


def _estimate_tokens(text: str) -> int:
    return len(_encoder.encode(text))


@dataclass
class Chunk:
    chunk_id: str
    document_id: str
    chunk_type: str
    search_text: str
    source_format: str
    token_estimate: int
    metadata: dict = field(default_factory=dict)
    embedding: list[float] | None = None


def _is_boundary(prev: Element | None, curr: Element) -> bool:
    """Determine if the current element starts a new chunk."""
    if prev is None:
        return False

    if isinstance(curr, HeadingElement):
        return True

    if isinstance(curr, TableRowElement) and curr.is_section_break:
        return True

    # Transition between element types
    if type(prev) != type(curr):
        return True

    # Boundary between email headers and non-header KV pairs
    if isinstance(prev, KVPairElement) and isinstance(curr, KVPairElement):
        if prev.metadata.get("is_email_header") != curr.metadata.get("is_email_header"):
            return True

    # Different table or sheet
    prev_table = prev.metadata.get("table_index")
    curr_table = curr.metadata.get("table_index")
    if prev_table is not None and curr_table is not None and prev_table != curr_table:
        return True

    prev_sheet = prev.metadata.get("sheet_name")
    curr_sheet = curr.metadata.get("sheet_name")
    if prev_sheet is not None and curr_sheet is not None and prev_sheet != curr_sheet:
        return True

    return False


def _determine_chunk_type(elements: list[Element]) -> str:
    types = {type(e) for e in elements}
    if types == {KVPairElement} or (KVPairElement in types and len(types) == 1):
        if all(e.metadata.get("is_email_header") for e in elements):
            return "email_header"
        return "kv_group"
    if TableRowElement in types:
        return "table_chunk"
    if HeadingElement in types and len(types) == 1:
        return "text"
    return "text"


def _merge_metadata(elements: list[Element]) -> dict:
    """Collect metadata from all elements, keeping first/last values for ranges."""
    merged: dict = {}
    for elem in elements:
        for k, v in elem.metadata.items():
            if k not in merged:
                merged[k] = v

    first_row = elements[0].metadata.get("row_index")
    last_row = elements[-1].metadata.get("row_index")
    if first_row is not None and last_row is not None:
        merged["row_index_start"] = first_row
        merged["row_index_end"] = last_row
    merged.pop("row_index", None)

    return merged


def chunk_elements(
    elements: list[Element],
    document_id: str,
    source_format: str,
    token_budget: int = DEFAULT_TOKEN_BUDGET,
) -> list[Chunk]:
    """Merge elements into composite chunks respecting boundaries and budget."""
    if not elements:
        return []

    chunks: list[Chunk] = []
    buffer: list[Element] = []
    buffer_tokens: int = 0
    section_stack: list[tuple[int, str]] = []

    def _section_label() -> str:
        return " > ".join(text for _, text in section_stack)

    def _push_section(level: int, text: str) -> None:
        while section_stack and section_stack[-1][0] >= level:
            section_stack.pop()
        section_stack.append((level, text))

    def flush():
        nonlocal buffer, buffer_tokens
        if not buffer:
            return

        label = _section_label()
        texts = []
        if label:
            texts.append(f"[{label}]")
        texts.extend(elem.text for elem in buffer)
        search_text = "\n".join(texts)

        meta = _merge_metadata(buffer)
        if label:
            meta["section_label"] = label

        chunk = Chunk(
            chunk_id=str(uuid.uuid4()),
            document_id=document_id,
            chunk_type=_determine_chunk_type(buffer),
            search_text=search_text,
            source_format=source_format,
            token_estimate=_estimate_tokens(search_text),
            metadata=meta,
        )
        chunks.append(chunk)
        buffer = []
        buffer_tokens = 0

    for elem in elements:
        if isinstance(elem, HeadingElement):
            flush()
            _push_section(elem.level, elem.text)
            buffer.append(elem)
            buffer_tokens += _estimate_tokens(elem.text)
            flush()
            continue

        if isinstance(elem, TableRowElement) and elem.is_section_break:
            flush()
            _push_section(2, elem.cells[0].strip())
            continue

        elem_tokens = _estimate_tokens(elem.text)

        if _is_boundary(buffer[-1] if buffer else None, elem):
            flush()

        if buffer_tokens + elem_tokens > token_budget and buffer:
            flush()

        buffer.append(elem)
        buffer_tokens += elem_tokens

    flush()
    return chunks
