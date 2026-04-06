"""Main normalizer: parse any supported file into chunks.

Integration note: ``normalize_file`` is the integration entry point for the
Ingest service.  ``normalize_directory`` is a convenience wrapper for local
development scripts.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .chunker import Chunk, chunk_elements
from .parse_docx import parse_docx
from .parse_eml import parse_eml
from .parse_pdf import parse_pdf
from .parse_xlsx import parse_xlsx

log = logging.getLogger(__name__)

PARSERS = {
    ".xlsx": parse_xlsx,
    ".xls": parse_xlsx,
    ".docx": parse_docx,
    ".pdf": parse_pdf,
    ".eml": parse_eml,
}


def normalize_file(
    path: Path | str,
    document_id: str | None = None,
    token_budget: int = 512,
) -> list[Chunk]:
    """Parse a file and return a list of chunks ready for indexing.

    Args:
        path: Filesystem path to the document.
        document_id: Unique identifier for this document.  When ``None``,
            falls back to the filename (suitable for local dev only).
        token_budget: Max tokens per chunk.
    """
    path = Path(path)

    suffix = path.suffix.lower()
    parser = PARSERS.get(suffix)
    if parser is None:
        raise ValueError(f"Unsupported file format: {suffix}")

    elements = parser(path)

    source_format = suffix.lstrip(".")
    chunks = chunk_elements(
        elements=elements,
        document_id=document_id or path.name,
        source_format=source_format,
        token_budget=token_budget,
    )

    return chunks


def normalize_directory(
    directory: Path | str,
    token_budget: int = 512,
) -> tuple[list[Chunk], list[str]]:
    """Parse all supported files in a directory.

    Returns:
        A tuple of (chunks, failures) where failures is a list of filenames
        that could not be parsed.
    """
    directory = Path(directory)
    all_chunks: list[Chunk] = []
    failures: list[str] = []

    for path in sorted(directory.iterdir()):
        if path.suffix.lower() in PARSERS:
            try:
                chunks = normalize_file(path, token_budget=token_budget)
                all_chunks.extend(chunks)
            except Exception:
                log.exception("Failed to parse %s", path.name)
                failures.append(path.name)

    return all_chunks, failures
