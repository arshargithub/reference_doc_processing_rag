"""Main normalizer: parse any supported file into chunks."""

from __future__ import annotations

from pathlib import Path

from .chunker import Chunk, chunk_elements
from .parse_docx import parse_docx
from .parse_eml import parse_eml
from .parse_pdf import parse_pdf
from .parse_xlsx import parse_xlsx

PARSERS = {
    ".xlsx": parse_xlsx,
    ".xls": parse_xlsx,
    ".docx": parse_docx,
    ".pdf": parse_pdf,
    ".eml": parse_eml,
}


def normalize_file(path: Path | str, token_budget: int = 512) -> list[Chunk]:
    """Parse a file and return a list of chunks ready for indexing."""
    path = Path(path)

    suffix = path.suffix.lower()
    parser = PARSERS.get(suffix)
    if parser is None:
        raise ValueError(f"Unsupported file format: {suffix}")

    elements = parser(path)

    source_format = suffix.lstrip(".")
    chunks = chunk_elements(
        elements=elements,
        document_id=path.name,
        source_format=source_format,
        token_budget=token_budget,
    )

    return chunks


def normalize_directory(directory: Path | str, token_budget: int = 512) -> list[Chunk]:
    """Parse all supported files in a directory."""
    directory = Path(directory)
    all_chunks: list[Chunk] = []

    for path in sorted(directory.iterdir()):
        if path.suffix.lower() in PARSERS:
            chunks = normalize_file(path, token_budget=token_budget)
            all_chunks.extend(chunks)

    return all_chunks
