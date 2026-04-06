"""End-to-end pipeline: normalize files, embed, and index into Elasticsearch."""

from __future__ import annotations

import logging
from pathlib import Path

from src.normalize.normalize import normalize_directory
from .embedder import Embedder
from .es_index import bulk_index_chunks, create_index, get_client

log = logging.getLogger(__name__)


def index_directory(
    directory: str | Path,
    request_id: str,
    es_url: str = "http://localhost:9200",
    model_path: str = "models/bge-large-en-v1.5",
    token_budget: int = 512,
    recreate_index: bool = False,
) -> dict:
    """Normalize, embed, and index all files in a directory.

    Returns stats about the indexing operation.
    """
    log.info("Normalizing files in %s", directory)
    chunks, parse_failures = normalize_directory(directory, token_budget=token_budget)
    doc_count = len(set(c.document_id for c in chunks))
    log.info("%d chunks from %d documents (%d parse failures)", len(chunks), doc_count, len(parse_failures))

    if not chunks:
        return {"indexed": 0, "errors": 0, "parse_failures": parse_failures}

    log.info("Embedding %d chunks", len(chunks))
    embedder = Embedder(model_path=model_path)
    docs = embedder.embed_chunks(chunks)

    log.info("Indexing into Elasticsearch")
    es = get_client(es_url)
    create_index(es, delete_existing=recreate_index)
    stats = bulk_index_chunks(es, docs, request_id=request_id)
    stats["parse_failures"] = parse_failures

    log.info("Indexed %d chunks, %d errors", stats["indexed"], stats["errors"])
    return stats
