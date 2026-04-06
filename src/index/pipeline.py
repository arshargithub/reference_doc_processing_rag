"""End-to-end pipeline: normalize files, embed, and index into Elasticsearch."""

from __future__ import annotations

from pathlib import Path

from src.normalize.normalize import normalize_directory
from .embedder import Embedder
from .es_index import bulk_index_chunks, create_index, get_client


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
    print(f"Normalizing files in {directory}...")
    chunks = normalize_directory(directory, token_budget=token_budget)
    print(f"  {len(chunks)} chunks from {len(set(c.document_id for c in chunks))} documents")

    print("Embedding chunks...")
    embedder = Embedder(model_path=model_path)
    docs = embedder.embed_chunks(chunks)

    print("Indexing into Elasticsearch...")
    es = get_client(es_url)
    create_index(es, delete_existing=recreate_index)
    stats = bulk_index_chunks(es, docs, request_id=request_id)

    print(f"  Indexed {stats['indexed']} chunks, {stats['errors']} errors")
    return stats
