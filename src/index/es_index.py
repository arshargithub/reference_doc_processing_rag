"""Elasticsearch index management and document indexing."""

from __future__ import annotations

from elasticsearch import Elasticsearch

INDEX_NAME = "evidence_chunks"

MAPPING = {
    "mappings": {
        "properties": {
            "chunk_id": {"type": "keyword"},
            "request_id": {"type": "keyword"},
            "document_id": {"type": "keyword"},
            "chunk_type": {"type": "keyword"},
            "source_format": {"type": "keyword"},
            "search_text": {"type": "text"},
            "embedding": {
                "type": "dense_vector",
                "dims": 1024,
                "index": True,
                "similarity": "cosine",
            },
            "section_label": {
                "type": "keyword",
                "fields": {
                    "text": {"type": "text"},
                },
            },
            "sheet_name": {"type": "keyword"},
            "page_number": {"type": "integer"},
            "row_index_start": {"type": "integer"},
            "row_index_end": {"type": "integer"},
            "token_estimate": {"type": "integer"},
        }
    }
}


def get_client(url: str = "http://localhost:9200") -> Elasticsearch:
    return Elasticsearch(url)


def create_index(es: Elasticsearch, delete_existing: bool = False) -> None:
    if es.indices.exists(index=INDEX_NAME):
        if delete_existing:
            es.indices.delete(index=INDEX_NAME)
        else:
            return
    es.indices.create(index=INDEX_NAME, body=MAPPING)


def index_chunk(es: Elasticsearch, chunk_doc: dict, request_id: str) -> None:
    """Index a single chunk document."""
    chunk_doc["request_id"] = request_id
    es.index(index=INDEX_NAME, id=chunk_doc["chunk_id"], document=chunk_doc)


def bulk_index_chunks(
    es: Elasticsearch, chunk_docs: list[dict], request_id: str
) -> dict:
    """Bulk index chunk documents. Returns stats."""
    actions = []
    for doc in chunk_docs:
        doc["request_id"] = request_id
        actions.append({"index": {"_index": INDEX_NAME, "_id": doc["chunk_id"]}})
        actions.append(doc)

    if not actions:
        return {"indexed": 0}

    result = es.bulk(operations=actions, refresh="wait_for")
    errors = [item for item in result["items"] if item["index"].get("error")]
    return {"indexed": len(chunk_docs), "errors": len(errors)}
