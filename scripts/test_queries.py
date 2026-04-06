"""Run manual test queries against the indexed sample data.

Usage:
    python scripts/test_queries.py "your search query"
    python scripts/test_queries.py "your search query" --hybrid
"""

from __future__ import annotations

import sys

from sentence_transformers import SentenceTransformer

from src.index.es_index import INDEX_NAME, get_client

MODEL_PATH = "models/bge-large-en-v1.5"


def bm25_query(es, query_text: str, top_k: int = 10, request_id: str | None = None):
    """Pure BM25 text search."""
    body: dict = {
        "size": top_k,
        "query": {
            "bool": {
                "must": [{"match": {"search_text": query_text}}],
            }
        },
    }
    if request_id:
        body["query"]["bool"]["filter"] = [{"term": {"request_id": request_id}}]
    return es.search(index=INDEX_NAME, body=body)


def hybrid_query(
    es, query_text: str, query_vector: list[float], top_k: int = 10,
    request_id: str | None = None,
):
    """Hybrid BM25 + vector search."""
    body: dict = {
        "size": top_k,
        "query": {
            "bool": {
                "must": [{"match": {"search_text": query_text}}],
                "should": [
                    {
                        "script_score": {
                            "query": {"match_all": {}},
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                                "params": {"query_vector": query_vector},
                            },
                        }
                    }
                ],
            }
        },
    }
    if request_id:
        body["query"]["bool"]["filter"] = [{"term": {"request_id": request_id}}]
    return es.search(index=INDEX_NAME, body=body)


def print_results(results):
    hits = results["hits"]["hits"]
    print(f"\n{'='*80}")
    print(f"Found {results['hits']['total']['value']} results (showing top {len(hits)})")
    print(f"{'='*80}")

    for i, hit in enumerate(hits):
        src = hit["_source"]
        print(f"\n--- Result {i+1} (score: {hit['_score']:.4f}) ---")
        print(f"  doc: {src['document_id']}  |  type: {src['chunk_type']}  |  format: {src['source_format']}")
        if src.get("section_label"):
            print(f"  section: {src['section_label']}")
        if src.get("sheet_name"):
            print(f"  sheet: {src['sheet_name']}")
        text = src["search_text"]
        if len(text) > 300:
            text = text[:300] + "..."
        print(f"  text: {text}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_queries.py \"your query\" [--hybrid]")
        sys.exit(1)

    query_text = sys.argv[1]
    use_hybrid = "--hybrid" in sys.argv

    es = get_client()
    print(f"Query: \"{query_text}\"")
    print(f"Mode: {'hybrid (BM25 + vector)' if use_hybrid else 'BM25 only'}")

    if use_hybrid:
        print("Loading embedding model...")
        model = SentenceTransformer(MODEL_PATH)
        query_vector = model.encode(query_text).tolist()
        results = hybrid_query(es, query_text, query_vector, request_id="sample_1")
    else:
        results = bm25_query(es, query_text, request_id="sample_1")

    print_results(results)


if __name__ == "__main__":
    main()
