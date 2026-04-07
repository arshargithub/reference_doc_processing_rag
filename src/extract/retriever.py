"""Evidence retrieval from Elasticsearch.

Given an ``ExtractionTask`` (which carries a retrieval query and optional
chunk_type hints), execute a hybrid search and return ranked chunks
trimmed to a token budget.

Quality levers:
- **section_label boosting**: For identifier-scoped tasks (array batches),
  chunks whose ``section_label`` contains the target identifier are
  boosted.  This prevents cross-class contamination.
- **chunk_type boosting (not filtering)**: Chunk type hints are applied
  as ``should`` clauses (score boost), not ``filter`` clauses.  This
  means the right evidence surfaces even if it lives in an unexpected
  chunk type.
- **Retrieval diagnostics**: At INFO level, the retriever logs the top
  chunks returned per task so retrieval quality can be debugged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

from src.index.es_index import INDEX_NAME

from .models import ExtractionTask

log = logging.getLogger(__name__)

DEFAULT_CONTEXT_BUDGET = 8000


@dataclass
class RetrievedChunk:
    """A single chunk returned by retrieval, ready for prompt assembly."""

    chunk_id: str
    search_text: str
    score: float
    token_estimate: int
    metadata: dict


class Retriever:
    """Wraps Elasticsearch hybrid search for extraction tasks."""

    def __init__(
        self,
        es: Elasticsearch,
        embedding_model: SentenceTransformer,
        default_top_k: int = 30,
    ) -> None:
        self._es = es
        self._model = embedding_model
        self._top_k = default_top_k

    def retrieve(
        self,
        task: ExtractionTask,
        request_id: str,
        *,
        top_k: int | None = None,
        context_budget: int = DEFAULT_CONTEXT_BUDGET,
    ) -> list[RetrievedChunk]:
        """Execute hybrid retrieval for *task* and return chunks
        trimmed to *context_budget* tokens.
        """
        k = top_k or self._top_k
        query_text = task.retrieval_query
        query_vector = self._model.encode(query_text).tolist()

        identifier = None
        if task.array_config and task.array_config.item_identifier:
            identifier = task.array_config.item_identifier

        body = self._build_query(
            query_text, query_vector, request_id, task, k,
            identifier=identifier,
        )
        response = self._es.search(index=INDEX_NAME, body=body)

        chunks = self._parse_hits(response)
        trimmed = self._trim_to_budget(chunks, context_budget)

        self._log_retrieval(task, chunks, trimmed, query_text)
        return trimmed

    def _build_query(
        self,
        query_text: str,
        query_vector: list[float],
        request_id: str,
        task: ExtractionTask,
        top_k: int,
        *,
        identifier: str | None = None,
    ) -> dict:
        filters: list[dict] = [{"term": {"request_id": request_id}}]

        should: list[dict] = [
            {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.qv, 'embedding') + 1.0",
                        "params": {"qv": query_vector},
                    },
                }
            }
        ]

        # Chunk type hints as boost, not filter
        chunk_types = task.retrieval_filters.get("chunk_type", [])
        if chunk_types:
            should.append({
                "terms": {"chunk_type": chunk_types, "boost": 2.0},
            })

        # Section label boost for identifier-scoped tasks
        if identifier:
            should.append({
                "match": {
                    "section_label.text": {
                        "query": identifier,
                        "boost": 5.0,
                    }
                }
            })
            should.append({
                "match": {
                    "search_text": {
                        "query": identifier,
                        "boost": 3.0,
                    }
                }
            })

        return {
            "size": top_k,
            "query": {
                "bool": {
                    "filter": filters,
                    "must": [{"match": {"search_text": query_text}}],
                    "should": should,
                }
            },
        }

    @staticmethod
    def _parse_hits(response: dict) -> list[RetrievedChunk]:
        chunks: list[RetrievedChunk] = []
        for hit in response["hits"]["hits"]:
            src = hit["_source"]
            chunks.append(RetrievedChunk(
                chunk_id=src["chunk_id"],
                search_text=src["search_text"],
                score=hit["_score"],
                token_estimate=src.get("token_estimate", 0),
                metadata={
                    k: src[k]
                    for k in (
                        "document_id", "chunk_type", "source_format",
                        "section_label", "sheet_name", "page_number",
                        "row_index_start", "row_index_end",
                    )
                    if k in src
                },
            ))
        return chunks

    @staticmethod
    def _trim_to_budget(
        chunks: list[RetrievedChunk],
        budget: int,
    ) -> list[RetrievedChunk]:
        """Keep top-scoring chunks that fit within *budget* tokens."""
        result: list[RetrievedChunk] = []
        running = 0
        for chunk in chunks:
            cost = chunk.token_estimate or 100
            if running + cost > budget:
                break
            result.append(chunk)
            running += cost
        return result

    @staticmethod
    def _log_retrieval(
        task: ExtractionTask,
        all_chunks: list[RetrievedChunk],
        trimmed: list[RetrievedChunk],
        query: str,
    ) -> None:
        identifier = ""
        if task.array_config and task.array_config.item_identifier:
            identifier = f" [{task.array_config.item_identifier}]"

        log.info(
            "Task %s (%s)%s: %d retrieved, %d after trim (query: %.60s)",
            task.task_id[:8], task.task_type, identifier,
            len(all_chunks), len(trimmed), query,
        )
        for i, c in enumerate(trimmed[:5]):
            section = c.metadata.get("section_label", "")
            doc = c.metadata.get("document_id", "")
            ctype = c.metadata.get("chunk_type", "")
            log.info(
                "  [%d] score=%.2f doc=%s type=%s section=%s text=%.80s",
                i, c.score, doc, ctype, section,
                c.search_text.replace("\n", " "),
            )
