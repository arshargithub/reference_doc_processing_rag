"""Intent classification using curated evidence from Elasticsearch.

Queries ES for a small, predictable set of context chunks (email body,
email headers, heading/title chunks) and asks the LLM to classify the
request intent.
"""

from __future__ import annotations

import logging

from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field

from src.index.es_index import INDEX_NAME
from src.llm.client import LLMClient

log = logging.getLogger(__name__)

CLASSIFY_SYSTEM_PROMPT = """\
You are a document classification agent.  Your job is to determine the
intent of an inbound request based on the provided evidence chunks.

Rules:
- Choose exactly one intent from the provided list.
- If none of the intents fit, choose "unknown".
- Provide a confidence level: "high", "medium", or "low".
- Provide a brief rationale for your classification.
"""


class ClassificationResult(BaseModel):
    """Structured output from the classification LLM call."""

    intent: str = Field(description="The classified intent identifier.")
    confidence: str = Field(description="Confidence level: high, medium, or low.")
    rationale: str = Field(description="Brief explanation for the classification.")


def classify(
    es: Elasticsearch,
    llm: LLMClient,
    request_id: str,
    intents: list[str],
    intent_descriptions: dict[str, str] | None = None,
) -> ClassificationResult:
    """Classify a request by curating context from ES and calling the LLM."""
    context_chunks = _curate_context(es, request_id)

    if not context_chunks:
        log.warning("No context chunks found for request %s", request_id)
        return ClassificationResult(
            intent="unknown",
            confidence="low",
            rationale="No evidence chunks found for classification.",
        )

    evidence_text = "\n\n---\n\n".join(
        f"[{c['chunk_type']}] [{c.get('document_id', '')}]\n{c['search_text']}"
        for c in context_chunks
    )

    intent_list = _format_intents(intents, intent_descriptions)

    user_message = (
        f"Classify the intent of this request.\n\n"
        f"Possible intents:\n{intent_list}\n\n"
        f"Evidence:\n{evidence_text}"
    )

    messages = [
        {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    log.info(
        "Classifying request %s with %d context chunks",
        request_id, len(context_chunks),
    )

    result = llm.extract_structured(messages, ClassificationResult)
    if not isinstance(result, ClassificationResult):
        raise TypeError(f"Expected ClassificationResult, got {type(result)}")

    log.info(
        "Classification: intent=%s, confidence=%s, rationale=%.80s",
        result.intent, result.confidence, result.rationale,
    )
    return result


def _curate_context(
    es: Elasticsearch,
    request_id: str,
    max_chunks: int = 15,
) -> list[dict]:
    """Retrieve a small, curated set of chunks for classification.

    Prioritises email headers, email body, and heading chunks because
    these are the most informative for intent classification.
    """
    priority_types = ["email_header", "text", "kv_group"]

    body = {
        "size": max_chunks,
        "query": {
            "bool": {
                "filter": [{"term": {"request_id": request_id}}],
                "should": [
                    {"terms": {"chunk_type": priority_types}},
                ],
            }
        },
        "sort": [
            {"_score": "desc"},
            {"chunk_type": {"order": "asc"}},
        ],
    }

    response = es.search(index=INDEX_NAME, body=body)
    return [hit["_source"] for hit in response["hits"]["hits"]]


def _format_intents(
    intents: list[str],
    descriptions: dict[str, str] | None,
) -> str:
    lines: list[str] = []
    for intent in intents:
        desc = (descriptions or {}).get(intent, "")
        if desc:
            lines.append(f"- {intent}: {desc}")
        else:
            lines.append(f"- {intent}")
    lines.append("- unknown: None of the above intents match.")
    return "\n".join(lines)
