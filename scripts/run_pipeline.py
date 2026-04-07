"""End-to-end pipeline: Normalize -> Index -> Classify -> Extract.

This is the LOCAL orchestrator.  It replaces Kafka with direct function
calls.  Everything it calls transfers to work; this script itself does
not.

Usage:
    PYTHONPATH=. python scripts/run_pipeline.py <directory> <request_id> [--recreate]

Example:
    PYTHONPATH=. python scripts/run_pipeline.py sample_files/sample_1 sample_1 --recreate

Requires:
    - Elasticsearch running (docker compose up -d)
    - OPENAI_API_KEY in .env or environment
    - Embedding model at models/bge-large-en-v1.5
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from src.classify.classifier import classify
from src.extract.runner import run_extraction
from src.extract.retriever import Retriever
from src.index.es_index import get_client
from src.index.pipeline import index_directory
from src.llm.client import OpenAIClient

from domains.sample_insurance.intents import INTENTS, INTENT_DESCRIPTIONS
from domains.sample_insurance.strategy import STRATEGIES

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-30s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

MODEL_PATH = "models/bge-large-en-v1.5"


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: PYTHONPATH=. python scripts/run_pipeline.py "
            "<directory> <request_id> [--recreate]"
        )
        sys.exit(1)

    directory = sys.argv[1]
    request_id = sys.argv[2]
    recreate = "--recreate" in sys.argv

    # --- 1. Ingest: Normalize + Embed + Index ---
    log.info("=== INGEST ===")
    stats = index_directory(
        directory=directory,
        request_id=request_id,
        recreate_index=recreate,
    )
    log.info("Index stats: %s", stats)
    parse_failures = stats.get("parse_failures", [])

    # --- 2. Classify ---
    log.info("=== CLASSIFY ===")
    es = get_client()
    llm = OpenAIClient()

    classification = classify(
        es=es,
        llm=llm,
        request_id=request_id,
        intents=INTENTS,
        intent_descriptions=INTENT_DESCRIPTIONS,
    )
    log.info(
        "Classification: intent=%s confidence=%s rationale=%s",
        classification.intent,
        classification.confidence,
        classification.rationale,
    )

    if classification.intent == "unknown":
        log.warning("Intent classified as unknown. Stopping.")
        sys.exit(0)

    strategy = STRATEGIES.get(classification.intent)
    if strategy is None:
        log.warning(
            "No extraction strategy for intent '%s'. Stopping.",
            classification.intent,
        )
        sys.exit(0)

    # --- 3. Extract ---
    log.info("=== EXTRACT ===")
    embedding_model = SentenceTransformer(MODEL_PATH)
    retriever = Retriever(es=es, embedding_model=embedding_model)

    envelope = run_extraction(
        schema=strategy.schema,
        retriever=retriever,
        llm=llm,
        request_id=request_id,
        intent=classification.intent,
        intent_instructions=strategy.instructions,
        output_token_budget=strategy.output_token_budget,
        confidence_threshold=strategy.confidence_threshold,
        max_repair_iterations=strategy.max_repair_iterations,
        business_rules=[
            (r.name, r.check) for r in strategy.validation_rules
        ],
        array_batch_size_overrides=strategy.array_batch_size_override,
        parse_failures=parse_failures,
    )

    # --- 4. Output ---
    log.info("=== RESULT ===")
    output = envelope.model_dump(exclude_none=True)
    print(json.dumps(output, indent=2, default=str))

    report = envelope.validation_report
    log.info(
        "Validation: passed=%s completeness=%.1f%% issues=%d",
        report.passed,
        report.completeness * 100,
        len(report.issues),
    )
    for issue in report.issues:
        log.warning("  %s: %s - %s", issue.issue_type, issue.field_path, issue.message)

    if parse_failures:
        log.warning("Parse failures: %s", parse_failures)


if __name__ == "__main__":
    main()
