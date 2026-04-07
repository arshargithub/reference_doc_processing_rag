"""Extract service orchestrator.

Chains Plan -> Retrieve -> Extract -> Merge -> Validate -> Repair.
At work, this becomes the Kafka consumer callback.  Locally, it is
called directly by the orchestrator script.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel

from src.llm.client import LLMClient

from .extractor import extract
from .merger import merge_array_results, merge_results
from .models import (
    ExtractionEnvelope,
    ExtractionResult,
    ExtractionTask,
    FieldResult,
    ValidationReport,
)
from .planner import (
    ArrayDiscoveryResult,
    plan_array_batches,
    plan_extraction,
)
from .repairer import repair
from .retriever import Retriever
from .validator import validate

log = logging.getLogger(__name__)

MAX_CONCURRENT_EXTRACTIONS = 6


def run_extraction(
    schema: type[BaseModel],
    retriever: Retriever,
    llm: LLMClient,
    request_id: str,
    intent: str,
    intent_instructions: str = "",
    output_token_budget: int = 2000,
    confidence_threshold: float = 0.7,
    max_repair_iterations: int = 2,
    business_rules: list | None = None,
    array_batch_size_overrides: dict[str, int] | None = None,
    parse_failures: list[str] | None = None,
) -> ExtractionEnvelope:
    """Run the full extraction pipeline synchronously.

    This is the main entry point for the Extract service.
    """
    log.info("Starting extraction for request %s, intent %s", request_id, intent)

    # --- Plan ---
    tasks = plan_extraction(schema, intent_instructions, output_token_budget)
    scalar_tasks = [t for t in tasks if t.task_type == "scalar_group"]
    discovery_tasks = [t for t in tasks if t.task_type == "array_discovery"]

    log.info(
        "Plan: %d scalar groups, %d array discoveries",
        len(scalar_tasks), len(discovery_tasks),
    )

    # --- Retrieve + Extract (scalar groups) ---
    scalar_results = _run_tasks(scalar_tasks, retriever, llm, request_id)

    # --- Array discovery + batches ---
    array_results: list[ExtractionResult] = []
    for disc_task in discovery_tasks:
        field_path = disc_task.array_config.array_field_path  # type: ignore[union-attr]
        element_schema = _resolve_element_schema(schema, field_path)
        if element_schema is None:
            log.warning("Could not resolve element schema for %s", field_path)
            continue

        # Discovery
        disc_chunks = retriever.retrieve(disc_task, request_id)
        disc_result = extract(disc_task, disc_chunks, llm)

        discovery = _parse_discovery(disc_result)
        log.info(
            "Array %s: discovered %d items: %s",
            field_path, discovery.count, discovery.identifiers,
        )

        if discovery.count == 0:
            continue

        # Batch tasks
        override = (array_batch_size_overrides or {}).get(field_path)
        batch_tasks = plan_array_batches(
            field_path,
            element_schema,
            discovery,
            intent_instructions,
            output_token_budget,
            batch_size_override=override,
        )
        log.info("Array %s: %d batch tasks", field_path, len(batch_tasks))

        batch_results = _run_tasks(batch_tasks, retriever, llm, request_id)
        array_results.extend(batch_results)

    # --- Merge ---
    all_results = scalar_results + array_results
    populated, provenance = merge_results(all_results, schema)

    log.info(
        "Merge: %d field results from %d tasks",
        len(provenance), len(all_results),
    )

    # --- Validate ---
    report = validate(
        populated, schema, provenance,
        confidence_threshold=confidence_threshold,
        business_rules=business_rules,
    )

    if not report.passed:
        log.info(
            "Validation: %d issues, entering repair loop",
            len(report.issues),
        )

        # --- Repair ---
        populated, provenance, report = repair(
            report, populated, provenance, schema,
            retriever, llm, request_id,
            intent_instructions=intent_instructions,
            confidence_threshold=confidence_threshold,
            business_rules=business_rules,
            max_iterations=max_repair_iterations,
        )

    log.info(
        "Extraction complete: passed=%s, completeness=%.1f%%, issues=%d",
        report.passed, report.completeness * 100, len(report.issues),
    )

    return ExtractionEnvelope(
        request_id=request_id,
        intent=intent,
        result=populated,
        provenance=provenance,
        validation_report=report,
        parse_failures=parse_failures or [],
    )


def _run_tasks(
    tasks: list[ExtractionTask],
    retriever: Retriever,
    llm: LLMClient,
    request_id: str,
) -> list[ExtractionResult]:
    """Execute extraction tasks sequentially (async version can use
    semaphore-bounded concurrency).
    """
    results: list[ExtractionResult] = []
    for task in tasks:
        chunks = retriever.retrieve(task, request_id)
        result = extract(task, chunks, llm)
        results.append(result)
    return results


def _resolve_element_schema(
    schema: type[BaseModel],
    field_path: str,
) -> type[BaseModel] | None:
    """Walk the schema to find the element type of a list[BaseModel] field.

    Handles deeply nested paths like
    ``employee_classes[0].extended_health_care.paramedical.paramedical_practitioners``
    by traversing through array element types and nested models.
    """
    from typing import get_args, get_origin
    from src.extract.planner import _unwrap_optional

    parts = field_path.split(".")
    current = schema
    for i, part in enumerate(parts):
        clean = part.split("[")[0]
        if not hasattr(current, "model_fields"):
            return None
        info = current.model_fields.get(clean)
        if info is None:
            return None
        ann = _unwrap_optional(info.annotation)
        if get_origin(ann) is list:
            args = get_args(ann)
            if not args or not isinstance(args[0], type):
                return None
            elem = args[0]
            if i == len(parts) - 1:
                if issubclass(elem, BaseModel):
                    return elem
                return None
            # Continue traversal into the list element type
            if issubclass(elem, BaseModel):
                current = elem
            else:
                return None
        elif isinstance(ann, type) and issubclass(ann, BaseModel):
            current = ann
        else:
            return None
    return None


def _parse_discovery(result: ExtractionResult) -> ArrayDiscoveryResult:
    """Extract discovery data from an ExtractionResult."""
    import ast

    count_fr = result.fields.get("count")
    ids_fr = result.fields.get("identifiers")

    count = 0
    if count_fr and count_fr.value is not None:
        try:
            count = int(count_fr.value)
        except (ValueError, TypeError):
            count = 0

    identifiers: list[str] = []
    if ids_fr and ids_fr.value is not None:
        raw = ids_fr.value
        if isinstance(raw, list):
            identifiers = [str(x) for x in raw]
        elif isinstance(raw, str):
            # Value is a stringified list, e.g. "['Executives', 'Full Time']"
            try:
                parsed = ast.literal_eval(raw)
                if isinstance(parsed, list):
                    identifiers = [str(x) for x in parsed]
            except (ValueError, SyntaxError):
                identifiers = [s.strip() for s in raw.split(",") if s.strip()]

    return ArrayDiscoveryResult(count=count, identifiers=identifiers)
