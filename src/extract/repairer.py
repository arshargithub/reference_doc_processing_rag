"""Repair loop for failed extraction fields.

Identifies fields that failed validation (missing, low-confidence, rule
failures), broadens retrieval queries, re-extracts just those fields,
and merges repair results into the existing extraction.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from pydantic import BaseModel, Field, create_model

from src.llm.client import LLMClient

from .extractor import extract
from .merger import merge_results
from .models import (
    ExtractionResult,
    ExtractionTask,
    FieldResult,
    ValidationReport,
)
from .retriever import Retriever
from .validator import validate

log = logging.getLogger(__name__)


def repair(
    report: ValidationReport,
    populated: dict[str, Any],
    provenance: dict[str, FieldResult],
    schema: type[BaseModel],
    retriever: Retriever,
    llm: LLMClient,
    request_id: str,
    intent_instructions: str = "",
    confidence_threshold: float = 0.7,
    business_rules: list | None = None,
    max_iterations: int = 2,
) -> tuple[dict[str, Any], dict[str, FieldResult], ValidationReport]:
    """Run the repair loop up to *max_iterations* times.

    Returns the updated (populated, provenance, report) tuple.
    """
    current_populated = populated
    current_provenance = provenance
    current_report = report

    for iteration in range(1, max_iterations + 1):
        if current_report.passed:
            break

        failed_paths = _collect_failed_paths(current_report)
        if not failed_paths:
            break

        log.info(
            "Repair iteration %d: %d fields to retry",
            iteration, len(failed_paths),
        )

        repair_task, safe_to_original = _build_repair_task(
            failed_paths, schema, intent_instructions,
        )
        chunks = retriever.retrieve(repair_task, request_id, context_budget=6000)
        repair_result = extract(repair_task, chunks, llm)

        for path, fr in repair_result.fields.items():
            original_path = safe_to_original.get(path, path)
            if fr.value is not None:
                current_provenance[original_path] = fr

        current_populated, _ = merge_results(
            [ExtractionResult(task_id="merged", fields=current_provenance)],
            schema,
        )

        current_report = validate(
            current_populated,
            schema,
            current_provenance,
            confidence_threshold=confidence_threshold,
            business_rules=business_rules,
        )
        current_report.iteration = iteration

    return current_populated, current_provenance, current_report


def _collect_failed_paths(report: ValidationReport) -> list[str]:
    paths: list[str] = []
    for issue in report.issues:
        if issue.issue_type in ("missing_required", "low_confidence", "rule_failure"):
            if issue.field_path not in paths:
                paths.append(issue.field_path)
    return paths


def _build_repair_task(
    failed_paths: list[str],
    schema: type[BaseModel],
    intent_instructions: str,
) -> tuple[ExtractionTask, dict[str, str]]:
    """Build a single extraction task targeting only the failed fields.

    Returns ``(task, safe_to_original)`` where *safe_to_original* maps
    the underscore-flattened field names in the LLM response back to the
    original dotted paths.
    """
    query_parts: list[str] = []
    for path in failed_paths:
        query_parts.append(path.replace(".", " ").replace("_", " "))

        info = _resolve_field_info(schema, path)
        if info and info.description:
            query_parts.append(info.description)
        if info:
            extra = info.json_schema_extra or {}
            query_parts.extend(extra.get("aliases", []))
            query_parts.extend(extra.get("keywords", []))

    repair_schema, safe_to_original = _build_repair_model(failed_paths, schema)

    return ExtractionTask(
        task_id=str(uuid.uuid4()),
        task_type="scalar_group",
        field_paths=failed_paths,
        output_schema=repair_schema,
        retrieval_query=" ".join(query_parts),
        prompt_instructions=(
            f"{intent_instructions}\n\n"
            f"REPAIR: The following fields were missing or low-confidence "
            f"in a previous extraction attempt.  Try harder to find them "
            f"in the evidence.  Fields: {', '.join(failed_paths)}"
        ),
        token_budget=2000,
    ), safe_to_original


def _resolve_field_info(schema: type[BaseModel], path: str):
    """Walk dotted path to find the FieldInfo, or None."""
    from src.extract.planner import _unwrap_optional

    parts = path.split(".")
    current = schema
    for i, part in enumerate(parts):
        clean = part.split("[")[0]
        if not hasattr(current, "model_fields"):
            return None
        info = current.model_fields.get(clean)
        if info is None:
            return None
        if i == len(parts) - 1:
            return info
        ann = _unwrap_optional(info.annotation)
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            current = ann
        else:
            return info
    return None


def _build_repair_model(
    paths: list[str],
    schema: type[BaseModel],
) -> tuple[type[BaseModel], dict[str, str]]:
    """Create a dynamic Pydantic model for just the failed fields.

    Returns ``(model, safe_to_original)`` mapping.
    """
    definitions: dict[str, Any] = {}
    safe_to_original: dict[str, str] = {}
    for path in paths:
        safe = path.replace(".", "_").replace("[", "").replace("]", "")
        safe_to_original[safe] = path
        info = _resolve_field_info(schema, path)
        ann = info.annotation if info else str
        desc = info.description if info else path
        definitions[safe] = (ann | None, Field(description=desc, default=None))

    return create_model("RepairExtract", **definitions), safe_to_original
