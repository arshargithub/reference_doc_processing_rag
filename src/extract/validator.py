"""Validation of merged extraction results.

Checks schema constraints, business rules, completeness, and confidence
thresholds.  Produces a ``ValidationReport``.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from pydantic import BaseModel, ValidationError

from .models import FieldResult, ValidationIssue, ValidationReport

log = logging.getLogger(__name__)


def validate(
    populated: dict[str, Any],
    schema: type[BaseModel],
    provenance: dict[str, FieldResult],
    confidence_threshold: float = 0.7,
    business_rules: list[tuple[str, Callable[[BaseModel], str | None]]] | None = None,
) -> ValidationReport:
    """Run all validation checks and return a report."""
    issues: list[ValidationIssue] = []
    iteration = 0

    # 1. Schema validation (types, required fields)
    try:
        instance = schema.model_validate(populated)
    except ValidationError as exc:
        for err in exc.errors():
            field_path = ".".join(str(p) for p in err["loc"])
            issues.append(ValidationIssue(
                field_path=field_path,
                issue_type="schema_error",
                message=err["msg"],
            ))
        instance = None

    # 2. Completeness -- check for null/missing required fields
    _check_completeness(schema, populated, "", issues)

    # 3. Confidence thresholds
    for path, fr in provenance.items():
        if fr.value is not None and fr.confidence < confidence_threshold:
            issues.append(ValidationIssue(
                field_path=path,
                issue_type="low_confidence",
                message=f"Confidence {fr.confidence:.2f} below threshold {confidence_threshold}",
            ))

    # 4. Business rules
    if instance and business_rules:
        for rule_name, check_fn in business_rules:
            try:
                msg = check_fn(instance)
                if msg:
                    issues.append(ValidationIssue(
                        field_path=rule_name,
                        issue_type="rule_failure",
                        message=msg,
                    ))
            except Exception as exc:
                log.warning("Business rule %s raised: %s", rule_name, exc)

    # Compute completeness score (clamped to 1.0; arrays inflate the count)
    total_fields = max(_count_schema_fields(schema), len(provenance))
    extracted = sum(1 for fr in provenance.values() if fr.value is not None)
    completeness = min(extracted / total_fields, 1.0) if total_fields > 0 else 1.0

    passed = len(issues) == 0

    return ValidationReport(
        passed=passed,
        issues=issues,
        completeness=completeness,
        iteration=iteration,
    )


def _check_completeness(
    model: type[BaseModel],
    data: dict[str, Any],
    prefix: str,
    issues: list[ValidationIssue],
) -> None:
    """Flag required fields that are null or missing."""
    for name, info in model.model_fields.items():
        path = f"{prefix}.{name}" if prefix else name
        value = data.get(name)
        if not info.is_required():
            continue
        if value is None:
            issues.append(ValidationIssue(
                field_path=path,
                issue_type="missing_required",
                message=f"Required field '{name}' is missing or null",
            ))


def _count_schema_fields(model: type[BaseModel]) -> int:
    """Count total leaf fields in a schema (shallow, for completeness %)."""
    count = 0
    for info in model.model_fields.values():
        ann = info.annotation
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            count += _count_schema_fields(ann)
        else:
            count += 1
    return count
