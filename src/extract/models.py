"""Shared data models for the Extract service pipeline.

These models are the contract between Plan, Retrieve, Extract, Merge,
Validate, and Repair stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Plan stage outputs
# ---------------------------------------------------------------------------

@dataclass
class ArrayConfig:
    """Describes how an array extraction task is scoped."""

    array_field_path: str
    batch_start: int = 0
    batch_size: int = 20
    total_count: int | None = None
    item_identifier: str | None = None


@dataclass
class ExtractionTask:
    """A single bounded unit of work produced by the planner."""

    task_id: str
    task_type: str  # "scalar_group", "array_discovery", "array_batch"
    field_paths: list[str]
    output_schema: type[BaseModel]
    retrieval_query: str
    retrieval_filters: dict = field(default_factory=dict)
    prompt_instructions: str = ""
    token_budget: int = 2000
    array_config: ArrayConfig | None = None


# ---------------------------------------------------------------------------
# Extract stage outputs
# ---------------------------------------------------------------------------

class FieldResult(BaseModel):
    """Extraction result for a single field, with provenance."""

    value: str | None = None
    confidence: float = Field(0.0, ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)


class FieldExtraction(BaseModel):
    """A single extracted field in the LLM response list."""

    field_name: str = Field(description="Name of the extracted field.")
    value: str | None = Field(None, description="Extracted value, or null if not found.")
    confidence: float = Field(description="Confidence score from 0.0 to 1.0.")
    evidence: list[str] = Field(description="List of chunk_ids supporting this value.")


class LLMExtractionResponse(BaseModel):
    """Schema the LLM actually returns -- a flat list of field extractions."""

    extractions: list[FieldExtraction] = Field(
        description="One entry per field that was requested for extraction."
    )


class ExtractionResult(BaseModel):
    """Internal representation after converting LLM response."""

    task_id: str
    fields: dict[str, FieldResult] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Validate stage outputs
# ---------------------------------------------------------------------------

class ValidationIssue(BaseModel):
    """A single validation problem."""

    field_path: str
    issue_type: str  # "missing_required", "low_confidence", "rule_failure"
    message: str


class ValidationReport(BaseModel):
    """Output from the Validate stage."""

    passed: bool = True
    issues: list[ValidationIssue] = Field(default_factory=list)
    completeness: float = Field(1.0, ge=0.0, le=1.0)
    iteration: int = 0


# ---------------------------------------------------------------------------
# Pipeline envelope
# ---------------------------------------------------------------------------

class ExtractionEnvelope(BaseModel):
    """Wraps the domain extraction result with pipeline metadata.

    The domain schema (``result``) contains only extracted data.
    Pipeline-generated metadata lives here, outside the domain model.
    """

    request_id: str
    intent: str
    result: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, FieldResult] = Field(default_factory=dict)
    validation_report: ValidationReport = Field(default_factory=ValidationReport)
    parse_failures: list[str] = Field(default_factory=list)
