"""Extraction strategies for the sample insurance domain.

Each intent maps to an ``ExtractionStrategy`` that provides the Pydantic
schema, intent-level instructions, validation rules, and tuning knobs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from pydantic import BaseModel

from src.schemas.group_quote_schema import InsuranceExtraction


@dataclass
class ValidationRule:
    """A named business-rule check applied after merge."""

    name: str
    description: str
    check: Callable[[BaseModel], str | None]


@dataclass
class ExtractionStrategy:
    """Per-intent configuration consumed by the Extract service."""

    intent: str
    schema: type[BaseModel]
    instructions: str
    validation_rules: list[ValidationRule] = field(default_factory=list)
    confidence_threshold: float = 0.7
    max_repair_iterations: int = 2
    output_token_budget: int = 2000
    array_batch_size_override: dict[str, int] = field(default_factory=dict)


def _rule_group_size_positive(model: BaseModel) -> str | None:
    ci = getattr(model, "client_information", None)
    if ci and ci.group_size is not None and ci.group_size <= 0:
        return "client_information.group_size must be positive"
    return None


def _rule_employee_class_has_name(model: BaseModel) -> str | None:
    classes = getattr(model, "employee_classes", [])
    for idx, ec in enumerate(classes):
        if not ec.class_name or not ec.class_name.strip():
            return f"employee_classes[{idx}].class_name is required"
    return None


GROUP_QUOTE_STRATEGY = ExtractionStrategy(
    intent="group_insurance_quote_request",
    schema=InsuranceExtraction,
    instructions=(
        "This is a group insurance quote request.  Documents typically include "
        "an email from a broker, an employee census spreadsheet, plan design "
        "specifications (sometimes in PDF, DOCX, or spreadsheet tabs), and "
        "details about existing coverage.\n\n"
        "When extracting monetary values, always include currency if stated.  "
        "Dates should be in ISO 8601 format (YYYY-MM-DD).  Percentages should "
        "include the '%' symbol.\n\n"
        "Employee classes may be identified by labels like 'Class A', "
        "'Executives', 'Full Time', 'Hourly', etc.  Each class has its own "
        "set of benefit coverages."
    ),
    validation_rules=[
        ValidationRule(
            name="group_size_positive",
            description="Group size must be a positive number",
            check=_rule_group_size_positive,
        ),
        ValidationRule(
            name="employee_class_has_name",
            description="Every employee class must have a class_name",
            check=_rule_employee_class_has_name,
        ),
    ],
    confidence_threshold=0.7,
    max_repair_iterations=2,
    output_token_budget=2000,
)

STRATEGIES: dict[str, ExtractionStrategy] = {
    "group_insurance_quote_request": GROUP_QUOTE_STRATEGY,
}
