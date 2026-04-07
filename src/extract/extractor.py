"""Extraction via LLM structured output.

Assembles a prompt from an ``ExtractionTask`` and retrieved evidence,
calls the LLM, and returns an ``ExtractionResult``.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

from src.llm.client import LLMClient

from .models import ExtractionResult, ExtractionTask, FieldResult, LLMExtractionResponse
from .retriever import RetrievedChunk

log = logging.getLogger(__name__)

UNIVERSAL_PROMPT = """\
You are a document extraction agent.  Your job is to extract structured
data from the provided evidence chunks.  Recall is critical — missing a
value that IS present in the evidence is worse than extracting it at
lower confidence.

Rules:
- Extract from the provided evidence only.  Never fabricate data.
- If data is present but you are uncertain which schema field it maps
  to, extract your best match and lower the confidence score (e.g. 0.4-0.6).
  Only return null when NO plausible value exists in the evidence.
- Provide a confidence score (0.0 to 1.0) for each extracted field:
    1.0  = exact label match and unambiguous value
    0.7+ = reasonable match, minor label mismatch
    0.4-0.6 = value is plausible but mapping is uncertain
    < 0.4 = very uncertain
- Cite the chunk_id(s) that support each value in the evidence list.
- Dates should be ISO 8601 (YYYY-MM-DD) when possible.
- Percentages should include the '%' symbol.
- Monetary values should include currency if stated.

Table / comparison format guidance:
- Evidence may contain MULTI-COLUMN COMPARISON TABLES formatted as:
    Benefit: <label> | <Class1>: <value1> | <Class2>: <value2> | ...
  When extracting for a specific class/identifier, look at the column
  matching that class and ignore values from other columns.
- Table rows may use generic labels like "Additional Attribute N".
  Try to match them to schema fields by value type and context.
- The same logical field may appear across multiple rows under slightly
  different labels.  Prefer the row whose label best matches the field
  description.
"""


def _format_evidence(chunks: list[RetrievedChunk]) -> str:
    parts: list[str] = []
    for chunk in chunks:
        meta_parts = []
        m = chunk.metadata
        if m.get("document_id"):
            meta_parts.append(f"doc={m['document_id']}")
        if m.get("section_label"):
            meta_parts.append(f"section={m['section_label']}")
        if m.get("sheet_name"):
            meta_parts.append(f"sheet={m['sheet_name']}")
        if m.get("chunk_type"):
            meta_parts.append(f"type={m['chunk_type']}")
        header = f"[chunk_id={chunk.chunk_id}] [{', '.join(meta_parts)}]"
        parts.append(f"{header}\n{chunk.search_text}")
    return "\n\n---\n\n".join(parts)


def _format_field_list(task: ExtractionTask) -> str:
    """Produce a human-readable list of fields the LLM should extract,
    including type hints and aliases from json_schema_extra.
    """
    schema = task.output_schema
    lines: list[str] = []
    for name, info in schema.model_fields.items():
        desc = info.description or ""
        parts = [f"- {name}: {desc}"]

        extra = info.json_schema_extra or {}
        aliases = extra.get("aliases", [])
        keywords = extra.get("keywords", [])
        if aliases:
            parts.append(f"  (also known as: {', '.join(aliases)})")
        if keywords:
            parts.append(f"  (look for: {', '.join(keywords)})")

        ann = info.annotation
        ann_str = getattr(ann, "__name__", str(ann)) if ann else ""
        if "str" in ann_str.lower():
            parts.append("  [expects: text/string value]")
        elif "float" in ann_str.lower() or "int" in ann_str.lower():
            parts.append("  [expects: numeric value]")

        lines.append(" ".join(parts) if len(parts) == 1 else "\n".join(parts))
    return "\n".join(lines)


def _build_leaf_to_path_map(task: ExtractionTask) -> dict[str, str]:
    """Map leaf field names to their full dotted paths.

    E.g. ``{'sales_representative': 'sales_information.sales_representative'}``
    """
    mapping: dict[str, str] = {}
    for full_path in task.field_paths:
        leaf = full_path.rsplit(".", 1)[-1]
        leaf_clean = leaf.split("[")[0]
        mapping[leaf_clean] = full_path
    return mapping


def extract(
    task: ExtractionTask,
    chunks: list[RetrievedChunk],
    llm: LLMClient,
) -> ExtractionResult:
    """Run a single extraction task against retrieved evidence."""
    if not chunks:
        log.warning("Task %s: no evidence chunks, returning empty result", task.task_id[:8])
        return ExtractionResult(task_id=task.task_id)

    evidence_text = _format_evidence(chunks)
    field_list = _format_field_list(task)

    user_message = (
        f"{task.prompt_instructions}\n\n"
        f"Extract the following fields:\n{field_list}\n\n"
        f"Evidence:\n{evidence_text}"
    )

    messages = [
        {"role": "system", "content": UNIVERSAL_PROMPT},
        {"role": "user", "content": user_message},
    ]

    log.info(
        "Task %s (%s): extracting %d fields from %d chunks",
        task.task_id[:8], task.task_type, len(task.field_paths), len(chunks),
    )

    # Discovery tasks use their own output schema directly
    if task.task_type == "array_discovery":
        result = llm.extract_structured(messages, task.output_schema)
        fields: dict[str, FieldResult] = {}
        for name in task.output_schema.model_fields:
            val = getattr(result, name, None)
            fields[name] = FieldResult(
                value=str(val) if val is not None else None,
                confidence=1.0,
                evidence=[c.chunk_id for c in chunks[:3]],
            )
        return ExtractionResult(task_id=task.task_id, fields=fields)

    # Scalar / batch tasks use the generic LLMExtractionResponse
    llm_response = llm.extract_structured(messages, LLMExtractionResponse)

    if not isinstance(llm_response, LLMExtractionResponse):
        raise TypeError(f"Expected LLMExtractionResponse, got {type(llm_response)}")

    leaf_map = _build_leaf_to_path_map(task)
    fields = {}
    for fe in llm_response.extractions:
        full_path = leaf_map.get(fe.field_name, fe.field_name)
        fields[full_path] = FieldResult(
            value=fe.value,
            confidence=fe.confidence,
            evidence=fe.evidence,
        )

    return ExtractionResult(task_id=task.task_id, fields=fields)
