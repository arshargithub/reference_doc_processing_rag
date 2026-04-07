"""Deterministic merge of extraction task results.

Combines ``ExtractionResult`` objects from multiple tasks into a single
populated instance of the intent's Pydantic schema.  No LLM involved.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel

from .models import ExtractionResult, FieldResult

log = logging.getLogger(__name__)


def merge_results(
    results: list[ExtractionResult],
    schema: type[BaseModel],
) -> tuple[dict[str, Any], dict[str, FieldResult]]:
    """Merge extraction results into a schema-shaped dict + provenance map.

    Returns:
        (populated_dict, provenance) where populated_dict can be passed
        to ``schema.model_validate()`` and provenance maps field paths
        to their ``FieldResult``.
    """
    provenance: dict[str, FieldResult] = {}

    for result in results:
        for field_path, field_result in result.fields.items():
            existing = provenance.get(field_path)
            if existing is None or field_result.confidence > existing.confidence:
                provenance[field_path] = field_result
            elif (
                field_result.confidence == existing.confidence
                and len(field_result.evidence) > len(existing.evidence)
            ):
                provenance[field_path] = field_result

    populated = _build_nested_dict(provenance)
    return populated, provenance


def merge_array_results(
    results: list[ExtractionResult],
    field_path: str,
) -> tuple[list[dict[str, Any]], dict[str, FieldResult]]:
    """Merge batch results for an array field.

    Results are expected to arrive in batch order.  Items are
    concatenated; provenance is keyed with array indices.

    Returns:
        (items_list, provenance)
    """
    items: list[dict[str, Any]] = []
    provenance: dict[str, FieldResult] = {}

    for result in results:
        batch_items = result.fields.get("items")
        if batch_items and batch_items.value is not None:
            if isinstance(batch_items.value, list):
                for i, item in enumerate(batch_items.value):
                    global_idx = len(items)
                    if isinstance(item, dict):
                        items.append(item)
                    elif isinstance(item, BaseModel):
                        items.append(item.model_dump())
                    else:
                        items.append({"value": item})

                    for fname, fr in result.fields.items():
                        if fname == "items":
                            continue
                        prov_key = f"{field_path}[{global_idx}].{fname}"
                        provenance[prov_key] = fr

        for fname, fr in result.fields.items():
            if fname.startswith(f"{field_path}["):
                provenance[fname] = fr
                _set_nested(items, fname, fr.value, field_path)

    return items, provenance


def _build_nested_dict(provenance: dict[str, FieldResult]) -> dict[str, Any]:
    """Convert flat dotted field paths into a nested dict."""
    root: dict[str, Any] = {}
    for path, fr in provenance.items():
        _set_by_path(root, path, fr.value)
    return root


def _set_by_path(d: dict, path: str, value: Any) -> None:
    """Set a value in a nested dict using a dotted path with optional
    array indices like ``employee_classes[0].class_name``.
    """
    parts = _split_path(path)
    current: Any = d
    for i, part in enumerate(parts[:-1]):
        key, idx = _parse_part(part)
        if idx is not None:
            current.setdefault(key, [])
            while len(current[key]) <= idx:
                current[key].append({})
            current = current[key][idx]
        else:
            next_part_key, next_idx = _parse_part(parts[i + 1])
            if next_idx is not None:
                current.setdefault(key, [])
            else:
                current.setdefault(key, {})
            current = current[key]

    last_key, last_idx = _parse_part(parts[-1])
    if last_idx is not None:
        current.setdefault(last_key, [])
        while len(current[last_key]) <= last_idx:
            current[last_key].append(None)
        current[last_key][last_idx] = value
    else:
        current[last_key] = value


def _set_nested(
    items: list,
    full_path: str,
    value: Any,
    array_prefix: str,
) -> None:
    """Set a value inside the items list for array-scoped paths."""
    suffix = full_path[len(array_prefix):]
    if not suffix.startswith("["):
        return
    bracket_end = suffix.index("]")
    idx = int(suffix[1:bracket_end])
    rest = suffix[bracket_end + 1:].lstrip(".")

    while len(items) <= idx:
        items.append({})
    if rest:
        _set_by_path(items[idx], rest, value)
    else:
        items[idx] = value


def _split_path(path: str) -> list[str]:
    """Split ``a.b[0].c`` into ``['a', 'b[0]', 'c']``."""
    result: list[str] = []
    for segment in path.split("."):
        if segment:
            result.append(segment)
    return result


def _parse_part(part: str) -> tuple[str, int | None]:
    """``'items[2]'`` -> ``('items', 2)``, ``'name'`` -> ``('name', None)``."""
    if "[" in part:
        key, rest = part.split("[", 1)
        idx = int(rest.rstrip("]"))
        return key, idx
    return part, None
