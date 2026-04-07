"""Extraction planner -- the schema compiler.

Walks a Pydantic extraction schema and produces bounded
``ExtractionTask`` objects.  Has no domain knowledge; all intelligence
about *what* to extract lives in field metadata and the extraction
strategy provided by the business domain.

Two entry points:

* ``plan_extraction`` -- first-pass planning (scalar groups + array
  discovery tasks).
* ``plan_array_batches`` -- called after array discovery results come
  back to produce batch tasks.
"""

from __future__ import annotations

import uuid
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo

from .models import ArrayConfig, ExtractionTask

# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------

_SCALAR_TOKEN_ESTIMATES: dict[type, int] = {
    str: 30,
    int: 15,
    float: 15,
    bool: 10,
}
_PROVENANCE_OVERHEAD = 40  # confidence + evidence per field
_JSON_OVERHEAD = 20        # braces, commas, structural tokens


def _unwrap_optional(annotation: Any) -> Any:
    """``Optional[X]`` -> ``X``, passthrough otherwise."""
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_list_of_model(annotation: Any) -> tuple[bool, type[BaseModel] | None]:
    inner = _unwrap_optional(annotation)
    if get_origin(inner) is list:
        args = get_args(inner)
        if args and isinstance(args[0], type) and issubclass(args[0], BaseModel):
            return True, args[0]
    return False, None


def _is_model_type(annotation: Any) -> tuple[bool, type[BaseModel] | None]:
    inner = _unwrap_optional(annotation)
    if isinstance(inner, type) and issubclass(inner, BaseModel):
        return True, inner
    return False, None


def _is_scalar(annotation: Any) -> bool:
    is_list, _ = _is_list_of_model(annotation)
    is_model, _ = _is_model_type(annotation)
    return not is_list and not is_model


def estimate_field_tokens(info: FieldInfo) -> int:
    """Estimate output tokens for a single field (excluding provenance)."""
    ann = _unwrap_optional(info.annotation)

    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return estimate_model_tokens(ann)

    if get_origin(ann) is list:
        args = get_args(ann)
        if args and isinstance(args[0], type) and issubclass(args[0], BaseModel):
            return 0  # arrays handled separately
        return 50  # list[str] etc.

    return _SCALAR_TOKEN_ESTIMATES.get(ann, 30)


def estimate_model_tokens(model: type[BaseModel]) -> int:
    """Estimate total output tokens for all declared fields in *model*."""
    total = _JSON_OVERHEAD
    for info in model.model_fields.values():
        total += estimate_field_tokens(info) + _PROVENANCE_OVERHEAD
    return total


# ---------------------------------------------------------------------------
# Retrieval query construction
# ---------------------------------------------------------------------------

def _build_retrieval_query(
    field_infos: dict[str, FieldInfo],
    parent_meta: dict | None = None,
) -> str:
    """Build a retrieval query string from field metadata.

    Sources (in order):
    1. Field names (always available)
    2. Field descriptions
    3. aliases / keywords from ``json_schema_extra``
    4. Parent-level keywords (from the field on the parent model that
       references this nested model)
    """
    parts: list[str] = []

    if parent_meta:
        parts.extend(parent_meta.get("aliases", []))
        parts.extend(parent_meta.get("keywords", []))

    for name, info in field_infos.items():
        parts.append(name.replace("_", " "))
        if info.description:
            parts.append(info.description)
        extra = info.json_schema_extra or {}
        parts.extend(extra.get("aliases", []))
        parts.extend(extra.get("keywords", []))

    seen: set[str] = set()
    unique: list[str] = []
    for p in parts:
        low = p.lower().strip()
        if low and low not in seen:
            seen.add(low)
            unique.append(p.strip())
    return " ".join(unique)


def _collect_chunk_type_hints(
    field_infos: dict[str, FieldInfo],
    parent_meta: dict | None = None,
) -> list[str]:
    hints: set[str] = set()
    if parent_meta:
        hints.update(parent_meta.get("chunk_type_hints", []))
    for info in field_infos.values():
        extra = info.json_schema_extra or {}
        hints.update(extra.get("chunk_type_hints", []))
    return sorted(hints)


# ---------------------------------------------------------------------------
# Sub-model construction
# ---------------------------------------------------------------------------

def _build_sub_model(
    field_infos: dict[str, FieldInfo],
    group_name: str,
) -> type[BaseModel]:
    """Dynamically create a Pydantic model containing only *field_infos*."""
    definitions: dict[str, Any] = {}
    for name, info in field_infos.items():
        definitions[name] = (
            info.annotation,
            Field(description=info.description, default=None),
        )
    return create_model(f"Extract_{group_name}", **definitions)


# ---------------------------------------------------------------------------
# Instruction attachment
# ---------------------------------------------------------------------------

def _build_prompt_instructions(
    field_infos: dict[str, FieldInfo],
    intent_instructions: str,
) -> str:
    parts: list[str] = []
    if intent_instructions:
        parts.append(intent_instructions)
    for name, info in field_infos.items():
        extra = info.json_schema_extra or {}
        field_instr = extra.get("instructions", "")
        if field_instr:
            parts.append(f"For {name}: {field_instr}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Decomposition
# ---------------------------------------------------------------------------

def _full_path(prefix: str, name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


def _decompose(
    model: type[BaseModel],
    path_prefix: str,
    intent_instructions: str,
    output_token_budget: int,
    tasks: list[ExtractionTask],
    parent_meta: dict | None = None,
    *,
    _depth: int = 0,
) -> None:
    """Recursively decompose *model* into ``ExtractionTask`` objects.

    ``_depth`` tracks array nesting depth.  When already inside an array
    element (depth >= 1), nested arrays are treated as scalar fields to
    prevent combinatorial task explosion.
    """
    fields = model.model_fields

    scalar_fields: dict[str, FieldInfo] = {}
    nested_fields: dict[str, tuple[FieldInfo, type[BaseModel]]] = {}
    array_fields: dict[str, tuple[FieldInfo, type[BaseModel]]] = {}

    for name, info in fields.items():
        is_list, elem = _is_list_of_model(info.annotation)
        if is_list and _depth < 1:
            array_fields[name] = (info, elem)  # type: ignore[arg-type]
            continue

        is_model, mtype = _is_model_type(info.annotation)
        if is_model:
            nested_fields[name] = (info, mtype)  # type: ignore[arg-type]
            continue

        scalar_fields[name] = info

    # Scalars go in one group; nested models ALWAYS recurse so
    # their leaf fields become individually extractable strings.
    if scalar_fields:
        sc_est = _JSON_OVERHEAD + sum(
            estimate_field_tokens(i) + _PROVENANCE_OVERHEAD
            for i in scalar_fields.values()
        )
        group_name = (path_prefix.replace(".", "_") or "root")
        if nested_fields:
            group_name += "_scalars"
        field_paths = [_full_path(path_prefix, n) for n in scalar_fields]
        tasks.append(ExtractionTask(
            task_id=str(uuid.uuid4()),
            task_type="scalar_group",
            field_paths=field_paths,
            output_schema=_build_sub_model(scalar_fields, group_name),
            retrieval_query=_build_retrieval_query(
                {**scalar_fields, **{n: i for n, (i, _) in nested_fields.items()}},
                parent_meta,
            ),
            retrieval_filters=_make_filters(scalar_fields, parent_meta),
            prompt_instructions=_build_prompt_instructions(
                scalar_fields, intent_instructions,
            ),
            token_budget=sc_est,
        ))

    for name, (info, mtype) in nested_fields.items():
        field_meta = info.json_schema_extra or {}
        _decompose(
            mtype,
            _full_path(path_prefix, name),
            intent_instructions,
            output_token_budget,
            tasks,
            parent_meta=field_meta,
            _depth=_depth,
        )

    # Arrays always produce discovery tasks
    for name, (info, elem_type) in array_fields.items():
        tasks.append(_make_discovery_task(
            _full_path(path_prefix, name),
            info,
            elem_type,
            intent_instructions,
        ))


def _make_filters(
    field_infos: dict[str, FieldInfo],
    parent_meta: dict | None,
) -> dict:
    hints = _collect_chunk_type_hints(field_infos, parent_meta)
    if hints:
        return {"chunk_type": hints}
    return {}


# ---------------------------------------------------------------------------
# Array discovery / batch tasks
# ---------------------------------------------------------------------------

class ArrayDiscoveryResult(BaseModel):
    """LLM response schema for the array discovery step."""
    count: int = Field(description="Number of distinct items found in the evidence.")
    identifiers: list[str] = Field(
        default_factory=list,
        description="Short identifying label for each item (e.g., class name, employee ID).",
    )


def _make_discovery_task(
    field_path: str,
    field_info: FieldInfo,
    element_type: type[BaseModel],
    intent_instructions: str,
) -> ExtractionTask:
    extra = field_info.json_schema_extra or {}
    query_parts: list[str] = [field_path.replace(".", " ").replace("_", " ")]
    if field_info.description:
        query_parts.append(field_info.description)
    query_parts.extend(extra.get("keywords", []))
    query_parts.extend(extra.get("aliases", []))

    return ExtractionTask(
        task_id=str(uuid.uuid4()),
        task_type="array_discovery",
        field_paths=[field_path],
        output_schema=ArrayDiscoveryResult,
        retrieval_query=" ".join(query_parts),
        retrieval_filters=_make_filters({field_path.rsplit(".", 1)[-1]: field_info}, None),
        prompt_instructions=(
            f"{intent_instructions}\n\n"
            f"Determine how many distinct items exist for '{field_path}'. "
            f"Return a count and, if available, a short identifying label "
            f"for each item."
        ),
        token_budget=200,
        array_config=ArrayConfig(array_field_path=field_path),
    )


def plan_array_batches(
    field_path: str,
    element_schema: type[BaseModel],
    discovery: ArrayDiscoveryResult,
    intent_instructions: str,
    output_token_budget: int = 2000,
    batch_size_override: int | None = None,
) -> list[ExtractionTask]:
    """Produce batch tasks after array discovery completes.

    If the element type is small enough, items are batched directly.
    If it's too large (e.g., ``EmployeeClass`` with nested coverages),
    each item is decomposed into sub-group tasks scoped to that item's
    identifier.
    """
    per_element = estimate_model_tokens(element_schema)
    count = discovery.count
    identifiers = discovery.identifiers or [str(i + 1) for i in range(count)]

    if per_element <= output_token_budget:
        # Simple batching
        batch_size = batch_size_override or max(
            1, output_token_budget // per_element
        )
        return _batch_simple(
            field_path, element_schema, count, identifiers,
            batch_size, intent_instructions, output_token_budget,
        )

    # Complex element: decompose per identifier
    return _batch_decomposed(
        field_path, element_schema, identifiers,
        intent_instructions, output_token_budget,
    )


def _batch_simple(
    field_path: str,
    element_schema: type[BaseModel],
    count: int,
    identifiers: list[str],
    batch_size: int,
    intent_instructions: str,
    budget: int,
) -> list[ExtractionTask]:
    tasks: list[ExtractionTask] = []
    query_base = _build_retrieval_query(element_schema.model_fields)

    for start in range(0, count, batch_size):
        end = min(start + batch_size, count)
        batch_ids = identifiers[start:end]
        scoped_query = f"{' '.join(batch_ids)} {query_base}"

        tasks.append(ExtractionTask(
            task_id=str(uuid.uuid4()),
            task_type="array_batch",
            field_paths=[field_path],
            output_schema=_build_batch_model(element_schema, field_path),
            retrieval_query=scoped_query,
            prompt_instructions=(
                f"{intent_instructions}\n\n"
                f"Extract items {start + 1} through {end} of '{field_path}'. "
                f"Identifiers: {', '.join(batch_ids)}."
            ),
            token_budget=budget,
            array_config=ArrayConfig(
                array_field_path=field_path,
                batch_start=start,
                batch_size=batch_size,
                total_count=count,
            ),
        ))
    return tasks


def _batch_decomposed(
    field_path: str,
    element_schema: type[BaseModel],
    identifiers: list[str],
    intent_instructions: str,
    budget: int,
) -> list[ExtractionTask]:
    """Each identifier gets its own set of decomposed sub-group tasks."""
    tasks: list[ExtractionTask] = []

    for idx, identifier in enumerate(identifiers):
        sub_tasks: list[ExtractionTask] = []
        _decompose(
            element_schema,
            f"{field_path}[{idx}]",
            intent_instructions,
            budget,
            sub_tasks,
            _depth=1,
        )
        for t in sub_tasks:
            t.retrieval_query = f"{identifier} {t.retrieval_query}"
            t.prompt_instructions = (
                f"IMPORTANT: You are extracting data ONLY for "
                f"'{identifier}' (item {idx + 1} of {len(identifiers)} "
                f"in '{field_path}'). Ignore data for other items.\n\n"
                f"{t.prompt_instructions}"
            )
            t.array_config = ArrayConfig(
                array_field_path=field_path,
                batch_start=idx,
                batch_size=1,
                total_count=len(identifiers),
                item_identifier=identifier,
            )
        tasks.extend(sub_tasks)
    return tasks


def _build_batch_model(
    element_schema: type[BaseModel],
    field_path: str,
) -> type[BaseModel]:
    safe_name = field_path.replace(".", "_").replace("[", "").replace("]", "")
    return create_model(
        f"Batch_{safe_name}",
        items=(list[element_schema], Field(description=f"Extracted items for {field_path}")),  # type: ignore[valid-type]
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def plan_extraction(
    schema: type[BaseModel],
    intent_instructions: str = "",
    output_token_budget: int = 2000,
) -> list[ExtractionTask]:
    """First-pass planning: produce scalar_group and array_discovery tasks.

    Call ``plan_array_batches`` after discovery results come back to
    produce the batch tasks.
    """
    tasks: list[ExtractionTask] = []
    _decompose(schema, "", intent_instructions, output_token_budget, tasks)
    return tasks
