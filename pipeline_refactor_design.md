# Pipeline Refactor: RAG-Based Document Processing

## 1. Context

### Current Pipeline

Four decoupled services communicating through Kafka:

```
Ingest  ─── Kafka ───►  Classify  ─── Kafka ───►  Extract  ─── Kafka ───►  Dispatch
```

- **Ingest**: Receives raw documents from inbound sources (e.g., emailed requests with attachments). Stores raw files in S3. Creates a Postgres record for idempotency and status tracking.
- **Classify**: Agentic stage. LLM agent with document parsing tools identifies the intent (e.g., group insurance quote, wire transfer, claim). Produces structured output: intent, confidence, rationale.
- **Extract**: Agentic stage. One LLM agent instance per document. Each agent receives the full Pydantic extraction schema for the classified intent and extracts what it can. Outputs are merged deterministically across documents.
- **Dispatch**: Routes the extracted data downstream.

### Challenges

1. **Context window overflow**: Large inbound documents exceed the LLM context window as parsing tool calls accumulate.
2. **Output token overflow**: Extraction schemas with hundreds of entities exceed the LLM's max output token limit. This is amplified by array data (dozens of plan designs, thousands of employees).
3. **Domain coupling**: The core pipeline must remain agnostic. Business domains provide intents, schemas, and instructions as configuration. No intent-specific logic in the pipeline code.

### Goals

- Handle 50-70 MB request bundles safely within 16 GB pod memory
- Avoid LLM context window and output token limits via bounded, batched extraction tasks
- Provide full provenance (confidence + evidence chunk IDs) for every extracted value
- Be schema-agnostic and intent-agnostic -- no domain-specific logic in the core pipeline
- Be resumable and observable at every stage

### Non-Goals

- One-shot extraction across all documents
- Schema-specific or intent-specific hardcoding in pipeline code
- Using LLMs for orchestration or merging (merge is deterministic)

### Key Architectural Shift

From **document-oriented extraction** (one agent per document, extract everything, merge) to **field-oriented extraction** (for each group of fields, retrieve relevant evidence from all documents, extract that group).

---

## 2. Proposed Architecture

### Physical Architecture: 4 Services + Kafka

The refactored pipeline retains the same 4-service, 3-topic Kafka topology:

```
Ingest  ─── Kafka ───►  Classify  ─── Kafka ───►  Extract  ─── Kafka ───►  Dispatch
```

What changes is the internal behavior of the services. The Ingest service now also normalizes documents. The Extract service replaces the single-shot agentic approach with a structured plan-retrieve-extract loop that includes validation and self-repair.

### Logical Steps Within Each Service

Each physical service contains one or more logical steps. Logical steps are internal to a service -- they are function calls and control flow, not Kafka-connected components.

| Physical Service | Logical Steps | What Changed |
|-----------------|---------------|-------------|
| **Ingest** | Receive + Store + Normalize (parse, chunk, embed, index) | Added: Normalize. Documents are parsed into chunks and indexed in Elasticsearch as part of ingestion. |
| **Classify** | Classify | Simplified: Agent reads curated chunks from Elasticsearch instead of using parsing tools directly. |
| **Extract** | Plan → Retrieve → Extract → Merge → Validate → Repair | Replaced: Single-shot agentic extraction replaced by a structured, iterative loop with bounded tasks. |
| **Dispatch** | Dispatch | No change. |

### Data Flow

```
              INGEST SERVICE                    CLASSIFY SERVICE
┌─────────────────────────────────┐        ┌──────────────────────┐
│  Receive raw docs               │        │  Read curated chunks │
│         │                       │        │  from ES (headings,  │
│         ▼                       │        │  email body/headers) │
│  Store in S3 + Postgres         │        │         │            │
│         │                       │        │         ▼            │
│         ▼                       │        │  LLM classification  │
│  Normalize:                     │        │         │            │
│    Parse → Chunk → Embed → ES   │        │         ▼            │
│                                 │        │  {intent, confidence}│
└────────────┬────────────────────┘        └──────────┬───────────┘
             │                                        │
             ▼                                        ▼
    ┌── Kafka Topic ──┐                      ┌── Kafka Topic ──┐
    │ {request_id,    │                      │ {request_id,    │
    │  document_ids}  │                      │  intent}        │
    └────────┬────────┘                      └────────┬────────┘
             │                                        │
             ▼                                        ▼
     CLASSIFY SERVICE                         EXTRACT SERVICE
                                 ┌──────────────────────────────────────┐
                                 │  Load schema + strategy for intent   │
                                 │         │                            │
                                 │         ▼                            │
                                 │  PLAN: decompose schema → tasks      │
                                 │         │                            │
                                 │         ▼                            │
                                 │  ┌─────────────────────────┐        │
                                 │  │ For each task:           │        │
                                 │  │   RETRIEVE: query ES     │◄── ES │
                                 │  │   EXTRACT:  call LLM     │        │
                                 │  └─────────────────────────┘        │
                                 │         │                            │
                                 │         ▼                            │
                                 │  MERGE: assemble results             │
                                 │         │                            │
                                 │         ▼                            │
                                 │  VALIDATE: check result              │
                                 │         │                            │
                                 │    pass? ──no──► REPAIR              │
                                 │         │        (re-retrieve,       │
                                 │         │         re-extract,        │
                                 │        yes        re-merge,          │
                                 │         │         re-validate)       │
                                 │         ▼         max 2 iterations   │
                                 │  Emit extraction result              │
                                 └──────────────────┬───────────────────┘
                                                    │
                                                    ▼
                                           ┌── Kafka Topic ──┐
                                           │ {request_id,    │
                                           │  result,        │
                                           │  report}        │
                                           └────────┬────────┘
                                                    │
                                                    ▼
                                             DISPATCH SERVICE
```

### Kafka Topic Payloads

Kafka events are lightweight routing messages. All heavy data (raw files, chunks, embeddings) lives in S3 and Elasticsearch, referenced by `request_id`.

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `ingest.completed` | Ingest | Classify | `{request_id, document_ids}` |
| `classify.completed` | Classify | Extract | `{request_id, intent, confidence, rationale}` |
| `extract.completed` | Extract | Dispatch | `{request_id, extraction_result, validation_report}` |

### Parallelism: Normalize and Classify

Normalize runs as part of the Ingest service. The Kafka event to Classify fires after normalization is complete. However, normalization and classification operate on different data: normalization indexes all document content into ES, while classification only needs a handful of curated chunks (email body, headings). A future optimization could allow the Classify service to start reading from ES before the Ingest service fully completes, but the baseline design processes them sequentially across the Kafka boundary.

```
Time ──────────────────────────────────────────────────────────►

INGEST SERVICE:   ████████████████████████
                  [receive][normalize    ]
                                          │ Kafka
CLASSIFY SERVICE:                         ██████████
                                          [classify ]
                                                     │ Kafka
EXTRACT SERVICE:                                     ████████████████████████
                                                     [plan][retr+extr][merge+val+repair]
                                                                                        │ Kafka
DISPATCH SERVICE:                                                                       ████
```

---

## 3. Ingest Service

### Responsibility

Receive raw documents, store them, and prepare them for downstream consumption by parsing, chunking, embedding, and indexing into Elasticsearch.

### Logical Steps

1. **Receive**: Accept inbound documents (e.g., from an email listener, API gateway, or file drop).
2. **Store**: Write raw files to S3. Create a tracking record in Postgres with status `ingested`.
3. **Normalize**: Parse, chunk, embed, and index (detailed below).
4. **Emit**: Publish `{request_id, document_ids}` to `ingest.completed` Kafka topic. Update Postgres status to `normalized`.

### 3.1 Normalize: Parse

Format-specific parsers produce a flat list of structural elements. Parsers are format-aware but content-agnostic.

Supported formats: XLSX, DOCX, PDF, EML.

- **XLSX**: Detects KV layouts (column A = label, column B = value) even when extra columns (C, D) have occasional values. In KV mode, rows with a key but no value are emitted as level-2 headings (sub-section headers). Multi-column tabular data is parsed as table rows with headers from the first non-empty row.
- **DOCX**: Headings from paragraph styles, table rows from tables, with section break detection.
- **PDF**: Classifies lines as headings (ALL CAPS, "X - Y" patterns), KV pairs ("Key: Value"), or text. Tables detected by pdfplumber are parsed as table rows.
- **EML**: Email headers (From, To, Subject, Date) as KV pairs tagged with `is_email_header`. Body as text.

#### Element Types

| Type | What it represents |
|------|-------------------|
| `text` | Free-form paragraph or narrative |
| `heading` | Structural heading or title (with level) |
| `table_row` | One row from a table or spreadsheet |
| `kv_pair` | A key-value pair |

### 3.2 Normalize: Chunk

Walk elements and merge adjacent ones into composite chunks, respecting structural boundaries and a token budget (~512 tokens).

Structural boundaries: headings, section breaks, sheet transitions, element type transitions, email header / body transitions. The chunker never interprets the meaning of content.

#### Hierarchical Section Context

The chunker maintains a section heading stack keyed by heading level. When a level-1 heading appears, it resets the stack. When a level-2 heading appears, it pops any existing level-2+ entries and pushes the new one. The resulting section label is the stack joined with ` > `.

Example: a document with "Plan Design - Executives" (level 1) followed by "DENTAL" (level 2) produces `section_label: "Plan Design - Executives > DENTAL"`. This label is prepended to the chunk's `search_text` as `[Plan Design - Executives > DENTAL]` and stored in metadata. When a budget split forces multiple chunks within the same section, all chunks carry the same hierarchical label.

#### Chunk Types

| Type | What it represents | How produced |
|------|-------------------|-------------|
| `text` | Merged paragraphs or narrative | Default for `TextElement` and heading-only groups |
| `table_chunk` | Group of table rows with column headers | Groups of `TableRowElement` |
| `kv_group` | Group of key-value pairs | Groups of `KVPairElement` (non-email) |
| `email_header` | Email metadata (From, To, Subject, Date) | `KVPairElement` with `is_email_header` metadata from EML parser |

### 3.3 Normalize: Embed

Encode each chunk's `search_text` using a local embedding model (bge-large-en-v1.5, 1024 dims).

### 3.4 Normalize: Index

Bulk-insert chunks into Elasticsearch with both lexical (`search_text`) and semantic (`embedding`) fields.

#### Elasticsearch Index Schema

Index name: `evidence_chunks`

```json
{
  "mappings": {
    "properties": {
      "chunk_id":        {"type": "keyword"},
      "request_id":      {"type": "keyword"},
      "document_id":     {"type": "keyword"},
      "chunk_type":      {"type": "keyword"},
      "source_format":   {"type": "keyword"},
      "search_text":     {"type": "text"},
      "embedding":       {"type": "dense_vector", "dims": 1024, "index": true, "similarity": "cosine"},
      "section_label":   {"type": "keyword"},
      "sheet_name":      {"type": "keyword"},
      "page_number":     {"type": "integer"},
      "row_index_start": {"type": "integer"},
      "row_index_end":   {"type": "integer"},
      "token_estimate":  {"type": "integer"}
    }
  }
}
```

### Integration Entry Point

The `normalize_file(path, document_id, token_budget)` function is the integration entry point for the Ingest service. The `document_id` parameter accepts an identifier assigned by the Ingest service (e.g., UUID or S3 key). When omitted, it falls back to the filename (suitable for local dev only). The Ingest service calls this per document after downloading from S3 to a temp path.

Parsing errors are isolated per document. A corrupted or unsupported file is logged and skipped; it does not fail the entire request. The indexing stats include a `parse_failures` list so the validation report downstream can flag unparseable documents.

### Design Principles

- **Format-aware, content-agnostic**: Parsers know how to read XLSX, DOCX, PDF, EML. They do not know or care what the content means.
- **Structural boundaries, not semantic boundaries**: Chunks are split at headings, section breaks, sheet boundaries, and element type transitions.
- **Token-bounded**: Every chunk stays within a configurable token budget to ensure it fits within embedding model context and retrieval windows.
- **Metadata-rich**: Every chunk carries provenance (source file, page/sheet, section, row range) enabling the Extract service to cite evidence.

---

## 4. Classify Service

### Responsibility

Determine the intent of the request so the Extract service knows which schema and strategy to load.

### Logical Steps

1. **Consume**: Read `{request_id, document_ids}` from `ingest.completed` Kafka topic.
2. **Curate context**: Query Elasticsearch for a small set of curated chunks -- email body, email headers, heading/title chunks from each document. This is a bounded, predictable context.
3. **Classify**: Single LLM call with the curated context and the list of possible intents.
4. **Emit**: Publish `{request_id, intent, confidence, rationale}` to `classify.completed`. Update Postgres status to `classified`.

### Change from Current

The classify agent no longer needs parsing tools. Instead, it reads pre-indexed chunks from Elasticsearch. This makes classification faster, cheaper, and immune to context window overflow.

### Output

```json
{
  "intent": "group_insurance_quote_request",
  "confidence": "high",
  "rationale": "Email requests a group benefits quote, attachments include census and plan designs."
}
```

### Why Classify is a Separate Service

1. Classification is a routing decision. It determines which schema and strategy the Extract service loads. It must complete before extraction can begin.
2. An "unknown" or low-confidence classification might route to a human review queue rather than to extraction. That is a fundamentally different downstream path.
3. Different failure modes. A classification failure ("I don't know what this is") requires different alerting and remediation than an extraction failure ("I know what this is but couldn't extract field X").

---

## 5. Extract Service

### Responsibility

Given a classified intent and an Elasticsearch index full of evidence chunks, produce a validated extraction result conforming to the intent's Pydantic schema.

The Extract service contains six tightly coupled logical steps. They run within a single service because:

- **Data dependency**: Plan produces tasks, Retrieve needs tasks, Extract needs retrieved evidence, Merge needs extraction results. There is no natural Kafka boundary between them.
- **Iterative coupling**: Array handling requires Plan to run, then Extract (discovery), then Plan again (batch tasks based on discovery results). Repair requires re-running Retrieve + Extract for specific fields. These feedback loops would add unnecessary Kafka round-trip latency if split across services.
- **Infrastructure overlap**: Plan, Retrieve, Extract, Merge, Validate, and Repair all need the same dependencies (ES client, embedding model, LLM client, schema registry). Splitting them would duplicate infrastructure configuration.

### Logical Steps

1. **Consume**: Read `{request_id, intent, confidence}` from `classify.completed`.
2. **Load**: Load the Pydantic extraction schema and extraction strategy for the classified intent.
3. **Plan**: Decompose the schema into bounded extraction tasks.
4. **For each task**: Retrieve evidence from ES, then call the LLM to extract.
5. **Merge**: Combine all task outputs into a single schema instance.
6. **Validate**: Check the merged result against schema constraints and business rules.
7. **Repair** (if needed): Re-retrieve and re-extract for failed fields. Bounded to N iterations.
8. **Emit**: Publish `{request_id, extraction_result, validation_report}` to `extract.completed`. Update Postgres status to `extracted`.

### Internal Control Flow

```
Load schema + strategy
        │
        ▼
    PLAN ──────────────────────────────────────────┐
        │                                          │
        ▼                                          │
  [scalar_group tasks]    [array_discovery tasks]  │
        │                         │                │
        │                    RETRIEVE + EXTRACT    │
        │                         │                │
        │                    discovery results     │
        │                         │                │
        │                  PLAN (resume) ◄─────────┘
        │                         │
        │                  [array_batch tasks]
        │                         │
        ▼                         ▼
  ┌──────────────────────────────────────┐
  │ For each task (parallel, bounded):   │
  │   RETRIEVE: query ES for evidence    │
  │   EXTRACT: LLM call with evidence    │
  └──────────────────────────────────────┘
        │
        ▼
      MERGE
        │
        ▼
    VALIDATE
        │
   pass? ──no──► REPAIR (re-retrieve + re-extract for failed fields)
        │               │
       yes              ▼
        │         MERGE (repair results into existing)
        │               │
        │         VALIDATE (again, max 2 total iterations)
        │               │
        ▼               ▼
   Emit result + validation report
```

---

### 5.1 Plan

#### Responsibility

The planner is a **schema compiler**. It takes a Pydantic extraction schema and compiles it into a list of bounded extraction tasks. It has no domain knowledge. All intelligence about what to extract and how to find it lives in the schema metadata and extraction strategy, provided by the business domain.

#### Inputs

1. **Extraction schema**: A Pydantic `BaseModel` class hierarchy with fields, types, descriptions, and metadata.
2. **Extraction strategy**: Per-intent configuration with general instructions and optional per-field metadata.

#### Output

A list of `ExtractionTask` objects:

```python
@dataclass
class ExtractionTask:
    task_id: str
    task_type: str                # "scalar_group", "array_discovery", "array_batch"
    field_paths: list[str]        # e.g. ["broker_info.name", "broker_info.email"]
    output_schema: type[BaseModel]  # Pydantic sub-model for these fields
    retrieval_query: str          # search string for Elasticsearch
    retrieval_filters: dict       # optional chunk_type / metadata filters
    prompt_instructions: str      # relevant extraction instructions
    token_budget: int             # estimated output tokens
    array_config: ArrayConfig | None  # for array tasks only
```

#### Schema Decomposition

The planner walks the Pydantic schema tree recursively and produces candidate groups:

```
ExtractionSchema
├── broker_info: BrokerInfo              → candidate group (5 scalar fields)
├── client_info: ClientInfo              → candidate group (6 scalar fields)
├── existing_coverage: ExistingCoverage  → candidate group (8 scalar fields)
├── plan_designs: list[PlanDesign]       → array (special handling)
│   └── PlanDesign
│       ├── class_name: str
│       ├── life_insurance: LifeInsurance    → sub-group (15 fields)
│       ├── dental: DentalCoverage           → sub-group (12 fields)
│       ├── disability: DisabilityCoverage   → sub-group (20 fields)
│       └── ...
└── employees: list[Employee]            → array (special handling)
    └── Employee (8 scalar fields)
```

**Algorithm:**

```
def decompose(model_class, path="", budget=2000):
    fields = get_model_fields(model_class)
    
    if is_list_type(fields[path]):
        return create_array_tasks(element_type, path, budget)
    
    estimated_tokens = estimate_output_tokens(fields)
    
    if estimated_tokens <= budget:
        return [create_scalar_group(fields, path)]
    
    # Too large -- split by nested objects
    groups = []
    scalar_fields = [f for f in fields if is_scalar(f)]
    nested_fields = [f for f in fields if is_model(f)]
    
    if scalar_fields:
        groups.append(create_scalar_group(scalar_fields, path))
    
    for nested in nested_fields:
        groups.extend(decompose(nested.type, path + "." + nested.name, budget))
    
    return groups
```

This is purely structural. The planner doesn't know what `broker_info` or `dental` means. It sees nested `BaseModel` types and field counts.

#### Token Estimation

Each extraction task's output is a JSON object. Output tokens are estimated based on field types:

| Field Type | Estimated Output Tokens |
|-----------|------------------------|
| `str` | 30 (key + typical value + quotes) |
| `int`, `float` | 15 (key + number) |
| `bool` | 10 (key + true/false) |
| `Optional[T]` | same as T (null is cheap) |
| `list[str]` | 50 (key + short list) |
| Nested `BaseModel` | sum of its fields |

Per-field overhead for provenance (confidence + evidence chunk IDs): ~40 tokens per field.

**Formula:**

```
group_tokens = sum(field_estimate + provenance_overhead for each field) + json_structure_overhead
```

The output token budget per task is configurable (default: 2000 tokens). If a group exceeds the budget, it is split into smaller groups by walking deeper into the schema hierarchy.

#### Array Handling

Arrays (e.g., `list[Employee]`, `list[PlanDesign]`) cannot be extracted in one shot when the array is large. The planner creates a two-phase plan:

**Phase 1: Discovery**

A single extraction task that determines how many items exist and, optionally, what identifies each item.

```python
ExtractionTask(
    task_type="array_discovery",
    field_paths=["employees"],
    retrieval_query="employee census name EmpID",  # from field metadata
    prompt_instructions="Determine how many employees are listed. Return a count and, if available, a list of identifiers.",
    output_schema=ArrayDiscoveryResult,  # {"count": int, "identifiers": list[str]}
)
```

The Retrieve step fetches chunks that look like census/table data. The LLM scans the evidence and returns a count.

**Phase 2: Batched Extraction**

Based on the discovery result, the planner creates batch tasks:

```python
for batch_start in range(0, count, batch_size):
    ExtractionTask(
        task_type="array_batch",
        field_paths=["employees"],
        output_schema=list[Employee],  # the array element schema
        array_config=ArrayConfig(
            batch_start=batch_start,
            batch_size=batch_size,  # e.g., 20
            total_count=count,
        ),
        retrieval_query="employee census ...",
        prompt_instructions=f"Extract employees {batch_start+1} through {min(batch_start+batch_size, count)}.",
    )
```

The batch size is determined by:

```
batch_size = floor(output_token_budget / tokens_per_element)
tokens_per_element = estimate_output_tokens(ElementModel) + provenance_overhead
```

**Nested arrays** (e.g., `list[PlanDesign]` where each `PlanDesign` has many fields):

If the element type itself is too large for one extraction call, the planner applies decomposition recursively:

1. Discovery: how many plan designs exist, and what identifies each (e.g., class name).
2. For each plan design:
   - One task per sub-group within `PlanDesign` (e.g., one task for `dental`, one for `life_insurance`).
   - Each task's retrieval query is scoped to both the plan design identifier and the sub-group fields.

```
Discovery: "How many plan designs / employee classes?"
  → Result: 3 (Executives, Full Time, Part Time)

For "Executives":
  Task: life_insurance fields → query: "executives life insurance basic optional waiver"
  Task: dental fields → query: "executives dental basic major orthodontic"
  Task: disability fields → query: "executives disability short term long term"
  ...

For "Full Time":
  Task: life_insurance fields → query: "full time life insurance basic optional waiver"
  ...
```

#### Sub-Model Construction

Each extraction task needs a Pydantic model that represents just its fields, for use as the LLM's structured output schema. The planner constructs these dynamically:

```python
from pydantic import create_model

def build_sub_model(fields: list[FieldInfo], group_name: str) -> type[BaseModel]:
    field_definitions = {}
    for field in fields:
        field_definitions[field.name] = (
            field.annotation,
            Field(description=field.description, default=None)
        )
    return create_model(f"Extract_{group_name}", **field_definitions)
```

For provenance, each extraction task's output is wrapped:

```python
class FieldResult(BaseModel):
    value: Any
    confidence: float  # 0.0 to 1.0
    evidence: list[str]  # chunk_ids that support this value

class ExtractionResult(BaseModel):
    fields: dict[str, FieldResult]
```

The LLM receives the sub-model schema as its structured output format. The response is guaranteed to fit within the output token budget because the planner sized the group accordingly.

#### Retrieval Query Construction

For each extraction task, the planner builds a retrieval query from field metadata. The query is constructed mechanically from three sources:

**Source 1: Field names** (always available)

```python
query_parts = [field.name.replace("_", " ") for field in group_fields]
# ["broker name", "broker email", "broker company"]
```

**Source 2: Field descriptions** (from `Field(description=...)`)

```python
for field in group_fields:
    if field.description:
        query_parts.append(field.description)
# ["Full name of the insurance broker", "Broker's email address"]
```

**Source 3: Extraction strategy metadata** (from `json_schema_extra` or external config)

```python
for field in group_fields:
    meta = field.json_schema_extra or {}
    query_parts.extend(meta.get("aliases", []))
    query_parts.extend(meta.get("keywords", []))
# ["consultant name", "advisor", "submitted by"]
```

**Final query:**

```python
retrieval_query = " ".join(deduplicate(query_parts))
```

Optionally, the extraction strategy can provide `chunk_type_hints` (e.g., prefer `table_chunk` for census data) which become Elasticsearch filters:

```python
retrieval_filters = {"chunk_type": meta.get("chunk_type_hints", [])}
```

#### Instruction Attachment

Instructions are sliced by scope:

1. **Universal prompt** (from core pipeline): Always included. Contains the extraction rules that apply to all intents (e.g., "Use only provided evidence", "Return null if missing", "Provide confidence scores").

2. **Intent-level instructions** (from extraction strategy): Included in every extraction task for this intent (e.g., "When extracting monetary values, always include currency. Dates should be ISO 8601.").

3. **Field-level instructions** (from Pydantic field metadata): Included only in the task containing that field.

```python
def build_prompt_instructions(task, intent_instructions, universal_prompt):
    parts = [universal_prompt, intent_instructions]
    for field in task.fields:
        field_instruction = field.json_schema_extra.get("instructions", "")
        if field_instruction:
            parts.append(f"For {field.name}: {field_instruction}")
    return "\n\n".join(parts)
```

---

### 5.2 Retrieve

#### Responsibility

For each extraction task, query Elasticsearch to find the most relevant evidence chunks.

#### Query Structure

Hybrid search: BM25 (lexical) + cosine similarity (semantic), filtered by `request_id`. Scoring uses a weighted combination of BM25 and vector scores via Elasticsearch's `bool` query. An alternative is **Reciprocal Rank Fusion (RRF)**, which is more robust to score distribution differences between BM25 and vector retrieval. RRF can be adopted later if score weighting proves difficult to tune.

```python
def retrieve(task: ExtractionTask, request_id: str, top_k: int = 20):
    query_vector = embedding_model.encode(task.retrieval_query)
    
    body = {
        "size": top_k,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"request_id": request_id}},
                    # optional chunk_type filter from task
                ],
                "must": [
                    {"match": {"search_text": task.retrieval_query}}
                ],
                "should": [
                    {
                        "script_score": {
                            "query": {"match_all": {}},
                            "script": {
                                "source": "cosineSimilarity(params.qv, 'embedding') + 1.0",
                                "params": {"qv": query_vector}
                            }
                        }
                    }
                ]
            }
        }
    }
    return es.search(index="evidence_chunks", body=body)
```

#### Context Assembly

Retrieved chunks are assembled into the extraction prompt, ordered by relevance score, and trimmed to fit the LLM input context budget:

```
total_input_tokens = system_prompt + instructions + schema + evidence_chunks
```

If retrieved chunks exceed the input budget, lower-scoring chunks are dropped.

#### Context Expansion (Optional)

For table data, neighboring chunks from the same document/sheet/section can be included to provide row continuity.

#### Reranking (Optional, Not in Baseline)

If retrieval quality proves insufficient (right chunks retrieved but ranked poorly), a cross-encoder reranker can be inserted: retrieve top-50 from ES, rerank to top-20, then assemble into prompt. This is an optimization to add based on observed retrieval hit rate, not a prerequisite.

---

### 5.3 Extract (LLM Call)

#### Responsibility

For each extraction task, call the LLM with the assembled evidence and output schema. Produce structured extraction results with provenance.

#### Prompt Structure

```
[System prompt - universal rules]
[Intent-level instructions]
[Field-level instructions]

Extract the following fields:
[Sub-model schema as JSON schema or field list]

Evidence:
[Retrieved chunks, each labeled with chunk_id]

Rules:
- Extract only from the provided evidence
- Return null for any field not found in the evidence
- Provide a confidence score (0.0-1.0) for each field
- Cite the chunk_id(s) that support each extracted value
```

#### Output

```json
{
  "fields": {
    "broker_name": {
      "value": "Rahul Mehta",
      "confidence": 0.95,
      "evidence": ["chunk_abc123"]
    },
    "broker_email": {
      "value": "broker@examplebroker.com",
      "confidence": 0.98,
      "evidence": ["chunk_def456"]
    }
  }
}
```

#### Parallelism

Independent extraction tasks run in parallel with bounded concurrency:

```python
MAX_CONCURRENT_EXTRACTIONS = 6

async with semaphore(MAX_CONCURRENT_EXTRACTIONS):
    results = await gather(*[extract(task) for task in tasks])
```

Array batch tasks for the same array run sequentially or with controlled ordering to avoid duplicate extraction.

#### Deterministic Fields (No LLM)

Some fields can be extracted deterministically from structured metadata without calling the LLM:

- **Email headers**: From, To, Subject, Date are already parsed as `KVPairElement` entries with `is_email_header` metadata. If the extraction schema maps to these, the Plan stage can mark the corresponding extraction task as `deterministic: true` and the Merge stage can populate the fields directly from chunk metadata.
- **File metadata**: Source filename, format, page count -- available from normalization output.

This reduces LLM cost and latency for fields that require no interpretation.

---

### 5.4 Merge

#### Responsibility

Combine extraction results from all tasks into a single instance of the intent's Pydantic schema.

#### Algorithm

1. Collect all `FieldResult` objects keyed by field path.
2. For scalar fields: if multiple tasks extracted the same field (overlap), resolve by highest confidence; if tied, prefer the result with more evidence chunks.
3. For array fields: concatenate batch results in order.
4. For nested objects: assemble from their constituent field groups.

#### Output

A fully populated instance of the extraction schema, with provenance metadata attached.

---

### 5.5 Validate

#### Responsibility

Check the merged extraction result against:

1. **Schema validation**: Required fields present, correct types.
2. **Business rules**: Cross-field consistency (e.g., end_date > start_date, salary > 0). These rules are provided by the business domain in the extraction strategy.
3. **Completeness**: What percentage of fields were extracted vs. returned null?
4. **Confidence thresholds**: Flag fields below a configurable confidence threshold.

#### Output

A validation report listing:
- Missing required fields
- Failed business rules
- Low-confidence fields
- Overall extraction completeness score

If all checks pass, the Extract service emits the result. If checks fail, the Repair step runs.

---

### 5.6 Repair

#### Responsibility

For fields that failed validation (missing, low-confidence, failed rules), attempt targeted repair without re-extracting fields that already passed.

#### Algorithm

1. **Broaden retrieval**: Re-query Elasticsearch with expanded/alternative queries for the specific failed fields.
2. **Retry extraction**: Re-run LLM extraction for just the failed fields with the broadened evidence.
3. **Re-merge**: Merge repair results into the existing extraction result.
4. **Re-validate**: Run validation again on the updated result.

#### Bounded Retry

Maximum repair iterations are configurable (default: 2). After max retries, unresolved fields are flagged in the validation report for human review. The Extract service emits the best result it has along with the report.

---

## 6. Dispatch Service

### Responsibility

Route the validated extraction result downstream. No change from the current design.

### Logical Steps

1. **Consume**: Read `{request_id, extraction_result, validation_report}` from `extract.completed`.
2. **Route**: Deliver the result to the appropriate downstream system based on intent and business rules.
3. **Update**: Mark the Postgres record as `dispatched`.

---

## 7. Domain Configuration Model

### Principle

The core pipeline (all 4 services) is domain-agnostic. Business domains provide all domain-specific configuration.

### What Domains Provide

**1. Intents**

```python
INTENTS = [
    "group_insurance_quote_request",
    "wire_transfer_request",
    "insurance_claim",
    "invoice_payment_request",
    ...
]
```

**2. Extraction Schema (per intent)**

```python
class GroupInsuranceQuoteSchema(BaseModel):
    broker_info: BrokerInfo
    client_info: ClientInfo
    existing_coverage: ExistingCoverage
    plan_designs: list[PlanDesign]
    employees: list[Employee]

class BrokerInfo(BaseModel):
    name: str = Field(
        description="Full name of the insurance broker or consultant",
        json_schema_extra={
            "aliases": ["consultant name", "advisor name"],
            "keywords": ["broker", "consultant", "submitted by"],
            "instructions": "Look in the email signature or body for the broker name."
        }
    )
    email: str = Field(
        description="Broker's email address",
        json_schema_extra={
            "keywords": ["email", "from"],
            "chunk_type_hints": ["kv_group", "email_header"]
        }
    )
    ...
```

**3. Extraction Strategy (per intent)**

```python
class ExtractionStrategy(BaseModel):
    intent: str
    schema: type[BaseModel]
    instructions: str  # intent-level extraction guidance
    validation_rules: list[ValidationRule]  # business rules
    confidence_threshold: float = 0.7
    max_repair_iterations: int = 2
    output_token_budget: int = 2000
    array_batch_size_override: dict[str, int] = {}  # optional per-array
```

### What the Core Pipeline Provides

- Universal system prompt (extraction rules that apply to all intents)
- Ingest service: file storage, normalization, Elasticsearch index management
- Classify service: intent classification machinery
- Extract service: Plan, Retrieve, Extract, Merge, Validate, Repair machinery
- Dispatch service: downstream routing
- Request tracking and status management (Postgres)
- Observability and logging

---

## 8. Observability

### Metrics by Service

| Metric | Service | Logical Step | Purpose |
|--------|---------|-------------|---------|
| Normalization time | Ingest | Normalize | Parsing/embedding performance |
| Chunks per request | Ingest | Normalize | Input complexity |
| Classification confidence | Classify | Classify | Routing quality |
| Extraction tasks per request | Extract | Plan | Schema complexity |
| Retrieval latency per task | Extract | Retrieve | Elastic performance |
| Retrieval hit rate | Extract | Retrieve | Query quality |
| LLM extraction latency per task | Extract | Extract | LLM performance |
| Token usage per task | Extract | Extract | Cost tracking |
| Confidence distribution | Extract | Extract | Extraction quality |
| Validation pass rate | Extract | Validate | End-to-end quality |
| Repair success rate | Extract | Repair | Recovery effectiveness |
| Fields extracted vs. null | Extract | Merge | Completeness |

### Request Status Tracking (Postgres)

Status transitions map to physical service boundaries:

```
ingested → normalized → classified → extracted → dispatched
```

Each status transition is timestamped. The transition from `normalized` to `classified` corresponds to the Kafka handoff from Ingest to Classify. The transition from `classified` to `extracted` corresponds to the handoff from Classify to Extract. And so on.

Internal progress within the Extract service (planning, retrieving, extracting, merging, validating, repairing) is tracked via structured logging and metrics, not via Postgres status changes. This avoids excessive database writes for what is an internal loop.

Failures at any stage are recorded in Postgres with error details and a `failed` status.

---

## 9. Infrastructure

### Production

| Component | Used By | Purpose |
|-----------|---------|---------|
| S3 | Ingest | Raw document storage |
| PostgreSQL | All services | Request tracking, idempotency, status |
| Kafka | Between all services | Async event-driven communication |
| Elasticsearch 8.x | Ingest, Classify, Extract | Evidence chunk index (BM25 + vector search) |
| Embedding model | Ingest, Extract | bge-large-en-v1.5 (1024 dims), deployed from Artifactory |
| LLM (API) | Classify, Extract | Classification and extraction |

### Local Development

| Component | Purpose |
|-----------|---------|
| Local filesystem | Replaces S3 and Kafka (direct function calls between logical steps) |
| Elasticsearch (Docker) | Same as production |
| Embedding model (local) | Same model, loaded from local path |
| LLM API key | Same as production |

In local development, the 4-service topology collapses into a single process. Kafka is replaced by direct function calls. S3 is replaced by the local filesystem. Postgres is optional (can use in-memory state for prototyping).

---

## 10. Implementation Phases

### Phase 1: Normalize + Index (COMPLETE)

*Service: Ingest (normalize sub-step)*

- Format-specific parsers (XLSX, DOCX, PDF, EML)
- Structural chunker with boundary detection and token budgets
- Local embedding model integration (bge-large-en-v1.5)
- Elasticsearch index with hybrid search (BM25 + vector)
- Tested against three sample document bundles with different formats and layouts

### Phase 2: Plan

*Service: Extract (plan sub-step)*

- Schema introspection engine (walk Pydantic models)
- Token estimation for field groups
- Schema decomposition into bounded extraction tasks
- Array discovery and batch planning
- Sub-model construction with `pydantic.create_model`
- Retrieval query construction from field metadata

### Phase 3: Retrieve + Extract

*Service: Extract (retrieve + extract sub-steps)*

- Retrieval integration (Elastic queries per task)
- Context assembly with token budget enforcement
- LLM extraction with structured output
- Provenance tracking (confidence + evidence chunk IDs)
- Parallel extraction with bounded concurrency

### Phase 4: Merge + Validate + Repair

*Service: Extract (merge + validate + repair sub-steps)*

- Deterministic merge of extraction task outputs
- Conflict resolution (highest confidence, most evidence)
- Array concatenation and ordering
- Schema validation
- Business rule validation (from extraction strategy)
- Completeness scoring
- Targeted retrieval broadening for failed fields
- Bounded repair loop

### Phase 5: End-to-End Integration

*All services*

- Classify service refactor to use normalized chunks from ES
- Integration with existing Ingest and Dispatch stages
- Postgres status tracking integration
- Kafka topic setup and event contracts
- S3 integration for document storage

---

## 11. Failure Modes and Mitigations

| Failure Mode | Mitigation |
|-------------|-----------|
| Poor retrieval (right evidence not in top-K) | Hybrid search (BM25 + vector), hierarchical section labels, query expansion in Repair stage, optional reranking |
| LLM context window overflow | Token-bounded chunks (~512 tokens), context assembly with budget enforcement, lower-scoring chunks dropped |
| LLM output token overflow | Schema decomposition into bounded extraction tasks, token estimation per field group, configurable output budget |
| Large arrays (thousands of rows) | Two-phase array handling: discovery then batched extraction, batch size computed from token budget |
| Corrupted/unparseable documents | Per-document error isolation in normalization, parse failures reported in stats |
| Ambiguous section boundaries | Hierarchical section context, sheet_name metadata, structural boundary detection |
| Cross-class contamination in retrieval | Hierarchical section labels disambiguate (e.g., "Executives > DENTAL" vs "Full Time > DENTAL"), retrieval queries scoped by class identifier |

---

## 12. Key Design Principles

1. **Never extract from documents directly. Always extract from retrieved evidence.** The LLM never sees raw documents. It only sees relevant chunks retrieved from the Elasticsearch index.

2. **The planner is a schema compiler, not a domain expert.** It walks Pydantic types and produces bounded execution plans. Domain knowledge lives in field metadata and extraction strategies.

3. **Schema structure is the grouping signal.** Nested `BaseModel` types naturally define extraction groups. No heuristics or domain-specific grouping logic.

4. **Array handling is always two-phase.** Discovery first (how many items?), then batched extraction. This pattern applies universally to any `list[BaseModel]` field.

5. **The core pipeline is intent-agnostic.** All domain-specific configuration (schemas, instructions, validation rules) is provided externally by business domains.

6. **Normalization is format-aware but content-agnostic.** Parsers know file formats. They never interpret what content means.

7. **One path, not two.** Every request goes through the RAG pipeline. The pipeline naturally scales down for simple inputs and scales up for complex ones.

8. **Physical services are Kafka-connected. Logical steps are function calls.** The 4-service topology matches the current deployment model. All new complexity (plan, retrieve, merge, validate, repair) is internal to the Extract service.
