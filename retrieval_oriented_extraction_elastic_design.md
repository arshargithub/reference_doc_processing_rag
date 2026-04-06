# Retrieval-Oriented Extraction Design Using Elasticsearch (Production-Ready)

> **DEPRECATED**: This document is superseded by [`pipeline_refactor_design.md`](pipeline_refactor_design.md), which covers the full 4-service architecture, physical/logical stage mapping, and reflects the current implementation. This file is retained for historical reference only. Do not use it as a source of truth for mappings, field names, dimensions, or APIs.

## 1. Overview

This document defines a **production-grade design** for a retrieval-oriented extraction system using **Elasticsearch (Elastic)** as the unified retrieval layer. The system decomposes extraction into bounded, schema-agnostic work units and uses **hybrid retrieval (BM25 + vector)** to gather evidence for each unit.

Pipeline segment covered:

**Normalize → Plan → Retrieve → Extract → Merge → Validate → Repair → Dispatch**

---

## 2. Goals / Non-Goals

### Goals
- Handle 50–70 MB request bundles safely
- Operate within 16 GB pod memory
- Avoid LLM context/output limits via batching
- Provide full provenance for every extracted value
- Be schema-agnostic and intent-agnostic
- Be resumable and observable

### Non-Goals
- One-shot extraction across all documents
- Schema-specific hardcoding in pipeline code
- Using LLMs for orchestration/merging by default

---

## 3. High-Level Architecture

### Components
1. **Evidence Store** (S3 + Postgres + Elasticsearch)
2. **Extraction Planner** (schema → groups → plan)
3. **Retrieval Engine** (Elastic hybrid queries)
4. **Extraction Workers** (bounded LLM calls)
5. **Merge Engine** (deterministic consolidation)
6. **Validation Engine** (schema + business rules)
7. **Repair Loop** (targeted retries)

### Data Flow

```
Plan → (for each group)
  → Build Query
  → Retrieve Top-K Chunks
  → Assemble Prompt
  → LLM Extraction
  → Persist Field Results
→ Merge → Validate → Repair → Finalize
```

---

## 4. Elasticsearch Index Design

### 4.1 Index: `evidence_chunks`

Each document = one chunk.

### 4.2 Mapping (example)

```json
{
  "mappings": {
    "properties": {
      "chunk_id": {"type": "keyword"},
      "request_id": {"type": "keyword"},
      "document_id": {"type": "keyword"},
      "chunk_type": {"type": "keyword"},
      "document_type": {"type": "keyword"},

      "search_text": {"type": "text"},

      "embedding": {
        "type": "dense_vector",
        "dims": 1536,
        "index": true,
        "similarity": "cosine"
      },

      "page_number": {"type": "integer"},
      "sheet_name": {"type": "keyword"},
      "section_label": {"type": "keyword"},

      "token_estimate": {"type": "integer"},
      "created_at": {"type": "date"}
    }
  }
}
```

### 4.3 Indexing Strategy
- One document per chunk
- Store both lexical (`search_text`) and semantic (`embedding`)
- Include metadata for filtering and boosting

---

## 5. Hybrid Retrieval Design

### 5.1 Query Structure (Elastic DSL)

```json
{
  "size": 20,
  "query": {
    "bool": {
      "filter": [
        {"term": {"request_id": "req_123"}}
      ],
      "must": [
        {
          "multi_match": {
            "query": "customer name address",
            "fields": ["search_text"]
          }
        }
      ],
      "should": [
        {
          "script_score": {
            "query": {"match_all": {}},
            "script": {
              "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
              "params": {
                "query_vector": [/* vector */]
              }
            }
          }
        }
      ]
    }
  }
}
```

### 5.2 Scoring

Final score can be:
- weighted combination (BM25 + vector)
- OR RRF (recommended for robustness)

### 5.3 Metadata Filtering

Use filters for:
- `request_id`
- `chunk_type`
- `document_type`

Example:

```json
{"terms": {"chunk_type": ["table_chunk", "sheet_table_chunk"]}}
```

---

## 6. Retrieval Pipeline

### Step 1: Build Query

From field group:
- aliases
- description
- examples

Pseudo:

```python
def build_query(group):
    terms = group.aliases + group.keywords
    return " ".join(terms)
```

---

### Step 2: Execute Hybrid Search

```python
def retrieve_chunks(query, vector, filters, top_k=20):
    return elastic.search(...)
```

---

### Step 3: Context Expansion

- include neighboring chunks
- include same document/page segments

---

### Step 4: Optional Reranking

Use:
- cross-encoder
- or lightweight LLM scoring

---

## 7. Extraction Worker Design

### Input
- field_group
- retrieved_chunks

### Prompt Template

```
Extract the following fields:
- name
- address

Rules:
- Use only provided evidence
- Return null if missing

Evidence:
<chunks>
```

### Output Schema

```json
{
  "fields": [
    {
      "path": "request.name",
      "value": "John Smith",
      "confidence": 0.92,
      "evidence": ["chunk_123"]
    }
  ]
}
```

---

## 8. Special Handling

### Arrays (tables)

#### Phase 1: discovery
- detect number of rows

#### Phase 2: extraction

```python
for batch in paginate(rows, size=20):
    extract(batch)
```

---

### Deterministic Fields

Handled outside LLM:
- email headers
- file metadata

---

## 9. Merge Engine

### Responsibilities
- combine results
- resolve conflicts

### Conflict Resolution

Priority:
1. higher confidence
2. better source type
3. more supporting evidence

---

## 10. Validation Engine

### Checks
- required fields
- data types
- cross-field consistency

### Example

```python
if end_date < start_date:
    raise ValidationError
```

---

## 11. Repair Loop

### Trigger
- missing fields
- low confidence

### Actions

```python
if missing:
    broaden_query()
    retry_extraction()
```

---

## 12. Execution Orchestration

### Parallelism

- parallel groups: yes
- bounded concurrency: required

Example:

```python
MAX_WORKERS = 6
```

---

## 13. APIs

### Plan API

```
POST /plan
```

### Retrieve API

```
POST /retrieve
```

### Extract API

```
POST /extract
```

---

## 14. Sequence Flow

```
Planner → Retrieval → Extraction → Merge → Validate → Repair
```

---

## 15. Observability

Track:
- retrieval latency
- extraction latency
- token usage
- success rate
- confidence distribution

---

## 16. Failure Modes

### Poor retrieval
Mitigation:
- hybrid search
- query expansion

### Token overflow
Mitigation:
- strict grouping

### Large tables
Mitigation:
- pagination

---

## 17. Implementation Phases

### Phase 1
- Elastic index
- basic retrieval

### Phase 2
- hybrid queries
- extraction workers

### Phase 3
- merge + validation

### Phase 4
- repair loop

---

## 18. Key Principle

**Never extract from documents directly. Always extract from retrieved evidence.**

---

## 19. Next Steps

- implement Elastic index
- implement retrieval layer
- integrate with planner
- build extraction worker

---

End of document.

