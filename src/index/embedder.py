"""Embed chunks using the local sentence-transformers model."""

from __future__ import annotations

from sentence_transformers import SentenceTransformer

from src.normalize.chunker import Chunk

DEFAULT_MODEL_PATH = "models/bge-large-en-v1.5"


class Embedder:
    def __init__(self, model_path: str = DEFAULT_MODEL_PATH):
        self._model = SentenceTransformer(model_path)

    def embed_chunks(self, chunks: list[Chunk]) -> list[dict]:
        """Embed all chunks and return dicts ready for Elasticsearch indexing."""
        texts = [chunk.search_text for chunk in chunks]
        vectors = self._model.encode(texts, show_progress_bar=True)

        docs = []
        for chunk, vector in zip(chunks, vectors):
            doc = {
                "chunk_id": chunk.chunk_id,
                "document_id": chunk.document_id,
                "chunk_type": chunk.chunk_type,
                "source_format": chunk.source_format,
                "search_text": chunk.search_text,
                "embedding": vector.tolist(),
                "token_estimate": chunk.token_estimate,
            }
            for key in ("section_label", "sheet_name", "page_number",
                        "row_index_start", "row_index_end", "row_index"):
                if key in chunk.metadata:
                    doc[key] = chunk.metadata[key]
            docs.append(doc)

        return docs
