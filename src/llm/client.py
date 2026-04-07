"""LLM client abstraction.

Defines a ``Protocol`` that the pipeline uses for all LLM interactions.
The local implementation wraps the OpenAI SDK.  At work, implement the
same protocol using the enterprise LangChain gateway.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Protocol, runtime_checkable

from openai import OpenAI, RateLimitError
from pydantic import BaseModel

log = logging.getLogger(__name__)

MAX_RETRIES = 6
INITIAL_BACKOFF = 2.0


@runtime_checkable
class LLMClient(Protocol):
    """Minimal interface the pipeline needs from an LLM provider."""

    def extract_structured(
        self,
        messages: list[dict],
        response_model: type[BaseModel],
        *,
        temperature: float = 0.0,
    ) -> BaseModel:
        """Return a Pydantic model instance parsed from LLM output."""
        ...

    def generate(
        self,
        messages: list[dict],
        *,
        temperature: float = 0.0,
    ) -> str:
        """Return raw text from the LLM."""
        ...


class OpenAIClient:
    """OpenAI SDK implementation of :class:`LLMClient`.

    Includes exponential backoff for rate-limit errors (429), which is
    necessary for low-TPM dev-tier API keys.
    """

    def __init__(
        self,
        model: str = "gpt-4o",
        api_key: str | None = None,
    ) -> None:
        self._model = model
        self._client = OpenAI(api_key=api_key)

    def extract_structured(
        self,
        messages: list[dict],
        response_model: type[BaseModel],
        *,
        temperature: float = 0.0,
    ) -> BaseModel:
        response = self._call_with_retry(
            lambda: self._client.responses.parse(
                model=self._model,
                input=messages,
                text_format=response_model,
                temperature=temperature,
            )
        )
        result = response.output_parsed
        if result is None:
            raise RuntimeError(
                f"LLM returned no structured output. "
                f"Refusal: {response.refusal}"
            )
        log.debug(
            "Structured output: %s",
            json.dumps(result.model_dump(), default=str)[:500],
        )
        return result

    def generate(
        self,
        messages: list[dict],
        *,
        temperature: float = 0.0,
    ) -> str:
        response = self._call_with_retry(
            lambda: self._client.responses.create(
                model=self._model,
                input=messages,
                temperature=temperature,
            )
        )
        return response.output_text

    @staticmethod
    def _call_with_retry(fn, *, max_retries: int = MAX_RETRIES):
        backoff = INITIAL_BACKOFF
        for attempt in range(max_retries + 1):
            try:
                return fn()
            except RateLimitError as exc:
                if attempt == max_retries:
                    raise
                wait = backoff
                header_wait = _parse_retry_after(exc)
                if header_wait:
                    wait = max(wait, header_wait)
                log.warning(
                    "Rate limited (attempt %d/%d), waiting %.1fs",
                    attempt + 1, max_retries, wait,
                )
                time.sleep(wait)
                backoff = min(backoff * 2, 60.0)


def _parse_retry_after(exc: RateLimitError) -> float | None:
    """Try to extract a wait time from the error message."""
    msg = str(exc)
    import re
    m = re.search(r"try again in (\d+\.?\d*)s", msg, re.IGNORECASE)
    if m:
        return float(m.group(1)) + 0.5
    return None
