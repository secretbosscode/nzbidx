from __future__ import annotations

import logging
import os
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)

_OPENAI_URL = os.getenv("OPENAI_EMBED_URL", "https://api.openai.com/v1/embeddings")
_OPENAI_MODEL = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")
_OPENAI_KEY = os.getenv("OPENAI_API_KEY")

_OLLAMA_URL = os.getenv("OLLAMA_URL")
_OLLAMA_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")


def embed(text: str) -> Optional[List[float]]:
    """Return an embedding vector for ``text`` using a configured provider."""
    if _OLLAMA_URL:
        try:  # pragma: no cover - network errors
            resp = httpx.post(
                f"{_OLLAMA_URL}/api/embeddings",
                json={"prompt": text, "model": _OLLAMA_MODEL},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["embedding"]
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("embedding_failed", extra={"error": str(exc)})
            return None
    if _OPENAI_KEY:
        try:  # pragma: no cover - network errors
            resp = httpx.post(
                _OPENAI_URL,
                headers={"Authorization": f"Bearer {_OPENAI_KEY}"},
                json={"input": text, "model": _OPENAI_MODEL},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["data"][0]["embedding"]
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("embedding_failed", extra={"error": str(exc)})
            return None
    return None
