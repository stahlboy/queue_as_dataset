"""Data models for the queue processor."""

from typing import Literal
from pydantic import BaseModel, HttpUrl


class InterleavedChunk(BaseModel):
    """A chunk of interleaved content (text or image)."""
    type: Literal["text", "image"]
    value: str  # For text: the actual text. For image: URL or file reference


class ProcessedPage(BaseModel):
    """Result of processing a web page."""
    page_url: str
    chunks: list[InterleavedChunk]
    metadata: dict = {}


class QueueItemPayload(BaseModel):
    """Payload for a queue item."""
    page_url: str


class QueueItemCreate(BaseModel):
    """Request to create a new queue item."""
    page_url: HttpUrl


class QueueStats(BaseModel):
    """Queue statistics."""
    total: int
    pending: int
    processing: int
    completed: int
    failed: int
