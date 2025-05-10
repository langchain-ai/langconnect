from typing import Any

from pydantic import BaseModel


class DocumentCreate(BaseModel):
    content: str | None = None
    metadata: dict[str, Any] | None = None


class DocumentUpdate(BaseModel):
    content: str | None = None
    metadata: dict[str, Any] | None = None


class DocumentResponse(BaseModel):
    id: str
    collection_id: str
    content: str | None = None
    metadata: dict[str, Any] | None = None
    created_at: str | None = None
    updated_at: str | None = None


class SearchQuery(BaseModel):
    query: str
    limit: int | None = 10
    filter: dict[str, Any] | None = None


class SearchResult(BaseModel):
    id: str
    collection_id: str
    content: str | None = None
    metadata: dict[str, Any] | None = None
    similarity: float
    created_at: str | None = None
