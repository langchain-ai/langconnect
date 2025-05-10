from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import datetime

# =====================
# Collection Schemas
# =====================


class CollectionCreate(BaseModel):
    """Schema for creating a new collection."""

    name: str = Field(..., description="The unique name of the collection.")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata for the collection."
    )


class CollectionUpdate(BaseModel):
    """Schema for updating an existing collection."""

    name: Optional[str] = Field(None, description="New name for the collection.")
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="Updated metadata for the collection."
    )


class CollectionResponse(BaseModel):
    """Schema for representing a collection from PGVector."""

    # PGVector table has uuid (UUID), name (str), and cmetadata (JSONB)
    # We get these from list/get db functions
    uuid: str = Field(
        ..., description="The unique identifier (UUID) of the collection in PGVector."
    )
    name: str = Field(..., description="The name of the collection.")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata associated with the collection."
    )

    class Config:
        # Allows creating model from dict like
        # {'uuid': '...', 'name': '...', 'metadata': {...}}
        from_attributes = True


# =====================
# Document Schemas
# =====================


class DocumentBase(BaseModel):
    page_content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DocumentCreate(DocumentBase):
    collection_id: str
    embedding: Optional[List[float]] = (
        None  # Embedding can be added during creation or later
    )


class DocumentUpdate(BaseModel):
    page_content: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    embedding: Optional[List[float]] = None


class DocumentResponse(DocumentBase):
    id: str
    collection_id: str
    embedding: Optional[List[float]] = None  # Represent embedding as list of floats
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True
        from_attributes = True  # Pydantic v2 way
