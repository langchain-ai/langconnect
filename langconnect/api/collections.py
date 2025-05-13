from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from langconnect.auth import AuthenticatedUser, resolve_user
from langconnect.database import (
    create_pgvector_collection,
    delete_pgvector_collection,
    get_collection_by_id,
    get_collection_by_name,
    list_pgvector_collections,
    update_pgvector_collection,
)
from langconnect.models import CollectionCreate, CollectionResponse, CollectionUpdate

router = APIRouter(prefix="/collections", tags=["collections"])


@router.post(
    "",
    response_model=CollectionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def collections_create(
    collection_data: CollectionCreate,
    user: Annotated[AuthenticatedUser, Depends(resolve_user)],
):
    """Creates a new PGVector collection by name with optional metadata."""
    metadata = collection_data.metadata
    collection_name = collection_data.name
    # TODO(Eugene): Remove all the unnecessary requests.
    collection_info = await get_collection_by_name(user, collection_name)
    if collection_info:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Collection '{collection_name}' already exists.",
        )
    await create_pgvector_collection(user, collection_name, metadata)
    collection_info = await get_collection_by_name(user, collection_name)
    if not collection_info:
        raise HTTPException(
            status_code=500, detail="Failed to retrieve collection after creation"
        )
    return CollectionResponse(**collection_info)


@router.get("", response_model=list[CollectionResponse])
async def collections_list(user: Annotated[AuthenticatedUser, Depends(resolve_user)]):
    """Lists all available PGVector collections (name and UUID)."""
    collections = await list_pgvector_collections(user)
    return [CollectionResponse(**c) for c in collections]


@router.get("/{collection_id}", response_model=CollectionResponse)
async def collections_get(
    user: Annotated[AuthenticatedUser, Depends(resolve_user)],
    collection_id: UUID,
):
    """Retrieves details (name and UUID) of a specific PGVector collection."""
    collection = await get_collection_by_id(user, str(collection_id))
    if not collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Collection '{collection_id}' not found",
        )
    return CollectionResponse(**collection)


@router.delete("/{collection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def collections_delete(
    user: Annotated[AuthenticatedUser, Depends(resolve_user)],
    collection_id: UUID,
):
    """Deletes a specific PGVector collection by name."""
    await delete_pgvector_collection(user, str(collection_id))
    return HTTPException(
        status_code=status.HTTP_204_NO_CONTENT,
        detail=f"Collection '{collection_id}' deleted successfully.",
    )


@router.patch("/{collection_id}", response_model=CollectionResponse)
async def collections_update(
    user: Annotated[AuthenticatedUser, Depends(resolve_user)],
    collection_id: UUID,
    collection_data: CollectionUpdate,
):
    """Updates a specific PGVector collection's name and/or metadata."""
    # Update the collection
    updated_collection = await update_pgvector_collection(
        user,
        collection_id=str(collection_id),
        new_name=collection_data.name,
        metadata=collection_data.metadata,
    )

    if not updated_collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Failed to update collection '{collection_id}'",
        )

    return CollectionResponse(**updated_collection)
