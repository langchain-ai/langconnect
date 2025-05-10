from fastapi import APIRouter, HTTPException, status

from langconnect.database import (
    create_pgvector_collection,
    delete_pgvector_collection,
    get_pgvector_collection_details,
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
):
    """Creates a new PGVector collection by name with optional metadata."""
    collection_name = collection_data.name
    metadata = collection_data.metadata
    try:
        existing = await get_pgvector_collection_details(collection_name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Collection '{collection_name}' already exists.",
            )
        create_pgvector_collection(collection_name, metadata)
        created_collection = await get_pgvector_collection_details(collection_name)
        if not created_collection:
            raise HTTPException(
                status_code=500, detail="Failed to retrieve collection after creation"
            )
        return CollectionResponse(**created_collection)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create collection '{collection_name}': {e}",
        )


@router.get("", response_model=list[CollectionResponse])
async def collections_list():
    """Lists all available PGVector collections (name and UUID)."""
    try:
        collections = await list_pgvector_collections()
        return [CollectionResponse(**c) for c in collections]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list collections: {e}",
        )


@router.get("/{collection_name}", response_model=CollectionResponse)
async def collections_get(
    collection_name: str,
):
    """Retrieves details (name and UUID) of a specific PGVector collection."""
    try:
        collection = await get_pgvector_collection_details(collection_name)
        if not collection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{collection_name}' not found",
            )
        return CollectionResponse(**collection)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get collection '{collection_name}': {e}",
        )


@router.delete("/{collection_name}", status_code=status.HTTP_204_NO_CONTENT)
async def collections_delete(
    collection_name: str,
):
    """Deletes a specific PGVector collection by name."""
    try:
        existing = await get_pgvector_collection_details(collection_name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{collection_name}' not found",
            )

        await delete_pgvector_collection(collection_name)
        return
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete collection '{collection_name}': {e}",
        )


@router.patch("/{collection_name}", response_model=CollectionResponse)
async def collections_update(
    collection_name: str,
    collection_data: CollectionUpdate,
):
    """Updates a specific PGVector collection's name and/or metadata."""
    try:
        # Check if collection exists
        existing = await get_pgvector_collection_details(collection_name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{collection_name}' not found",
            )

        # If a new name is provided, check if it already exists (unless it's the same name)
        if collection_data.name and collection_data.name != collection_name:
            existing_with_new_name = await get_pgvector_collection_details(
                collection_data.name
            )
            if existing_with_new_name:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Collection with name '{collection_data.name}' already exists",
                )

        # Update the collection
        updated_collection = await update_pgvector_collection(
            collection_name=collection_name,
            new_name=collection_data.name,
            metadata=collection_data.metadata,
        )

        if not updated_collection:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update collection '{collection_name}'",
            )

        return CollectionResponse(**updated_collection)
    except HTTPException:
        # Re-raise HTTP exceptions to preserve their status codes
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update collection '{collection_name}': {e}",
        )
