import json
import logging
from typing import Any, TypedDict

from fastapi.exceptions import HTTPException

from langconnect.auth import AuthenticatedUser
from langconnect.database.connection import get_db_connection, get_vectorstore

logger = logging.getLogger(__name__)


async def assert_ownership_of_collection(
    user: AuthenticatedUser,
    collection_name: str,
) -> None:
    """Assert that the user owns the collection."""
    async with get_db_connection() as conn:
        query = """
            SELECT uuid
            FROM langchain_pg_collection 
            WHERE name = $1 AND cmetadata->>'owner_id' = $2;
        """
        record = await conn.fetchrow(query, collection_name, user.identity)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Collection '{collection_name}' does not exist or "
                    f"you do not own it."
                ),
            )
        # Verify that there is at most one record
        if len(record) > 1:
            # This should never occur and denotes a programming error.
            raise HTTPException(
                status_code=500,
            )


async def create_pgvector_collection(
    user: AuthenticatedUser, collection_name: str, metadata: dict[str, Any]
) -> None:
    """Explicitly creates a collection using PGVector with optional metadata.

    Note: This is often not necessary as adding documents implicitly creates it.
    PGVector.create_collection is synchronous, so run in executor.
    """
    if not isinstance(metadata, dict):
        raise TypeError(
            f"Programming error: metadata must be a dict. Got {type(metadata)}"
        )

    # The fields below are stored in the metadata column for now, but they
    # should be stored in separate columns.
    metadata["owner_id"] = user.identity

    # Calling this will create the collection w/ metadata in the database.
    # The PGVector class will always attempt to get/create a collection when
    # the class is instantiated.
    get_vectorstore(collection_name, collection_metadata=metadata)


async def list_pgvector_collections(user: AuthenticatedUser) -> list[dict[str, Any]]:
    """Lists all collections directly from the langchain_pg_collection table.

    Filters collections by matching the usowner_ider_id in the cmetadata JSONB field with the authenticated user's identity.
    """
    collections = []
    async with get_db_connection() as conn:
        query = """
            SELECT uuid, name, cmetadata
            FROM langchain_pg_collection 
            WHERE cmetadata->>'owner_id' = $1
            ORDER BY name;
        """
        records = await conn.fetch(query, user.identity)
        for record in records:
            # Handle cmetadata - it can be None, a string 'null', or a JSON string
            metadata = {}
            if record["cmetadata"] is not None and record["cmetadata"] != "null":
                try:
                    # If it's a JSON string, parse it
                    if isinstance(record["cmetadata"], str) and record[
                        "cmetadata"
                    ].startswith("{"):
                        metadata = json.loads(record["cmetadata"])
                    else:
                        metadata = record["cmetadata"]
                except Exception as e:
                    logger.exception(
                        f"Error parsing metadata in list_pgvector_collections: {e}"
                    )
                    # If parsing fails, use empty dict

            collection = {
                "uuid": str(record["uuid"]),
                "name": record["name"],
                "metadata": metadata,
            }
            collections.append(collection)
    return collections


class CollectionDetails(TypedDict):
    """TypedDict for collection details."""

    uuid: str
    """UUID of the collection."""
    name: str
    """Name of the collection."""
    metadata: dict[str, Any]
    """Metadata of the collection."""


async def get_pgvector_collection_details(
    user: AuthenticatedUser,
    collection_name: str,
) -> CollectionDetails | None:
    """Gets collection details (uuid, name, metadata) if it exists, None otherwise."""
    async with get_db_connection() as conn:
        query = """
            SELECT uuid, name, cmetadata 
            FROM langchain_pg_collection 
            WHERE name = $1 AND cmetadata->>'owner_id' = $2;
        """
        record = await conn.fetchrow(query, collection_name, user.identity)

        if record:
            # Handle cmetadata - it can be None, a string 'null', or a JSON string
            metadata = {}
            if record["cmetadata"] is not None and record["cmetadata"] != "null":
                try:
                    # If it's a JSON string, parse it
                    if isinstance(record["cmetadata"], str) and record[
                        "cmetadata"
                    ].startswith("{"):
                        metadata = json.loads(record["cmetadata"])
                    else:
                        metadata = record["cmetadata"]
                except Exception as e:
                    logger.exception(
                        f"Error parsing metadata in get_pgvector_collection_details: {e}"
                    )
                    raise
            return {
                "uuid": str(record["uuid"]),
                "name": record["name"],
                "metadata": metadata,
            }
    return None


async def delete_pgvector_collection(
    user: AuthenticatedUser, collection_name: str
) -> int:
    """Deletes a collection using PGVector.

    Return the number of rows deleted from the collections table.
    """
    async with get_db_connection() as conn:
        query = """
            DELETE FROM langchain_pg_collection 
            WHERE name = $1 AND cmetadata->>'owner_id' = $2;
        """
        results = await conn.execute(query, collection_name, user.identity)
        if not results.startswith("DELETE"):
            raise AssertionError(
                f"Error deleting collection '{collection_name}': {results}"
            )
        num_deleted = results.split(" ")[1]
        return num_deleted


async def update_pgvector_collection(
    user: AuthenticatedUser,
    collection_name: str,
    new_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Atomically update a collection's name and/or metadata in one DB call.

    - If `new_name` is None, the name is left unchanged.
    - If `metadata` is None, the JSON metadata column is left untouched.
    - Otherwise, cmetadata is replaced wholesale with the provided JSON.
    Returns the updated collection details, or None if no match.
    """
    if metadata is not None:
        metadata["owner_id"] = user.identity  # Ensure owner_id is set
    # Prepare the JSON blob, or None
    metadata_json = json.dumps(metadata) if metadata is not None else None

    async with get_db_connection() as conn:
        query = """
            UPDATE langchain_pg_collection
            SET
                name      = COALESCE($1, name),
                cmetadata = COALESCE($2::json, cmetadata)
            WHERE
                name = $3
              AND cmetadata->>'owner_id' = $4
            RETURNING uuid, name, cmetadata;
        """
        record = await conn.fetchrow(
            query,
            new_name,
            metadata_json,
            collection_name,
            user.identity,
        )

    if not record:
        return None

    # Parse returned cmetadata into a Python dict
    updated_metadata: dict[str, Any] = {}
    raw = record["cmetadata"]
    if raw and raw != "null":
        try:
            if isinstance(raw, str) and raw.startswith("{"):
                updated_metadata = json.loads(raw)
            else:
                updated_metadata = raw  # already a dict
        except Exception as e:
            logger.exception(f"Failed to parse updated metadata: {e}")

    return {
        "uuid": str(record["uuid"]),
        "name": record["name"],
        "metadata": updated_metadata,
    }
