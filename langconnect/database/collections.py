import json
import logging
from typing import Any, Optional, TypedDict

from fastapi import status
from fastapi.exceptions import HTTPException

from langconnect.database.connection import get_db_connection, get_vectorstore

logger = logging.getLogger(__name__)


class CollectionDetails(TypedDict):
    """TypedDict for collection details."""

    uuid: str
    name: str
    metadata: dict[str, Any]


class Collections:
    """Manages pgvector-backed collections, with user_id passed at runtime.
    """

    @staticmethod
    def _parse_metadata(raw: Any) -> dict[str, Any]:
        """Normalize JSONB/text metadata into a Python dict."""
        if raw is None or raw == "null":
            return {}
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                logger.exception("Failed to parse metadata JSON: %r", raw)
                return {}
        if isinstance(raw, dict):
            return raw
        return {}

    async def list(
        self,
        user_id: str,
    ) -> list[CollectionDetails]:
        """List all collections owned by the given user, ordered by name."""
        async with get_db_connection() as conn:
            records = await conn.fetch(
                """
                SELECT uuid, name, cmetadata
                  FROM langchain_pg_collection
                 WHERE cmetadata->>'owner_id' = $1
              ORDER BY name;
                """,
                user_id,
            )
        return [
            {
                "uuid": str(r["uuid"]),
                "name": r["name"],
                "metadata": self._parse_metadata(r["cmetadata"]),
            }
            for r in records
        ]

    async def get(
        self,
        user_id: str,
        collection_id: str,
    ) -> CollectionDetails | None:
        """Fetch a single collection by UUID, ensuring the user owns it."""
        async with get_db_connection() as conn:
            rec = await conn.fetchrow(
                """
                SELECT uuid, name, cmetadata
                  FROM langchain_pg_collection
                 WHERE uuid = $1
                   AND cmetadata->>'owner_id' = $2;
                """,
                collection_id,
                user_id,
            )
        if not rec:
            return None
        return {
            "uuid": str(rec["uuid"]),
            "name": rec["name"],
            "metadata": self._parse_metadata(rec["cmetadata"]),
        }

    async def create(
        self,
        user_id: str,
        collection_name: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> CollectionDetails:
        """Create a new collection.
        Raises 409 if a collection with that name already exists for this user.
        """
        # check for existing name
        existing = await self._get_by_name(user_id, collection_name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Collection '{collection_name}' already exists.",
            )

        metadata = metadata.copy() if metadata else {}
        metadata["owner_id"] = user_id

        # triggers PGVector to create both the vectorstore and DB entry
        get_vectorstore(collection_name, collection_metadata=metadata)

        # fetch the newly created one
        created = await self._get_by_name(user_id, collection_name)
        assert created, "Failed to create collection"
        return created

    async def update(
        self,
        user_id: str,
        collection_id: str,
        new_name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> CollectionDetails:
        """Rename and/or repopulate metadata of an existing collection.
        Raises 404 if no such collection.
        """
        if metadata is not None:
            metadata = metadata.copy()
            metadata["owner_id"] = user_id

        metadata_json = json.dumps(metadata) if metadata is not None else None

        async with get_db_connection() as conn:
            rec = await conn.fetchrow(
                """
                UPDATE langchain_pg_collection
                   SET name      = COALESCE($1, name),
                       cmetadata = COALESCE($2::json, cmetadata)
                 WHERE uuid = $3
                   AND cmetadata->>'owner_id' = $4
              RETURNING uuid, name, cmetadata;
                """,
                new_name,
                metadata_json,
                collection_id,
                user_id,
            )

        if not rec:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' not found or not owned by you.",
            )

        return {
            "uuid": str(rec["uuid"]),
            "name": rec["name"],
            "metadata": self._parse_metadata(rec["cmetadata"]),
        }

    async def delete(
        self,
        user_id: str,
        collection_id: str,
    ) -> int:
        """Delete a collection by UUID.
        Returns number of rows deleted (1).
        Raises 404 if no such collection.
        """
        async with get_db_connection() as conn:
            result = await conn.execute(
                """
                DELETE FROM langchain_pg_collection
                 WHERE uuid = $1
                   AND cmetadata->>'owner_id' = $2;
                """,
                collection_id,
                user_id,
            )
        count = int(result.split()[-1])
        if count == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' not found or not owned by you.",
            )
        return count

    async def _get_by_name(
        self,
        user_id: str,
        collection_name: str,
    ) -> Optional[CollectionDetails]:
        """Return details if a named collection exists, else None."""
        async with get_db_connection() as conn:
            rec = await conn.fetchrow(
                """
                SELECT uuid, name, cmetadata
                  FROM langchain_pg_collection
                 WHERE name = $1
                   AND cmetadata->>'owner_id' = $2;
                """,
                collection_name,
                user_id,
            )
        if not rec:
            return None
        return {
            "uuid": str(rec["uuid"]),
            "name": rec["name"],
            "metadata": self._parse_metadata(rec["cmetadata"]),
        }


# Singleton collections object.
COLLECTIONS = Collections()
