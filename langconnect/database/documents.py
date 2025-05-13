import json
import logging
import uuid
from typing import Any, Optional

import asyncpg
from langchain_core.documents import Document

from langconnect.auth import AuthenticatedUser
from langconnect.database.collections import get_collection_details_for_user
from langconnect.database.connection import (
    get_db_connection,
    get_vectorstore,
)

logger = logging.getLogger(__name__)


async def add_documents_to_vectorstore(
    user: AuthenticatedUser,
    collection_id: str,
    collection_name: str,
    documents: list[Document],
) -> list[str]:
    """Adds LangChain documents to the specified PGVector collection."""
    store = get_vectorstore(collection_name=collection_name)
    added_ids = store.add_documents(documents)
    return added_ids


async def list_documents_in_vectorstore(
    user: AuthenticatedUser,
    collection_id: str,
    limit: int = 10,
    offset: int = 0,
) -> list[dict[str, Any]] | None:
    """Lists unique documents based on 'file_id' in metadata from the vector store.
    Returns one representative entry per file_id.
    NOTE: This bypasses LangChain's abstraction for efficient unique listing.
    Requires direct asyncpg connection to query langchain_pg_embedding table.

    Only returns documents from collections owned by the authenticated user.

    Returns:
        - A list of dictionaries, each representing a document with its ID, content,
          metadata, and collection ID.
        - An empty list if no documents are found in the collection, but the collection exists.
        - None if the collection does not exist or is not owned by the user.
    """
    documents = []
    try:
        async with get_db_connection() as conn:
            # Get collection with owner check
            collection_query = """
            SELECT uuid FROM langchain_pg_collection 
            WHERE uuid = $1 AND cmetadata->>'owner_id' = $2
            """
            collection_record = await conn.fetchrow(
                collection_query,
                collection_id,
                user.identity if user else None,
            )

            if not collection_record:
                return None

            collection_uuid = collection_record["uuid"]

            query = """
            WITH UniqueFileChunks AS (
                SELECT DISTINCT ON (cmetadata->>'file_id')
                    id, 
                    cmetadata->>'file_id' as file_id 
                FROM langchain_pg_embedding
                WHERE collection_id = $1
                  AND cmetadata->>'file_id' IS NOT NULL 
                ORDER BY cmetadata->>'file_id', id 
            )
            SELECT emb.id, emb.document, emb.cmetadata
            FROM langchain_pg_embedding emb
            JOIN UniqueFileChunks ufc ON emb.id = ufc.id
            ORDER BY ufc.file_id 
            LIMIT $2 OFFSET $3;
            """
            records = await conn.fetch(query, collection_uuid, limit, offset)

            for record in records:
                try:
                    metadata_dict = (
                        json.loads(record["cmetadata"]) if record["cmetadata"] else {}
                    )
                except json.JSONDecodeError:
                    metadata_dict = {"error": "Failed to parse metadata"}
                    logger.info(
                        f"Warning: Could not parse metadata for document ID {record['id']}: {record['cmetadata']}"
                    )

                documents.append(
                    {
                        "id": str(record["id"]),
                        "content": record["document"],
                        "metadata": metadata_dict,
                        "collection_id": str(collection_uuid),
                    }
                )
    except asyncpg.exceptions.UndefinedTableError:
        logger.info(
            f"Table langchain_pg_embedding or langchain_pg_collection "
            f"not found for collection '{collection_id}'. Returning empty list."
        )
        return []
    except Exception as e:
        logger.info(f"Error listing documents from vector store: {e}")
        return []

    return documents


async def get_document(
    document_id: str,
    user: AuthenticatedUser = None,
) -> Optional[dict[str, Any]]:
    """Gets a single document by its ID from the vector store's underlying table.
    Requires direct SQL access.

    Only returns documents from collections owned by the authenticated user.
    """
    if user is None:
        raise ValueError("User must be provided")

    try:
        doc_uuid = uuid.UUID(document_id)
    except ValueError:
        logger.info(f"Error: Invalid document ID format: {document_id}")
        return None

    try:
        async with get_db_connection() as conn:
            # Join with collection table to verify ownership
            query = """
                SELECT e.uuid, e.document, e.cmetadata 
                FROM langchain_pg_embedding e
                JOIN langchain_pg_collection c ON e.collection_id = c.uuid
                WHERE e.uuid = $1 AND c.cmetadata->>'owner_id' = $2
            """
            record = await conn.fetchrow(
                query, doc_uuid, user.identity if user else None
            )

            if record:
                return {
                    "id": str(record["uuid"]),
                    "content": record["document"],
                    "metadata": record["cmetadata"],
                }
            return None
    except Exception as e:
        logger.info(f"Error getting document {document_id} from vector store: {e}")
        return None


async def delete_documents_from_vectorstore(
    user: AuthenticatedUser,
    collection_id: str,
    file_ids: list[str],
) -> bool:
    """Deletes all document chunks associated with the given file_ids
    from the specified PGVector collection using direct SQL.
    Assumes file_ids are stored in the 'file_id' key of the cmetadata JSONB field.

    Only deletes documents from collections owned by the authenticated user.
    """
    if not file_ids:
        return True  # Nothing to delete
    try:
        async with get_db_connection() as conn:
            # 1. Get collection UUID with owner check
            collection_query = """
            SELECT uuid FROM langchain_pg_collection 
            WHERE uuid = $1 AND cmetadata->>'owner_id' = $2
            """
            collection_record = await conn.fetchrow(
                collection_query,
                collection_id,
                user.identity if user else None,
            )

            if not collection_record:
                logger.info(
                    f"Warning: Collection '{collection_id}' not found for deletion or not owned by user."
                )
                return False  # Indicate failure as collection doesn't exist or user doesn't own it

            collection_uuid = collection_record["uuid"]

            # 2. Prepare and execute DELETE statement
            # Use jsonb containment operator @> or ->> for checking
            query = """
            DELETE FROM langchain_pg_embedding
            WHERE collection_id = $1 AND cmetadata->>'file_id' = ANY($2::text[])
            RETURNING id; -- Optionally return deleted IDs to count
            """
            # Ensure file_ids are strings
            file_ids_str = [str(fid) for fid in file_ids]

            # Execute the delete command
            result = await conn.execute(query, collection_uuid, file_ids_str)

            # Parse the result string like 'DELETE 5' to get the count
            try:
                deleted_count = int(result.split()[-1])
                logger.info(
                    f"Deleted {deleted_count} chunks for file_ids {file_ids} in "
                    f"collection '{collection_id}'."
                )
            except (IndexError, ValueError):
                # Handle cases where the result string might be unexpected
                logger.info(
                    f"Deletion executed for file_ids {file_ids} in "
                    f"collection '{collection_id}', but count parsing "
                    f"failed. Result: {result}"
                )
            return True  # Indicate success

    except asyncpg.exceptions.UndefinedTableError:
        logger.info(
            f"Warning: Table langchain_pg_embedding or langchain_pg_collection not "
            f"found for deletion in collection '{collection_id}'."
        )
        return False
    except Exception as e:
        logger.info(
            f"Error deleting documents by file_ids {file_ids} "
            f"from collection {collection_id}: {e}"
        )
        return False


async def search_documents_in_vectorstore(
    user: AuthenticatedUser,
    collection_id: str,
    query: str,
    limit: int = 4,
) -> list[dict[str, Any]]:
    """Performs semantic similarity search within the specified PGVector collection."""
    collection_details = await get_collection_details_for_user(
        user=user,
        collection_id=collection_id,
    )
    store = get_vectorstore(
        collection_name=collection_details["name"],
    )

    results_with_scores = store.similarity_search_with_score(query, k=limit)

    formatted_results = []
    for doc, score in results_with_scores:
        doc_id = doc.id
        formatted_results.append(
            {
                "id": doc_id,
                "page_content": doc.page_content,
                "metadata": doc.metadata,
                "score": score,
            }
        )
    return formatted_results
