import json
import logging
import uuid
from typing import Any, Optional

import asyncpg
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from langconnect.database.connection import (
    get_db_connection,
    get_vectorstore,
)
from langconnect.defaults import DEFAULT_EMBEDDINGS

logger = logging.getLogger(__name__)


def add_documents_to_vectorstore(
    collection_name: str,
    documents: list[Document],
    embeddings: Embeddings = DEFAULT_EMBEDDINGS,
) -> list[str]:
    """Adds LangChain documents to the specified PGVector collection."""
    store = get_vectorstore(
        collection_name=collection_name,
        embeddings=embeddings,
    )
    added_ids = store.add_documents(documents, ids=None)  # Let PGVector generate IDs
    return added_ids


async def list_documents_in_vectorstore(
    collection_name: str,
    limit: int = 10,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Lists unique documents based on 'file_id' in metadata from the vector store.
    Returns one representative entry per file_id.
    NOTE: This bypasses LangChain's abstraction for efficient unique listing.
    Requires direct asyncpg connection to query langchain_pg_embedding table.
    """
    documents = []
    try:
        async with get_db_connection() as conn:
            collection_record = await conn.fetchrow(
                "SELECT uuid FROM langchain_pg_collection WHERE name = $1",
                collection_name,
            )
            if not collection_record:
                logger.info(f"Warning: Collection '{collection_name}' not found.")
                return []

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
            f"Warning: Table langchain_pg_embedding or langchain_pg_collection not found for collection '{collection_name}'. Returning empty list."
        )
        return []
    except Exception as e:
        logger.info(f"Error listing documents from vector store: {e}")
        return []

    return documents


async def get_document_from_vectorstore(
    document_id: str,
) -> Optional[dict[str, Any]]:
    """Gets a single document by its ID from the vector store's underlying table.
    Requires direct SQL access.
    """
    try:
        doc_uuid = uuid.UUID(document_id)
    except ValueError:
        logger.info(f"Error: Invalid document ID format: {document_id}")
        return None

    try:
        async with get_db_connection() as conn:
            query = """
                SELECT uuid, document, cmetadata FROM langchain_pg_embedding
                WHERE uuid = $1 
            """
            record = await conn.fetchrow(query, doc_uuid)

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
    collection_name: str,
    file_ids: list[str],
) -> bool:
    """Deletes all document chunks associated with the given file_ids
    from the specified PGVector collection using direct SQL.
    Assumes file_ids are stored in the 'file_id' key of the cmetadata JSONB field.
    """
    if not file_ids:
        return True  # Nothing to delete

    deleted_count = 0
    try:
        async with get_db_connection() as conn:
            # 1. Get collection UUID
            collection_record = await conn.fetchrow(
                "SELECT uuid FROM langchain_pg_collection WHERE name = $1",
                collection_name,
            )
            if not collection_record:
                logger.info(
                    f"Warning: Collection '{collection_name}' not found for deletion."
                )
                return False  # Indicate failure as collection doesn't exist

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
                    f"Deleted {deleted_count} chunks for file_ids {file_ids} in collection '{collection_name}'."
                )
            except (IndexError, ValueError):
                # Handle cases where the result string might be unexpected
                logger.info(
                    f"Deletion executed for file_ids {file_ids} in collection '{collection_name}', but count parsing failed. Result: {result}"
                )
                # Consider success if execute didn't raise an error
                deleted_count = -1  # Indicate count unknown

            return True  # Indicate success

    except asyncpg.exceptions.UndefinedTableError:
        logger.info(
            f"Warning: Table langchain_pg_embedding or langchain_pg_collection not found for deletion in collection '{collection_name}'."
        )
        return False
    except Exception as e:
        logger.info(
            f"Error deleting documents by file_ids {file_ids} from collection {collection_name}: {e}"
        )
        return False


def search_documents_in_vectorstore(
    collection_name: str,
    query: str,
    limit: int = 4,
    embeddings: Embeddings = DEFAULT_EMBEDDINGS,
) -> list[dict[str, Any]]:
    """Performs semantic similarity search within the specified PGVector collection."""
    store = get_vectorstore(
        collection_name=collection_name,
        embeddings=embeddings,
    )

    results_with_scores = store.similarity_search_with_score(query, k=limit)

    formatted_results = []
    for doc, score in results_with_scores:
        doc_id = doc.metadata.get("uuid") or doc.metadata.get("id")
        formatted_results.append(
            {
                "id": str(doc_id) if doc_id else None,
                "page_content": doc.page_content,
                "metadata": doc.metadata,
                "score": score,
            }
        )
    return formatted_results


def record_to_dict(record) -> Optional[dict[str, Any]]:
    """Converts an asyncpg Record to a dictionary (useful for direct DB access)."""
    if record is None:
        return None
    return {
        key: (str(value) if isinstance(value, uuid.UUID) else value)
        for key, value in record.items()
        if key != "embedding"
    }
