import json
from uuid import UUID

from tests.unit_tests.fixtures import (
    get_async_test_client,
)

USER_1_HEADERS = {
    "Authorization": "Bearer user1",
}

USER_2_HEADERS = {
    "Authorization": "Bearer user2",
}

NO_SUCH_USER_HEADERS = {
    "Authorization": "Bearer no_such_user",
}


async def test_documents_create_and_list_and_delete_and_search() -> None:
    """Test creating, listing, deleting, and searching documents."""
    async with get_async_test_client() as client:
        # Create a collection for documents
        collection_name = "docs_test_col"
        col_payload = {"name": collection_name, "metadata": {"purpose": "doc-test"}}
        create_col = await client.post(
            "/collections", json=col_payload, headers=USER_1_HEADERS
        )
        assert create_col.status_code == 201
        collection_data = create_col.json()
        collection_id = collection_data["uuid"]

        # Prepare a simple text file
        file_content = b"Hello world. This is a test document."
        files = [("files", ("test.txt", file_content, "text/plain"))]
        # Create documents without metadata
        resp = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        # added_chunk_ids should be a non-empty list of UUIDs
        ids = data["added_chunk_ids"]
        assert isinstance(ids, list)
        assert ids
        for chunk_id in ids:
            # Validate each is a UUID string
            UUID(chunk_id)

        # List documents in collection, default limit 10
        list_resp = await client.get(
            f"/collections/{collection_id}/documents", headers=USER_1_HEADERS
        )
        assert list_resp.status_code == 200
        docs = list_resp.json()
        assert isinstance(docs, list)
        assert docs
        # Each doc should have id and text fields

        assert len(docs) == 1
        assert docs[0]["content"] == "Hello world. This is a test document."

        # Search documents with a valid query
        search_payload = {"query": "test document", "limit": 5}
        search_resp = await client.post(
            f"/collections/{collection_id}/documents/search",
            json=search_payload,
            headers=USER_1_HEADERS,
        )
        assert search_resp.status_code == 200
        results = search_resp.json()
        assert isinstance(results, list)
        # Each result should have id, score, text
        assert len(results) == 1
        assert results[0] == {
            "id": docs[0]["id"],
            "score": results[0]["score"],
            "page_content": "Hello world. This is a test document.",
            "metadata": {
                "file_id": docs[0]["metadata"]["file_id"],
                "source": None,
            },
        }

        # Delete a document
        doc_id = docs[0]["id"]
        del_resp = await client.delete(
            f"/collections/{collection_id}/documents/{doc_id}",
            headers=USER_1_HEADERS,
        )
        assert del_resp.status_code == 200
        assert del_resp.json() == {"success": True}

        # Delete non-existent document gracefully
        del_resp2 = await client.delete(
            f"/collections/{collection_id}/documents/{doc_id}",
            headers=USER_1_HEADERS,
        )
        # Should still return success True or 200/204; here assume 200
        assert del_resp2.status_code in (200, 204)


async def test_documents_create_with_invalid_metadata_json() -> None:
    """Test creating documents with invalid metadata JSON."""
    async with get_async_test_client() as client:
        # Create a collection
        col_name = "meta_test_col"
        collection_response = await client.post(
            "/collections",
            json={"name": col_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare file
        file_content = b"Sample"
        files = [("files", ("a.txt", file_content, "text/plain"))]
        # Provide invalid JSON
        resp = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            data={"metadatas_json": "not-a-json"},
            headers=USER_1_HEADERS,
        )
        assert resp.status_code == 400


async def test_documents_search_empty_query() -> None:
    """Test searching documents with an empty query."""
    async with get_async_test_client() as client:
        # Create a collection for search test
        col_name = "search_test_col"
        collection_response = await client.post(
            "/collections",
            json={"name": col_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Attempt search with empty query
        resp = await client.post(
            f"/collections/{collection_id}/documents/search",
            json={"query": "", "limit": 3},
            headers=USER_1_HEADERS,
        )
        assert resp.status_code == 400
        assert "Search query cannot be empty" in resp.json()["detail"]


async def test_documents_in_nonexistent_collection() -> None:
    """Test operations on documents in a non-existent collection."""
    async with get_async_test_client() as client:
        # Try listing documents in missing collection
        no_such_collection = "12345678-1234-5678-1234-567812345678"
        response = await client.get(
            f"/collections/{no_such_collection}/documents", headers=USER_1_HEADERS
        )
        assert response.status_code == 404

        # Try uploading to a non existent collection
        file_content = b"X"
        files = [("files", ("x.txt", file_content, "text/plain"))]
        upload_resp = await client.post(
            f"/collections/{no_such_collection}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )
        assert upload_resp.status_code == 404
        assert "Collection not found" in upload_resp.json()["detail"]

        # Try deleting from missing collection/document
        del_resp = await client.delete(
            f"/collections/{no_such_collection}/documents/abcdef",
            headers=USER_1_HEADERS,
        )
        assert del_resp.status_code == 404

        # Try search in missing collection
        search_resp = await client.post(
            f"/collections/{no_such_collection}/documents/search",
            json={"query": "foo"},
            headers=USER_1_HEADERS,
        )
        # Not found or 404
        assert search_resp.status_code == 404


async def test_documents_create_with_valid_text_file_and_metadata() -> None:
    """Test creating documents with a valid text file and metadata."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_with_metadata"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare a text file with content
        file_content = b"This is a test document with metadata."
        files = [("files", ("metadata_test.txt", file_content, "text/plain"))]

        # Prepare metadata as JSON
        metadata = [{"source": "test", "author": "user1", "importance": "high"}]
        metadata_json = json.dumps(metadata)

        # Create document with metadata
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            data={"metadatas_json": metadata_json},
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "added_chunk_ids" in data
        ids = data["added_chunk_ids"]
        assert isinstance(ids, list)
        assert len(ids) > 0

        # Verify each ID is a valid UUID
        for chunk_id in ids:
            UUID(chunk_id)  # This will raise an exception if invalid

        # Verify document was added by listing documents
        list_response = await client.get(
            f"/collections/{collection_id}/documents",
            headers=USER_1_HEADERS,
        )
        assert list_response.status_code == 200
        documents = list_response.json()
        assert len(documents) == 1

        # Verify metadata was attached
        doc = documents[0]
        assert "metadata" in doc
        assert "file_id" in doc["metadata"]
        # The file_id will be a new UUID, so we can't check the exact value


async def test_documents_create_with_valid_text_file_without_metadata() -> None:
    """Test creating documents with a valid text file without metadata."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_without_metadata"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare a text file with content
        file_content = b"This is a test document without metadata."
        files = [("files", ("no_metadata_test.txt", file_content, "text/plain"))]

        # Create document without metadata
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "added_chunk_ids" in data
        ids = data["added_chunk_ids"]
        assert isinstance(ids, list)
        assert len(ids) > 0

        # Verify document was added by listing documents
        list_response = await client.get(
            f"/collections/{collection_id}/documents",
            headers=USER_1_HEADERS,
        )
        assert list_response.status_code == 200
        documents = list_response.json()
        assert len(documents) > 0
        # Verify content is in the document
        assert documents[0]["content"] == "This is a test document without metadata."


async def test_documents_create_with_empty_file() -> None:
    """Test creating documents with an empty file."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_empty_file"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare an empty file
        file_content = b""
        files = [("files", ("empty.txt", file_content, "text/plain"))]

        # Create document with empty file
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )

        # Empty files should be rejected with 400 Bad Request
        assert response.status_code == 400
        data = response.json()
        assert "Failed to process any documents" in data["detail"]


async def test_documents_create_with_invalid_metadata_format() -> None:
    """Test creating documents with invalid metadata format."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_invalid_metadata"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare a text file with content
        file_content = b"This is a test document with invalid metadata."
        files = [("files", ("invalid_metadata.txt", file_content, "text/plain"))]

        # Invalid JSON format for metadata
        invalid_metadata = "not a json"

        # Create document with invalid metadata
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            data={"metadatas_json": invalid_metadata},
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 400

        # Test with metadata that's not a list
        invalid_metadata_not_list = json.dumps({"key": "value"})
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            data={"metadatas_json": invalid_metadata_not_list},
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 400


async def test_documents_create_with_non_existent_collection() -> None:
    """Test creating documents in a non-existent collection."""
    async with get_async_test_client() as client:
        # Prepare a text file with content
        file_content = b"This is a test document for a non-existent collection."
        files = [("files", ("nonexistent.txt", file_content, "text/plain"))]

        # Try to create document in a non-existent collection
        uuid = "12345678-1234-5678-1234-567812345678"
        response = await client.post(
            f"/collections/{uuid}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 404
        data = response.json()
        assert "Collection not found" in data["detail"]


async def test_documents_create_with_multiple_files():
    """Test creating documents with multiple files."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_multiple_files"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare multiple files
        files = [
            ("files", ("file1.txt", b"Content of file 1", "text/plain")),
            ("files", ("file2.txt", b"Content of file 2", "text/plain")),
        ]

        # Create document with multiple files
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "added_chunk_ids" in data
        ids = data["added_chunk_ids"]
        assert isinstance(ids, list)
        # We should have at least 2 chunks (one for each file)
        assert len(ids) >= 2

        # Verify documents were added by listing documents
        list_response = await client.get(
            f"/collections/{collection_id}/documents",
            headers=USER_1_HEADERS,
        )
        assert list_response.status_code == 200
        documents = list_response.json()
        # The number of documents returned might not match the number of files
        # exactly, as documents are chunked and only one chunk per file_id is returned
        assert len(documents) > 0


async def test_documents_create_with_mismatched_metadata():
    """Test creating documents with metadata count not matching files count."""
    async with get_async_test_client() as client:
        # Create a collection first
        collection_name = "doc_test_mismatched_metadata"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare multiple files
        files = [
            ("files", ("file1.txt", b"Content of file 1", "text/plain")),
            ("files", ("file2.txt", b"Content of file 2", "text/plain")),
        ]

        # Metadata with only one entry for two files
        metadata = [{"source": "test"}]
        metadata_json = json.dumps(metadata)

        # Create document with mismatched metadata
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            data={"metadatas_json": metadata_json},
            headers=USER_1_HEADERS,
        )

        assert response.status_code == 400
        data = response.json()
        assert "does not match number of files" in data["detail"]


async def test_documents_create_ownership_validation():
    """Test creating documents with a different user than the collection owner."""
    async with get_async_test_client() as client:
        # Create a collection as USER_1
        collection_name = "doc_test_ownership"
        collection_response = await client.post(
            "/collections",
            json={"name": collection_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        assert collection_response.status_code == 201
        collection_data = collection_response.json()
        collection_id = collection_data["uuid"]

        # Prepare a file
        file_content = b"This is a test document for ownership validation."
        files = [("files", ("ownership.txt", file_content, "text/plain"))]

        # Try to create document as USER_2
        response = await client.post(
            f"/collections/{collection_id}/documents",
            files=files,
            headers=USER_2_HEADERS,
        )

        # Should return 404 as USER_2 can't see USER_1's collection
        assert response.status_code == 404
        data = response.json()
        assert "Collection not found" in data["detail"]
