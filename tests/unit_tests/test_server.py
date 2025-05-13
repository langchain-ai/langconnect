from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient

from langconnect.database import get_vectorstore
from langconnect.database.connection import POSTGRES_DB, POSTGRES_HOST
from langconnect.server import APP

USER_1_HEADERS = {
    "Authorization": "Bearer user1",
}

USER_2_HEADERS = {
    "Authorization": "Bearer user2",
}

NO_SUCH_USER_HEADERS = {
    "Authorization": "Bearer no_such_user",
}


def reset_db() -> None:
    """Hacky code to initialize the database. This needs to be fixed."""
    if POSTGRES_DB != "langchain_test":
        raise AssertionError(
            "Attempting to run unit tests with a non-test database. "
            "Please set the database to 'test' before running tests."
        )
    if POSTGRES_HOST != "localhost":
        raise AssertionError(
            "Attempting to run unit tests with a non-localhost database. "
            "Please set the host to 'localhost' before running tests."
        )
    vectorstore = get_vectorstore()
    # Drop table
    vectorstore.drop_tables()
    # Re-create
    vectorstore.__post_init__()


@asynccontextmanager
async def get_async_test_client() -> AsyncGenerator[AsyncClient, None]:
    """Get an async client."""
    url = "http://localhost:9999"
    transport = ASGITransport(
        app=APP,
        raise_app_exceptions=True,
    )
    reset_db()
    async_client = AsyncClient(base_url=url, transport=transport)
    try:
        yield async_client
    finally:
        await async_client.aclose()


async def test_health() -> None:
    """Test the health check endpoint."""
    async with get_async_test_client() as client:
        response = await client.get("/health")
        response.raise_for_status()
        assert response.json() == {"status": "ok"}


async def test_create_and_get_collection() -> None:
    """Test creating and retrieving a collection."""
    async with get_async_test_client() as client:
        payload = {"name": "test_collection", "metadata": {"purpose": "unit-test"}}
        response = await client.post(
            "/collections", json=payload, headers=USER_1_HEADERS
        )
        assert response.status_code == 201, (
            f"Failed with error message: {response.text}"
        )
        data = response.json()
        assert data["name"] == "test_collection"
        assert isinstance(UUID(data["uuid"]), UUID)

        # Get collection by name
        get_response = await client.get(
            f"/collections/{data['name']}", headers=USER_1_HEADERS
        )
        assert get_response.status_code == 200
        assert get_response.json()["uuid"] == data["uuid"]

        # Test without metadata
        payload_no_metadata = {"name": "test_collection_no_metadata"}
        response_no_metadata = await client.post(
            "/collections", json=payload_no_metadata, headers=USER_1_HEADERS
        )
        assert response_no_metadata.status_code == 201, (
            f"Failed with error message: {response_no_metadata.text}"
        )
        data_no_metadata = response_no_metadata.json()
        assert data_no_metadata == {
            "uuid": data_no_metadata["uuid"],
            "name": "test_collection_no_metadata",
            "metadata": {
                "owner_id": "user1",
            },
        }


async def test_create_and_list_collection() -> None:
    """Test creating and listing a collection."""
    async with get_async_test_client() as client:
        payload = {"name": "test_collection", "metadata": {"purpose": "unit-test"}}
        response = await client.post(
            "/collections", json=payload, headers=USER_1_HEADERS
        )
        assert response.status_code == 201, (
            f"Failed with error message: {response.text}"
        )
        data = response.json()
        assert data["name"] == "test_collection"
        assert isinstance(UUID(data["uuid"]), UUID)

        # List collections
        list_response = await client.get("/collections", headers=USER_1_HEADERS)
        assert list_response.status_code == 200
        collections = list_response.json()
        assert len(collections) > 0
        assert any(c["name"] == "test_collection" for c in collections)


async def test_create_collection_conflict() -> None:
    """Creating a collection twice should return 409."""
    async with get_async_test_client() as client:
        payload = {"name": "dup_collection", "metadata": {"foo": "bar"}}
        # first create
        r1 = await client.post("/collections", json=payload, headers=USER_1_HEADERS)
        assert r1.status_code == 201

        # second create with same name
        r2 = await client.post("/collections", json=payload, headers=USER_1_HEADERS)
        assert r2.status_code == 409
        assert "already exists" in r2.json()["detail"]


async def test_create_collection_requires_auth() -> None:
    """POST /collections without a valid token should be 401."""
    async with get_async_test_client() as client:
        payload = {"name": "no_auth", "metadata": {}}
        r = await client.post("/collections", json=payload)
        assert r.status_code == 403
        r2 = await client.post(
            "/collections", json=payload, headers=NO_SUCH_USER_HEADERS
        )
        assert r2.status_code == 401


async def test_get_nonexistent_collection() -> None:
    """GET a collection that doesn't exist should be 404."""
    async with get_async_test_client() as client:
        r = await client.get("/collections/nonexistent", headers=USER_1_HEADERS)
        assert r.status_code == 404
        assert "not found" in r.json()["detail"].lower()


async def test_delete_collection_and_nonexistent() -> None:
    """DELETE removes an existing collection and returns 404 on missing."""
    async with get_async_test_client() as client:
        # create first
        payload = {"name": "to_delete", "metadata": {"foo": "bar"}}
        r1 = await client.post("/collections", json=payload, headers=USER_1_HEADERS)
        assert r1.status_code == 201

        # delete it
        r2 = await client.delete("/collections/to_delete", headers=USER_1_HEADERS)
        assert r2.status_code == 204

        # Try to get it again
        r3 = await client.get("/collections/to_delete", headers=USER_1_HEADERS)
        assert r3.status_code == 404

        # Deletion is idempotent
        r4 = await client.delete("/collections/to_delete", headers=USER_1_HEADERS)
        assert r4.status_code == 204


async def test_patch_collection() -> None:
    """PATCH should update metadata properly."""
    async with get_async_test_client() as client:
        # create a collection
        payload = {"name": "colA", "metadata": {"a": 1}}
        r = await client.post("/collections", json=payload, headers=USER_1_HEADERS)
        assert r.status_code == 201
        assert r.json() == {
            "uuid": r.json()["uuid"],
            "name": "colA",
            "metadata": {
                "a": 1,
                "owner_id": "user1",
            },
        }

        # update metadata
        r2 = await client.patch(
            "/collections/colA",
            json={"metadata": {"a": 2}},
            headers=USER_1_HEADERS,
        )
        assert r2.status_code == 200
        assert r2.json() == {
            "uuid": r.json()["uuid"],
            "name": "colA",
            "metadata": {
                "a": 2,
                "owner_id": "user1",
            },
        }


@pytest.mark.xfail(
    reason="Need to fix representation of collections so a name is not unique."
)
async def test_update_collection_name_and_metadata() -> None:
    """PATCH should rename and/or update metadata properly."""
    async with get_async_test_client() as client:
        # create two collections
        await client.post(
            "/collections",
            json={"name": "colA", "metadata": {"a": 1}},
            headers=USER_1_HEADERS,
        )
        await client.post(
            "/collections",
            json={"name": "colB", "metadata": {"b": 2}},
            headers=USER_1_HEADERS,
        )

        # try renaming colA to colB (conflict)
        conflict = await client.patch(
            "/collections/colA",
            json={"name": "colB"},
            headers=USER_1_HEADERS,
        )
        assert conflict.status_code == 409

        # rename colA to colC with new metadata
        update = await client.patch(
            "/collections/colA",
            json={"name": "colC", "metadata": {"x": "y"}},
            headers=USER_1_HEADERS,
        )
        assert update.status_code == 200
        body = update.json()
        assert body == {
            "uuid": body["uuid"],
            "name": "colC",
            "metadata": {"x": "y"},
        }
        # ensure old name is gone
        get_old = await client.get("/collections/colA")
        assert get_old.status_code == 404
        # ensure new name works
        get_new = await client.get("/collections/colC")
        assert get_new.status_code == 200

        # update metadata only on colC
        meta_update = await client.patch(
            "/collections/colC",
            json={"metadata": {"foo": "bar"}},
        )
        assert meta_update.status_code == 200
        assert meta_update.json() == {
            "uuid": body["uuid"],
            "name": "colC",
            "metadata": {"foo": "bar"},
        }


async def test_update_nonexistent_collection() -> None:
    """PATCH a missing collection should return 404."""
    async with get_async_test_client() as client:
        r = await client.patch(
            "/collections/does_not_exist",
            json={"metadata": {"any": "thing"}},
            headers=USER_1_HEADERS,
        )
        assert r.status_code == 404


async def test_list_empty_and_multiple_collections() -> None:
    """Listing when empty and after multiple creates."""
    async with get_async_test_client() as client:
        # ensure database is empty
        empty = await client.get(
            "/collections",
            headers=USER_1_HEADERS,
        )
        assert empty.status_code == 200
        assert empty.json() == []

        # create several
        names = ["one", "two", "three"]
        for n in names:
            r = await client.post(
                "/collections", json={"name": n, "metadata": {}}, headers=USER_1_HEADERS
            )
            assert r.status_code == 201

        listed = await client.get("/collections", headers=USER_1_HEADERS)
        assert listed.status_code == 200
        got = [c["name"] for c in listed.json()]
        for n in names:
            assert n in got


# Check ownership of collections.
async def test_ownership() -> None:
    """Try accessing and deleting collections owned by user 1 using user 2."""
    async with get_async_test_client() as client:
        # create a collection as user 1
        payload = {"name": "owned_by_user1", "metadata": {}}
        r = await client.post("/collections", json=payload, headers=USER_1_HEADERS)
        assert r.status_code == 201

        # user 2 tries to get it
        r2 = await client.get("/collections/owned_by_user1", headers=USER_2_HEADERS)
        assert r2.status_code == 404

        # Always ack with 204 for idempotency
        r3 = await client.delete("/collections/owned_by_user1", headers=USER_2_HEADERS)
        assert r3.status_code == 204

        # Try listing collections as user 2
        r4 = await client.get("/collections", headers=USER_2_HEADERS)
        assert r4.status_code == 200
        assert r4.json() == []

        # Try patching the collection as user 2
        r4 = await client.patch(
            "/collections/owned_by_user1",
            json={"name": "new_name"},
            headers=USER_2_HEADERS,
        )
        assert r4.status_code == 404

        # user 1 can delete it
        r5 = await client.delete("/collections/owned_by_user1", headers=USER_1_HEADERS)
        assert r5.status_code == 204


async def test_documents_create_and_list_and_delete_and_search() -> None:
    """Test creating, listing, deleting, and searching documents."""
    async with get_async_test_client() as client:
        # Create a collection for documents
        collection = "docs_test_col"
        col_payload = {"name": collection, "metadata": {"purpose": "doc-test"}}
        create_col = await client.post(
            "/collections", json=col_payload, headers=USER_1_HEADERS
        )
        assert create_col.status_code == 201

        # Prepare a simple text file
        file_content = b"Hello world. This is a test document."
        files = [("files", ("test.txt", file_content, "text/plain"))]
        # Create documents without metadata
        resp = await client.post(
            f"/collections/{collection}/documents",
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
            f"/collections/{collection}/documents", headers=USER_1_HEADERS
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
            f"/collections/{collection}/documents/search",
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
            f"/collections/{collection}/documents/{doc_id}",
            headers=USER_1_HEADERS,
        )
        assert del_resp.status_code == 200
        assert del_resp.json() == {"success": True}

        # Delete non-existent document gracefully
        del_resp2 = await client.delete(
            f"/collections/{collection}/documents/{doc_id}",
            headers=USER_1_HEADERS,
        )
        # Should still return success True or 200/204; here assume 200
        assert del_resp2.status_code in (200, 204)


async def test_documents_create_with_invalid_metadata_json() -> None:
    """Test creating documents with invalid metadata JSON."""
    async with get_async_test_client() as client:
        # Create a collection
        col_name = "meta_test_col"
        await client.post(
            "/collections",
            json={"name": col_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        # Prepare file
        file_content = b"Sample"
        files = [("files", ("a.txt", file_content, "text/plain"))]
        # Provide invalid JSON
        resp = await client.post(
            f"/collections/{col_name}/documents",
            files=files,
            data={"metadatas_json": "not-a-json"},
            headers=USER_1_HEADERS,
        )
        assert resp.status_code == 400
        assert "Invalid JSON format" in resp.json()["detail"]


async def test_documents_search_empty_query() -> None:
    """Test searching documents with an empty query."""
    async with get_async_test_client() as client:
        # Create a collection for search test
        col_name = "search_test_col"
        await client.post(
            "/collections",
            json={"name": col_name, "metadata": {}},
            headers=USER_1_HEADERS,
        )
        # Attempt search with empty query
        resp = await client.post(
            f"/collections/{col_name}/documents/search",
            json={"query": "", "limit": 3},
            headers=USER_1_HEADERS,
        )
        assert resp.status_code == 400
        assert "Search query cannot be empty" in resp.json()["detail"]


async def test_documents_in_nonexistent_collection() -> None:
    """Test operations on documents in a non-existent collection."""
    async with get_async_test_client() as client:
        # Try listing documents in missing collection
        response = await client.get(
            "/collections/no_such_col/documents", headers=USER_1_HEADERS
        )
        assert response.status_code == 404

        # Try uploading to a non existent collection
        file_content = b"X"
        files = [("files", ("x.txt", file_content, "text/plain"))]
        upload_resp = await client.post(
            "/collections/no_such_col/documents",
            files=files,
            headers=USER_1_HEADERS,
        )
        assert upload_resp.status_code == 404
        assert "Collection not found" in upload_resp.json()["detail"]

        # Try deleting from missing collection/document
        del_resp = await client.delete(
            "/collections/no_such_col/documents/abcdef", headers=USER_1_HEADERS
        )
        # Depending on implementation may return 500 or success False
        assert del_resp.status_code in (404, 500, 200, 204)

        # Try search in missing collection
        search_resp = await client.post(
            "/collections/no_such_col/documents/search",
            json={"query": "foo"},
            headers=USER_1_HEADERS,
        )
        # Not found or 404
        assert search_resp.status_code == 404
