from uuid import UUID

import pytest

from tests.unit_tests.fixtures import get_async_test_client

USER_1_HEADERS = {
    "Authorization": "Bearer user1",
}

USER_2_HEADERS = {
    "Authorization": "Bearer user2",
}

NO_SUCH_USER_HEADERS = {
    "Authorization": "Bearer no_such_user",
}


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
