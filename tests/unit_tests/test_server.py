from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

from httpx import ASGITransport, AsyncClient

from langconnect.database import get_vectorstore
from langconnect.database.connection import POSTGRES_DB, POSTGRES_HOST
from langconnect.server import APP


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
        response = await client.post("/collections", json=payload)
        assert response.status_code == 201, (
            f"Failed with error message: {response.text}"
        )
        data = response.json()
        assert data["name"] == "test_collection"
        assert isinstance(UUID(data["uuid"]), UUID)

        # Get collection by name
        get_response = await client.get(f"/collections/{data['name']}")
        assert get_response.status_code == 200
        assert get_response.json()["uuid"] == data["uuid"]
