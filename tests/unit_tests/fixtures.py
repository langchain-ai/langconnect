from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from httpx import ASGITransport, AsyncClient

from langconnect import config
from langconnect.database.connection import get_vectorstore
from langconnect.server import APP


def reset_db() -> None:
    """Hacky code to initialize the database. This needs to be fixed."""
    if config.POSTGRES_DB != "langchain_test":
        raise AssertionError(
            "Attempting to run unit tests with a non-test database. "
            "Please set the database to 'test' before running tests."
        )
    if config.POSTGRES_HOST != "localhost":
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
