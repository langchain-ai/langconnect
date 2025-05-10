from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional

from httpx import ASGITransport, AsyncClient

from langconnect.server import APP


@asynccontextmanager
async def get_async_test_client(
    *, path: Optional[str] = None, raise_app_exceptions: bool = True
) -> AsyncGenerator[AsyncClient, None]:
    """Get an async client."""
    url = "http://localhost:9999"
    if path:
        url += path
    transport = ASGITransport(
        app=APP,
        raise_app_exceptions=raise_app_exceptions,
    )
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
