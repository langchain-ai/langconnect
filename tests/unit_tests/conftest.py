import os

import pytest

from langconnect.database.connection import close_db_pool

if "OPENAI_API_KEY" in os.environ:
    raise AssertionError(
        "Attempting to run unit tests with an OpenAI key in the environment. "
        "Please remove the key from the environment before running tests."
    )

os.environ["OPENAI_API_KEY"] = "test_key"


@pytest.fixture(autouse=True)
async def reset_db_pool() -> None:
    """Close the asyncpg pool after each test.

    With pytest-asyncio 1.x and function-scoped event loops, the global
    asyncpg pool is bound to the event loop that created it. Without cleanup,
    subsequent tests fail with 'RuntimeError: Event loop is closed' when
    they attempt to reuse a pool from a now-closed loop.
    """
    yield
    await close_db_pool()
