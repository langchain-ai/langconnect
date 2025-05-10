import os

from langconnect.database.connection import get_vectorstore

if "OPENAI_API_KEY" in os.environ:
    raise AssertionError(
        "Attempting to run unit tests with an OpenAI key in the environment. "
        "Please remove the key from the environment before running tests."
    )

os.environ["OPENAI_API_KEY"] = "test_key"


def init_db() -> None:
    """Hacky code to initialize the database.

    This needs to be fixed.
    """
    get_vectorstore()


init_db()
