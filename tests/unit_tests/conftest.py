import os

if "OPENAI_API_KEY" in os.environ:
    raise AssertionError(
        "Attempting to run unit tests with an OpenAI key in the environment. "
        "Please remove the key from the environment before running tests."
    )

os.environ["OPENAI_API_KEY"] = "test_key"
