import os

from langchain_core.embeddings import DeterministicFakeEmbedding
from langchain_openai import OpenAIEmbeddings

# We'll need to improve this configuration so we're not modifying a prod path
# for testing purposes.
if os.environ.get("IS_TESTING", "false").strip().lower() == "true":
    # Use a fake embedding for testing
    DEFAULT_EMBEDDINGS = DeterministicFakeEmbedding(size=512)
else:
    DEFAULT_EMBEDDINGS = OpenAIEmbeddings()
DEFAULT_COLLECTION_NAME = "default_collection"
