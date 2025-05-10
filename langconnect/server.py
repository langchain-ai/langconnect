import json
import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from langconnect.api import admin_router, collections_router, documents_router
from langconnect.utils import lifespan

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Read allowed origins from environment variable
ALLOW_ORIGINS_JSON = os.getenv("ALLOW_ORIGINS")

if ALLOW_ORIGINS_JSON:
    ALLOWED_ORIGINS = json.loads(ALLOW_ORIGINS_JSON.strip())
    logger.info(f"ALLOW_ORIGINS environment variable set to: {ALLOW_ORIGINS_JSON}")
else:
    ALLOWED_ORIGINS = ()
    logger.warning("ALLOW_ORIGINS environment variable not set.")

# Initialize FastAPI app
APP = FastAPI(
    title="LangConnect API",
    description="A REST API for a RAG system using FastAPI and LangChain",
    version="0.1.0",
    lifespan=lifespan,
)

# Add CORS middleware
APP.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
APP.include_router(collections_router)
APP.include_router(documents_router)
APP.include_router(admin_router)


@APP.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("langconnect.server:APP", host="0.0.0.0", port=8080)
