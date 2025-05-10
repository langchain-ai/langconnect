import logging

from langconnect.database.connection import get_vectorstore

logger = logging.getLogger(__name__)


async def initialize_database():
    """Initialize the database by creating necessary extensions and tables."""
    logger.info("Starting database initialization...")

    get_vectorstore()
    logger.info("Database initialization complete.")
