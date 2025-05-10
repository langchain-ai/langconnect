import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from langconnect.services import index_document

LOGGER = logging.getLogger(__name__)

# Create a task queue for background processing
TASK_QUEUE: asyncio.Queue = asyncio.Queue()


async def process_task(task):
    """Process a task from the queue."""
    task_type = task.get("type")
    if task_type == "index_document":
        await index_document(task.get("collection_name"), task.get("document_data"))
    # Add other task types as needed
    LOGGER.info(f"Processed task: {task}")


async def worker():
    """Background worker for processing tasks."""
    LOGGER.info("Background worker started.")
    while True:
        try:
            task = await TASK_QUEUE.get()
            if not task:
                LOGGER.info("Worker received sentinel, exiting.")
                break

            LOGGER.info(f"Worker got a new task: {task}")
            await process_task(task)
        except Exception as exc:
            LOGGER.exception(f"Error in worker: {exc}")
        finally:
            TASK_QUEUE.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    LOGGER.info("App is starting up. Creating background worker...")
    loop = asyncio.get_running_loop()
    loop.create_task(worker())
    yield
    LOGGER.info("App is shutting down. Stopping background worker...")
    TASK_QUEUE.put_nowait(None)


def add_task_to_queue(task):
    """Add a task to the background processing queue."""
    TASK_QUEUE.put_nowait(task)
