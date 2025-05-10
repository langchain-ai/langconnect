import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from langchain_core.documents import Document

from langconnect.database import (
    add_documents_to_vectorstore,
    delete_documents_from_vectorstore,
    get_pgvector_collection_details,
    list_documents_in_vectorstore,
    search_documents_in_vectorstore,
)
from langconnect.models import DocumentResponse, SearchQuery, SearchResult
from langconnect.services import process_document

router = APIRouter(tags=["documents"])


@router.post("/collections/{collection_name}/documents", response_model=dict[str, Any])
async def documents_create(
    collection_name: str,
    files: list[UploadFile] = File(...),
    metadatas_json: str | None = Form(None),
):
    """Processes and indexes (adds) new document files with optional metadata."""
    try:
        collection = await get_pgvector_collection_details(collection_name)
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking collection: {e!s}")

    metadatas = []
    if metadatas_json:
        try:
            metadatas = json.loads(metadatas_json)
            if not isinstance(metadatas, list):
                raise ValueError("Metadatas must be a list.")
            if len(metadatas) != len(files):
                raise ValueError(
                    f"Number of metadata objects ({len(metadatas)}) does not match number of files ({len(files)})."
                )
            # Optional: Further validation to ensure each item in metadatas is a dict
            if not all(isinstance(m, dict) for m in metadatas):
                raise ValueError("Each item in metadatas list must be a dictionary.")
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400, detail="Invalid JSON format for metadatas."
            )
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            # Catch unexpected errors during metadata processing
            raise HTTPException(
                status_code=500, detail=f"Error processing metadatas: {e!s}"
            )
    else:
        # If no metadata JSON is provided, create a list of Nones
        metadatas = [None] * len(files)

    all_langchain_docs: list[Document] = []
    processed_files_count = 0
    failed_files = []

    # Pair files with their corresponding metadata
    for file, metadata in zip(files, metadatas, strict=False):
        try:
            # Pass metadata to process_document
            langchain_docs = await process_document(file, metadata=metadata)
            if langchain_docs:
                all_langchain_docs.extend(langchain_docs)
                processed_files_count += 1
            else:
                print(
                    f"Warning: File {file.filename} resulted in no processable documents."
                )
                # Decide if this constitutes a failure
                # failed_files.append(file.filename)

        except Exception as proc_exc:
            # Log the error and the file that caused it
            print(f"Error processing file {file.filename}: {proc_exc}")
            failed_files.append(file.filename)
            # Decide on behavior: continue processing others or fail fast?
            # For now, let's collect failures and report them, but continue processing.

    # If after processing all files, none yielded documents, raise error
    if not all_langchain_docs:
        error_detail = "Failed to process any documents from the provided files."
        if failed_files:
            error_detail += f" Files that failed processing: {', '.join(failed_files)}."
        raise HTTPException(status_code=400, detail=error_detail)

    # If some files failed but others succeeded, proceed with adding successful ones
    # but maybe inform the user about the failures.

    try:
        added_ids = add_documents_to_vectorstore(collection_name, all_langchain_docs)
        if not added_ids:
            # This might indicate a problem with the vector store itself
            raise HTTPException(
                status_code=500,
                detail="Failed to add document(s) to vector store after processing.",
            )

        # Construct response message
        success_message = f"{len(added_ids)} document chunk(s) from {processed_files_count} file(s) added successfully."
        response_data = {
            "success": True,
            "message": success_message,
            "added_chunk_ids": added_ids,
        }

        if failed_files:
            response_data["warnings"] = (
                f"Processing failed for files: {', '.join(failed_files)}"
            )
            # Consider if partial success should change the overall status/message

        return response_data

    except HTTPException as http_exc:
        # Reraise HTTPExceptions from add_documents_to_vectorstore or previous checks
        raise http_exc
    except Exception as add_exc:
        # Handle exceptions during the vector store addition process
        print(f"Error adding documents to vector store: {add_exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add documents to vector store: {add_exc!s}",
        )


@router.get(
    "/collections/{collection_name}/documents", response_model=list[DocumentResponse]
)
async def documents_list(
    collection_name: str,
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Lists documents within a specific collection."""
    documents = await list_documents_in_vectorstore(
        collection_name, limit=limit, offset=offset
    )
    return documents


@router.delete(
    "/collections/{collection_name}/documents/{document_id}",
    response_model=dict[str, bool],
)
async def documents_delete(
    collection_name: str,
    document_id: str,
):
    """Deletes a specific document (chunk) from a collection by its vector store ID."""
    success = await delete_documents_from_vectorstore(collection_name, [document_id])
    if not success:
        raise HTTPException(
            status_code=500, detail="Failed to delete document from vector store"
        )

    return {"success": True}


@router.post(
    "/collections/{collection_name}/documents/search", response_model=list[SearchResult]
)
def documents_search(
    collection_name: str,
    search_query: SearchQuery,
):
    """Performs semantic search for documents within a specific collection."""
    if not search_query.query:
        raise HTTPException(status_code=400, detail="Search query cannot be empty")

    results = search_documents_in_vectorstore(
        collection_name,
        query=search_query.query,
        limit=search_query.limit or 10,
    )
    return results
