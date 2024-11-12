from typing import Callable, Literal

from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.utils.logging import logger

from datatrove.data import Document
import requests
import gzip
import io
import aiohttp
import asyncio
import orjson
from orjson import JSONDecodeError

# Asynchronous version of get_file_content
async def get_file_content(link, session: aiohttp.ClientSession, verbose=False):
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with session.get(link, timeout=timeout) as response:
            if response.status == 200:
                file_content = await response.read()  # Non-blocking read
                with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as gz:
                    decompressed_content = gz.read()
                return decompressed_content.decode('utf-8')
            else:
                print(f"Failed to download file. Status code: {response.status}")
    except asyncio.TimeoutError:
        print(f"Timeout while trying to download: {link}")
    except Exception as e:
        if verbose:
            print(f"An error occurred: {e}")
    return None

# Asynchronous function for processing each line
async def process_line(line, li, session):
    try:
        object = orjson.loads(line)
        content = await get_file_content(object['text'], session)  # Pass session to each request
        if content:
            document = Document(content, object['id'])
            return document

        return None
    except (EOFError, JSONDecodeError) as e:
        logger.warning(f"Error when reading line {li}: {e}")
        return None

class UrlReader(BaseDiskReader):
    """Read data from JSONL files.
        Will read each line as a separate document.

    Args:
        data_folder: a str, tuple or DataFolder object representing a path/filesystem
        paths_file: optionally provide a file with one path per line (without the `data_folder` prefix) to read.
        compression: the compression to use (default: "infer")
        limit: limit the number of documents to read. Useful for debugging
        skip: skip the first n rows
        file_progress: show progress bar for files
        doc_progress: show progress bar for documents
        adapter: function to adapt the data dict from the source to a Document.
            Takes as input: (self, data: dict, path: str, id_in_file: int | str)
                self allows access to self.text_key and self.id_key
            Returns: a dict with at least a "text" and "id" keys
        text_key: the key containing the text data (default: "text").
        id_key: the key containing the id for each sample (default: "id").
        default_metadata: a dictionary with any data that should be added to all samples' metadata
        recursive: whether to search files recursively. Ignored if paths_file is provided
        glob_pattern: pattern that all files must match exactly to be included (relative to data_folder). Ignored if paths_file is provided
        shuffle_files: shuffle the files within the returned shard. Mostly used for data viz. purposes, do not use with dedup blocks
    """

    name = "🐿 url"
    _requires_dependencies = ["orjson"]

    def __init__(
        self,
        data_folder: DataFolderLike,
        paths_file: DataFileLike | None = None,
        compression: Literal["infer", "gzip", "zstd"] | None = "infer",
        limit: int = -1,
        skip: int = 0,
        file_progress: bool = False,
        doc_progress: bool = False,
        adapter: Callable = None,
        text_key: str = "text",
        id_key: str = "id",
        default_metadata: dict = None,
        recursive: bool = True,
        glob_pattern: str | None = None,
        shuffle_files: bool = False,
    ):
        super().__init__(
            data_folder,
            paths_file,
            limit,
            skip,
            file_progress,
            doc_progress,
            adapter,
            text_key,
            id_key,
            default_metadata,
            recursive,
            glob_pattern,
            shuffle_files,
        )
        self.compression = compression

    def read_file(self, filepath: str):

        def run_async_tasks(lines):
            """Runs asynchronous tasks for each line in a synchronous context."""

            async def async_read_file(lines):
                async with aiohttp.ClientSession() as session:
                    tasks = [process_line(line, li, session) for li, line in enumerate(lines)]
                    results = await asyncio.gather(*tasks)  # Run all tasks concurrently
                    return results

            # Run the asyncio event loop to handle asynchronous code
            return asyncio.run(async_read_file(lines))

        # The actual synchronous method
        try:
            with self.data_folder.open(filepath, "r", compression=self.compression) as f:
                lines = list(f)  # Read all lines from the file

                # Run asynchronous tasks for each line and retrieve results
                documents = run_async_tasks(lines)

                # Yield non-None documents synchronously
                for document in documents:
                    if document:
                        yield document
        except UnicodeDecodeError as e:
            logger.warning(f"File `{filepath}` may be corrupted: raised UnicodeDecodeError ({e})")
