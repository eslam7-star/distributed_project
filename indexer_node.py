from mpi4py import MPI
import time
import logging
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import os
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

INDEX_DIR = "indexer_index"

def setup_index():
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)  # Clear existing index for fresh start

    os.mkdir(INDEX_DIR)

    schema = Schema(url=ID(stored=True, unique=True), content=TEXT)
    return create_in(INDEX_DIR, schema)

def indexer_process():
    """
    Process for an indexer node.
    Receives web page content and indexes it using Whoosh.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.info(f"Indexer node started with rank {rank} of {size}")

    index = setup_index()
    writer = index.writer()

    while True:
        status = MPI.Status()
        content_to_index = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)
        source_rank = status.Get_source()

        if not content_to_index:
            logging.info(f"Indexer {rank} received shutdown signal. Exiting.")
            break

        try:
            url = content_to_index.get("url", "")
            text = content_to_index.get("content", "")

            if url and text:
                writer.add_document(url=url, content=text)
                logging.info(f"Indexer {rank} indexed: {url}")
            else:
                logging.warning(f"Indexer {rank} received incomplete data: {content_to_index}")

            comm.send(f"Indexer {rank} - Indexed content from {url}", dest=0, tag=99)  # Status back to master

        except Exception as e:
            logging.error(f"Indexer {rank} error indexing content from {source_rank}: {e}")
            comm.send(f"Indexer {rank} - Error indexing: {e}", dest=0, tag=999)

    # Commit when done
    writer.commit()
    logging.info(f"Indexer {rank} finished and committed index.")

if __name__ == '__main__':
    indexer_process()