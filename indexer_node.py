from mpi4py import MPI
import time
import logging
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import os
import shutil
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

INDEX_DIR = "indexer_index"

def setup_index():
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)  # Clear existing index for fresh start

    os.mkdir(INDEX_DIR)

    schema = Schema(url=ID(stored=True, unique=True), content=TEXT, keywords=TEXT)
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
            keywords = content_to_index.get("keywords", "")

            if url and text:
                writer.add_document(url=url, content=text, keywords=keywords)
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

def search_keywords(query):
    """
    Perform a simple keyword search on the index.
    """
    index = open_dir(INDEX_DIR)
    searcher = index.searcher()

    query_parser = QueryParser("keywords", schema=index.schema)
    query_obj = query_parser.parse(query)

    results = searcher.search(query_obj)

    for result in results:
        print(f"Found match: {result['url']}")

    searcher.close()

def main():
    parser = argparse.ArgumentParser(description="Indexer and Keyword Search")
    parser.add_argument('--search', type=str, help='Search for a keyword in the index')
    args = parser.parse_args()

    if args.search:
        search_keywords(args.search)
    else:
        # Start indexing process if no search argument is provided
        indexer_process()

if __name__ == '__main__':
    main()
