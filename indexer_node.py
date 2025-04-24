from mpi4py import MPI
import time
import logging
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import os
import shutil
from bs4 import BeautifulSoup
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

INDEX_DIR = "indexer_index"

def setup_index():
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)  # Clear existing index for fresh start

    os.mkdir(INDEX_DIR)

    # Schema includes title and content
    schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True), title=TEXT(stored=True))
    return create_in(INDEX_DIR, schema)

def extract_title_and_body(url):
    """
    Extract title and body (important headings H1, H2) from the HTML content of a page.
    """
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract the title of the page
            title = soup.title.string if soup.title else ""

            # Extract all important headings (H1, H2) as body
            body = " ".join([h1.get_text() for h1 in soup.find_all("h1")] + [h2.get_text() for h2 in soup.find_all("h2")])

            return title, body
        else:
            logging.warning(f"Failed to retrieve URL: {url}")
            return "", ""
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return "", ""

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
            title, body = extract_title_and_body(url)

            if url and title and body:
                writer.add_document(url=url, content=body, title=title)
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
    Perform a simple keyword search on the index (title and content).
    """
    index = open_dir(INDEX_DIR)
    searcher = index.searcher()

    query_parser = QueryParser("content", schema=index.schema) 
    query_obj = query_parser.parse(query)

    results = searcher.search(query_obj)

    for result in results:
        print(f"Found match: {result['url']}")
        print(f"Title: {result['title']}")
        print(f"Content: {result['content']}")
        print()

    searcher.close()

if __name__ == '__main__':
    indexer_process()
