import argparse
from whoosh.index import open_dir
from whoosh.qparser import QueryParser

INDEX_DIR = "indexer_index"

def search_keywords(query_string):
    """
    Search for the given query in the index and display the results.
    """
    index = open_dir(INDEX_DIR)
    searcher = index.searcher()

    query_parser = QueryParser("content", schema=index.schema)  
    query_obj = query_parser.parse(query_string)

    results = searcher.search(query_obj)

    if results:
        print(f"Found {len(results)} result(s) for '{query_string}':\n")
        for result in results:
            print(f"URL: {result['url']}")
            print(f"Title: {result['title']}")
            print(f"Content: {result['content']}")
            print("-" * 50)
    else:
        print(f"No results found for '{query_string}'.")

    searcher.close()

def main():
    parser = argparse.ArgumentParser(description="Search for a keyword in the Whoosh index.")
    parser.add_argument('--search', type=str, required=True, help="The keyword to search for in the index")
    args = parser.parse_args()

    search_keywords(args.search)

if __name__ == '__main__':
    main()
