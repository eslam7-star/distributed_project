import argparse
from whoosh.index import open_dir
from whoosh.qparser import QueryParser

def search(query_text):
    try:
        ix = open_dir("index")
        with ix.searcher() as searcher:
            query = QueryParser("content", ix.schema).parse(query_text)
            results = searcher.search(query, limit=10)
            
            print(f"\nFound {len(results)} results:")
            for hit in results:
                print(f"\nTitle: {hit['title']}")
                print(f"URL: {hit['url']}")
                print(f"Score: {hit.score:.2f}")
                print(f"Content: {hit['content'][:100]}...")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", help="Search query")
    args = parser.parse_args()
    
    if args.search:
        search(args.search)
    else:
        while True:
            query = input("\nSearch query (or 'exit'): ").strip()
            if query.lower() == 'exit':
                break
            search(query)