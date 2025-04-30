#!/usr/bin/env python3
import os
from whoosh.index import open_dir, create_in, exists_in
from whoosh.qparser import QueryParser
from whoosh.highlight import HtmlFormatter, ContextFragmenter
from whoosh.fields import Schema, TEXT, ID

class SearchEngine:
    def __init__(self, index_dir="index"):
        self.index_dir = index_dir
        self.schema = Schema(
            url=ID(stored=True),
            title=TEXT(stored=True),
            content=TEXT(stored=True)
        )
        self._ensure_index_exists()

    def _ensure_index_exists(self):
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)
        if not exists_in(self.index_dir):
            create_in(self.index_dir, self.schema)

    def search(self, query_text, limit=10):
        if not os.listdir(self.index_dir):
            return {"error": "Index is empty", "results": []}

        try:
            ix = open_dir(self.index_dir)
            results = []
            
            with ix.searcher() as searcher:
                query = QueryParser("content", ix.schema).parse(query_text)
                hits = searcher.search(query, limit=limit)
                
                hits.fragmenter = ContextFragmenter(maxchars=200)
                hits.formatter = HtmlFormatter(tagname="mark")
                
                results = [{
                    "url": hit["url"],
                    "title": hit["title"],
                    "snippet": hit.highlights("content"),
                    "relevance": f"{hit.score:.2f}"
                } for hit in hits]
            
            return {"success": True, "results": results}

        except Exception as e:
            return {"error": str(e), "results": []}

def display_results(search_response):
    if "error" in search_response:
        print(f"Error: {search_response['error']}")
        return

    results = search_response.get("results", [])
    print(f"\nFound {len(results)} results:")
    
    for idx, result in enumerate(results, 1):
        print(f"\n{idx}. [{result['relevance']}] {result['title']}")
        print(f"   {result['url']}")
        print(f"   {result['snippet']}")

def main():
    engine = SearchEngine()
    
    print("\n Web Crawler Search Engine")
    print("Type 'exit' to quit\n")
    
    while True:
        query = input("Search query: ").strip()
        
        if query.lower() in ('exit', 'quit'):
            break
            
        if not query:
            print("Please enter a search term")
            continue
            
        response = engine.search(query)
        display_results(response)

if __name__ == "__main__":
    main()