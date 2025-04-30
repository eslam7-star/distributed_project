from whoosh.index import open_dir
from whoosh.qparser import QueryParser, MultifieldParser, OrGroup
from whoosh import scoring
import argparse

def search(query_text, domain_filter=None):
    try:
        ix = open_dir("index")
        searcher = ix.searcher(weighting=scoring.TF_IDF())
        
        parser = MultifieldParser(["title", "content"], ix.schema, group=OrGroup)
        parser.add_plugin(whoosh.qparser.FuzzyTermPlugin())
        query = parser.parse(query_text)
        
        if domain_filter:
            query = query & whoosh.query.Term("domain", domain_filter)
        
        results = searcher.search(query, limit=20)
        
        print(f"\nResults ({len(results)}):")
        for i, hit in enumerate(results, 1):
            print(f"\n{i}. [{hit.score:.2f}] {hit['title']}")
            print(f"   {hit['domain']} | {hit['url']}")
            print(f"   {hit['content'][:200]}...")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", help="Search query")
    parser.add_argument("--domain", help="Filter by domain")
    args = parser.parse_args()
    
    if args.search:
        search(args.search, args.domain)
    else:
        while True:
            query = input("\nSearch query (or 'exit'): ").strip()
            if query.lower() in ('exit', 'quit'):
                break
            if 'domain:' in query:
                parts = query.split('domain:')
                domain = parts[1].split()[0]
                query_text = ' '.join(parts[1].split()[1:])
                search(query_text, domain)
            else:
                search(query)